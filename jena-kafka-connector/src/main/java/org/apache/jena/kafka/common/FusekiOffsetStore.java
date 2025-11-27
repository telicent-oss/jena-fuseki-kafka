package org.apache.jena.kafka.common;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import io.telicent.smart.cache.sources.offsets.MemoryOffsetStore;
import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.kafka.JenaKafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.function.Predicate;

public class FusekiOffsetStore extends MemoryOffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(FusekiOffsetStore.class);

    // Static constants for field names in the states file
    public static final String FIELD_DATASET = "dataset";
    public static final String LEGACY_FIELD_ENDPOINT = "endpoint";
    public static final String FIELD_OFFSETS = "offsets";
    public static final String LEGACY_FIELD_TOPIC = "topic";
    public static final String LEGACY_FIELD_OFFSET = "offset";

    private static final TypeReference<Map<String, Object>> GENERIC_MAP_TYPE = new TypeReference<>() {
    };
    /**
     * Additional file extension added to state file paths, used by {@link #readStateFile()} if it encounters a state
     * file that violates our stream constraints, i.e. is too large.  This prevents us getting stuck forever trying to
     * process an excessively large state file while still allowing administrators to come along later and inspect the
     * discarded file to try and understand why it was corrupted.
     * <p>
     * If a file with this extension already exists then it will have a further {@code -N} suffix appended e.g.
     * {@code .discarded-3} so that every unique corrupt state file is preserved.
     * </p>
     */
    public static final String DISCARDED_EXTENSION = ".discarded";
    /**
     * Additional file extension added to state file paths, used by {@link #writeStateFile(File)} to write the latest
     * state to a temporary file prior to replacing the actual state file.  This is used as a defensive measure to
     * reduce the risk of writing a corrupted state file.
     */
    public static final String TEMP_EXTENSION = ".temp";
    /**
     * Additional file extension added to state file paths, used by {@link #writeStateFile(File)} to take a backup of
     * the previously written state file prior to overwriting it.  This will be deleted once
     * {@link #writeStateFile(File)} completes successfully.
     */
    public static final String BACKUP_EXTENSION = ".backup";

    @Getter
    private final String datasetName;
    private final String consumerGroup;
    private final File stateFile;
    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_LONG_FOR_INTS);

    /**
     * Creates a new offset store
     *
     * @param datasetName   Name of the dataset these offsets are associated with
     * @param consumerGroup Kafka Consumer Group ID, primarily only required for reading in legacy format state files
     *                      from Fuseki Kafka 1.x
     * @param stateFile     Persistent state file to read/write, if {@code null} then the offsets will be
     *                      non-persistent, this is generally only useful for testing purposes
     */
    @Builder
    FusekiOffsetStore(String datasetName, String consumerGroup, File stateFile) {
        if (StringUtils.isBlank(datasetName)) {
            throw new IllegalArgumentException("Dataset name cannot be null or empty");
        }
        this.datasetName = datasetName;
        this.consumerGroup = consumerGroup;
        this.stateFile = stateFile;

        // Ensure we set readConstraints on the size of the state file we will allow to be parsed
        // This helps to protect us against any disk corruption that might occur and render the state file to appear
        // the wrong size, in that scenario we'll refuse to load it
        //@formatter:off
        StreamReadConstraints readConstraints =
                StreamReadConstraints.builder()
                                     .maxDocumentLength(5 * 1024 * 1024)
                                     .maxNestingDepth(3)
                                     .build();
        //@formatter:on
        this.mapper.getFactory().setStreamReadConstraints(readConstraints);

        if (this.stateFile != null) {
            // Read in existing state file
            this.readStateFile();
        }
    }

    /**
     * Reads the state file in, this includes handling automatic recovery of potential file corruption, and automatic
     * migration from legacy state file format into the current state file format.
     * <p>
     * See {@link #writeStateFile(File)} for details of the temporary and backup state files that get written during a
     * write operation.  If during read we find the main file is corrupted then we'll attempt to use those files to
     * recover the state file.
     * </p>
     */
    private void readStateFile() {
        try {
            if (this.stateFile.exists() && this.stateFile.length() > 0) {
                LOGGER.info("Attempting to load state file {} (size on disk {})...", this.stateFile.getAbsolutePath(),
                            FusekiProjector.byteCountToDisplaySize(this.stateFile.length()));

                // Make an attempt to read the JSON from the state file
                Map<String, Object> stateMap;
                try {
                    stateMap = this.mapper.readValue(this.stateFile, GENERIC_MAP_TYPE);
                } catch (StreamConstraintsException e) {
                    // Can be encountered if the state file is corrupted so that it reports a really large size, or has
                    // been manually edited and/or otherwise populated with junk data that doesn't trigger some other
                    // Jackson error condition
                    //
                    // If it exceeds the stream constraints we define assume the file is corrupted and move it to a new
                    // file with a .discarded suffix so operators can inspect the corrupted file later
                    LOGGER.warn(
                            "State file {} violated stream constraints (size on disk {}), this likely indicates disk corruption as state files should be small",
                            this.stateFile.getAbsolutePath(),
                            FusekiProjector.byteCountToDisplaySize(this.stateFile.length()));
                    File discardFile = getNextDiscardFile();
                    LOGGER.warn("Attempting to move state file to {} and ignoring its contents",
                                discardFile.getAbsolutePath());
                    Files.move(this.stateFile.toPath(), discardFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    LOGGER.warn("Discarded state file available at {} for manual inspection",
                                discardFile.getAbsolutePath());

                    tryRecoverStateFile();

                    return;
                }

                // Load basic fields used for configuration sanity checking
                String datasetName = stateMap.getOrDefault(FIELD_DATASET, "").toString();
                if (StringUtils.isBlank(datasetName)) {
                    throw new JenaKafkaException(
                            "No dataset name found in state file " + this.stateFile.getAbsolutePath());
                }

                // Check whether the state file contains legacy fields and if so migrate those to the new format
                migrateLegacyStateFile(stateMap);

                // Load current offsets
                try {
                    // NB - Since we've used Jackson to parse the state file as a generic map the only way this cast
                    //      doesn't work is if the state field is malformed i.e. the offsets field is not an object
                    @SuppressWarnings("unchecked") Map<String, Object> storedOffsets =
                            (Map<String, Object>) stateMap.getOrDefault(FIELD_OFFSETS, new HashMap<>());
                    this.offsets.putAll(storedOffsets);
                } catch (ClassCastException ex) {
                    throw new JenaKafkaException(
                            "State file " + this.stateFile.getAbsolutePath() + " contains an offsets field whose value is not a JSON object");
                }

                // Configuration sanity checks
                if (!this.datasetName.equals(datasetName)) {
                    throw new JenaKafkaException(
                            "Dataset name does not match: this=" + this.datasetName + " / read=" + datasetName);
                }

                LOGGER.info("State file {} loaded successfully with {} offsets present",
                            this.stateFile.getAbsolutePath(), this.offsets.size());
            } else {
                LOGGER.info("No pre-existing state file {} to load", this.stateFile.getAbsolutePath());
            }
        } catch (IOException e) {
            try {
                tryRecoverStateFile();
            } catch (IOException e2) {
                // If recovery fails log the error and throw the first IO error that originally got us here
                LOGGER.warn("IO error trying to recover state files: {}", e2.getMessage());
            }

            throw new JenaKafkaException("Error reading state file", e);
        }
    }

    /**
     * Gets the next available discard file
     *
     * @return Next discard file that doesn't currently exist
     */
    private File getNextDiscardFile() {
        File nextDiscard = getFileWithAdditionalExtension(this.stateFile, DISCARDED_EXTENSION);
        int counter = 0;
        while (nextDiscard.exists()) {
            nextDiscard = getFileWithAdditionalExtension(this.stateFile, DISCARDED_EXTENSION + "-" + ++counter);
        }
        return nextDiscard;
    }

    /**
     * Checks whether the loaded state file contains any legacy fields, if so either warns and discards no longer used
     * fields, or converts their values to the new format
     *
     * @param stateMap Loaded state file data
     */
    private void migrateLegacyStateFile(Map<String, Object> stateMap) {
        String endpoint = stateMap.getOrDefault(LEGACY_FIELD_ENDPOINT, "").toString();
        warnOnLegacyField(endpoint, StringUtils::isNotBlank, LEGACY_FIELD_ENDPOINT);

        // Load legacy offset
        String topic = stateMap.getOrDefault(LEGACY_FIELD_TOPIC, "").toString();
        warnOnLegacyField(topic, StringUtils::isNotBlank, LEGACY_FIELD_TOPIC);
        try {
            Long offset = (Long) stateMap.getOrDefault(LEGACY_FIELD_OFFSET, -1L);
            warnOnLegacyField(offset, x -> x != -1, LEGACY_FIELD_OFFSET);
            if (StringUtils.isNotBlank(topic)) {
                // Legacy code only allowed for reading a single partition topic
                // KafkaEventSource expects offsets to be written with both topic and partition
                // Therefore if legacy topic and offset fields are present these MUST refer to partition 0 of
                // the topic so set that offset now so it is used and persisted
                LOGGER.info("Interpreted legacy fields in state file to set Offset {} for Topic Partition {}-0",
                            offset + 1, topic);
                if (StringUtils.isBlank(this.consumerGroup)) {
                    throw new JenaKafkaException("Must supply a consumer group when reading in a legacy state file");
                }
                // NB - The legacy state files stored an offset that was off by 1 relative to the actual Kafka
                //      offset that should have been stored.  In testing upgrading of legacy state files this
                //      meant the code incorrectly re-processed the last event on the topic.  While this was
                //      generally harmless it's better to correct the off by 1 error when upgrading to avoid
                //      this!
                this.offsets.put(KafkaEventSource.externalOffsetStoreKey(topic, 0, this.consumerGroup), offset + 1);
            }
        } catch (ClassCastException e) {
            throw new JenaKafkaException(
                    "State file " + this.stateFile.getAbsolutePath() + " contains a legacy offset field whose value is not a JSON number");
        }
    }

    private void tryRecoverStateFile() throws IOException {
        // Due to how writeStateFile() now functions with defensively writing both backup and temporary
        // files we may be able to recover one of those if they still exist
        File tempStateFile = getFileWithAdditionalExtension(this.stateFile, TEMP_EXTENSION);
        File backupStateFile = getFileWithAdditionalExtension(this.stateFile, BACKUP_EXTENSION);
        if (tempStateFile.exists()) {
            tryRecoverStateFile(tempStateFile);
        } else if (backupStateFile.exists()) {
            tryRecoverStateFile(backupStateFile);
        } else {
            LOGGER.warn(
                    "No temporary/backup state file found to recover from, as a result data loading may now re-process previously loaded data depending on your other Kafka connector options");
        }
    }

    private void tryRecoverStateFile(File recoveryFile) throws IOException {
        LOGGER.info("Attempting to recover state file from {}", recoveryFile.getAbsolutePath());
        Files.move(recoveryFile.toPath(), stateFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        LOGGER.info("Recovered state file to {}, attempting to read recovered state file...",
                    stateFile.getAbsolutePath());
        // Try reading state file again, this won't create an infinite loop as we've moved the file so can't
        // trigger this function with the same file again
        readStateFile();
        LOGGER.info("Successfully recovered state file from {}", recoveryFile.getAbsolutePath());
    }

    private File getFileWithAdditionalExtension(File file, String extension) {
        return new File(file.getAbsolutePath() + extension);
    }

    private <T> void warnOnLegacyField(T value, Predicate<T> valueExists, String field) {
        if (valueExists.test(value)) {
            LOGGER.warn("State file {} in legacy format, {} field will not be persisted on next write.",
                        this.stateFile.getAbsolutePath(), field);
        }
    }

    @Override
    protected void flushInternal() {
        if (this.stateFile != null) {
            writeStateFile(this.stateFile);
        }
    }

    /**
     * Writes the offsets, and associated sanity checking metadata to the given state file
     * <p>
     * In order to reduce risk from losing the state file due to partial write/disk corruption/etc. we take a defensive
     * approach to writing out the state files.  We firstly take a backup of the existing state file if it exists, then
     * we write the current state to a temporary file, and if that succeeds we move the temporary file to the actual
     * state file location provided.  Finally, if we created a backup file at the start of the write process we now
     * remove it.
     * </p>
     * <p>
     * Note that {@link #readStateFile()} will use the temporary and backup files to recover the state file should it
     * find the provided state file to be corrupted, or exceed constraints on the file size.
     * </p>
     *
     * @param file State file
     */
    private void writeStateFile(File file) {
        Map<String, Object> stateMap = new LinkedHashMap<>();
        stateMap.put(FIELD_DATASET, this.datasetName);
        stateMap.put(FIELD_OFFSETS, this.offsets);

        // Check that if this is an existing state file we are overwriting that the destination file is writable
        if (file.exists() && !file.canWrite()) {
            throw new JenaKafkaException("State file " + file.getAbsolutePath() + " is not writable");
        }

        // NB We have seen rare circumstances where the state file got corrupted beyond our control, therefore the
        //    following code is intended to be defensive and allow us to recover from these scenarios by keeping track of
        //    multiple versions of the state file during a write operation
        try {
            // First copy the existing state file as a backup
            File backupStateFile = null;
            if (file.exists()) {
                backupStateFile = getFileWithAdditionalExtension(file, BACKUP_EXTENSION);
                LOGGER.debug("Creating state file backup {}", backupStateFile.getAbsolutePath());
                Files.copy(file.toPath(), backupStateFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                LOGGER.debug("Created state file backup {}", backupStateFile.getAbsolutePath());
            }

            // Secondly write the current state to a temporary state file
            File tempStateFile = getFileWithAdditionalExtension(file, TEMP_EXTENSION);
            LOGGER.debug("Writing state file to temporary file {}", tempStateFile.getAbsolutePath());
            this.mapper.writeValue(tempStateFile, stateMap);
            LOGGER.debug("Wrote state file to temporary file {}", tempStateFile.getAbsolutePath());

            // Next move the temporary state file to the final location
            LOGGER.debug("Moving temporary state file to {}", file.toPath());
            Files.move(tempStateFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            LOGGER.debug("Moved state file to {}", file.toPath());

            // Finally remove the backup state file
            if (backupStateFile != null) {
                LOGGER.debug("Deleting backup state file {}", backupStateFile.getAbsolutePath());
                Files.delete(backupStateFile.toPath());
                LOGGER.debug("Deleted backup state file {}", backupStateFile.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new JenaKafkaException("Error writing state file " + file.getAbsolutePath(), e);
        }
    }

    /**
     * Writes a copy of the current state of the offset store to the given file
     *
     * @param file File
     * @throws IllegalArgumentException Thrown if the given file is the same as the file this offset store was
     *                                  configured with
     * @throws JenaKafkaException       Thrown if unable to write out the state file
     */
    public void copyTo(File file) {
        if (Objects.equals(file, this.stateFile)) {
            throw new IllegalArgumentException("Can't copy to the state file this offset store owns!");
        }
        this.writeStateFile(file);
    }

    @Override
    protected void closeInternal() {
        this.flushInternal();
    }

    /**
     * Gets all the topic partition to offset entries
     *
     * @return Offsets
     */
    public Set<Map.Entry<String, Object>> offsets() {
        return Collections.unmodifiableSet(this.offsets.entrySet());
    }
}
