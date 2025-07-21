package org.apache.jena.kafka.common;

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
        if (this.stateFile != null) {
            // Read in existing state file
            this.readStateFile();
        }
    }

    private void readStateFile() {
        try {
            if (this.stateFile.exists() && this.stateFile.length() > 0) {
                Map<String, Object> stateMap = this.mapper.readValue(this.stateFile, GENERIC_MAP_TYPE);

                // Load basic fields used for configuration sanity checking
                String datasetName = stateMap.getOrDefault(FIELD_DATASET, "").toString();
                if (StringUtils.isBlank(datasetName)) {
                    throw new JenaKafkaException(
                            "No dataset name found in state file " + this.stateFile.getAbsolutePath());
                }
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
                            throw new JenaKafkaException(
                                    "Must supply a consumer group when reading in a legacy state file");
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
            }
        } catch (IOException e) {
            throw new JenaKafkaException("Error reading state file", e);
        }
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
     *
     * @param file State file
     */
    private void writeStateFile(File file) {
        Map<String, Object> stateMap = new LinkedHashMap<>();
        stateMap.put(FIELD_DATASET, this.datasetName);
        stateMap.put(FIELD_OFFSETS, this.offsets);

        try {
            this.mapper.writeValue(file, stateMap);
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
