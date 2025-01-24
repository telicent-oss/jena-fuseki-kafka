package org.apache.jena.kafka.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.telicent.smart.cache.sources.offsets.MemoryOffsetStore;
import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.kafka.JenaKafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class FusekiOffsetStore extends MemoryOffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(FusekiOffsetStore.class);

    // Static constants for field names in the states file
    private static final String FIELD_DATASET = "dataset";
    private static final String FIELD_ENDPOINT = "endpoint";
    private static final String FIELD_OFFSETS = "offsets";
    private static final String LEGACY_FIELD_TOPIC = "topic";
    private static final String LEGACY_FIELD_OFFSET = "offset";

    private static final TypeReference<Map<String, Object>> GENERIC_MAP_TYPE = new TypeReference<>() {
    };

    @Getter
    private final String dispatchPath;
    @Getter
    private final String remoteEndpoint;
    private final File stateFile;
    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_LONG_FOR_INTS);

    @Builder
    FusekiOffsetStore(String dispatchPath, String remoteEndpoint, File stateFile) {
        this.dispatchPath = dispatchPath;
        this.remoteEndpoint = remoteEndpoint;
        this.stateFile = stateFile;
        if (this.stateFile != null) {
            // Read in existing state file
            this.readStateFile();
        }
    }

    private void readStateFile() {
        try {
            Map<String, Object> stateMap = this.mapper.readValue(this.stateFile, GENERIC_MAP_TYPE);

            // Load basic fields used for configuration sanity checking
            String datasetName = stateMap.getOrDefault(FIELD_DATASET, "").toString();
            if (StringUtils.isBlank(datasetName)) {
                throw new JenaKafkaException("No dataset name found in state file " + this.stateFile.getAbsolutePath());
            }
            String endpoint = stateMap.getOrDefault(FIELD_ENDPOINT, "").toString();

            // Load legacy offset
            String topic = stateMap.getOrDefault(LEGACY_FIELD_TOPIC, "").toString();
            warnOnLegacyField(topic, StringUtils::isNotBlank, LEGACY_FIELD_TOPIC);
            Long offset = (Long) stateMap.getOrDefault(LEGACY_FIELD_OFFSET, -1L);
            warnOnLegacyField(offset, x -> x != -1, LEGACY_FIELD_OFFSET);
            if (StringUtils.isNotBlank(topic)) {
                // Legacy code only allowed for reading a single partition topic
                // KafkaEventSource expects offsets to be written with both topic and partition
                // Therefore if legacy topic and offset fields are present these MUST refer to partition 0 of the topic
                // so set that offset now so it is used and persisted
                LOGGER.info("Interpreted legacy fields in state file to set Offset {} for Topic Partition {}-0", offset,
                            topic);
                this.offsets.put(topic + "-0", offset);
            }

            // Load current offsets
            try {
                // NB - Since we've used Jackson to parse the state file as a generic map the only way this cast doesn't
                //      work is if the state field is malformed i.e. the offsets field is not an object
                @SuppressWarnings("unchecked")
                Map<String, Object> storedOffsets =
                        (Map<String, Object>) stateMap.getOrDefault(FIELD_OFFSETS, new HashMap<>());
                this.offsets.putAll(storedOffsets);
            } catch (ClassCastException ex) {
                throw new JenaKafkaException(
                        "State file " + this.stateFile.getAbsolutePath() + " contains an offsets field which is not a JSON object");
            }

            // Configuration sanity checks
            if (!this.dispatchPath.equals(datasetName)) {
                throw new JenaKafkaException(
                        "Dataset name does not match: this=" + this.dispatchPath + " / read=" + datasetName);
            }
            if (!Objects.equals(this.remoteEndpoint, endpoint)) {
                throw new JenaKafkaException(
                        "Endpoint name does not match: this=" + this.remoteEndpoint + " / read=" + endpoint);
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
            Map<String, Object> stateMap = new LinkedHashMap<>();
            stateMap.put(FIELD_DATASET, this.dispatchPath);
            if (StringUtils.isNotBlank(this.remoteEndpoint)) {
                stateMap.put(FIELD_ENDPOINT, this.remoteEndpoint);
            }
            stateMap.put(FIELD_OFFSETS, this.offsets);

            try {
                this.mapper.writeValue(this.stateFile, stateMap);
            } catch (IOException e) {
                throw new JenaKafkaException("Error writing state file", e);
            }
        }
    }

    @Override
    protected void closeInternal() {
        this.flushInternal();
    }
}
