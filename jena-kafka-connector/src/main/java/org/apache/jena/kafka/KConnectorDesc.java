/*
 *  Copyright (c) Telicent Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.jena.kafka;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * Details of a connector to Kafka.
 * <p>
 * The actual machinery of reading from Kafka comes from <a
 * href="https://github.com/telicent-oss/smart-caches-core/blob/main/docs/event-sources/kafka.md">Telicent Smart Cache
 * Core Libraries</a>.
 * </p>
 */
@Getter
@ToString
@EqualsAndHashCode
public class KConnectorDesc {

    // Source
    private final List<String> topics;
    private final String dlqTopic;
    private final String bootstrapServers;

    /**
     * -- GETTER -- The destination of events on the Kafka topic
     *
     * @return Fuseki dataset service name e.g. {@code /ds}
     */
    // Destination - Dataset name within the Fuseki server
    private final String datasetName;

    private final boolean syncTopic;
    private final boolean replayTopic;

    // State tracking.
    private final String stateFile;

    // Kafka consumer setup.
    private final Properties kafkaConsumerProps;

    public KConnectorDesc(List<String> topics, String bootstrapServers, String datasetName,
                          String stateFile, boolean syncTopic, boolean replayTopic, String dlqTopic,
                          Properties kafkaConsumerProps) {
        this.topics = Objects.requireNonNull(topics, "topics cannot be null");
        if (this.topics.isEmpty()) {
            throw new IllegalArgumentException("topics cannot be empty");
        }
        this.dlqTopic = dlqTopic;
        if (StringUtils.isNotBlank(this.dlqTopic) && this.topics.contains(this.dlqTopic)) {
            throw new JenaKafkaException(
                    "Can't configure the DLQ topic as " + this.dlqTopic + " as this is also an input topic!");
        }
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers cannot be null");
        this.datasetName = datasetName;
        this.syncTopic = syncTopic;
        this.replayTopic = replayTopic;
        this.stateFile = stateFile;
        this.kafkaConsumerProps = kafkaConsumerProps;

        boolean hasLocalFusekiService = StringUtils.isNotBlank(datasetName);
        if (!hasLocalFusekiService) {
            throw new JenaKafkaException("ConnectorFK built with no local dispatch path");
        }
    }

    /**
     * Gets the Consumer Group ID
     *
     * @return Consumer Group ID
     */
    public String getConsumerGroupId() {
        return this.kafkaConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    /**
     * Gets the maximum poll records as configured for the connector
     *
     * @return Maximum poll records
     */
    public int getMaxPollRecords() {
        String rawValue = this.kafkaConsumerProps.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        if (StringUtils.isBlank(rawValue)) {
            return SysJenaKafka.KAFKA_FETCH_POLL_SIZE;
        } else {
            return Integer.parseInt(rawValue);
        }
    }

}
