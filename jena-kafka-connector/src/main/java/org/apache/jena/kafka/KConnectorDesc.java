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

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;

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
 * <p>
 * Transaction management and handling is done by {@link org.apache.jena.kafka.common.FusekiProjector}, a number of
 * methods on the connector provide advanced configuration tuning for this.  The values returned by these methods are
 * driven from custom Fuseki Kafka configuration properties supplied as part of the Kafka configuration properties
 * available via {@link #getKafkaConsumerProps()}.  See individual methods for what configuration properties to use, and
 * the defaults used if not explicitly configured.
 * </p>
 */
@Getter
@ToString
@EqualsAndHashCode
public class KConnectorDesc {

    private static final Predicate<Integer> IS_POSITIVE_INTEGER = x -> x != null && x > 0;
    private static final Predicate<Long> IS_POSITIVE_LONG = x -> x != null && x > 0;
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

    /**
     * Creates a new connector descriptor
     *
     * @param topics             Topic(s) to connect to
     * @param bootstrapServers   Bootstrap servers to use
     * @param datasetName        Dataset name
     * @param stateFile          State file
     * @param syncTopic          Whether to synchronise data with the input topics
     * @param replayTopic        Whether to replay data from the input topics
     * @param dlqTopic           Optional dead letter queue (DLQ) topic to use for malformed events
     * @param kafkaConsumerProps Additional Kafka configuration properties
     */
    public KConnectorDesc(List<String> topics, String bootstrapServers, String datasetName, String stateFile,
                          boolean syncTopic, boolean replayTopic, String dlqTopic, Properties kafkaConsumerProps) {
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
        this.kafkaConsumerProps = Objects.requireNonNullElse(kafkaConsumerProps, new Properties());

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
     * Obtains configuration from Kafka Properties
     *
     * @param key          Configuration Key
     * @param parser       Parser to convert the configured value to the desired value type
     * @param validator    Validator to check the configured value conforms to expectations, invalid values are ignored
     * @param defaultValue Default value to use if value is not set, or cannot be parsed
     * @param <T>          Value type
     * @return             Configuration value, possibly the supplied {@code defaultValue}
     */
    private <T> T fromKafkaProperties(String key, Function<String, T> parser, Predicate<T> validator, T defaultValue) {
        String rawValue = this.kafkaConsumerProps.getProperty(key);
        if (StringUtils.isBlank(rawValue)) {
            return defaultValue;
        } else {
            try {
                T value = parser.apply(rawValue);
                if (validator.test(value)) {
                    return value;
                } else {
                    FusekiKafka.LOG.warn("Kafka Configuration property {} had an invalid value: {}", key, value);
                    return defaultValue;
                }
            } catch (Throwable e) {
                FusekiKafka.LOG.warn("Kafka Configuration property {} had a non-parseable value: {}", key, rawValue);
                return defaultValue;
            }
        }
    }

    /**
     * Obtains configuration from Kafka Properties
     *
     * @param keys         Configuration Keys in order of preference
     * @param parser       Parser to convert the configured value to the desired value type
     * @param validator    Validator to check the configured value conforms to expectations, invalid values are ignored
     * @param defaultValue Default value to use if value is not set, or cannot be parsed
     * @param <T>          Value type
     * @return             First valid configuration value found, possibly the supplied {@code defaultValue}
     */
    private <T> T fromKafkaProperties(String[] keys, Function<String, T> parser, Predicate<T> validator,
                                      T defaultValue) {
        for (String key : keys) {
            T value = fromKafkaProperties(key, parser, validator, null);
            if (value != null) {
                return value;
            }
        }
        return defaultValue;
    }

    /**
     * Gets the maximum poll records as configured for the connector
     * <p>
     * May be configured via the standard Kafka configuration property {@value ConsumerConfig#MAX_POLL_RECORDS_CONFIG},
     * default is {@value SysJenaKafka#KAFKA_FETCH_POLL_SIZE} records.
     * </p>
     *
     * @return Maximum poll records
     */
    public int getMaxPollRecords() {
        return fromKafkaProperties(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer::parseInt, IS_POSITIVE_INTEGER,
                                   SysJenaKafka.KAFKA_FETCH_POLL_SIZE);
    }

    /**
     * Gets the batch size to use, in number of events, this is taken from the following in precedence order:
     * <ol>
     *     <li>The custom Fuseki Kafka configuration property {@value SysJenaKafka#FUSEKI_KAFKA_BATCH_SIZE}</li>
     *     <li>The Kafka configuration property {@value ConsumerConfig#MAX_POLL_RECORDS_CONFIG}</li>
     *     <li>The Fuseki Kafka default {@value SysJenaKafka#KAFKA_FETCH_POLL_SIZE}</li>
     * </ol>
     * <p>
     * Default is {@value SysJenaKafka#DEFAULT_BATCH_SIZE} events.
     * </p>
     *
     * @return Batch size, in number of events, to use
     */
    public int getBatchSize() {
        return fromKafkaProperties(
                new String[] { SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE, ConsumerConfig.MAX_POLL_RECORDS_CONFIG },
                Integer::parseInt, IS_POSITIVE_INTEGER, SysJenaKafka.DEFAULT_BATCH_SIZE);
    }

    /**
     * Gets the batch size to use, in number of bytes, this is taken from the following in precedence order:
     * <ol>
     *     <li>The custom Fuseki Kafka configuration property {@value SysJenaKafka#FUSEKI_KAFKA_BATCH_SIZE_BYTES}</li>
     *     <li>The Kafka configuration property {@value ConsumerConfig#FETCH_MAX_BYTES_CONFIG}</li>
     *     <li>The Kafka configuration property {@value ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG}</li>
     *     <li>The Fuseki Kafka default {@value SysJenaKafka#DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD}</li>
     * </ol>
     * <p>
     * Note that this configuration is only used when the {@link #getHighLagThreshold()} is exceeded.  Default is
     * {@value SysJenaKafka#DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD} bytes.
     * </p>
     *
     * @return Batch size, in number of bytes, to use
     */
    public long getBatchSizeBytes() {
        return fromKafkaProperties(new String[] {
                SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_BYTES,
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG
        }, Long::parseLong, IS_POSITIVE_LONG, SysJenaKafka.DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD);
    }

    /**
     * Gets the number of batch sizes to track in order to average batch sizes over this and determine whether we're
     * connected to low volume topics.
     * <p>
     * Once at least this many batches have been processed if the average batch size falls below the configured
     * {@link #getLowVolumeBatchSizeThreshold()} then the {@link org.apache.jena.kafka.common.FusekiProjector} switches
     * over to low volume batching mode.  In this mode it does not automatically commit as soon as it reaches lag of 0
     * instead waiting until either the batch size, or the {@link #getMaxTransactionDuration()} is exceeded.
     * </p>
     * <p>
     * May be configured by setting the custom Fuseki Kafka configuration property
     * {@value SysJenaKafka#FUSEKI_KAFKA_BATCH_SIZE_TRACKING_WINDOW}. Default is
     * {@value SysJenaKafka#DEFAULT_BATCH_SIZE_TRACKING_WINDOW} batches.
     * </p>
     *
     * @return Number of batch sizes to track
     */
    public int getBatchSizeTrackingWindow() {
        return fromKafkaProperties(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_TRACKING_WINDOW, Integer::parseInt,
                                   IS_POSITIVE_INTEGER, SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW);
    }

    /**
     * Gets the low volume batch size threshold
     * <p>
     * If the average batch size over the last {@link #getBatchSizeTrackingWindow()} batches is less than, or equal to,
     * this threshold then the {@link org.apache.jena.kafka.common.FusekiProjector} switches over to low volume batching
     * mode.  See {@link #getBatchSizeTrackingWindow()} for more details on this.
     * </p>
     * <p>
     * May be configured by setting the custom Fuseki Kafka configuration property
     * {@value SysJenaKafka#FUSEKI_KAFKA_LOW_VOLUME_THRESHOLD} in the consumer properties provided to this connector.
     * Defaults to {@value SysJenaKafka#DEFAULT_AVERAGE_BATCH_SIZE_LOW_VOLUME_THRESHOLD}.
     * </p>
     *
     * @return Low volume batch size threshold
     */
    public int getLowVolumeBatchSizeThreshold() {
        return fromKafkaProperties(SysJenaKafka.FUSEKI_KAFKA_LOW_VOLUME_THRESHOLD, Integer::parseInt, x -> x >= 0,
                                   SysJenaKafka.DEFAULT_AVERAGE_BATCH_SIZE_LOW_VOLUME_THRESHOLD);
    }

    /**
     * Gets the high lag threshold, in number of events.
     * <p>
     * If the lag exceeds this then the {@link org.apache.jena.kafka.common.FusekiProjector} switches to high lag
     * batching mode.  In this mode it no longer honours the {@link #getBatchSize()} instead only committing once
     * {@link #getBatchSizeBytes()} has been reached.
     * </p>
     * <p>
     * May be configured by setting the custom Fuseki Kafka configuration property
     * {@value SysJenaKafka#FUSEKI_KAFKA_HIGH_LAG_THRESHOLD} in the consumer properties provided to this connector.
     * Default is {@value SysJenaKafka#DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD} events.
     * </p>
     *
     * @return High lag threshold
     */
    public long getHighLagThreshold() {
        return fromKafkaProperties(SysJenaKafka.FUSEKI_KAFKA_HIGH_LAG_THRESHOLD, Long::parseLong, IS_POSITIVE_LONG,
                                   SysJenaKafka.DEFAULT_HIGH_LAG_THRESHOLD);
    }

    /**
     * Gets the maximum transaction duration
     * <p>
     * This is expressed as an ISO 8601 Duration in the Kafka configuration e.g. {@code PT1M} is 1 minute.
     * </p>
     * <p>
     * May be configured by setting the custom Fuseki Kafka configuration property
     * {@value SysJenaKafka#FUSEKI_KAFKA_MAX_TRANSACTION_DURATION} in the consumer properties provided to this
     * connector.  Default is {@link SysJenaKafka#DEFAULT_MAX_TRANSACTION_DURATION}, which is currently 5 minutes.
     * </p>
     *
     * @return Max transaction duration
     */
    public Duration getMaxTransactionDuration() {
        return fromKafkaProperties(SysJenaKafka.FUSEKI_KAFKA_MAX_TRANSACTION_DURATION, Duration::parse,
                                   SysJenaKafka::isValidDuration, SysJenaKafka.DEFAULT_MAX_TRANSACTION_DURATION);
    }

}
