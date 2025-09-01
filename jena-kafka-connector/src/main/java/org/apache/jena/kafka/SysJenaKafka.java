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
import java.util.Properties;

import io.telicent.smart.cache.sources.EventSource;
import org.apache.jena.atlas.lib.Version;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class SysJenaKafka {

    public static final String PATH = "org.apache.jena.kafka";

    /**
     * The product name
     */
    public static final String NAME = "Apache Jena Kafka Connector";

    /**
     * Software version taken from the jar file.
     */
    public static final String VERSION = Version.versionForClass(FusekiKafka.class).orElse("<development>");
    /**
     * Default maximum transaction duration for the {@link org.apache.jena.kafka.common.FusekiProjector}, if a
     * transaction exceeds this time then it will be committed
     */
    public static final Duration DEFAULT_MAX_TRANSACTION_DURATION = Duration.ofMinutes(5);
    /**
     * Default number of recent batch sizes which will be tracked by the
     * {@link org.apache.jena.kafka.common.FusekiProjector} to detect scenarios where we're receiving low volumes of
     * data and thus may be generating too small batches
     */
    public static final int DEFAULT_BATCH_SIZE_TRACKING_WINDOW = 25;
    /**
     * The threshold, in number of events, at/below which topics are considered low volume and the
     * {@link org.apache.jena.kafka.common.FusekiProjector} will switch to low volume batching mode.
     * <p>
     * When in low volume batching mode we do not immediately commit on reaching zero lag, rather we wait until we are
     * definitely stalled (no new events from a {@link EventSource#poll(Duration)}) or until our maximum transaction
     * duration has been exceeded.
     * </p>
     */
    public static final int DEFAULT_AVERAGE_BATCH_SIZE_LOW_VOLUME_THRESHOLD = 10;
    /**
     * The threshold, in number of events, beyond which topics are considered high lag and the
     * {@link org.apache.jena.kafka.common.FusekiProjector} will switch to high lag batching mode.  See
     * {@link #DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD} for more details.
     */
    public static final long DEFAULT_HIGH_LAG_THRESHOLD = 10_000;
    /**
     * High lag batch byte threshold (50MB)
     * <p>
     * When the {@link org.apache.jena.kafka.common.FusekiProjector} detects that it is experiencing high lag in
     * catching up with a Kafka topic it stops respecting the batch size (number of events) threshold and instead
     * switches to maximising batch size by going by batch size in bytes instead.  As in practise most events are quite
     * small (few kilobytes to few hundred kilobytes) just batching by number of events may lead to smaller than desired
     * batches which introduces more frequent transaction overheads.  Therefore, changing batching mode to be data size
     * based leads to more predictable and stable batch sizes when catching up on high lag.
     * </p>
     */
    public static final long DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD = 50 * 1024 * 1024;
    /**
     * Custom Fuseki Kafka configuration property used for {@link KConnectorDesc#getBatchSize()}
     */
    public static final String FUSEKI_KAFKA_BATCH_SIZE = "fuseki.kafka.batch.size";
    /**
     * Custom Fuseki Kafka configuration property used for {@link KConnectorDesc#getBatchSizeBytes()}
     */
    public static final String FUSEKI_KAFKA_BATCH_SIZE_BYTES = "fuseki.kafka.batch.size.bytes";
    /**
     * Custom Fuseki Kafka configuration property used for {@link KConnectorDesc#getMaxTransactionDuration()}
     */
    public static final String FUSEKI_KAFKA_MAX_TRANSACTION_DURATION = "fuseki.kafka.max.transaction.duration";
    /**
     * Custom Fuseki Kafka configuration property used for {@link KConnectorDesc#getBatchSizeTrackingWindow()}
     */
    public static final String FUSEKI_KAFKA_BATCH_SIZE_TRACKING_WINDOW = "fuseki.kafka.batch.size.tracking";
    /**
     * Custom Fuseki Kafka configuration property used for {@link KConnectorDesc#getLowVolumeBatchSizeThreshold()}
     */
    public static final String FUSEKI_KAFKA_LOW_VOLUME_THRESHOLD = "fuseki.kafka.low.volume.threshold";
    /**
     * Custom Fuseki Kafka configuration property used for {@link KConnectorDesc#getHighLagThreshold()}
     */
    public static final String FUSEKI_KAFKA_HIGH_LAG_THRESHOLD = "fuseki.kafka.high.lag.threshold";

    /**
     * Size in bytes per {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)} in a system.
     * <p>
     * This sets {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG}
     * ({@value ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG}) which has a Kafka default of
     * {@value ConsumerConfig#DEFAULT_MAX_PARTITION_FETCH_BYTES}.
     * <p>
     * To replicate data, we need Fuseki or user application to see all the data in-order which forces the choice of one
     * partition. See also {@link ConsumerConfig#FETCH_MAX_BYTES_CONFIG} which has a Kafka default of
     * {@value ConsumerConfig#DEFAULT_FETCH_MAX_BYTES}.
     */
    public static int KAFKA_FETCH_BYTE_SIZE = 50 * 1024 * 1024;

    /**
     * Size in messages per {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)} in a system.
     * <p>
     * This sets {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} ({@code max.poll.records}) which has a Kafka default of
     * {@value ConsumerConfig#DEFAULT_MAX_POLL_RECORDS}.
     */
    public static int KAFKA_FETCH_POLL_SIZE = 5000;
    /**
     * Default batch size, in number of events, if not otherwise configured
     */
    public static int DEFAULT_BATCH_SIZE = 5000;

    /**
     * Kafka consumer properties.
     */
    public static Properties consumerProperties(String server) {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Set the various fetch related configuration, we increase these from their defaults so that we're more likely
        // to fetch and process a larger batch of messages in one go
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, KAFKA_FETCH_BYTE_SIZE);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, KAFKA_FETCH_BYTE_SIZE);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KAFKA_FETCH_POLL_SIZE);

        // Set the bootstrap server(s)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        return props;
    }

    /**
     * Checks whether the given duration is valid for configuration purposes i.e. it is non-null, non-negative and
     * non-zero
     *
     * @param duration Duration to test
     * @return True if valid, false otherwise
     */
    public static boolean isValidDuration(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }
}
