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
}
