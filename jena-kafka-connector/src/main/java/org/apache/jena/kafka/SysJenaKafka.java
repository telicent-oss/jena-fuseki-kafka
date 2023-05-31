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

import java.util.Optional;
import java.util.Properties;

import org.apache.jena.util.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class SysJenaKafka {

    public static final String PATH         = "org.apache.jena.kafka";
    private static String metadataLocation  = "org/apache/jena/kafka/jena-kafka.xml";
    private static Metadata metadata        = new Metadata(metadataLocation);

    /** The product name */
    public static final String NAME         = "Apache Jena Kafka Connector";

    // TODO Remove after Jena 4.9.0 update.
    // Copied from jena development. This can be replaced by Version.versionForClass
    // when jena 4.9.0 is available.
    private static Optional<String> versionForClass(Class<?> cls) {
        String x = cls.getPackage().getImplementationVersion();
        return Optional.ofNullable(x);
    }

    /** Software version taken from the jar file. */
    public static final String VERSION      = /*Version.*/versionForClass(FusekiKafka.class).orElse("<development>");

    public static void init() {}

    /**
     * Size in bytes per consumer.poll in a system.
     * <p>
     * This sets {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG}
     * ({@code max.partition.fetch.bytes}) which has a Kafka default of 1Mb.
     * <p>
     * To replicate data, we need Fuseki or user application to see all the data
     * in-order which forces the choice of one partition. See also
     * {@link ConsumerConfig#FETCH_MAX_BYTES_CONFIG} which has a Kafka default of
     * 50Mb.
     */
    private static int KafkaFetchBytesSize = 50 * 1024 * 1024;

    /**
     * Size in messages per consumer.poll in a system.
     * <p>
     * This sets {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} ({@code max.poll.records})
     * which has a Kafka default of 500.
     */
    private static int KafkaFetchPollSize = 5000;

    /**
     * Kafka consumer properties.
     */
    public static Properties consumerProperties(String server) {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, KafkaFetchBytesSize);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaFetchPollSize);

        // Default is 50M
        //props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 50*1024*1024);
        props.put("bootstrap.servers", server);
        return props;
    }
}
