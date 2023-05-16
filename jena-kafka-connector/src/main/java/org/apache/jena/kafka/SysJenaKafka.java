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

import java.util.Properties;

import org.apache.jena.util.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class SysJenaKafka {

    public static final String PATH         = "org.apache.jena.kafka";
    private static String metadataLocation  = "org/apache/jena/kafka/jena-kafka.xml";
    private static Metadata metadata        = new Metadata(metadataLocation);

    /** The product name */
    public static final String NAME         = "Apache Jena Kafka Connector";
    /** The full name of the current ARQ version */
    public static final String VERSION      = metadata.get(PATH+".version", "unknown");
    /** The date and time at which this release was built */
    public static final String BUILD_DATE   = metadata.get(PATH+".build.datetime", "unset");

    public static void init() {}

    /**
     * Size in bytes per consumer.poll in a system.
     * <p>
     * This sets {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG} which has a
     * Kafka default of 1Mb.
     * <p>
     * To replicate data, we need Fuseki or user application to see all the data
     * in-order which forces the choice of one partition. See also
     * {@link ConsumerConfig#FETCH_MAX_BYTES_CONFIG} which has a Kafka default of
     * 50Mb.
     */
    private static int KafkaFetchSize = 20 * 1024 * 1024;

    /**
     * Kafka consumer properties.
     */
    public static Properties consumerProperties(String server) {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Default is 1M
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, KafkaFetchSize);
        // Default is 50M
        //props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 50*1024*1024);
        props.put("bootstrap.servers", server);
        return props;
    }
}
