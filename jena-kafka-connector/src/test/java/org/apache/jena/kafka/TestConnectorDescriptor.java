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

import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TestConnectorDescriptor {
    public static String DIR = "src/test/files";

    static {
        JenaSystem.init();
        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
    }

    @Test
    public void givenSimpleConfiguration_whenAssemblingConnector_thenAsExpected() {
        // Given and When
        KConnectorDesc conn = connectorByType("assem-connector-1.ttl");

        // Then
        Assertions.assertNotNull(conn);
        Assertions.assertNotNull(conn.getBootstrapServers());
        Assertions.assertNotNull(conn.getKafkaConsumerProps());
        Properties properties = conn.getKafkaConsumerProps();
        Assertions.assertTrue(properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG));
        Assertions.assertEquals(KafkaConnectorAssembler.DEFAULT_CONSUMER_GROUP_ID,
                                properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    public void givenFullConfiguration_whenAssemblingConnector_thenAsExpected() {
        // Given and When
        KConnectorDesc conn = connectorByType("assem-connector-2.ttl");

        // Then
        Assertions.assertNotNull(conn);
        Assertions.assertNotNull(conn.getBootstrapServers());
        Properties properties = conn.getKafkaConsumerProps();
        Assertions.assertTrue(properties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertTrue(properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG));
        Assertions.assertEquals("example", properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        Assertions.assertEquals("100", properties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        Assertions.assertNotNull(conn.getStateFile());
        Assertions.assertEquals("State.state", conn.getStateFile());
    }

    public static KConnectorDesc connectorByType(String filename) {
        return (KConnectorDesc) AssemblerUtils.build(DIR + "/" + filename, KafkaConnectorAssembler.getType());
    }

    @Test
    public void givenNoTopics_whenConstructingConnector_thenIllegalArgument() {
        // Given, When and Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> new KConnectorDesc(List.of(), null, null, null, true, false, null, null));
    }

    @Test
    public void givenNoDataset_whenConstructingConnector_thenJenaKafkaError() {
        // Given, When and Then
        Assertions.assertThrows(JenaKafkaException.class, () -> new KConnectorDesc(List.of("test"), "test", null, null, true, false, null, null));
    }

    @Test
    public void givenKafkaProperties_whenQueryingConsumerGroup_thenCorrectValueReturned() {
        // Given
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "example");
        KConnectorDesc conn = new KConnectorDesc(List.of("test"), "test", "/ds", null, true, false, null, properties);

        // When
        String groupId = conn.getConsumerGroupId();

        // Then
        Assertions.assertEquals("example", groupId);
    }

    @Test
    public void givenCustomFusekiKafkaProperties_whenGettingAdvancedConfiguration_thenAsExpected() {
        // Given
        Properties properties = new Properties();
        properties.put(SysJenaKafka.FUSEKI_KAFKA_HIGH_LAG_THRESHOLD, "100000");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_TRACKING_WINDOW, "50");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_LOW_VOLUME_THRESHOLD, "25");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_MAX_TRANSACTION_DURATION, "PT1M");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE, "10000");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_BYTES, Long.toString(100 * 1024 * 1024));
        KConnectorDesc conn = new KConnectorDesc(List.of("test"), "test", "/ds", null, true, false, null, properties);

        // When and Then
        Assertions.assertEquals(10000, conn.getBatchSize());
        Assertions.assertEquals(100L * 1024L * 1024L, conn.getBatchSizeBytes());
        Assertions.assertEquals(Duration.ofMinutes(1), conn.getMaxTransactionDuration());
        Assertions.assertEquals(25, conn.getLowVolumeBatchSizeThreshold());
        Assertions.assertEquals(50, conn.getBatchSizeTrackingWindow());
        Assertions.assertEquals(100000L, conn.getHighLagThreshold());
    }

    @Test
    public void givenCustomFusekiKafkaPropertiesWithNonParseableValues_whenGettingAdvancedConfiguration_thenDefaultsReturned() {
        // Given
        Properties properties = new Properties();
        properties.put(SysJenaKafka.FUSEKI_KAFKA_HIGH_LAG_THRESHOLD, "foo");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_TRACKING_WINDOW, "bar");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_LOW_VOLUME_THRESHOLD, "foo");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_MAX_TRANSACTION_DURATION, "bar");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE, "foo");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_BYTES, "bar");
        KConnectorDesc conn = new KConnectorDesc(List.of("test"), "test", "/ds", null, true, false, null, properties);

        // When and Then
        Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE, conn.getBatchSize());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD, conn.getBatchSizeBytes());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_MAX_TRANSACTION_DURATION, conn.getMaxTransactionDuration());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_AVERAGE_BATCH_SIZE_LOW_VOLUME_THRESHOLD, conn.getLowVolumeBatchSizeThreshold());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW, conn.getBatchSizeTrackingWindow());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_HIGH_LAG_THRESHOLD, conn.getHighLagThreshold());
    }

    @Test
    public void givenCustomFusekiKafkaPropertiesWithOutOfRangeValues_whenGettingAdvancedConfiguration_thenDefaultsReturned() {
        // Given
        Properties properties = new Properties();
        properties.put(SysJenaKafka.FUSEKI_KAFKA_HIGH_LAG_THRESHOLD, "-1");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_TRACKING_WINDOW, "-1");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_LOW_VOLUME_THRESHOLD, "-1");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_MAX_TRANSACTION_DURATION, "PT0M");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE, "-1");
        properties.put(SysJenaKafka.FUSEKI_KAFKA_BATCH_SIZE_BYTES, "-1");
        KConnectorDesc conn = new KConnectorDesc(List.of("test"), "test", "/ds", null, true, false, null, properties);

        // When and Then
        Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE, conn.getBatchSize());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_HIGH_LAG_BATCH_BYTE_THRESHOLD, conn.getBatchSizeBytes());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_MAX_TRANSACTION_DURATION, conn.getMaxTransactionDuration());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_AVERAGE_BATCH_SIZE_LOW_VOLUME_THRESHOLD, conn.getLowVolumeBatchSizeThreshold());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW, conn.getBatchSizeTrackingWindow());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_HIGH_LAG_THRESHOLD, conn.getHighLagThreshold());
    }

    @Test
    public void givenKafkaPropertiesUsableForAdvancedConfiguration_whenGettingAdvancedConfiguration_thenPropertyValuesReturned() {
        // Given
        Properties properties = new Properties();
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2500");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Long.toString(10 * 1024 * 1024));
        KConnectorDesc conn = new KConnectorDesc(List.of("test"), "test", "/ds", null, true, false, null, properties);

        // When and Then
        Assertions.assertEquals(2500, conn.getBatchSize());
        Assertions.assertEquals(10L * 1024L * 1024L, conn.getBatchSizeBytes());
    }

}
