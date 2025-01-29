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
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class TestConnectorDescriptor {
    private static String DIR = "src/test/files";

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

    private KConnectorDesc connectorByType(String filename) {
        return (KConnectorDesc) AssemblerUtils.build(DIR + "/" + filename, KafkaConnectorAssembler.getType());
    }

}
