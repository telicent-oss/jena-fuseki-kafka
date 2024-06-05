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

package org.apache.jena.fuseki.kafka;

import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class TestConnectorDescriptor {
    private static String DIR = "src/test/files";

    static {
        JenaSystem.init();
        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
    }

    @Test
    public void descriptor_1() {
        KConnectorDesc conn = connectorByType("assem-connector-1.ttl");
        assertNotNull(conn);
        assertNotNull(conn.getBootstrapServers());
        assertNotNull(conn.getKafkaConsumerProps());
    }

    @Test
    public void descriptor_2() {
        KConnectorDesc conn = connectorByType("assem-connector-2.ttl");
        assertNotNull(conn);
        assertNotNull(conn.getBootstrapServers());
        Properties properties = conn.getKafkaConsumerProps();

        assertTrue(properties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertTrue(properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("100", properties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));

        assertNotNull(conn.getStateFile());
        assertEquals("State.state", conn.getStateFile());
    }

    private KConnectorDesc connectorByType(String filename) {
        KConnectorDesc conn =
                (KConnectorDesc) AssemblerUtils.build(DIR + "/" + filename, KafkaConnectorAssembler.getType());
        return conn;
    }

}
