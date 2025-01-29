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

import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.kafka.BasicKafkaTestCluster;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import io.telicent.smart.cache.sources.kafka.policies.KafkaReadPolicies;
import org.apache.jena.kafka.common.FusekiOffsetStore;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.awaitility.Awaitility;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.kafka.lib.FKLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// These tests must run in order.
public class DockerTestFK {
    // Logging

    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    private static final String DSG_NAME = "/ds";
    private static final DatasetGraph DSG = DatasetGraphFactory.createTxnMem();
    /**
     * Intentionally protected so derived test classes can inject alternative Kafka cluster implementations for testing
     */
    protected KafkaTestCluster kafka = new BasicKafkaTestCluster();

    @BeforeClass
    public void beforeClass() {
        // Start Kafka Test Cluster
        kafka.setup();

        // Inject test data to Kafka
        String DIR = "src/test/files";
        FKLib.sendFiles(producerProps(), KafkaTestCluster.DEFAULT_TOPIC, List.of(DIR + "/data.ttl"));
        FKLib.sendFiles(producerProps(), KafkaTestCluster.DEFAULT_TOPIC, List.of(DIR + "/data.nq"));
    }

    @AfterMethod
    public void after() {
        FKS.resetPollThreads();
    }

    Properties consumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        consumerProps.putAll(kafka.getClientProperties());
        return consumerProps;
    }

    Properties producerProps() {
        Properties producerProps = new Properties();
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.putAll(kafka.getClientProperties());
        return producerProps;
    }

    @AfterClass
    public void afterClass() {
        Log.info("TestFK", "Stopping testcontainer for Kafka");
        LogCtl.setLevel(NetworkClient.class, "error");
        kafka.teardown();
    }

    @Test(priority = 3)
    public void givenBlankOffsets_whenConsumingFromTopic_thenExpectedMessagesFound() {
        // GIven
        FusekiOffsetStore offsets = createNonPersistentState();
        AtomicInteger count = new AtomicInteger(0);
        KafkaEventSource<Bytes, Bytes> source = KafkaEventSource.<Bytes, Bytes>create()
                                                                .bootstrapServers(this.kafka.getBootstrapServers())
                                                                .consumerConfig(this.kafka.getClientProperties())
                                                                .topic(KafkaTestCluster.DEFAULT_TOPIC)
                                                                .consumerGroup("test")
                                                                .keyDeserializer(BytesDeserializer.class)
                                                                .valueDeserializer(BytesDeserializer.class)
                                                                .externalOffsetStore(offsets)
                                                                .readPolicy(
                                                                        KafkaReadPolicies.fromExternalOffsets(offsets,
                                                                                                              0))
                                                                .build();

        // When
        try {
            while (count.get() < 2) {
                Event<Bytes, Bytes> event = source.poll(Duration.ofSeconds(5));
                Assert.assertNotNull(event);
                count.incrementAndGet();
            }

            // Then
            Assert.assertEquals(count.get(), 2);
            Assert.assertNull(source.poll(Duration.ofSeconds(3)));
        } finally {
            source.close();
        }
    }

    private static FusekiOffsetStore createNonPersistentState() {
        return FusekiOffsetStore.builder().datasetName(DSG_NAME).build();
    }

    @Test(priority = 4)
    public void givenBlankOffsets_whenRunningFusekiKafka_thenDataIsLoaded() {
        // Given
        FusekiOffsetStore offsets = createNonPersistentState();

        // When
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            // Then
            //@formatter:off
            // NB - For secure clusters it takes longer to poll the data because of secure connection establishment
            //      overheads so allow some time for the data to be polled from Kafka and applied to the dataset
            Awaitility.await()
                      .pollInterval(Duration.ofSeconds(3))
                      .atMost(Duration.ofSeconds(10))
                      .until(() -> {
                            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
                            RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
                            int count = ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
                            return count;
                        }, x -> x == 2);
            //@formatter:on
        } finally {
            server.stop();
        }
    }

    // TODO Need to restore these tests once we've restored the ability to restore offsets

    @Test(priority = 5, enabled = false)
    public void fk05_restore() {
        // Assumes the topic exists and has data.
        String countQuery = "SELECT (count(*) AS ?C) {?s ?p ?o}";
        FusekiOffsetStore offsets = createNonPersistentState();
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
            DSG.clear();
            RowSet rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
            int count = ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
            Assert.assertEquals(count, 0);
            FKS.restoreOffsetForDataset("", 0L);
            Awaitility.await()
                      .atMost(30, TimeUnit.SECONDS)
                      .pollInterval(5, TimeUnit.SECONDS)
                      .until(FKS.restoreOffsetMap::isEmpty);
            rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
            count = ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
            Assert.assertEquals(count, 2);
        } finally {
            server.stop();
        }
    }

    @Test(priority = 6, enabled = false)
    public void fk06_restore_ignore() {
        // Assumes the topic exists and has data.
        FKS.restoreOffsetForDataset("ignore", 1L);
        String countQuery = "SELECT (count(*) AS ?C) {?s ?p ?o}";
        FusekiOffsetStore offsets = createNonPersistentState();
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
            DSG.clear();
            RowSet rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
            int count = ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
            Assert.assertEquals(count, 0);
            FKS.restoreOffsetMap.clear();
            FKS.restoreOffsetForDataset("", 2L);
            Awaitility.await()
                      .atMost(30, TimeUnit.SECONDS)
                      .pollInterval(5, TimeUnit.SECONDS)
                      .until(FKS.restoreOffsetMap::isEmpty);
            rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
            count = ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
            Assert.assertEquals(count, 0);
        } finally {
            server.stop();
        }
    }


    @Test(priority = 7, enabled = false)
    public void fk07_restore_beginning() {
        // Assumes the topic exists and has data.
        String countQuery = "SELECT (count(*) AS ?C) {?s ?p ?o}";
        FusekiOffsetStore offsets = createNonPersistentState();
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
            DSG.clear();
            RowSet rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
            int count = ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
            Assert.assertEquals(count, 0);
            FKS.restoreOffsetForDataset("", -5L);
            Awaitility.await()
                      .atMost(30, TimeUnit.SECONDS)
                      .pollInterval(5, TimeUnit.SECONDS)
                      .until(FKS.restoreOffsetMap::isEmpty);
            rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
            count = ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
            Assert.assertEquals(count, 2);
        } finally {
            server.stop();
        }
    }

    private FusekiServer startFuseki(FusekiOffsetStore offsets, Properties consumerProps) {
        // Automatic
        FusekiServer server = FusekiServer.create().port(0)
                                          //.verbose(true)
                                          .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                          .add(DSG_NAME, DSG).build();
        KConnectorDesc conn =
                new KConnectorDesc(List.of(KafkaTestCluster.DEFAULT_TOPIC), this.kafka.getBootstrapServers(), DSG_NAME,
                                   null, false, true, null, consumerProps);
        // Manual call to set up the server.
        FKS.addConnectorToServer(conn, server, offsets);
        server.start();
        return server;
    }
}
