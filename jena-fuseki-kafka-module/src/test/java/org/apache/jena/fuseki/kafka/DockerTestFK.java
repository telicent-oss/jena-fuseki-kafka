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
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
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
    private static final AtomicInteger counter = new AtomicInteger(0);
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
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + counter.incrementAndGet());
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

    @Test(priority = 1)
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

    @Test(priority = 2)
    public void givenBlankOffsets_whenRunningFusekiKafka_thenDataIsLoaded() {
        // Given
        FusekiOffsetStore offsets = createNonPersistentState();

        // When
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            // Then
            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
            DockerTestConfigFK.waitForDataCount(URL, 2);
        } finally {
            server.stop();
        }
    }

    @Test(priority = 3)
    public void givenOffsetsRestored_whenRunningFusekiKafka_thenDataIsLoaded() {
        // Given
        FusekiOffsetStore offsets = createNonPersistentState();
        DSG.clear();
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
            DockerTestConfigFK.waitForDataCount(URL, 2);
            DSG.clear();
            DockerTestConfigFK.waitForDataCount(URL, 0);

            // When
            FusekiOffsetStore newOffsets = FusekiOffsetStore.builder().datasetName(DSG_NAME).build();
            newOffsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(KafkaTestCluster.DEFAULT_TOPIC, 0, "test"), 0L);
            FKS.restoreOffsetForDataset(DSG_NAME, newOffsets);

            // Then
            DockerTestConfigFK.waitForDataCount(URL, 2);
        } finally {
            server.stop();
        }
    }

    @Test(priority = 4)
    public void givenOffsetsToRestoreForMultipleDatasets_whenRunningFusekiKafka_thenDataIsLoadedAsExpected() {
        // Given
        FusekiOffsetStore ignored = FusekiOffsetStore.builder().datasetName("ignore").build();
        ignored.saveOffset(KafkaEventSource.externalOffsetStoreKey("test", 0, "test"), 1L);
        FKS.restoreOffsetForDataset("ignore", ignored);
        FusekiOffsetStore offsets = createNonPersistentState();
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            // When
            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
            DSG.clear();
            DockerTestConfigFK.waitForDataCount(URL, 0);
            FusekiOffsetStore newOffsets = FusekiOffsetStore.builder().datasetName(DSG_NAME).build();
            newOffsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(KafkaTestCluster.DEFAULT_TOPIC, 0, "test"), 2L);
            FKS.restoreOffsetForDataset("", newOffsets);

            // Then
            DockerTestConfigFK.waitForDataCount(URL, 0);
        } finally {
            server.stop();
        }
    }


    @Test(priority = 5)
    public void givenNegativeOffsetsToRestore_whenRunningFusekiKafka_thenNoDataIsLoaded() {
        // Given
        FusekiOffsetStore offsets = createNonPersistentState();
        FusekiServer server = startFuseki(offsets, consumerProps());
        try {
            // When
            String URL = "http://localhost:" + server.getHttpPort() + DSG_NAME;
            DSG.clear();
            DockerTestConfigFK.waitForDataCount(URL, 0);
            FusekiOffsetStore newOffsets = FusekiOffsetStore.builder().datasetName(DSG_NAME).build();
            newOffsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(KafkaTestCluster.DEFAULT_TOPIC, 0, "test"), -5L);
            FKS.restoreOffsetForDataset("", newOffsets);

            // Then
            DockerTestConfigFK.waitForDataCount(URL, 0);
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
        FKS.addConnectorToServer(conn, server, offsets, null);
        server.start();
        return server;
    }
}
