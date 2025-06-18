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

import io.telicent.smart.cache.sources.kafka.BasicKafkaTestCluster;
import io.telicent.smart.cache.sources.kafka.FlakyKafkaTest;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.kafka.common.FusekiOffsetStore;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testng.annotations.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class DockerTestFK {
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
    protected KafkaTestCluster kafka = null;

    /**
     * Creates a new instance of the Kafka Test cluster
     *
     * @return Kafka Test cluster
     */
    protected KafkaTestCluster createTestCluster() {
        return new BasicKafkaTestCluster();
    }

    @BeforeMethod
    public void setupTest() throws InterruptedException {
        // Start Kafka Test Cluster
        this.kafka = createTestCluster();
        kafka.setup();

        // Inject test data to Kafka
        String DIR = "src/test/files";
        FKLib.sendFiles(producerProps(), KafkaTestCluster.DEFAULT_TOPIC, List.of(DIR + "/data.ttl"));
        FKLib.sendFiles(producerProps(), KafkaTestCluster.DEFAULT_TOPIC, List.of(DIR + "/data.nq"));
    }

    @AfterMethod
    public void teardownTest() {
        FKS.resetPollThreads();
        this.kafka.teardown();
        this.kafka = null;
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
    public void teardown() {
        FKS.resetPollThreads();
        if (this.kafka != null) {
            kafka.teardown();
        }
    }

    private static FusekiOffsetStore createNonPersistentState() {
        return FusekiOffsetStore.builder().datasetName(DSG_NAME).build();
    }

    @Test(retryAnalyzer = FlakyKafkaTest.class)
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

    @Test(retryAnalyzer = FlakyKafkaTest.class)
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
            newOffsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(KafkaTestCluster.DEFAULT_TOPIC, 0, "test"),
                                  0L);
            FKS.restoreOffsetForDataset(DSG_NAME, newOffsets);

            // Then
            DockerTestConfigFK.waitForDataCount(URL, 2);
        } finally {
            server.stop();
        }
    }

    @Test(retryAnalyzer = FlakyKafkaTest.class)
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
            newOffsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(KafkaTestCluster.DEFAULT_TOPIC, 0, "test"),
                                  2L);
            FKS.restoreOffsetForDataset("", newOffsets);

            // Then
            DockerTestConfigFK.waitForDataCount(URL, 0);
        } finally {
            server.stop();
        }
    }


    @Test(retryAnalyzer = FlakyKafkaTest.class)
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
            newOffsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(KafkaTestCluster.DEFAULT_TOPIC, 0, "test"),
                                  -5L);
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

    @Test(expectedExceptions = FusekiKafkaException.class, expectedExceptionsMessageRegExp = "No dataset found.*")
    public void givenMisconfiguredDatasetName_whenAddingConnectorToServer_thenFails() {
        // Given
        KConnectorDesc conn =
                new KConnectorDesc(List.of(KafkaTestCluster.DEFAULT_TOPIC), this.kafka.getBootstrapServers(), "/wrong",
                                   null, false, true, null, consumerProps());
        FusekiServer server = FusekiServer.create()
                                          .port(0)
                                          .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                          .add(DSG_NAME, DSG).build();
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName(DSG_NAME).build();

        // When and Then
        FKS.addConnectorToServer(conn, server, offsets, null);
    }
}
