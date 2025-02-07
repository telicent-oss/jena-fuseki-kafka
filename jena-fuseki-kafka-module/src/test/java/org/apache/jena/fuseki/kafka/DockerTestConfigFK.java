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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.TelicentHeaders;
import io.telicent.smart.cache.sources.kafka.BasicKafkaTestCluster;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.kafka.common.FusekiOffsetStore;
import org.apache.jena.kafka.utils.EnvVariables;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.system.G;
import org.apache.jena.vocabulary.RDF;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.*;

public class DockerTestConfigFK {
    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    /**
     * Intentionally protected so derived test classes can override the default implementation used
     */
    protected KafkaTestCluster kafka = null;
    private static final String DIR = "src/test/files";

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
        kafka = createTestCluster();
        kafka.setup();
        resetTopics();

        // As Kafka is a distributed system there's a race condition that can happen when the topics aren't fully
        // created, and we try to write to them resulting in the events being lost
        // Inserting a small sleep here avoids that happening.
        Thread.sleep(500);
    }

    private void resetTopics() {
        kafka.resetTopic("RDF0");
        kafka.resetTopic("RDF1");
        kafka.resetTopic("RDF2");
        kafka.resetTopic("RDF_Patch");
        kafka.resetTopic("bad-rdf");
    }

    Properties producerProps() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProps.putAll(kafka.getClientProperties());
        return producerProps;
    }

    @AfterMethod
    public void teardownTest() throws InterruptedException {
        FKS.resetPollThreads();
        kafka.teardown();
        kafka = null;
    }

    @AfterClass
    public void teardown() {
        FKS.resetPollThreads();
        if (kafka != null) {
            kafka.teardown();
        }
    }

    private static final String STATE_DIR = "target/state";

    @Test
    public void givenSingleConnector_whenRunningFusekiKafka_thenDataAsExpected() {
        // Given
        String TOPIC = "RDF0";
        Graph graph = loadConfiguration("/config-connector.ttl");

        // When
        FusekiServer server = createFusekiServer(graph);
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR + "/data.ttl"));
        server.start();
        try {
            // Then
            waitForDataCount("http://localhost:" + server.getHttpPort() + "/ds", 1);
        } finally {
            server.stop();
        }
    }

    private Graph loadConfiguration(String x) {
        Graph graph =
                configuration(DIR + x, kafka.getBootstrapServers(), kafka.getClientProperties());

        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);
        return graph;
    }

    public static void waitForDataCount(String url, int expectedCount) {
        //@formatter:off
            Awaitility.await()
                      .pollDelay(Duration.ZERO)
                      .pollInterval(Duration.ofSeconds(5))
                      .atMost(Duration.ofSeconds(15))
                      .until(() -> count(url), x -> x == expectedCount);
            //@formatter:on
    }

    // Two connectors
    @Test
    public void givenTwoConnectorsToDifferentDatasets_whenRunningFusekiKafka_thenEachDatasetAsExpected() {
        // Given
        String TOPIC1 = "RDF1";
        String TOPIC2 = "RDF2";
        Graph graph = loadConfiguration("/config-connector-2.ttl");

        // When
        FusekiServer server = createFusekiServer(graph);
        // Two triples in topic 2
        // One triple in topic 1
        FKLib.sendFiles(producerProps(), TOPIC2, List.of(DIR + "/data.ttl", DIR + "/data2.ttl"));
        FKLib.sendFiles(producerProps(), TOPIC1, List.of(DIR + "/data.ttl"));
        // Do after the "send" so the first poll picks them up.
        server.start();
        try {
            // Then
            String URL1 = "http://localhost:" + server.getHttpPort() + "/ds1";
            waitForDataCount(URL1, 1);
            String URL2 = "http://localhost:" + server.getHttpPort() + "/ds2";
            waitForDataCount(URL2, 2);
        } finally {
            server.stop();
        }
    }

    // Two connectors, one dataset
    @Test
    public void givenTwoConnectorsToSameDataset_whenRunningFusekiKafka_thenDataFromBothTopicsVisible() {
        String TOPIC1 = "RDF1";
        String TOPIC2 = "RDF2";
        Graph graph = loadConfiguration("/config-connector-3.ttl");

        FusekiServer server = createFusekiServer(graph);
        // One triple on each topic.
        FKLib.sendFiles(producerProps(), TOPIC2, List.of(DIR + "/data2.ttl"));
        FKLib.sendFiles(producerProps(), TOPIC1, List.of(DIR + "/data.ttl"));
        server.start();
        try {
            String URL = "http://localhost:" + server.getHttpPort() + "/ds0";
            waitForDataCount(URL, 2);
        } finally {
            server.stop();
        }
    }

    @Test
    public void givenOneConnectorsTwoTopics_whenRunningFusekiKafka_thenDataFromBothTopicsVisible() {
        String TOPIC1 = "RDF1";
        String TOPIC2 = "RDF2";
        Graph graph = loadConfiguration("/config-connector-4.ttl");

        FusekiServer server = createFusekiServer(graph);
        // One triple on each topic.
        FKLib.sendFiles(producerProps(), TOPIC2, List.of(DIR + "/data2.ttl"));
        FKLib.sendFiles(producerProps(), TOPIC1, List.of(DIR + "/data.ttl"));
        server.start();
        try {
            String URL = "http://localhost:" + server.getHttpPort() + "/ds0";
            waitForDataCount(URL, 2);
        } finally {
            server.stop();
        }
    }

    // RDF Patch
    @Test
    public void givenSingleConnector_whenRunningFusekiKafkaWithPatchEvents_thenPatchAppliedAsExpected() {
        // Given
        String TOPIC = "RDF_Patch";
        Graph graph = loadConfiguration("/config-connector-patch.ttl");

        // When
        FusekiServer server = createFusekiServer(graph);
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR + "/patch1.rdfp"));
        server.start();
        try {
            // Then
            String URL = "http://localhost:" + server.getHttpPort() + "/ds";
            waitForDataCount(URL, 4);
        } finally {
            server.stop();
        }
    }

    @Test
    public void givenConnectorNoSyncNoReplay_whenRunningFusekiKafka_thenNoDataIsLoaded() {
        // Given
        String TOPIC = "RDF0";
        Graph graph = loadConfiguration("/config-connector-latest.ttl");

        // When
        FusekiServer server = createFusekiServer(graph);
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR + "/data.ttl"));
        server.start();
        try {
            // Then
            waitForDataCount("http://localhost:" + server.getHttpPort() + "/ds", 0);
        } finally {
            server.stop();
        }
    }

    private static FusekiServer createFusekiServer(Graph graph) {
        return FusekiServer.create()
                           .port(0)
                           .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                           .parseConfig(ModelFactory.createModelForGraph(graph))
                           .build();
    }

    @Test
    public void givenConnectorWithDlq_whenRunningFusekiKafkaWithMalformedEvents_thenGoodDataApplied_andBadEventsGoToDlq() {
        // Given
        Graph graph = loadConfiguration("/config-connector-dlq.ttl");

        // When
        FusekiServer server = createFusekiServer(graph);

        FKLib.sendFiles(producerProps(), "RDF0",
                        List.of(DIR + "/data.ttl", DIR + "/malformed.ttl", DIR + "/data2.ttl"));
        server.start();
        try {
            // Then
            String URL = "http://localhost:" + server.getHttpPort() + "/ds";
            waitForDataCount(URL, 2);
        } finally {
            server.stop();
        }

        // And
        verifyDlqContents();
    }

    @Test
    public void givenConnectorWithDlq_whenRunningFusekiKafkaWithMalformedEventsThatFailDuringApplication_thenGoodDataApplied_andBadEventsGoToDlq() {
        // Given
        Graph graph = loadConfiguration("/config-connector-dlq.ttl");

        // When
        FusekiServer server = createFusekiServer(graph);

        FKLib.sendFiles(producerProps(), "RDF0",
                        List.of(DIR + "/data.ttl", DIR + "/malformed.rdfp", DIR + "/data2.ttl"));
        server.start();
        try {
            // Then
            String URL = "http://localhost:" + server.getHttpPort() + "/ds";
            waitForDataCount(URL, 2);
        } finally {
            server.stop();
        }

        // And
        verifyDlqContents();
    }

    private void verifyDlqContents() {
        KafkaEventSource<Bytes, Bytes> source = KafkaEventSource.<Bytes, Bytes>create()
                                                                .topic("bad-rdf")
                                                                .bootstrapServers(this.kafka.getBootstrapServers())
                                                                .consumerGroup("bad-rdf-check-1")
                                                                .consumerConfig(this.kafka.getClientProperties())
                                                                .keyDeserializer(BytesDeserializer.class)
                                                                .valueDeserializer(BytesDeserializer.class)
                                                                .build();
        try {
            Event<Bytes, Bytes> badEvent = source.poll(Duration.ofSeconds(10));
            Assert.assertNotNull(badEvent);
            Assert.assertNotNull(badEvent.lastHeader(TelicentHeaders.DEAD_LETTER_REASON));
        } finally {
            source.close();
        }
    }

    // Env Variable use
    @Test
    public void givenEnvironmentVariableConfiguration_whenRunningFusekiKafka_thenDataAsExpected() {
        try {
            // Given
            System.setProperty("TEST_BOOTSTRAP_SERVER", this.kafka.getBootstrapServers());
            System.clearProperty("TEST_KAFKA_TOPIC");
            System.setProperty("TEST_CONSUMER_GROUP", "connector-6-" + System.currentTimeMillis());
            Graph graph = loadConfiguration("/config-connector-env.ttl");

            // When
            FusekiServer server = createFusekiServer(graph);
            FKLib.sendFiles(producerProps(), "RDF0", List.of(DIR + "/data.ttl"));
            server.start();
            try {
                // Then
                String URL = "http://localhost:" + server.getHttpPort() + "/ds";
                waitForDataCount(URL, 1);
            } finally {
                server.stop();
            }
        } finally {
            System.clearProperty("TEST_BOOTSTRAP_SERVER");
            System.clearProperty("TEST_KAFKA_TOPIC");
            System.clearProperty("TEST_CONSUMER_GROUP");
        }
    }

    @Test
    public void givenSingleConnectorWithPreexistingOffsets_whenRunningFusekiKafka_thenNoDataIsLoaded() {
        // Given
        String TOPIC = "RDF0";
        Graph graph = loadConfiguration("/config-connector.ttl");
        String groupId = generateUniqueConsumerId(graph);
        File stateFile = new File(STATE_DIR + "/Replay-RDF0.state");
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName("/ds").stateFile(stateFile).build();
        offsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(TOPIC, 0, groupId), 1);
        offsets.saveOffset(KafkaEventSource.externalOffsetStoreKey(TOPIC, 1, "another"), 0);
        offsets.flush();

        // When
        FusekiServer server = createFusekiServer(graph);
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR + "/data.ttl"));
        server.start();
        try {
            // Then
            waitForDataCount("http://localhost:" + server.getHttpPort() + "/ds", 0);
        } finally {
            server.stop();
        }
    }

    private static String generateUniqueConsumerId(Graph graph) {
        Triple groupIds =
                graph.stream(Node.ANY, KafkaConnectorAssembler.pKafkaGroupId, Node.ANY).findFirst().orElse(null);
        graph.delete(groupIds);
        String uniqueId = "connector-" + System.nanoTime();
        graph.add(groupIds.getSubject(), groupIds.getPredicate(), NodeFactory.createLiteralString(uniqueId));
        return uniqueId;
    }

    private static int count(String URL) {
        RowSet rowSet = QueryExecHTTP.service(URL)
                                     .query("SELECT (count(*) AS ?C) { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }")
                                     .select();
        return ((Number) rowSet.next().get("C").getLiteralValue()).intValue();
    }

    // Read a configuration and update it for the mock server.
    protected static Graph configuration(String filename, String bootstrapServers, Properties props) {
        // Fix up!
        Graph graph = RDFParser.source(filename).toGraph();

        // Replace the placeholder bootstrap servers with the real bootstrap servers
        List<Triple> triplesBootstrapServers =
                G.find(graph, Node.ANY, KafkaConnectorAssembler.pKafkaBootstrapServers, Node.ANY).toList();
        triplesBootstrapServers.forEach(t -> {
            if (!t.getObject().getLiteralLexicalForm().startsWith(EnvVariables.ENV_PREFIX)) {
                graph.delete(t);
                graph.add(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralString(bootstrapServers));
            }
        });

        // Rewrites state file paths to point into our target/state directory for tests
        FileOps.ensureDir(STATE_DIR);
        List<Triple> triplesStateFile = G.find(graph, Node.ANY, KafkaConnectorAssembler.pStateFile, Node.ANY).toList();
        triplesStateFile.forEach(t -> {
            graph.delete(t);
            String fn = t.getObject().getLiteralLexicalForm();
            graph.add(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralString(STATE_DIR + "/" + fn));
        });

        // Make the Consumer Group IDs unique within tests just to ensure no collisions between tests otherwise
        // that can lead to Consumer Group Rebalance deadlocks
        for (Triple t : graph.stream(Node.ANY, KafkaConnectorAssembler.pKafkaGroupId, Node.ANY).toList()) {
            if (!t.getObject().getLiteralLexicalForm().startsWith(EnvVariables.ENV_PREFIX)) {
                graph.delete(t);
                graph.add(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralString(
                        t.getObject().getLiteralLexicalForm() + "-" + System.nanoTime()));
            }
        }

        // If extra client properties are needed inject via an external properties file
        if (!props.isEmpty()) {
            try {
                // Write Kafka properties out to a temporary file
                File propsFile = Files.createTempFile("kafka", ".properties").toFile();
                try (FileOutputStream output = new FileOutputStream(propsFile)) {
                    props.store(output, null);
                }

                // For each Kafka connector in the configuration inject the fk:configFile triple pointing to our
                // temporary properties file for our Secure Cluster
                List<Triple> connectors =
                        G.find(graph, Node.ANY, RDF.type.asNode(), KafkaConnectorAssembler.tKafkaConnector.asNode())
                         .toList();
                connectors.forEach(t -> {
                    graph.add(t.getSubject(), KafkaConnectorAssembler.pKafkaPropertyFile,
                              NodeFactory.createURI(propsFile.toURI().toString()));
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return graph;
    }
}
