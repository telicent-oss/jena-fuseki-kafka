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

import io.telicent.smart.cache.sources.kafka.BasicKafkaTestCluster;
import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.kafka.lib.FKLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.system.G;
import org.apache.jena.vocabulary.RDF;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DockerTestConfigFK {
    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    /**
     * Intentionally protected so derived test classes can override the default implementation used
     */
    protected KafkaTestCluster kafka = new BasicKafkaTestCluster();
    private static final String DIR = "src/test/files";

<<<<<<< HEAD
    @BeforeClass
    public void beforeClass() {
        Log.info("TestIntegrationFK","Starting testcontainer for Kafka");
=======
    // Logs to silence,
    private static final String[] XLOGS = {
            AdminClientConfig.class.getName(),
            // NetworkClient is noisy with warnings about can't connect.
            NetworkClient.class.getName(),
            FetchSessionHandler.class.getName(),
            ConsumerConfig.class.getName(),
            ProducerConfig.class.getName(),
            AppInfoParser.class.getName()
    };

    private static void adjustLogs(String level) {
        for (String logName : XLOGS) {
            LogCtl.setLevel(logName, level);
        }
    }

    @BeforeClass
    public void beforeClass() {
        adjustLogs("Warning");
        Log.info("TestIntegrationFK", "Starting testcontainer for Kafka");

        kafka.setup();
        kafka.resetTopic("RDF0");
        kafka.resetTopic("RDF1");
        kafka.resetTopic("RDF2");
        kafka.resetTopic("RDF_Patch");
    }

    Properties producerProps() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProps.putAll(kafka.getClientProperties());
        return producerProps;
    }

    @AfterMethod
    public void after() {
        FKS.resetPollThreads();
    }

    @AfterClass
    public void afterClass() {
        Log.info("TestIntegrationFK", "Stopping testcontainer for Kafka");
        FKS.resetPollThreads();
        kafka.teardown();
    }

    private static final String STATE_DIR = "target/state";

    @Test(priority = 1)
    public void givenSingleConnector_whenRunningFusekiKafka_thenDataAsExpected() {
        // Given
        String TOPIC = "RDF0";
        Graph graph =
                configuration(DIR + "/config-connector.ttl", kafka.getBootstrapServers(), kafka.getClientProperties());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        // When
        FusekiServer server = FusekiServer.create().port(0)
                                          //.verbose(true)
                                          .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                          .parseConfig(ModelFactory.createModelForGraph(graph)).build();
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR + "/data.ttl"));
        server.start();
        try {
            // Then
            waitForDataCount("http://localhost:" + server.getHttpPort() + "/ds", 1);
        } finally {
            server.stop();
        }
    }

    public static void waitForDataCount(String url, int expectedCount) {
        //@formatter:off
            Awaitility.await()
                      .pollDelay(Duration.ZERO)
                      .pollInterval(Duration.ofSeconds(3))
                      .atMost(Duration.ofSeconds(10))
                      .until(() -> count(url), x -> x == expectedCount);
            //@formatter:on
    }

    // Two connectors
    @Test(priority = 2)
    public void givenTwoConnectorsToDifferentDatasets_whenRunningFusekiKafka_thenEachDatasetAsExpected() {
        // Given
        String TOPIC1 = "RDF1";
        String TOPIC2 = "RDF2";
        Graph graph = configuration(DIR + "/config-connector-2.ttl", kafka.getBootstrapServers(),
                                    kafka.getClientProperties());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        // When
        FusekiServer server = FusekiServer.create().port(0)
                                          //.verbose(true)
                                          .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                          .parseConfig(ModelFactory.createModelForGraph(graph)).build();
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
    @Test(priority = 3)
    public void givenTwoConnectorsToSameDataset_whenRunningFusekiKafka_thenDataFromBothTopicsVisible() {
        String TOPIC1 = "RDF1";
        String TOPIC2 = "RDF2";
        Graph graph = configuration(DIR + "/config-connector-3.ttl", kafka.getBootstrapServers(),
                                    kafka.getClientProperties());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        FusekiServer server = FusekiServer.create().port(0)
                                          //.verbose(true)
                                          .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                          .parseConfig(ModelFactory.createModelForGraph(graph)).build();
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
    @Test(priority = 4)
    public void givenSingleConnector_whenRunningFusekiKafkaWithPatchEvents_thenPatchAppliedAsExpected() {
        // Given
        String TOPIC = "RDF_Patch";
        Graph graph = configuration(DIR + "/config-connector-patch.ttl", kafka.getBootstrapServers(),
                                    kafka.getClientProperties());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        // When
        FusekiServer server = FusekiServer.create().port(0)
                                          //.verbose(true)
                                          .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                          .parseConfig(ModelFactory.createModelForGraph(graph)).build();

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

    // Env Variable use
    @Test(priority = 5)
    public void givenEnvironmentVariableConfiguration_whenRunningFusekiKafka_thenDataAsExpected() {
        // Given
        System.setProperty("TEST_BOOTSTRAP_SERVER", "localhost:9092");
        System.clearProperty("TEST_KAFKA_TOPIC");
        Graph graph = configuration(DIR + "/config-connector-env.ttl", kafka.getBootstrapServers(),
                                    kafka.getClientProperties());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        // When
        FusekiServer server =
                FusekiServer.create().port(0).fusekiModules(FusekiModules.create(new FMod_FusekiKafka())).parseConfig(ModelFactory.createModelForGraph(graph)).build();
        FKLib.sendFiles(producerProps(), "RDF0", List.of(DIR + "/data.ttl"));
        server.start();
        try {
            // Then
            String URL = "http://localhost:" + server.getHttpPort() + "/ds";
            waitForDataCount(URL, 1);
        } finally {
            server.stop();
        }
        System.clearProperty("TEST_BOOTSTRAP_SERVER");
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
        List<Triple> triplesBootstrapServers =
                G.find(graph, null, KafkaConnectorAssembler.pKafkaBootstrapServers, null).toList();
        triplesBootstrapServers.forEach(t -> {
            graph.delete(t);
            graph.add(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralString(bootstrapServers));
        });

        FileOps.ensureDir("target/state");
        List<Triple> triplesStateFile = G.find(graph, null, KafkaConnectorAssembler.pStateFile, null).toList();
        triplesStateFile.forEach(t -> {
            graph.delete(t);
            String fn = t.getObject().getLiteralLexicalForm();
            graph.add(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralString(STATE_DIR + "/" + fn));
        });

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
                        G.find(graph, null, RDF.type.asNode(), KafkaConnectorAssembler.tKafkaConnector.asNode())
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
