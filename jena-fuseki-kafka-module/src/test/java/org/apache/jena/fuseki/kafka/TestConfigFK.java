/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.fuseki.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Properties;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.kafka.lib.FKLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.other.G;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
// These tests must run in order.
public class TestConfigFK {
    static {
        JenaSystem.init();
        FusekiLogging.setLogging();
    }

    private static MockKafka mock;
    private static String DIR = "src/test/files";

    // Logs to silence,
    private static String [] XLOGS = {
        AdminClientConfig.class.getName() ,
        NetworkClient.class.getName() ,
        FetchSessionHandler.class.getName() ,
        ConsumerConfig.class.getName() ,
        ProducerConfig.class.getName() ,
        AppInfoParser.class.getName()
    };

    private static void adjustLogs(String level) {
        for ( String logName : XLOGS ) {
            LogCtl.setLevel(logName, level);
        }
    }

    @BeforeAll public static void beforeClass() {
        adjustLogs("Warning");
        Log.info("TestIntegrationFK","Starting testcontainer for Kafka");
        mock = new MockKafka();
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", mock.getServer());
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", mock.getServer());

        mock.createTopic("RDF0");
        mock.createTopic("RDF1");
        mock.createTopic("RDF2");
    }

    @AfterAll public static void afterClass2() {
        FMod_FusekiKafka.resetPollThreads();
    }

    Properties consumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", mock.getServer());
        return consumerProps;
    }

    Properties producerProps() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", mock.getServer());
        return producerProps;
    }

    @AfterAll public static void afterClass() {
        Log.info("TestIntegrationFK","Stopping testcontainer for Kafka");
        FMod_FusekiKafka.resetPollThreads();
        LogCtl.setLevel(NetworkClient.class, "error");
        mock.stop();
        adjustLogs("info");
    }

    private static String STATE_DIR = "target/state";

    @Test public void fk01_fuseki() {
        String TOPIC = "RDF0";
        Graph graph = configuration(DIR+"/config-connector.ttl", mock.getServer());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        // Configuration knows the topic name.
        FusekiServer server = FusekiServer.create()
                .port(0)
                //.verbose(true)
                .parseConfig(ModelFactory.createModelForGraph(graph))
                .build();
        FKLib.send(producerProps(), TOPIC, List.of(DIR+"/data.ttl"));
        server.start();
        try {
            String URL = "http://localhost:"+server.getHttpPort()+"/ds";
            RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
            int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
            assertEquals(1, count);
        } finally { server.stop(); }
    }

    // Two connectors
    @Test public void fk02_fuseki() {
        String TOPIC1 = "RDF1";
        String TOPIC2 = "RDF2";
        Graph graph = configuration(DIR+"/config-connector-2.ttl", mock.getServer());
        //RDFWriter.source(graph).lang(Lang.TTL).output(System.out);
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        // Configuration knows the topic name.
        FusekiServer server = FusekiServer.create()
                .port(0)
                //.verbose(true)
                .parseConfig(ModelFactory.createModelForGraph(graph))
                .build();
        // Two triples in topic 2
        // One triple in topic 1
        FKLib.send(producerProps(), TOPIC2, List.of(DIR+"/update.ru"));
        FKLib.send(producerProps(), TOPIC1, List.of(DIR+"/data.ttl"));
        FKLib.send(producerProps(), TOPIC2, List.of(DIR+"/data.ttl"));
        // Do after the "send" so the first poll picks them up.
        server.start();
        try {
            String URL1 = "http://localhost:"+server.getHttpPort()+"/ds1";
            int count1 = count(URL1);
            assertEquals(1, count1);
            String URL2 = "http://localhost:"+server.getHttpPort()+"/ds2";
            int count2 = count(URL2);
            assertEquals(2, count2);
        } finally { server.stop(); }
    }

    // Two connectors, one dataset
    @Test public void fk03_fuseki() {
        String TOPIC1 = "RDF1";
        String TOPIC2 = "RDF2";
        Graph graph = configuration(DIR+"/config-connector-3.ttl", mock.getServer());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        FusekiServer server = FusekiServer.create()
                .port(0)
                //.verbose(true)
                .parseConfig(ModelFactory.createModelForGraph(graph))
                .build();
        // One triple on each topic.
        FKLib.send(producerProps(), TOPIC2, List.of(DIR+"/update.ru"));
        FKLib.send(producerProps(), TOPIC1, List.of(DIR+"/data.ttl"));
        server.start();
        try {
            String URL = "http://localhost:"+server.getHttpPort()+"/ds0";
            int count = count(URL);
            assertEquals(2, count);
        } finally { server.stop(); }
    }

    private static int count(String URL) {
        RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
        int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        return count;
    }

    // Read a configuration and update it for the mock server.
    private Graph configuration(String filename, String bootstrapServers) {
        // Fix up!
        Graph graph = RDFParser.source(filename).toGraph();
        List<Triple> triplesBootstrapServers = G.find(graph,
                                                      null,
                                                      KafkaConnectorAssembler.pKafkaBootstrapServers,
                                                      null).toList();
        triplesBootstrapServers.forEach(t->{
            graph.delete(t);
            graph.add(t.getSubject(), t.getPredicate(), NodeFactory.createLiteral(bootstrapServers));
        });

        FileOps.ensureDir("target/state");
        List<Triple> triplesStateFile = G.find(graph, null, KafkaConnectorAssembler.pStateFile, null).toList();
        triplesStateFile.forEach(t->{
            graph.delete(t);
            String fn = t.getObject().getLiteralLexicalForm();
            graph.add(t.getSubject(), t.getPredicate(), NodeFactory.createLiteral(STATE_DIR+"/"+fn));
        });

        return graph;
    }
}
