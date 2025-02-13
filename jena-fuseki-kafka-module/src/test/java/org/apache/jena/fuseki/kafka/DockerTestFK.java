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
import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.awaitility.Awaitility;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.kafka.lib.FKLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

// These tests must run in order.
public class DockerTestFK {
    // Logging

    static {
        JenaSystem.init();
        FusekiLogging.setLogging();
    }
    private static final String TOPIC ="TEST";
    private static final String DSG_NAME ="/ds";
    private static final DatasetGraph DSG = DatasetGraphFactory.createTxnMem();
    /**
     * Intentionally protected so derived test classes can inject alternative Kafka cluster implementations for testing
     */
    protected KafkaTestCluster kafka = new BasicKafkaTestCluster();

    // Logs to silence,
    private static final String [] XLOGS = {
        AdminClientConfig.class.getName() ,
        // NetworkClient is noisy with warnings about can't connect.
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

    @BeforeClass
    public void beforeClass() {
        adjustLogs("Warning");
        Log.info("TestFK","Starting testcontainer for Kafka");
        kafka.setup();
    }

    @AfterMethod
    public void after() {
        FKS.resetPollThreads();
    }

    Properties consumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        consumerProps.putAll(kafka.getClientProperties());
        return consumerProps;
    }

    Properties producerProps() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProps.putAll(kafka.getClientProperties());
        return producerProps;
    }

    @AfterClass
    public void afterClass() {
        Log.info("TestFK","Stopping testcontainer for Kafka");
        LogCtl.setLevel(NetworkClient.class, "error");
        kafka.teardown();
        adjustLogs("info");
    }

    @Test(priority = 1)
    public void fk01_topic() throws ExecutionException, InterruptedException {
        kafka.resetTopic(TOPIC);
        Collection<String> topics = kafka.getAdminClient().listTopics().names().get();
        Assert.assertTrue(topics.contains(TOPIC));
    }

    @Test(priority = 2)
    public void fk02_send() {
        String DIR = "src/test/files";
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR +"/data.ttl", DIR +"/update.ru"));
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR +"/data-nq"));
    }

    @Test(priority = 3)
    public void fk03_receive() {
        Properties cProps = consumerProps();
        DataState dataState = DataState.createEphemeral(TOPIC);
        AtomicInteger count = new AtomicInteger(0);
        BiConsumer<ConsumerRecord<String, String>, Long> handler = ( crec, offset) -> count.incrementAndGet();
        FKLib.receive(dataState, cProps, TOPIC, handler);
        Assert.assertEquals(count.get(), 3);
    }

    @Test(priority = 4) public void fk04_fuseki() {
        // Assumes the topic exists and has data.
        DataState dataState = DataState.createEphemeral(TOPIC);
        FusekiServer server = startFuseki(dataState, consumerProps());
        String URL = "http://localhost:"+server.getHttpPort()+ DSG_NAME;
        RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
        int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        Assert.assertEquals(count, 2);
    }

    @Test(priority = 5) public void fk05_restore() {
        // Assumes the topic exists and has data.
        String countQuery = "SELECT (count(*) AS ?C) {?s ?p ?o}";
        DataState dataState = DataState.createEphemeral(TOPIC);
        FusekiServer server = startFuseki(dataState, consumerProps());
        String URL = "http://localhost:"+server.getHttpPort()+ DSG_NAME;
        DSG.clear();
        RowSet rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
        int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        Assert.assertEquals(count, 0);
        FKS.restoreOffsetForDataset("", 0L);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(FKS.restoreOffsetMap::isEmpty);
        rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
        count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        Assert.assertEquals(count, 2);
    }

    @Test(priority = 6) public void fk06_restore_ignore() {
        // Assumes the topic exists and has data.
        FKS.restoreOffsetForDataset("ignore", 1L);
        String countQuery = "SELECT (count(*) AS ?C) {?s ?p ?o}";
        DataState dataState = DataState.createEphemeral(TOPIC);
        FusekiServer server = startFuseki(dataState, consumerProps());
        String URL = "http://localhost:"+server.getHttpPort()+ DSG_NAME;
        DSG.clear();
        RowSet rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
        int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        Assert.assertEquals(count, 0);
        FKS.restoreOffsetMap.clear();
        FKS.restoreOffsetForDataset("", 2L);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(FKS.restoreOffsetMap::isEmpty);
        rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
        count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        Assert.assertEquals(count, 0);
    }


    @Test(priority = 7) public void fk07_restore_beginning() {
        // Assumes the topic exists and has data.
        String countQuery = "SELECT (count(*) AS ?C) {?s ?p ?o}";
        DataState dataState = DataState.createEphemeral(TOPIC);
        FusekiServer server = startFuseki(dataState, consumerProps());
        String URL = "http://localhost:"+server.getHttpPort()+ DSG_NAME;
        DSG.clear();
        RowSet rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
        int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        Assert.assertEquals(count, 0);
        FKS.restoreOffsetForDataset("", -5L);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(FKS.restoreOffsetMap::isEmpty);
        rowSet = QueryExecHTTP.service(URL).query(countQuery).select();
        count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        Assert.assertEquals(count, 2);
    }


    private static FusekiServer startFuseki(DataState dataState, Properties consumerProps) {
        // Automatic
        //FusekiModules.add(new FMod_FusekiKafka());
        FusekiServer server = FusekiServer.create()
                .port(0)
                //.verbose(true)
                .add(DSG_NAME,  DSG)
                .build();
        KConnectorDesc conn = new KConnectorDesc(TOPIC, null, DSG_NAME, null, null,
                                                           false, true,
                                                           consumerProps);
        // Manual call to setup the server.
        FKBatchProcessor batchProcessor = FKS.plainFKBatchProcessor(conn, server.getServletContext());
        FKS.addConnectorToServer(conn, server, dataState, batchProcessor);
        server.start();
        return server;
    }
}
