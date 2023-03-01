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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.kafka.lib.FKLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.ConnectorDescriptor;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.After;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
// These tests must run in order.
public class TestFK {
    // Logging

    static { FusekiLogging.setLogging(); }
    private static final String TOPIC ="TEST";
    private static final String DSG ="/ds";
    private static MockKafka mock;
    private static String DIR = "src/test/files";

    // Logs to silence,
    private static String [] XLOGS = {
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

    @BeforeAll public static void beforeClass() {
        adjustLogs("Warning");
        Log.info("TestFK","Starting testcontainer for Kafka");
        mock = new MockKafka();
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", mock.getServer());
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", mock.getServer());
    }

    @After public void after() {
        FKS.resetPollThreads();
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
        Log.info("TestFK","Stopping testcontainer for Kafka");
        LogCtl.setLevel(NetworkClient.class, "error");
        mock.stop();
        adjustLogs("info");
    }

    @Test public void fk01_topic() {
        mock.createTopic(TOPIC);
        Collection<String> topics = mock.listTopics();
        assertTrue(topics.contains(TOPIC));
    }

    @Test public void fk02_send() {
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR+"/data.ttl",DIR+"/update.ru"));
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR+"/data-nq"));
    }

    @Test public void fk03_receive() {
        Properties cProps = consumerProps();
        DataState dataState = DataState.createEphemeral(TOPIC);
        AtomicInteger count = new AtomicInteger(0);
        BiConsumer<ConsumerRecord<String, String>, Long> handler = ( crec, offset) -> count.incrementAndGet();
        FKLib.receive(dataState, cProps, TOPIC, handler);
        assertEquals(3, count.get());
    }

    @Test public void fk04_fuseki() {
        DataState dataState = DataState.createEphemeral(TOPIC);
        FusekiServer server = startFuseki(dataState, consumerProps());
        String URL = "http://localhost:"+server.getHttpPort()+DSG;
        RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
        int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        assertEquals(2, count);
    }

    private static FusekiServer startFuseki(DataState dataState, Properties consumerProps) {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        // Automatic
        //FusekiModules.add(new FMod_FusekiKafka());
        FusekiServer server = FusekiServer.create()
                .port(0)
                //.verbose(true)
                .add(DSG,  dsg)
                .build();
        ConnectorDescriptor conn = new ConnectorDescriptor(TOPIC, null/*bootStrapservers*/, DSG, null/*remoteEndpoint*/, null/*stateFile*/,
                                                           false/*syncTopic*/, true/*replayTopic*/,
                                                           consumerProps,
                                                           false, (x)->System.out);
        // Manual call to setup the server.
        FKS.addConnectorToServer(conn, server, dataState);
        server.start();
        return server;
    }
}
