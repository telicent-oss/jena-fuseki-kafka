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

package org.apache.jena.fuseki.kafka.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.kafka.FMod_FusekiKafka;
import org.apache.jena.fuseki.kafka.lib.FKLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.ConnectorFK;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
// These tests must run in order.
public class TestFK {
    // Logging

    static { FusekiLogging.setLogging(); }
    private static final String TOPIC ="TEST";
    private static final String DSG ="/ds";
    private static final int PORT = 4040;
    private static MockKafka mock;
    private static String DIR = "src/test/files";

    @BeforeAll public static void beforeClass() {
        Log.info("TestFK","Starting testcontainer for Kafka");
        mock = new MockKafka();
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", mock.getServer());
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", mock.getServer());
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
        LogCtl.setLevel(NetworkClient.class, "error");
        LogCtl.setLevel(FetchSessionHandler.class, "error");
        Log.info("TestFK","Stopping testcontainer for Kafka");
        mock.stop();
        LogCtl.setLevel(FetchSessionHandler.class, "info");
        LogCtl.setLevel(NetworkClient.class, "info");
    }

    @Test public void fk01_topic() {
        mock.createTopic(TOPIC);
        Collection<String> topics = mock.listTopics();
        assertTrue(topics.contains(TOPIC));
    }

    @Test public void fk02_send() {
        FKLib.send(producerProps(), TOPIC, List.of(DIR+"/data.ttl",DIR+"/update.ru"));
        FKLib.send(producerProps(), TOPIC, List.of(DIR+"/data-nq"));
    }

    @Test public void fk03_fuseki() {
        DataState dataState = DataState.createEphemeral(TOPIC);
        FusekiServer server = startFuseki(dataState, consumerProps());
        String URL = "http://localhost:"+server.getHttpPort()+DSG;
        RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
        int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
        assertEquals(2, count);
    }

    @Test public void fk04_receive() {
        Properties cProps = consumerProps();
        DataState dataState = DataState.createEphemeral(TOPIC);
        AtomicInteger count = new AtomicInteger(0);
        BiConsumer<ConsumerRecord<String, String>, Long> handler = ( crec, offset) -> count.incrementAndGet();
        FKLib.receive(dataState, cProps, TOPIC, handler);
        assertEquals(3, count.get());
    }

    //@Test public void fk05_fuseki_config_connector() {}
    //   Need to figure out how to setup and pick up the Kafka broker port.
    //     ServletAttributes.
    //   Maybe have a form of FusekiModule that calls out for Kafka setup and DataState.

    private static FusekiServer startFuseki(DataState dataState, Properties consumerProps) {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        // Automatic; but relying on a configuration file to find the connector description.
        // ServletContext?
        //FusekiModules.add(new FMod_FusekiKafka());
        FusekiServer server = FusekiServer.create()
                .port(0)
                //.verbose(true)
                .add(DSG,  dsg)
                .build();
        ConnectorFK conn = new ConnectorFK(TOPIC, DSG, null/*remoteEndpoint*/, null/*stateFile*/,
                                           false/*syncTopic*/, true/*replayTopic*/,
                                           consumerProps);
        // Manual call to setup the server.
        FMod_FusekiKafka.addConnectorToServer(conn, server, dataState);
        server.start();
        return server;
    }

//    public static String  ctForFile(String fn) {
//        String ct = null;
//        if ( ct == null ) {
//            String ext = FileUtils.getFilenameExt(fn);
//            if ( Lib.equals("ru", ext) )
//                ct = WebContent.contentTypeSPARQLUpdate;
//            else {
//                Lang lang = RDFLanguages.filenameToLang(fn);
//                if ( lang != null )
//                    ct = lang.getContentType().getContentTypeStr();
//            }
//        }
//        return ct;
//    }
//
//    // Send files
//    public static void send(MockKafka mock, Properties props, String...files) {
//        try ( StringSerializer serString1 = new StringSerializer();
//              StringSerializer serString2 = new StringSerializer();
//              Producer<String, String> producer = new KafkaProducer<>(props, serString1, serString2)) {
//
//            for ( String fn : files ) {
//                RecordMetadata res = sendFile(producer, null, TOPIC, fn);
//                if ( res == null )
//                    System.out.println("Error");
//                else if ( ! res.hasOffset() )
//                    System.out.println("No offset");
//                else
//                    System.out.println("Send: Offset = "+res.offset());
//            }
//        }
//    }
//
//    private static RecordMetadata sendFile(Producer<String, String> producer, Integer partition, String topic, String fn) {
//        String ct = ctForFile(fn);
//        String body = IO.readWholeFileAsUTF8(fn);
//        List<Header> headers = ( ct != null ) ? List.of(header(HttpNames.hContentType, ct)) : List.of();
//        RecordMetadata res = sendBody(producer, partition, topic, headers, body);
//        return res;
//    }
//
//
//    private static RecordMetadata sendBody(Producer<String, String> producer, Integer partition, String topic, List<Header> headers, String body) {
//        try {
//            ProducerRecord<String, String> pRec = new ProducerRecord<>(topic, partition, null, null, body, headers);
//            Future<RecordMetadata> f = producer.send(pRec);
//            RecordMetadata res = f.get();
//            return res;
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    static Header header(String key, String value) {
//        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
//    }
}
