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

import static org.apache.jena.kafka.FusekiKafka.LOG;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletContext;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.FusekiServer.Builder;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.server.Dispatcher;
import org.apache.jena.kafka.ActionFK;
import org.apache.jena.kafka.ConnectorFK;
import org.apache.jena.kafka.DeserializerActionFK;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.kafka.common.PersistentState;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FMod_FusekiKafka implements FusekiModule {

    public FMod_FusekiKafka() {}

    @Override
    public void start() {
        Fuseki.configLog.info("Fuseki-Kafka connector module");
        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
    }

    private static final String attrNS = KafkaConnectorAssembler.getNS();
    private static final String attrConnectionFK = attrNS + "connectorFK";

    private String modName = UUID.randomUUID().toString();

    @Override
    public String name() {
        return modName;
    }

    @Override
    public void prepare(FusekiServer.Builder builder, Set<String> names, Model configModel) {
        if ( configModel == null ) {
            // FmtLog.error(LOG, "No server configuration. Can't build connector");
            return ;
        }
        List<Resource> connectors = GraphUtils.findRootsByType(configModel, KafkaConnectorAssembler.getType());
        if ( connectors.isEmpty() ) {
            // FmtLog.error(LOG, "No connector in server configuration");
            return;
        }
        connectors.forEach(c -> oneConnector(builder, c, configModel));
    }

    /*package*/ void oneConnector(FusekiServer.Builder builder, Resource connector, Model configModel) {
        ConnectorFK conn;
        try {
            conn = (ConnectorFK)Assembler.general.open(connector);
        } catch (JenaException ex) {
            FmtLog.error(LOG, "Failed to build a connector", ex);
            return;
        }

        String dispatchURI = conn.getLocalDispatchPath();
        String remoteEndpoint = conn.getRemoteEndpoint();

//        // Endpoint to dataset.
//        String datasetName = datasetName(dispatchURI);
//        DatasetGraph dsg = builder.getDataset(datasetName);
//        if ( dsg == null )
//            throw new FusekiKafkaException("No datasets for '" + conn.getLocalEndpoint() + "'");
        PersistentState state = new PersistentState(conn.getStateFile());

        DataState dataState = DataState.restoreOrCreate(state, dispatchURI, remoteEndpoint, conn.getTopic());
        long lastOffset = dataState.getOffset();
        FmtLog.info(LOG, "Initial offset for topic %s = %d (%s)", conn.getTopic(), lastOffset, dispatchURI);

        Pair<ConnectorFK, DataState> pair = Pair.create(conn, dataState);
        addServletAttribute(builder, attrConnectionFK, pair);
    }

    @SuppressWarnings("unchecked")
    private void addServletAttribute(Builder builder, String attribute, Object obj) {
        Object x = builder.getServletAttribute(attribute);
        List<Object> values;
        if ( x == null )
            values = new ArrayList<>();
        else if ( !(x instanceof List<?> ) ) {
            FmtLog.warn(LOG, "Not a list for attribute %s", attribute);
            return;
        } else {
            values = (List<Object>)x;
        }
        values.add(obj);
        builder.addServletAttribute(attribute, values);
    }

    @Override
    public void serverBeforeStarting(FusekiServer server) {
        List<Pair<ConnectorFK, DataState>> connectors = connectors(server);
        if ( connectors == null )
            return;
        connectors.forEach(pair->{
            ConnectorFK conn = pair.getLeft();
            DataState dataState = pair.getRight();
            FmtLog.info(LOG, "Starting connector between topic %s and %s", conn.getTopic(),
                        conn.dispatchLocal() ? conn.getLocalDispatchPath() : conn.getRemoteEndpoint()
                    );
            addConnectorToServer(conn, server, dataState);
        });
    }

    @Override
    public void serverStopped(FusekiServer server) {
        List<Pair<ConnectorFK, DataState>> connectors = connectors(server);
        if ( connectors == null )
            return;
        connectors.forEach(pair->{
            ConnectorFK conn = pair.getLeft();
            DataState dataState = pair.getRight();
        });
    }

    private static List<Pair<ConnectorFK, DataState>> connectors(FusekiServer server) {
        ServletContext servletContext = server.getServletContext();
        Object obj = servletContext.getAttribute(attrConnectionFK);
        @SuppressWarnings("unchecked")
        List<Pair<ConnectorFK, DataState>> connectors = (List<Pair<ConnectorFK, DataState>>)obj;
        return connectors;
    }


    static void addConnectorToServer(ConnectorFK conn, FusekiServer server, DataState dataState) {
        String localDispatchPath = conn.getLocalDispatchPath();
        // Remote not (yet) supported.
        //String remoteEndpoint = conn.getRemoteEndpoint();

        String requestURI = localDispatchPath;

        // The HttpServletRequest is created by FKRequestProcessor.dispatch.
        RequestDispatcher dispatcher = (req, resp) -> Dispatcher.dispatch(req, resp);

        // -- Kafka Props
        Properties cProps = conn.getKafkaProps();
        StringDeserializer strDeser = new StringDeserializer();

        Deserializer<ActionFK> reqDer = new DeserializerActionFK();

        // -- Kafka
        Consumer<String, ActionFK> consumer = new KafkaConsumer<>(cProps, strDeser, reqDer);
        TopicPartition topicPartition = new TopicPartition(conn.getTopic(), 0);
        Collection<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        FKRequestProcessor requestProcessor = new FKRequestProcessor(dispatcher, requestURI, server.getServletContext());

        // -- Choose start point.
        // If true, ignore topic state and start at current.
        boolean syncTopic = conn.getSyncTopic();
        boolean replayTopic = conn.getReplayTopic();

        // LAST offset processed
        long stateOffset = dataState.getOffset();
        if ( stateOffset < 0 ) {
            FmtLog.info(LOG, "Initialize from topic %s", conn.getTopic());
            // consumer.seekToBeginning(Arrays.asList(topicPartition)); BUG
            replayTopic = true;
        }

        if ( replayTopic ) {
            setupReplayTopic(consumer, topicPartition, dataState);
        } else if ( syncTopic ) {
            setupSyncTopic(consumer, topicPartition, dataState);
        } else {
            setupNoSyncTopic(consumer, topicPartition, dataState);
        }
        
        String versionString = Meta.VERSION;
        if ( conn.getLocalDispatchPath() != null )
            FmtLog.info(LOG, "Start FusekiKafka (%s) : Topic = %s : Dataset = %s", versionString,  conn.getTopic(), conn.getLocalDispatchPath());
        else
            FmtLog.info(LOG, "Start FusekiKafka (%s) : Topic = %s : Relay = %s", versionString,  conn.getTopic(), conn.getRemoteEndpoint());

        // Do now for some catchup.
        oneTopicPoll(requestProcessor, consumer, dataState, Duration.ofMillis(500));

        // ASYNC
        startTopicPoll(requestProcessor, consumer, dataState, "Kafka:" + conn.getTopic());
    }

    /** Set to catch up on the topic at the next (first) call. */
    private static void setupSyncTopic(Consumer<String, ActionFK> consumer, TopicPartition topicPartition, DataState dataState) {
        long topicPosition = consumer.position(topicPartition);
        long stateOffset = dataState.getOffset();

        FmtLog.info(LOG, "State=%d  Topic next offset=%d", stateOffset, topicPosition);
        if ( (stateOffset >= 0) && (stateOffset >= topicPosition) ) {
            FmtLog.info(LOG, "Adjust state record %d -> %d", stateOffset, topicPosition - 1);
            stateOffset = topicPosition - 1;
            dataState.setOffset(stateOffset);
        } else if ( topicPosition != stateOffset + 1 ) {
            FmtLog.info(LOG, "Set sync %d -> %d", stateOffset, topicPosition - 1);
            consumer.seek(topicPartition, stateOffset + 1);
        } else {
            FmtLog.info(LOG, "Up to date: %d -> %d", stateOffset, topicPosition - 1);
        }
    }

    /**
     * Set to jump to the front of the topic, and so not resync on the the next
     * (first) call.
     */
    private static void setupNoSyncTopic(Consumer<String, ActionFK> consumer, TopicPartition topicPartition, DataState dataState) {
        long topicPosition = consumer.position(topicPartition);
        long stateOffset = dataState.getOffset();
        FmtLog.info(LOG, "No sync: State=%d  Topic offset=%d", stateOffset, topicPosition);
        dataState.setOffset(topicPosition);
    }

    /** Set to jump to the start of the topic. */
    private static void setupReplayTopic(Consumer<String, ActionFK> consumer, TopicPartition topicPartition, DataState dataState) {
        String topic = dataState.getTopic();
        long topicPosition = consumer.position(topicPartition);
        long stateOffset = dataState.getOffset();
        FmtLog.info(LOG, "Replay: Old state=%d  Topic offset=%d", stateOffset, topicPosition);
        // Assumes offsets from 0 (no expiry)

        // Here or in FK.receiverStep
        Map<TopicPartition, Long> m = consumer.beginningOffsets(List.of(topicPartition));
        long beginning = m.get(topicPartition);
        consumer.seek(topicPartition, beginning);
        dataState.setOffset(beginning);
    }

    private static ExecutorService threads = threadExecutor();

    private static ExecutorService threadExecutor() {
        return Executors.newFixedThreadPool(1, runnable -> {
            Thread th = new Thread(runnable);
            th.setDaemon(true);
            return th;
        });
    }

    /** The background threads */
    static void resetPollThreads() {
        threads.shutdown();
        threads = threadExecutor();
    }

    private static void startTopicPoll(FKRequestProcessor requestProcessor, Consumer<String, ActionFK> consumer, DataState dataState, String label) {
        Runnable task = () -> topicPoll(requestProcessor, consumer, dataState);
        threads.submit(task);
    }

    /** Polling task loop.*/
    private static void topicPoll(FKRequestProcessor requestProcessor, Consumer<String, ActionFK> consumer, DataState dataState) {
        Duration pollingDuration = Duration.ofMillis(5000);
        for ( ;; ) {
            boolean somethingReceived = oneTopicPoll(requestProcessor, consumer, dataState, pollingDuration);
        }
    }

    /** A polling attempt either returns some records or waits the polling duration. */
    private static boolean oneTopicPoll(FKRequestProcessor requestProcessor, Consumer<String, ActionFK> consumer, DataState dataState, Duration pollingDuration) {
        long lastOffsetState = dataState.getOffset();
        boolean somethingReceived = requestProcessor.receiver(consumer, dataState, pollingDuration);
        if ( somethingReceived ) {
            long newOffset = dataState.getOffset();
            FmtLog.debug(LOG, "Offset: %d -> %d", lastOffsetState, newOffset);
        }
        return somethingReceived;
    }

    // Better? One scheduled executor.
    // Completes with Kakfa and polling duration.
//  private static List<Runnable> tasks = new CopyOnWriteArrayList<Runnable>();
//
//  private static ScheduledExecutorService threads = threadExecutor();
//
//  private static ScheduledExecutorService threadExecutor() {
//      return Executors.newScheduledThreadPool(1);
//  }
//
//  /** The background threads */
//  static void resetPollThreads() {
//      threads.shutdown();
//      threads = threadExecutor();
//  }
//
//  private static void startTopicPoll(FKRequestProcessor requestProcessor, Consumer<String, ActionFK> consumer, DataState dataState, String label) {
//      Duration pollingDuration = Duration.ofMillis(5000);
//      Runnable task = () -> oneTopicPoll(requestProcessor, consumer, dataState, pollingDuration);
//      threads.scheduleAtFixedRate(task, 500, 10000, TimeUnit.MILLISECONDS);
//  }
//
////  /** Polling task loop.*/
////  private static void topicPoll(FKRequestProcessor requestProcessor, Consumer<String, ActionFK> consumer, DataState dataState) {
////      Duration pollingDuration = Duration.ofMillis(5000);
////      for ( ;; ) {
////          boolean somethingReceived = oneTopicPoll(requestProcessor, consumer, dataState, pollingDuration);
////      }
////  }
//
//  /** A polling attempt either returns some records or waits the polling duration. */
//  private static boolean oneTopicPoll(FKRequestProcessor requestProcessor, Consumer<String, ActionFK> consumer, DataState dataState, Duration pollingDuration) {
//      long lastOffsetState = dataState.getOffset();
//      boolean somethingReceived = requestProcessor.receiver(consumer, dataState, pollingDuration);
//      if ( somethingReceived ) {
//          long newOffset = dataState.getOffset();
//          FmtLog.debug(LOG, "Offset: %d -> %d", lastOffsetState, newOffset);
//      }
//      return somethingReceived;
//  }
}
