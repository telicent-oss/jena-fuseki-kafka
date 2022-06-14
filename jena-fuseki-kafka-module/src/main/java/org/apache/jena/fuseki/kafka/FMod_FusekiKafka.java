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

import javax.servlet.ServletContext;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.main.FusekiServer;
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
    private static final String attrDataState = attrNS + "dataState";

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

        if ( connectors.size() > 1 ) {
            FmtLog.warn(LOG, "Multiple connector configurations");
            return;
        }

        connectors.forEach(c -> oneConnector(builder, c, configModel));
    }

    public void oneConnector(FusekiServer.Builder builder, Resource connector, Model configModel) {
        ConnectorFK conn;
        try {
            // conn = (ConnectorFK)AssemblerUtils.build(configModel,
            // KafkaConnectorAssembler.getType());
            conn = (ConnectorFK)Assembler.general.open(connector);
        } catch (JenaException ex) {
            FmtLog.error(LOG, "Failed to build a connector", ex);
            return;
        }

        String dispatchURI = conn.getLocalDispatchPath();
        String endpoint = conn.getRemoteEndpoint();

//        // Endpoint to dataset.
//        DatasetGraph dsg = builder.getDataset(datasetName);
//        if ( dsg == null )
//            throw new FusekiKafkaException("No datasets for '" + conn.getLocalEndpoint() + "'");
        PersistentState state = new PersistentState(conn.getStateFile());

        DataState dataState = DataState.restoreOrCreate(state, dispatchURI, endpoint, conn.getTopic());
        long lastOffset = dataState.getOffset();
        FmtLog.info(LOG, "Initial offset for topic %s = %d (%s)", conn.getTopic(), lastOffset, dispatchURI);

        // Limitation. One connector per build.
        builder.addServletAttribute(attrConnectionFK, conn);
        builder.addServletAttribute(attrDataState, dataState);
    }

    @Override
    public void serverBeforeStarting(FusekiServer server) {
        // -- Wire connector
        ServletContext servletContext = server.getServletContext();
        ConnectorFK conn = (ConnectorFK)servletContext.getAttribute(attrConnectionFK);
        if ( conn == null )
            return;
        DataState dataState = (DataState)servletContext.getAttribute(attrDataState);
        FmtLog.info(LOG, "Starting connector between topic %s and %s", conn.getTopic(),
                    conn.dispatchLocal() ? conn.getLocalDispatchPath() : conn.getRemoteEndpoint()
                );
        addConnectorToServer(conn, server, dataState);
    }

    public static void addConnectorToServer(ConnectorFK conn, FusekiServer server, DataState dataState) {
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

    private static void startTopicPoll(FKRequestProcessor requestProcessor, Consumer<String, ActionFK> consumer, DataState dataState, String label) {
        Runnable task = () -> topicPoll(requestProcessor, consumer, dataState);
        // Executor
        Thread thread = new Thread(task, label);
        // Not ideal but transactions will protect the dataset.
        thread.setDaemon(true);
        thread.start();
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
}
