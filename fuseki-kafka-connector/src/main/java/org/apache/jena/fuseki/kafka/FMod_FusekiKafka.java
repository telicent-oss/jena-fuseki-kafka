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

import static org.apache.jena.fuseki.kafka.FusekiKafka.LOG;

import java.util.*;

import javax.servlet.ServletContext;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.Dispatcher;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FMod_FusekiKafka implements FusekiModule {

    @Override
    public void start() {
        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
    }

    private static final String attrNS = KafkaConnectorAssembler.getNS();
    private static final String attrConnectionFK = attrNS+"connectorFK";
    private static final String attrDataState = attrNS+"dataState";

    private String modName = UUID.randomUUID().toString();

    @Override
    public String name() {
        return modName;
    }

    @Override
    public void configuration(FusekiServer.Builder builder,
                              DataAccessPointRegistry dapRegistry,
                              Model configModel) {
        List<Resource> connectors = GraphUtils.findRootsByType(configModel, KafkaConnectorAssembler.getType()) ;
        if ( connectors.isEmpty() ) {
            FmtLog.error(LOG, "No connector in server configuration");
            return;
        }

        if ( connectors.size() > 1 ) {
            FmtLog.warn(LOG, "Multiple connector configurations");
            return;
        }

        ConnectorFK conn;
        try {
            conn = (ConnectorFK)AssemblerUtils.build(configModel, KafkaConnectorAssembler.getType());
        } catch (JenaException ex) {
            FmtLog.error(LOG, "Failed to build a connector", ex);
            return;
        }

        DatasetGraph dsg = dapRegistry.get(conn.getService()).getDataService().getDataset();
        if ( dsg == null )
            throw new FusekiKafkaException("No datsets for '"+conn.getService()+"'");
        PersistentState state = new PersistentState(conn.getStateFile());
        DataState dataState = new DataState(state, conn.getService(), conn.getTopic());
        long lastOffset = dataState.getOffset();
        FmtLog.info(LOG, "Initial offset for topic %s = %d", conn.getTopic(), lastOffset);

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
            return ;
        FmtLog.info(LOG, "Starting connector between topic %s and service %s", conn.getTopic(), conn.getService());
        DataState dataState = (DataState)servletContext.getAttribute(attrDataState);
        DatasetGraph dsg =
                server.getDataAccessPointRegistry().get(conn.getService()).getDataService().getDataset();

        RequestDispatcher dispatcher = (req, resp) -> Dispatcher.dispatch(req, resp);

        // -- Kafka Props
        Properties cProps = conn.getKafkaProps();
        StringDeserializer strDeser = new StringDeserializer();

        // Do work in deserializer or return a function that is the handler.
        // -- Via Fuseki dispatch
        Deserializer<Void> reqDer = new DeserializerDispatch(dispatcher, conn.getService(), servletContext);
        // -- Direct
        //Deserializer<Void> reqDer = new DeserializerAction(dsg);

        // -- Kafka
        Consumer<String, Void> consumer = new KafkaConsumer<String, Void>(cProps, strDeser, reqDer);
        TopicPartition topicPartition = new TopicPartition(conn.getTopic(), 0);
        Collection<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);

        // -- Choose start point.

        // LAST offset processed
        long stateOffset = dataState.getOffset();
        if ( stateOffset < 0 ) {
            FmtLog.info(LOG, "Initialize from topic %s", conn.getTopic());
            consumer.seekToBeginning(Arrays.asList(topicPartition));
        } else {}


        // Offset of NEXT record to be read.
        long topicPosition = consumer.position(topicPartition);

        FmtLog.info(LOG, "State=%d  Next offset=%d", stateOffset, topicPosition);
        if ( (stateOffset >= 0) && (stateOffset >= topicPosition) ) {
            FmtLog.info(LOG, "Adjust state record %d -> %d", stateOffset, topicPosition-1);
            dataState.setOffset(topicPosition-1);
        }

        // ASYNC
        startTopicPoll(consumer, dataState, "Kafka:"+conn.getTopic());
    }

    private void startTopicPoll(Consumer<String, Void> consumer, DataState dataState, String label) {
        Runnable task = ()->topicPoll(consumer, dataState);
        // Executor
        Thread thread = new Thread(task, label);
        // Not ideal but transactions will protect the dataset.
        thread.setDaemon(true);
        thread.start();
    }

    private static void topicPoll(Consumer<String, Void> consumer, DataState dataState) {
        for(;;) {
            long lastOffsetState = dataState.getOffset();
            boolean somethingReceived = FK.receiver(consumer, dataState);
            if ( somethingReceived ) {
                long newOffset = dataState.getOffset();
                FmtLog.debug(LOG, "Offset: %d -> %d", lastOffsetState, newOffset);
            }
        }
    }
}
