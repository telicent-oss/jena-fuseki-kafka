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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.FusekiServer.Builder;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.kafka.ConnectorDescriptor;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.kafka.common.PersistentState;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.util.graph.GraphUtils;

public class FMod_FusekiKafka implements FusekiModule {

    public FMod_FusekiKafka() {}

    @Override
    public void start() {
        Fuseki.configLog.info("Fuseki-Kafka connector module");
        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
    }

    // The Fuseki modules build lifecycle is same-thread.
    // This passes information from 'prepare' to 'serverBeforeStarting'

    private ThreadLocal<List<Pair<ConnectorDescriptor, DataState>>> buildState = ThreadLocal.withInitial(()->new ArrayList<>());
    //private Map<>

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
        ConnectorDescriptor conn;
        try {
            conn = (ConnectorDescriptor)Assembler.general.open(connector);
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
        recordConnector(builder, conn, dataState);
    }

    // Passing connector across stages by using a ThreadLocal.
    private void recordConnector(Builder builder, ConnectorDescriptor conn, DataState dataState) {
        Pair<ConnectorDescriptor, DataState> pair = Pair.create(conn, dataState);
        buildState.get().add(pair);
        FKRegistry.get().register(conn.getTopic(), conn);
    }

    private List<Pair<ConnectorDescriptor, DataState>> connectors(FusekiServer server) {
        return buildState.get();
    }

    @Override
    public void serverBeforeStarting(FusekiServer server) {
        List<Pair<ConnectorDescriptor, DataState>> connectors = connectors(server);
        if ( connectors == null )
            return;
        connectors.forEach(pair->{
            ConnectorDescriptor conn = pair.getLeft();
            DataState dataState = pair.getRight();
            FmtLog.info(LOG, "Starting connector between %s topic %s and %s", conn.getBootstrapServers(), conn.getTopic(),
                        conn.dispatchLocal() ? conn.getLocalDispatchPath() : conn.getRemoteEndpoint());
            FKS.addConnectorToServer(conn, server, dataState);
        });
    }

    @Override
    public void serverAfterStarting(FusekiServer server) {
        // Clear up thread local.
        buildState.remove();
    }

    @Override
    public void serverStopped(FusekiServer server) {
        List<Pair<ConnectorDescriptor, DataState>> connectors = connectors(server);
        if ( connectors == null )
            return;
        connectors.forEach(pair->{
            ConnectorDescriptor conn = pair.getLeft();
            DataState dataState = pair.getRight();
            FKRegistry.get().unregister(conn.getTopic());
        });
    }
}
