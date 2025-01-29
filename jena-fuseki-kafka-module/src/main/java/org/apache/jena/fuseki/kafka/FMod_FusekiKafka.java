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

import static org.apache.jena.kafka.FusekiKafka.LOG;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.FusekiServer.Builder;
import org.apache.jena.fuseki.main.sys.FusekiAutoModule;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.kafka.common.FusekiOffsetStore;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * Connect Kafka to a dataset. Messages on a Kafka topic are HTTP-like: updates (add data), SPARQL Update or RDF patch.
 */
public class FMod_FusekiKafka implements FusekiAutoModule {

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

    private static void init() {
        JenaSystem.init();

        boolean b = INITIALIZED.getAndSet(true);
        if (b)
        // Already done.
        {
            return;
        }
        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
    }

    public FMod_FusekiKafka() {
    }

    // The Fuseki modules build lifecycle is same-thread.
    // This passes information from 'prepare' to 'server'
    private final ThreadLocal<List<Pair<KConnectorDesc, FusekiOffsetStore>>> buildState =
            ThreadLocal.withInitial(ArrayList::new);

    @Override
    public String name() {
        return "FMod FusekiKafka";
    }

    protected String logMessage() {
        return String.format("Fuseki-Kafka Connector Module (%s)", SysJenaKafka.VERSION);
    }

    @Override
    public void prepare(FusekiServer.Builder builder, Set<String> names, Model configModel) {
        init();
        Log.info(Fuseki.configLog, logMessage());

        if (configModel == null) {
            FmtLog.warn(LOG, "No server configuration. Can't build Kafka connector(s) automatically!");
            return;
        }
        List<Resource> connectors = GraphUtils.findRootsByType(configModel, KafkaConnectorAssembler.getType());
        if (connectors.isEmpty()) {
            FmtLog.warn(LOG, "No Kafka connector(s) found in provided server configuration!");
            return;
        }
        connectors.forEach(connector -> oneConnector(builder, connector));
    }

    /*package*/ void oneConnector(Builder builder, Resource connector) {
        KConnectorDesc conn;
        try {
            conn = (KConnectorDesc) Assembler.general.open(connector);
        } catch (JenaException ex) {
            FmtLog.error(LOG, "Failed to build a connector", ex);
            return;
        }

        String datasetName = conn.getDatasetName();
        FusekiOffsetStore offsets = FusekiOffsetStore.builder()
                                                     .datasetName(datasetName)
                                                     .stateFile(conn.getStateFile() != null ?
                                                                new File(conn.getStateFile()) : null)
                                                     .consumerGroup(conn.getKafkaConsumerProps()
                                                                        .getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                                                     .build();
        for (Map.Entry<String, Object> offset : offsets.offsets()) {
            FmtLog.info(LOG, "Initial offset for topic partition %s = %s (%s)", offset.getKey(), offset.getValue(),
                        datasetName);
        }
        recordConnector(conn, offsets);
    }

    // Passing connector across stages by using a ThreadLocal.
    private void recordConnector(KConnectorDesc conn, FusekiOffsetStore offsets) {
        Pair<KConnectorDesc, FusekiOffsetStore> pair = Pair.create(conn, offsets);
        buildState.get().add(pair);
        FKRegistry.get().register(conn.getTopics(), conn);
    }

    private List<Pair<KConnectorDesc, FusekiOffsetStore>> connectors(FusekiServer server) {
        return buildState.get();
    }

    /**
     * Add each connector, with the state tracker, to the server configured server.
     * <p>
     * See notes on {@link #startKafkaConnectors(FusekiServer)} about potential pitfalls of starting connectors at this
     * point in the server lifecycle.  Derived modules <strong>MAY</strong> wish to override this method to not start
     * the connectors at this stage and override {@link #serverAfterStarting(FusekiServer)}, or another lifecycle method
     * instead.
     * </p>
     */
    @Override
    public void serverBeforeStarting(FusekiServer server) {
        // server(FusekiServer server) -- after build, before returning to builder caller
        // See also serverAfterStarting which is an even later delayed setup point.
        startKafkaConnectors(server);
    }

    /**
     * Starts the Kafka Connectors
     * <p>
     * This needs to be called once, and only once, by this module.  By default, this gets called from the
     * {@link #serverBeforeStarting(FusekiServer)} method, this means that Fuseki will guarantee it is up to date on all
     * topics before it starts servicing any requests.
     * </p>
     * <p>
     * Note that if the server is lagging far behind the Kafka topic(s) it is being subscribed to then it will take a
     * long time before Fuseki is able to service requests.  This may be problematic if you are deployed this in an
     * environment that relies on HTTP Health Probes as the HTTP Server is not started while Fuseki is catching up on
     * the Kafka topics.  This can result in the service being placed into a Crash Restart Loop as it fails health
     * checks and is restarted.
     * </p>
     * <p>
     * Also bear in mind that if the producers writing data to the Kafka topic(s) that Fuseki is consuming are doing so
     * at a rate faster than Fuseki can process those updates the service can potentially never reach a ready state.
     * </p>
     * <p>
     * Therefore, derived implementations <strong>MAY</strong> prefer to call this at a different point in the lifecycle
     * e.g. {@link #serverAfterStarting(FusekiServer)} and live with the graph being eventually consistent
     * </p>
     *
     * @param server Fuseki Server
     */
    protected void startKafkaConnectors(FusekiServer server) {
        List<Pair<KConnectorDesc, FusekiOffsetStore>> connectors = connectors(server);
        if (connectors == null) {
            return;
        }
        connectors.forEach(pair -> {
            KConnectorDesc conn = pair.getLeft();
            FusekiOffsetStore offsets = pair.getRight();
            FmtLog.info(LOG, "[%s] Starting connector between topic(s) %s on %s and endpoint %s",
                        StringUtils.join(conn.getTopics(), ", "), conn.getBootstrapServers(), conn.getDatasetName());
            FKS.addConnectorToServer(conn, server, offsets);
        });
    }

    @Override
    public void serverStopped(FusekiServer server) {
        List<Pair<KConnectorDesc, FusekiOffsetStore>> connectors = connectors(server);
        if (connectors == null) {
            return;
        }
        connectors.forEach(pair -> {
            KConnectorDesc conn = pair.getLeft();
            FKRegistry.get().unregister(conn.getTopics());
        });

        // Reset the poll threads otherwise they would continue running infinitely
        // On normal server shutdown that might be fine, in a test/dev environment this is undesirable
        FKS.resetPollThreads();
    }
}
