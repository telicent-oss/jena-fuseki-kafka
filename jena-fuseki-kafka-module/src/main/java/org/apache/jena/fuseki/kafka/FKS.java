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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.driver.ProjectorDriver;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.kafka.policies.KafkaReadPolicies;
import io.telicent.smart.cache.sources.kafka.policies.KafkaReadPolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.*;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.common.FusekiOffsetStore;
import org.apache.jena.kafka.common.FusekiProjector;
import org.apache.jena.kafka.common.FusekiSink;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;

import io.telicent.smart.cache.sources.kafka.KafkaRdfPayloadSource;
import org.apache.kafka.common.utils.Bytes;

/**
 * Functions for Fuseki-Kafka server setup.
 */
public class FKS {

    /**
     * Map for communicating the restoration of kafka offsets.
     */
    static ConcurrentHashMap<String, Long> restoreOffsetMap = new ConcurrentHashMap<>();

    /**
     * Add a connector to a server and starts the polling.
     * <p>
     * This is mainly called from {@link FMod_FusekiKafka#startKafkaConnectors(FusekiServer)}.  Public static visibility
     * is primarily to allow testing, or for developers to configure and control Kafka connectors in other ways.
     * </p>
     */
    public static void addConnectorToServer(KConnectorDesc conn, FusekiServer server, FusekiOffsetStore offsets) {
        String topicNames = StringUtils.join(conn.getTopics(), ", ");

        // NOTES
        //
        // To replicate a database with a mix of add and deletes, we need to see all the Kafka messages in-order,
        // which forces us to have only one partition.
        // However, for additive only databases we can have as many partitions as we want as the set semantics of RDF
        // means the graph will be eventually consistent
        // Smart Cache Core Libraries handle all the seeking logic based on the configured sync and replay flags, and
        // the offsets store (Fuseki Kafka's persistent state store)
        //
        // Therefore we leave the choice of partition setup to the deployment, trusting that the system operator
        // understands the implications of their choice and has chosen appropriately for their use case.

        // -- Choose start point.
        // If replay is true ignore topic state and start at current.
        // If sync is true continue from previous offsets.
        // If neither is true continue from latest offsets
        KafkaReadPolicy<Bytes, RdfPayload> readPolicy = conn.isReplayTopic() ? KafkaReadPolicies.fromBeginning() :
                                                        (conn.isSyncTopic() ?
                                                         KafkaReadPolicies.fromExternalOffsets(offsets, 0) :
                                                         KafkaReadPolicies.fromLatest());
        FmtLog.info(LOG, "Selected read policy for topics %s (replay: %s, sync: %s) is %s", topicNames,
                    conn.isReplayTopic(), conn.isSyncTopic(), readPolicy.getClass().getCanonicalName());

        // -- Kafka Event Source
        KafkaRdfPayloadSource<Bytes> source = KafkaRdfPayloadSource.<Bytes>createRdfPayload()
                                                                   .bootstrapServers(conn.getBootstrapServers())
                                                                   .topics(conn.getTopics())
                                                                   .externalOffsetStore(offsets)
                                                                   .readPolicy(readPolicy)
                                                                   .autoCommit(false)
                                                                   .consumerGroup(conn.getKafkaConsumerProps()
                                                                                      .getProperty(
                                                                                              ConsumerConfig.GROUP_ID_CONFIG))
                                                                   .consumerConfig(conn.getKafkaConsumerProps())
                                                                   .keyDeserializer(BytesDeserializer.class)
                                                                   .build();

        if (conn.getDatasetName() != null) {
            FmtLog.info(LOG, "[%s] Start FusekiKafka : Topic(s) = %s : Dataset = %s", topicNames, topicNames,
                        conn.getDatasetName());
        }

        // ASYNC
        DatasetGraph dsg = findDataset(server, conn.getDatasetName()).orElse(null);
        startTopicPoll(conn, source, dsg);
    }

    /**
     * Helper to find the database ({@link DatasetGraph}) associated with a URL path. Returns an {@code Optional} for
     * the {@link DatasetGraph} to indicate if it was found or not.
     */
    public static Optional<DatasetGraph> findDataset(FusekiServer server, String uriPath) {
        DataService dataService = findDataService(server, uriPath);
        if (dataService == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(dataService.getDataset());
    }

    private static DataService findDataService(FusekiServer server, String datasetName) {
        DataAccessPointRegistry dapRegistry = server.getDataAccessPointRegistry();
        datasetName = DataAccessPoint.canonical(datasetName);
        DataAccessPoint dap = dapRegistry.get(datasetName);
        if (dap == null) {
            return null;
        }
        return dap.getDataService();
    }

    private static ExecutorService EXECUTOR = threadExecutor();

    private static ExecutorService threadExecutor() {
        return Executors.newCachedThreadPool();
    }

    private static final List<ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>>> DRIVERS = new ArrayList<>();

    /**
     * The background threads
     */
    static void resetPollThreads() {
        // Explicitly cancel the projector drivers
        for (ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver : DRIVERS) {
            driver.cancel();
        }
        DRIVERS.clear();

        // Shutdown the executor now, this will issue interrupts on the spawned threads which should cause them to
        // shut down more promptly.  Once that is done wait a few seconds to give those threads chance to finish before
        // creating a fresh executor.
        EXECUTOR.shutdownNow();
        try {
            EXECUTOR.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Ignored
        }
        EXECUTOR = threadExecutor();
    }

    private static void startTopicPoll(KConnectorDesc connector, KafkaRdfPayloadSource<Bytes> source,
                                       DatasetGraph destination) {

        //@formatter:off
        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver =
                ProjectorDriver.<Bytes, RdfPayload, Event<Bytes, RdfPayload>>create()
                               .pollTimeout(FKConst.pollingWaitDuration)
                               .unlimited()
                               .maxStalls(6)
                               .reportBatchSize(10_000)
                               .source(source)
                               .projector(FusekiProjector.builder()
                                                         .source(source)
                                                         .dataset(destination)
                                                         .connector(connector)
                                                         .build())
                               .destination(FusekiSink.builder()
                                                      .dataset(destination)
                                                      .build())
                               .build();
        //@formatter:on

        // Submit for execution, and register for cancellation
        EXECUTOR.submit(driver);
        DRIVERS.add(driver);

        // Wait briefly for the projector driver thread to spin up
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignored
        }
    }

    /**
     * Updates map for dataset with offset to restore from
     *
     * @param datasetName relevant dataset
     * @param offset      desired offset
     */
    public static void restoreOffsetForDataset(String datasetName, Long offset) {
        restoreOffsetMap.put(datasetName, offset);
    }
}
