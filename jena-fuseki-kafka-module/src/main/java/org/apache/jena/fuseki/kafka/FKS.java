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
import java.util.concurrent.*;
import java.util.function.Function;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.projectors.driver.ProjectorDriver;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import io.telicent.smart.cache.sources.kafka.policies.KafkaReadPolicies;
import io.telicent.smart.cache.sources.kafka.policies.KafkaReadPolicy;
import io.telicent.smart.cache.sources.kafka.serializers.RdfPayloadSerializer;
import io.telicent.smart.cache.sources.kafka.sinks.KafkaSink;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.*;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.common.FusekiOffsetStore;
import org.apache.jena.kafka.common.FusekiProjector;
import org.apache.jena.kafka.common.FusekiSink;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;

import io.telicent.smart.cache.sources.kafka.KafkaRdfPayloadSource;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;

/**
 * Functions for Fuseki-Kafka server setup.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FKS {

    /**
     * Add a connector to a server and starts the polling.
     * <p>
     * This is mainly called from {@link FMod_FusekiKafka#startKafkaConnectors(FusekiServer)}.  Public static visibility
     * is primarily to allow testing, or for developers to configure and control Kafka connectors in other ways.
     * </p>
     *
     * @param conn        The Kafka connector descriptor
     * @param server      The Fuseki server the connector is being added to
     * @param offsets     The Fuseki Kafka offsets store
     * @param sinkBuilder Optional builder function that customises the sink that is used to actually apply changes to
     *                    the target dataset.  By default, this will build a {@link FusekiSink} if not otherwise
     *                    specified.  This <strong>SHOULD</strong> only be configured if you need to customise how
     *                    incoming RDF payloads are applied to the target dataset.
     */
    public static void addConnectorToServer(KConnectorDesc conn, FusekiServer server, FusekiOffsetStore offsets,
                                            Function<DatasetGraph, Sink<Event<Bytes, RdfPayload>>> sinkBuilder) {
        Objects.requireNonNull(conn);
        Objects.requireNonNull(server);
        Objects.requireNonNull(offsets);

        String topicNames = StringUtils.join(conn.getTopics(), ", ");

        // NOTES
        //
        // To replicate a database with a mix of add and deletes, we need to see all the Kafka messages in-order,
        // which forces us to have only one partition.
        // However, for additive only databases we can have as many partitions as we want as the set semantics of RDF
        // means the graph will be eventually consistent.
        // Smart Cache Core Libraries handle all the seeking logic based on the configured sync and replay flags, and
        // the offsets store (Fuseki Kafka's persistent state store)
        //
        // Therefore we leave the choice of partition setup to the deployment, trusting that the system operator
        // understands the implications of their choice and has chosen appropriately for their use case.

        // -- Choose start point.
        // If replay is true ignore topic state and start at beginning.
        // If sync is true continue from previous offsets.
        // If neither is true continue from latest offsets
        KafkaReadPolicy<Bytes, RdfPayload> readPolicy = conn.isReplayTopic() ? KafkaReadPolicies.fromBeginning() :
                                                        (conn.isSyncTopic() ?
                                                         KafkaReadPolicies.fromExternalOffsets(offsets, 0) :
                                                         KafkaReadPolicies.fromLatest());
        FmtLog.info(LOG, "[%s] Selected read policy (replay: %s, sync: %s) is %s", topicNames, conn.isReplayTopic(),
                    conn.isSyncTopic(), readPolicy.getClass().getSimpleName());

        // -- Kafka Event Source
        KafkaRdfPayloadSource<Bytes> source = KafkaRdfPayloadSource.<Bytes>createRdfPayload()
                                                                   .bootstrapServers(conn.getBootstrapServers())
                                                                   .topics(conn.getTopics())
                                                                   .externalOffsetStore(offsets)
                                                                   .readPolicy(readPolicy)
                                                                   .commitOnProcessed()
                                                                   .consumerGroup(conn.getConsumerGroupId())
                                                                   .maxPollRecords(conn.getMaxPollRecords())
                                                                   .consumerConfig(conn.getKafkaConsumerProps())
                                                                   .keyDeserializer(BytesDeserializer.class)
                                                                   .build();
        FmtLog.info(LOG, "[%s] Start FusekiKafka : Topic(s) = %s : Dataset = %s", topicNames, topicNames,
                    conn.getDatasetName());

        // ASYNC
        DatasetGraph dsg = findDataset(server, conn.getDatasetName()).orElse(null);
        if (dsg == null) {
            throw new FusekiKafkaException("No dataset found for dataset name '" + conn.getDatasetName() + "', this MUST be the base path to a dataset in your configuration");
        }
        startTopicPoll(conn, source, dsg, sinkBuilder != null ? sinkBuilder : FKS.defaultSinkBuilder());
    }

    /**
     * Helper to find the database ({@link DatasetGraph}) associated with a URL path. Returns an {@code Optional} for
     * the {@link DatasetGraph} to indicate if it was found or not.
     *
     * @param server  Fuseki Server instance
     * @param uriPath Dataset path
     */
    public static Optional<DatasetGraph> findDataset(FusekiServer server, String uriPath) {
        DataService dataService = findDataService(server, uriPath);
        if (dataService == null) {
            if (uriPath.lastIndexOf('/') > 0) {
                // Strip off the trailing path component and try again
                LOG.warn(
                        "Configured URI Path {} for Kafka Connector appears to contain additional path components beyond the base dataset name.  This is considered deprecated and future versions of this library will no longer permit this configuration and it should be adjusted accordingly.",
                        uriPath);
                return findDataset(server, uriPath.substring(0, uriPath.lastIndexOf('/')));
            } else {
                return Optional.empty();
            }
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

    /**
     * Find the connectors referring to the dataset and return the list of topics feeding into this dataset. This can be
     * the empty list.
     * <p>
     * This is primarily intended for use by extensions that build atop this module as it allows additional services to
     * discover the configured Kafka topic(s) for a given dataset and act upon those as needed.
     * </p>
     */
    @SuppressWarnings("unused")
    public static List<String> findTopics(String uriPath) {
        uriPath = DataAccessPoint.canonical(uriPath);
        List<String> topics = new ArrayList<>();
        for (KConnectorDesc fkConn : FKRegistry.get().getConnectors()) {
            String dispatchURI = fkConn.getDatasetName();
            if (dispatchURI.startsWith(uriPath)) {
                topics.addAll(fkConn.getTopics());
            }
        }
        return Collections.unmodifiableList(topics);
    }

    private static ExecutorService EXECUTOR = threadExecutor();

    private static ExecutorService threadExecutor() {
        return Executors.newCachedThreadPool();
    }

    private static final Map<String, List<ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>>>> DRIVERS =
            new HashMap<>();

    /**
     * The background threads
     */
    static void resetPollThreads() {
        // Explicitly cancel the projector drivers
        for (ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver : DRIVERS.values().stream().flatMap(
                Collection::stream).toList()) {
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
                                       DatasetGraph destination,
                                       Function<DatasetGraph, Sink<Event<Bytes, RdfPayload>>> sinkBuilder) {

        //@formatter:off
        Sink<Event<Bytes, RdfPayload>> dlq = null;
        if (StringUtils.isNotBlank(connector.getDlqTopic())) {
            dlq = KafkaSink.<Bytes, RdfPayload>create()
                            .topic(connector.getDlqTopic())
                            .bootstrapServers(connector.getBootstrapServers())
                            .producerConfig(connector.getKafkaConsumerProps())
                            .keySerializer(BytesSerializer.class)
                            .valueSerializer(RdfPayloadSerializer.class)
                            // NB - We want any failures in the DLQ to surface immediately
                            .noAsync()
                            .build();
        }
        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver =
                ProjectorDriver.<Bytes, RdfPayload, Event<Bytes, RdfPayload>>create()
                               .pollTimeout(FKConst.pollingWaitDuration)
                               .unlimited()
                               .reportBatchSize(10_000)
                               .source(source)
                               .projector(FusekiProjector.builder()
                                                         .source(source)
                                                         .dataset(destination)
                                                         .connector(connector)
                                                         .batchSize(connector.getMaxPollRecords())
                                                         .dlq(dlq)
                                                         .build())
                               .destination(sinkBuilder.apply(destination))
                               .build();
        //@formatter:on

        // Submit for execution, and register for cancellation
        Future<?> future = EXECUTOR.submit(driver);
        DRIVERS.computeIfAbsent(connector.getDatasetName(), x -> new ArrayList<>()).add(driver);

        // Wait briefly for the projector driver thread to spin up
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Ignored
        } catch (ExecutionException e) {
            // If the projector driver fails in the startup phase we should bail out immediately
            DRIVERS.getOrDefault(connector.getDatasetName(), new ArrayList<>()).remove(driver);
            throw new FusekiKafkaException("Connector failed to start up", e);
        } catch (TimeoutException e) {
            // Ignore, we can safely assume the projector driver thread started up cleanly
        }

        // TODO We should have a proper monitor thread that is periodically checking the projector driver threads to see
        //      if any of them have failed
    }

    /**
     * The default sink builder function used by
     * {@link #addConnectorToServer(KConnectorDesc, FusekiServer, FusekiOffsetStore, Function)} if a builder function is
     * not explicitly specified.
     * <p>
     * The default sink builder function just builds a {@link FusekiSink}
     * </p>
     *
     * @return Default sink builder function
     */
    public static Function<DatasetGraph, Sink<Event<Bytes, RdfPayload>>> defaultSinkBuilder() {
        return dsg -> FusekiSink.builder()
                                .dataset(dsg)
                                .build();
    }

    /**
     * Forces all currently active event sources
     *
     * @param datasetName relevant dataset
     * @param newOffsets  desired offsets to reset to
     */
    public static void restoreOffsetForDataset(String datasetName, FusekiOffsetStore newOffsets) {
        if (DRIVERS.containsKey(datasetName)) {
            for (ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver : DRIVERS.get(datasetName)) {
                if (driver.getSource() instanceof KafkaEventSource<Bytes, RdfPayload> kafkaSource) {
                    // Convert from offset store format into the map format KafkaEventSource expects
                    // Our external offset store keys are of the form <topic>-<partition>-<consumerGroup>
                    // Since we may be restoring from a state file that may have different group in it, e.g. we might be
                    // using a restore to spin up a new instance from a previous backup, we want to find the maximum
                    // offsets in that file as those will represent the most recent Kafka offsets
                    Map<TopicPartition, Long> kafkaOffsets = new HashMap<>();
                    for (Map.Entry<String, Object> offset : newOffsets.offsets()) {
                        TopicPartition partition = decodeExternalOffsetKey(offset.getKey());
                        kafkaOffsets.compute(partition, (k, v) -> (v == null) ? (Long) offset.getValue() :
                                                                  Math.max(v, (Long) offset.getValue()));
                    }
                    kafkaSource.resetOffsets(kafkaOffsets);
                }
            }
        }
    }

    private static TopicPartition decodeExternalOffsetKey(String key) {
        String[] parts = key.split("-", 3);
        return new TopicPartition(parts[0], Integer.parseInt(parts[1]));
    }
}
