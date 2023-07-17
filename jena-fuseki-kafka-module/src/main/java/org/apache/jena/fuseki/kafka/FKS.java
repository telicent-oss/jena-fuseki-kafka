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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.jena.kafka.FusekiKafka.LOG;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletContext;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.*;
import org.apache.jena.fuseki.servlets.ActionProcessor;
import org.apache.jena.kafka.DeserializerActionFK;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Functions for Fuseki-Kafka server setup.
 */
public class FKS {
    /**
     * Add a connector to a server.
     * Called from FusekiModule.serverBeforeStarting.
     * This setups on the polling.
     * This configures update.
     */
    static void addConnectorToServer(KConnectorDesc conn, FusekiServer server,
                                     DataState dataState, FKBatchProcessor batchProcessor) {
        String topicName = conn.getTopic();
        String localDispatchPath = conn.getLocalDispatchPath();
        // Remote not (yet) supported.
        //String remoteEndpoint = conn.getRemoteEndpoint();

        // -- Kafka Consumer
        Properties cProps = conn.getKafkaConsumerProps();
        StringDeserializer strDeser = new StringDeserializer();
        Deserializer<RequestFK> reqDer = new DeserializerActionFK();
        Consumer<String, RequestFK> consumer = new KafkaConsumer<>(cProps, strDeser, reqDer);

        // To replicate a database, we need to see all the Kafka messages in-order,
        // which forces us to have only one partition. We need a partition to be able to seek.
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        Collection<TopicPartition> partitions = List.of(topicPartition);
        consumer.assign(partitions);

        // -- Choose start point.
        // If true, ignore topic state and start at current.
        boolean syncTopic = conn.getSyncTopic();
        boolean replayTopic = conn.getReplayTopic();

        // Last offset processed
        long stateOffset = dataState.getLastOffset();
        if ( stateOffset < 0 ) {
            FmtLog.info(LOG, "[%s] Initialize from topic", conn.getTopic());
            // consumer.seekToBeginning(Arrays.asList(topicPartition)); BUG
            replayTopic = true;
        }

        checkKafkaTopicConnection(consumer, topicName);

        if ( replayTopic ) {
            setupReplayTopic(consumer, topicPartition, dataState);
        } else if ( syncTopic ) {
            setupSyncTopic(consumer, topicPartition, dataState);
        } else {
            setupNoSyncTopic(consumer, topicPartition, dataState);
        }

        if ( conn.getLocalDispatchPath() != null )
            FmtLog.info(LOG, "[%s] Start FusekiKafka : Topic = %s : Dataset = %s", topicName, topicName, conn.getLocalDispatchPath());
        else
            FmtLog.info(LOG, "[%s] Start FusekiKafka : Topic = %s : Relay = %s", topicName, topicName, conn.getRemoteEndpoint());

        // Do now for some catchup.
        oneTopicPoll(batchProcessor, consumer, dataState, FKConst.initialWaitDuration);

        FmtLog.info(LOG, "[%s] Initial sync : Offset = %d", topicName, dataState.getLastOffset());

        // ASYNC
        startTopicPoll(batchProcessor, consumer, dataState, "Kafka:" + topicName);
    }

    /**
     * Helper to find the dispatch of a URI to single endpoint
     * ({@link ActionProcessor}) or to a dataset direct. It depends whether the
     * uriPath is "{@code /database}" or "{@code /database/serviceEndpoint}".
     * <p>
     * If there is an endpoint, it must not be overloaded by request (query string,
     * content-type). If there is no endpoint, only a dataset, how requests are
     * processed is an extension requiring a subclass implementing
     * {@link FMod_FusekiKafka#makeFKBatchProcessor}).
     * <p>
     * This function throws an exception if the dataset does not exist or the endpoint does not exist.
     */
    public static Pair<ActionProcessor, DatasetGraph> findActionProcessorDataset(FusekiServer server, String uriPath) {
        // Dispatcher.locateDataAccessPoint -- simplified
        // URI to dataset and endpointName
        String datasetName = null;
        String endpointName = null;
        int idx = uriPath.lastIndexOf('/');
        if ( idx == -1 ) {
            // No "/"
            datasetName = uriPath;
            endpointName = null;
        } else if ( idx == 0 ) {
            // Starts with "/"
            datasetName = uriPath;
            endpointName = null;
        } else if (idx == uriPath.length()-1 ) {
            // Last character
            datasetName = uriPath;
            endpointName = null;
        } else {
            // A "/" mid path.
            datasetName = uriPath.substring(0, idx);
            endpointName = uriPath.substring(idx+1);
        }
        datasetName = DataAccessPoint.canonical(datasetName);
        // Find DataService
        DataService dataService = findDataService(server, datasetName);
        if ( dataService == null ) {
            String msg = String.format("Can't find a dataset service for '%s' (%s)", datasetName, uriPath);
            throw new FusekiKafkaException(msg);
        }

        // If dataset direct supported.
        if ( endpointName == null ) {
            // Dataset direct - no specific processor.
            return Pair.create(null, dataService.getDataset());
        }

        EndpointSet epSet = findEndpointSet(dataService, endpointName);
        if ( epSet == null ) {
            String msg = String.format("Can't find an endpoint for dataset service for '%s', endpoint '%s'  (%s)", datasetName, endpointName, uriPath);
            throw new FusekiKafkaException(msg);
        }

        if (epSet.isEmpty() ) {
            String msg = String.format("Empty endpoint set for dataset service for '%s', endpoint '%s'  (%s)", datasetName, endpointName, uriPath);
            throw new FusekiKafkaException(msg);
        }

        Endpoint ep = epSet.getExactlyOne();
        if ( ep == null ) {
            String msg = String.format("Multiple endpoints set for dataset service for '%s', endpoint '%s'  (%s)", datasetName, endpointName, uriPath);
            throw new FusekiKafkaException(msg);
        }
        ActionProcessor actionProcessor = ep.getProcessor();
        return Pair.create(actionProcessor, dataService.getDataset());
    }

    private static EndpointSet findEndpointSet(DataService dataService, String endpointName) {
        EndpointSet epSet = isEmpty(endpointName)
            ? dataService.getEndpointSet()
            : dataService.getEndpointSet(endpointName);
        return epSet;
    }

    private static DataService findDataService(FusekiServer server, String datasetName) {
        DataAccessPointRegistry dapRegistry = server.getDataAccessPointRegistry();
        datasetName = DataAccessPoint.canonical(datasetName);
        DataAccessPoint dap = dapRegistry.get(datasetName);
        if ( dap == null )
            return null;
        // Dispatcher.chooseEndpoint -- simplified, no overloading by content type.
        DataService dataService = dap.getDataService();
        return dataService;
    }

    /** Get the database * out of a Fuseki server. */
    public static DatasetGraph findDataset(FusekiServer server, String datasetName) {
        DataService dSrv = findDataService(server, datasetName);
        if ( dSrv == null )
            return null;
        return dSrv.getDataset();
    }

    /** Check connectivity so we can give specific messages */
    private static void checkKafkaTopicConnection(Consumer<String, RequestFK> consumer, String topicName) {
        //NetworkClient is noisy (warnings).
        Class<?> cls = NetworkClient.class;
        String logLevel = LogCtl.getLevel(cls);
        LogCtl.setLevel(cls, "Error");
        try {
            // Short timeout - this is a check, processing tries to continue.
            List<PartitionInfo> partitionInfo = consumer.partitionsFor(topicName, FKConst.checkKafkaDuration);
            if ( partitionInfo == null ) {
                FmtLog.error(LOG, "[%s] Unexpected - PartitionInfo list is null", topicName);
                return;
            }
            if ( partitionInfo.isEmpty() )
                FmtLog.warn(LOG, "[%s] Successfully contacted Kafka but no partitions for topic %s", topicName, topicName);
            else {
                if ( partitionInfo.size() != 1 )
                    FmtLog.info(LOG, "[%s] Successfully contacted Kafka topic partitions %s", topicName, partitionInfo);
                else {
                    PartitionInfo info = partitionInfo.get(0);
                    FmtLog.info(LOG, "[%s] Successfully contacted Kafka topic %s", topicName, info.topic());
                }
            }
        } catch (TimeoutException ex) {
            ex.printStackTrace(System.err);
            FmtLog.info(LOG, "[%s] Failed to contact Kafka broker for topic partition %s", topicName, topicName);
            // No server => no topic.
        } finally {
            LogCtl.setLevel(cls, logLevel);
        }
    }

    /** Set to catch up on the topic at the next (first) call. */
    private static void setupSyncTopic(Consumer<String, RequestFK> consumer, TopicPartition topicPartition, DataState dataState) {
        String topic = dataState.getTopic();
        long topicPosition = consumer.position(topicPartition);
        long stateOffset = dataState.getLastOffset();

        FmtLog.info(LOG, "[%s] State=%d  Topic next offset=%d", topic, stateOffset, topicPosition);
        if ( (stateOffset >= 0) && (stateOffset >= topicPosition) ) {
            FmtLog.info(LOG, "[%s] Adjust state record %d -> %d", topic, stateOffset, topicPosition - 1);
            stateOffset = topicPosition - 1;
            dataState.setLastOffset(stateOffset);
        } else if ( topicPosition != stateOffset + 1 ) {
            FmtLog.info(LOG, "[%s] Set sync %d -> %d", topic, stateOffset, topicPosition - 1);
            consumer.seek(topicPartition, stateOffset + 1);
        } else {
            FmtLog.info(LOG, "[%s] Up to date: %d -> %d", topic, stateOffset, topicPosition - 1);
        }
    }

    /**
     * Set to jump to the front of the topic, and so not resync on the the next
     * (first) call.
     */
    private static void setupNoSyncTopic(Consumer<String, RequestFK> consumer, TopicPartition topicPartition, DataState dataState) {
        String topic = dataState.getTopic();
        long topicPosition = consumer.position(topicPartition);
        long stateOffset = dataState.getLastOffset();
        FmtLog.info(LOG, "[%s] No sync: State=%d  Topic offset=%d", topic, stateOffset, topicPosition);
        dataState.setLastOffset(topicPosition);
    }

    /** Set to jump to the start of the topic. */
    private static void setupReplayTopic(Consumer<String, RequestFK> consumer, TopicPartition topicPartition, DataState dataState) {
        String topic = dataState.getTopic();
        long topicPosition = consumer.position(topicPartition);
        long stateOffset = dataState.getLastOffset();
        FmtLog.info(LOG, "[%s] Replay: Old state=%d  Topic offset=%d", topic, stateOffset, topicPosition);
        Map<TopicPartition, Long> m = consumer.beginningOffsets(List.of(topicPartition));
        // offset of next-to-read.
        long beginning = m.get(topicPartition);
        consumer.seek(topicPartition, beginning);
        dataState.setLastOffset(beginning-1);
    }

    private static ExecutorService threads = threadExecutor();

    private static ExecutorService threadExecutor() {
        return Executors.newCachedThreadPool();
    }

    /** The background threads */
    static void resetPollThreads() {
        threads.shutdown();
        threads = threadExecutor();
    }

    private static void startTopicPoll(FKBatchProcessor requestProcessor, Consumer<String, RequestFK> consumer, DataState dataState, String label) {
        Runnable task = () -> topicPoll(requestProcessor, consumer, dataState);
        threads.submit(task);
    }

    /** Polling task loop.*/
    private static void topicPoll(FKBatchProcessor requestProcessor, Consumer<String, RequestFK> consumer, DataState dataState) {
        for ( ;; ) {
            try {
                boolean somethingReceived = oneTopicPoll(requestProcessor, consumer, dataState, FKConst.pollingWaitDuration);
            } catch (Throwable th) {
                FmtLog.debug(LOG, th, "[%s] Unexpected Exception %s", dataState.getTopic(), dataState);
            }
        }
    }

    /** A polling attempt either returns some records or waits the polling duration. */
    private static boolean oneTopicPoll(FKBatchProcessor requestProcessor, Consumer<String, RequestFK> consumer, DataState dataState, Duration pollingDuration) {
        String topic = dataState.getTopic();
        long lastOffsetState = dataState.getLastOffset();
        boolean somethingReceived = requestProcessor.receiver(consumer, dataState, pollingDuration);
        if ( somethingReceived ) {
            long newOffset = dataState.getLastOffset();
            FmtLog.debug(LOG, "[%s] Offset: %d -> %d", topic, lastOffsetState, newOffset);
        } else
            FmtLog.debug(LOG, "[%s] Nothing received: Offset: %d", topic, lastOffsetState);
        return somethingReceived;
    }

    /**
     * Make a {@link FKBatchProcessor} for the Fuseki Server being built. This plain
     * batch processor is one that loops on the ConsumerRecords ({@link RequestFK})
     * sending each to the Fuseki server for dispatch. Other policies are possible
     * such as aggregating batches or directly applying to a dataset.
     */
    public static FKBatchProcessor plainFKBatchProcessor(KConnectorDesc conn, ServletContext servletContext) {
        String requestURI = conn.getLocalDispatchPath();
        FKProcessor requestProcessor = new FKProcessorFusekiDispatch(requestURI, servletContext);
        FKBatchProcessor batchProcessor = FKBatchProcessor.plain(requestProcessor);
        return batchProcessor;
    }
}
