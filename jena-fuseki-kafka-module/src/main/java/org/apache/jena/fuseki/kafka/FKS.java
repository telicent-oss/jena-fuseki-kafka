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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.DeserializerActionFK;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.common.DataState;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Functions for Fuseki-Kafka.
 */
public class FKS {

    /**
     * Add a connector to a server.
     * Called from FusekiModule.serverBeforeStarting.
     * This setups on the polling.
     * This configures update.
     */
    public static void addConnectorToServer(KConnectorDesc conn, FusekiServer server, DataState dataState) {
        String topicName = conn.getTopic();
        String localDispatchPath = conn.getLocalDispatchPath();
        // Remote not (yet) supported.
        //String remoteEndpoint = conn.getRemoteEndpoint();

        String requestURI = localDispatchPath;

        // The HttpServletRequest is created by FusekiRequestDispatcher.dispatch.
        RequestDispatcher dispatcher = new FusekiRequestDispatcher();
        FKRequestProcessor requestProcessor = new FKRequestProcessor(dispatcher, requestURI, server.getServletContext());

        // -- Kafka Consumer
        Properties cProps = conn.getKafkaConsumerProps();
        StringDeserializer strDeser = new StringDeserializer();
        Deserializer<RequestFK> reqDer = new DeserializerActionFK();
        Consumer<String, RequestFK> consumer = new KafkaConsumer<>(cProps, strDeser, reqDer);

        // ??
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        Collection<TopicPartition> partitions = List.of(topicPartition);
        consumer.assign(partitions);

        // -- Choose start point.
        // If true, ignore topic state and start at current.
        boolean syncTopic = conn.getSyncTopic();
        boolean replayTopic = conn.getReplayTopic();

        // Last offset processed
        long stateOffset = dataState.getOffset();
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

        String versionString = Meta.VERSION;
        if ( conn.getLocalDispatchPath() != null )
            FmtLog.info(LOG, "[%s] Start FusekiKafka (%s) : Topic = %s : Dataset = %s", topicName, versionString, topicName, conn.getLocalDispatchPath());
        else
            FmtLog.info(LOG, "[%s] Start FusekiKafka (%s) : Topic = %s : Relay = %s", topicName, versionString,  topicName, conn.getRemoteEndpoint());

        // Do now for some catchup.
        oneTopicPoll(requestProcessor, consumer, dataState, Duration.ofMillis(500));

        FmtLog.info(LOG, "[%s] Initial sync : Offset = %d", topicName, dataState.getOffset());

        // ASYNC
        startTopicPoll(requestProcessor, consumer, dataState, "Kafka:" + topicName);
    }

    /** Check connectivity so we can give specific messages */
    private static void checkKafkaTopicConnection(Consumer<String, RequestFK> consumer, String topicName) {
        //NetworkClient is noisy (warnings).
        Class<?> cls = NetworkClient.class;
        String logLevel = LogCtl.getLevel(cls);
        LogCtl.setLevel(cls, "Error");
        try {
            // Short timeout - this is a check, processing tries to continue.
            List<PartitionInfo> partitionInfo = consumer.partitionsFor(topicName, Duration.ofMillis(5000));
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
        long stateOffset = dataState.getOffset();

        FmtLog.info(LOG, "[%s] State=%d  Topic next offset=%d", topic, stateOffset, topicPosition);
        if ( (stateOffset >= 0) && (stateOffset >= topicPosition) ) {
            FmtLog.info(LOG, "[%s] Adjust state record %d -> %d", topic, stateOffset, topicPosition - 1);
            stateOffset = topicPosition - 1;
            dataState.setOffset(stateOffset);
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
        long stateOffset = dataState.getOffset();
        FmtLog.info(LOG, "[%s] No sync: State=%d  Topic offset=%d", topic, stateOffset, topicPosition);
        dataState.setOffset(topicPosition);
    }

    /** Set to jump to the start of the topic. */
    private static void setupReplayTopic(Consumer<String, RequestFK> consumer, TopicPartition topicPartition, DataState dataState) {
        String topic = dataState.getTopic();
        long topicPosition = consumer.position(topicPartition);
        long stateOffset = dataState.getOffset();
        FmtLog.info(LOG, "[%s] Replay: Old state=%d  Topic offset=%d", topic, stateOffset, topicPosition);
        // Assumes offsets from 0 (no expiry)

        // Here or in FK.receiverStep
        Map<TopicPartition, Long> m = consumer.beginningOffsets(List.of(topicPartition));
        long beginning = m.get(topicPartition);
        consumer.seek(topicPartition, beginning);
        dataState.setOffset(beginning);
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

    private static void startTopicPoll(FKRequestProcessor requestProcessor, Consumer<String, RequestFK> consumer, DataState dataState, String label) {
        Runnable task = () -> topicPoll(requestProcessor, consumer, dataState);
        threads.submit(task);
    }

    /** Polling task loop.*/
    private static void topicPoll(FKRequestProcessor requestProcessor, Consumer<String, RequestFK> consumer, DataState dataState) {
        Duration pollingDuration = Duration.ofMillis(5000);
        for ( ;; ) {
            try {
                boolean somethingReceived = oneTopicPoll(requestProcessor, consumer, dataState, pollingDuration);
            } catch (Throwable th) {
                FmtLog.debug(LOG, th, "[%s] Unexpected Exception %s", dataState.getTopic(), dataState);
            }
        }
    }

    /** A polling attempt either returns some records or waits the polling duration. */
    private static boolean oneTopicPoll(FKRequestProcessor requestProcessor, Consumer<String, RequestFK> consumer, DataState dataState, Duration pollingDuration) {
        String topic = dataState.getTopic();
        long lastOffsetState = dataState.getOffset();
        boolean somethingReceived = requestProcessor.receiver(consumer, dataState, pollingDuration);
        if ( somethingReceived ) {
            long newOffset = dataState.getOffset();
            FmtLog.debug(LOG, "[%s] Offset: %d -> %d", topic, lastOffsetState, newOffset);
        } else
            FmtLog.debug(LOG, "[%s] Nothing received: Offset: %d", topic, lastOffsetState);
        return somethingReceived;
    }

    // Better? A single scheduled executor.
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
//      String topic = dataState.getTopic();
//      long lastOffsetState = dataState.getOffset();
//      boolean somethingReceived = requestProcessor.receiver(consumer, dataState, pollingDuration);
//      if ( somethingReceived ) {
//          long newOffset = dataState.getOffset();
//          FmtLog.debug(LOG, "[%s] Offset: %d -> %d", topic, lastOffsetState, newOffset);
//      }
//      return somethingReceived;
//  }
}
