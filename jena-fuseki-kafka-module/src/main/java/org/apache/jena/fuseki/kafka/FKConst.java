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

import java.time.Duration;

/** Constants relating to Fuseki-Kafka connections */
public class FKConst {

    /**
     * Time to wait when pinging Kafka. Used to check Kafka available.
     * See {@link FKS#checkKafkaTopicConnection}.
     */
    public static final Duration checkKafkaDuration = Duration.ofMillis(500);

    /**
     * Time to wait during the initial sync from the topic during connector startup.
     * This is a quick sync for immediate (synchronous) catch-up during connector
     * setup. A large amount of catch is done asynchronously during the first time
     * round the polling loop.
     * See {@link FKS#addConnectorToServer} and {@link FKS#oneTopicPoll}.
     */
    public static final Duration initialWaitDuration = Duration.ofMillis(500);

    /**
     * Length of the wait when polling Kafka regularly.
     * See {@link FKS#topicPoll}.
     */
    public static final Duration pollingWaitDuration = Duration.ofMillis(10_000);

    /**
     * Length of the wait when polling Kafka after having received some data.
     * This is the loop in {@link FKBatchProcessor#receiverStep}.
     * See {@link FKS#topicPoll}.
     */
    public static final Duration pollingWaitDurationMore = Duration.ofMillis(10);

    /**
     * Kafka has a default message of 500 for consumer.poll
     * <p>
     * This setting is the number of times to loop per
     * receiver cycle calling {@link FKBatchProcessor#receiverStep}.
     * That is, the number of 500 message units to process in one polling loop.
     */
    public static final int MAX_LOOPS_PER_CYCLE = 10;
}
