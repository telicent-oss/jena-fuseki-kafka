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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.KConnectorDesc;

/**
 * Registry of active connectors.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FKRegistry {

    private static final FKRegistry singleton = new FKRegistry();

    /**
     * Return the current server-wide registry of Fuseki-Kafka connectors.
     */
    public static FKRegistry get() {
        return singleton;
    }

    // Topic to connector record.
    private final Map<String, KConnectorDesc> topicToConnector = new ConcurrentHashMap<>();
    private final Set<String> dlqTopics = new ConcurrentSkipListSet<>();

    /**
     * Return the {@link KConnectorDesc} for a topic.
     *
     * @return Connector registered for topic, {@code null} if no connector registered for given topic
     */
    public KConnectorDesc getConnectorDescriptor(String topicName) {
        return topicToConnector.get(topicName);
    }

    /**
     * Gets all the registered connectors
     *
     * @return Registered connectors
     */
    public Collection<KConnectorDesc> getConnectors() {
        return Collections.unmodifiableCollection(topicToConnector.values());
    }

    /**
     * Register a connector binding {@link KConnectorDesc} for the given topic(s)
     */
    public void register(List<String> topics, KConnectorDesc connectorDescriptor) {
        // Track DLQ topics as we don't allow a DLQ topic to also be an input topic otherwise a malformed message would
        // put connectors into a DLQ loop
        if (StringUtils.isNotBlank(connectorDescriptor.getDlqTopic())) {
            dlqTopics.add(connectorDescriptor.getDlqTopic());
        }

        // We only permit a single connector per-topic
        for (String topicName : topics) {
            if (topicToConnector.containsKey(topicName)) {
                throw new JenaKafkaException("Multiple connectors configured for same topic: " + topicName);
            }
            if (dlqTopics.contains(topicName)) {
                throw new JenaKafkaException(
                        "Can't configure a connector with input topic '" + topicName + "' as this is already a DLQ topic for another connector");
            }
            topicToConnector.put(topicName, connectorDescriptor);
        }
    }

    /**
     * Remove all registrations associated with the given topic(s)
     */
    public void unregister(List<String> topics, KConnectorDesc connectorDescriptor) {
        if (StringUtils.isNotBlank(connectorDescriptor.getDlqTopic())) {
            dlqTopics.remove(connectorDescriptor.getDlqTopic());
        }
        for (String topicName : topics) {
            topicToConnector.remove(topicName);
        }
    }

    /**
     * Resets the register, intended only for testing to ensure a blank slate
     */
    void reset() {
        topicToConnector.clear();
        dlqTopics.clear();
    }
}
