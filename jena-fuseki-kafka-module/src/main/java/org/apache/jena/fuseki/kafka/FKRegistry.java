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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.kafka.KConnectorDesc;

/**
 * Registry of active connectors.
 */
public class FKRegistry {

    private static final FKRegistry singleton   = new FKRegistry();

    /**
     * Return the current server-wide registry of Fuseki-Kafka connectors.
     */
    public static FKRegistry get() { return singleton; }

    // Topic to connector record.
    private Map<String, KConnectorDesc> topicToConnector = new ConcurrentHashMap<>();

    // Dispatch to topic.
    private Map<String, String> pathToTopic = new ConcurrentHashMap<>();

    private FKRegistry() { }

    /**
     * Return the Fuseki dispatch (request URI) for a topic.
     */
    public String getDispatchURI(String topicName) {
        KConnectorDesc conn = getConnectorDescriptor(topicName);
        return conn.getLocalDispatchPath();
    }

    /**
     * Return the {@link KConnectorDesc} for a topic.
     */
    public KConnectorDesc getConnectorDescriptor(String topicName) {
        return topicToConnector.get(topicName);
    }

    /**
     * Register a topic-service binding, with it {@link FKRequestProcessor} (may be null) and {@link KConnectorDesc}.
     */
    public void register(String topicName, KConnectorDesc connectorDescriptor) {
        topicToConnector.put(topicName, connectorDescriptor);
        if ( connectorDescriptor.getLocalDispatchPath() != null )
            pathToTopic.put(connectorDescriptor.getLocalDispatchPath(), topicName);
    }

    /**
     * Remove all registrations associated with a topic.
     */
    public void unregister(String topicName) {
        topicToConnector.remove(topicName);
        pathToTopic.remove(topicName);
    }
}
