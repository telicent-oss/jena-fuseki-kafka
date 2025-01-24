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

package org.apache.jena.kafka;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.logging.Log;

/**
 * Details of a connector to Kafka.
 * <p>
 * The machinery is in {@link io.telicent.smart.cache.sources.kafka.KafkaRdfPayloadSource} which reads from kafka, and
 * returns {@link io.telicent.smart.cache.payloads.RdfPayload} instances.
 * <p>
 * For Fuseki, the {@link io.telicent.smart.cache.payloads.RdfPayload} is handled by {@code FKRequestProcessor} which
 * dispatches the request to the main Fuseki execution path (includes Fuseki logging).
 */
@ToString
@EqualsAndHashCode
public class KConnectorDesc {

    // Source
    @Getter
    private final List<String> topics;
    @Getter
    private final String bootstrapServers;

    // Destination - URI path in this Fuseki server
    private final String fusekiDispatchPath;
    /**
     * -- GETTER --
     *  The destination of events on the Kafka topic as a "remote" endpoint.
     *  <p>
     *  Note that the endpoint need not actually be remote, this could simply be a local endpoint configured on the
     *  Fuseki server if you want to apply normal HTTP processing to the requests.
     *  </p>
     */
    // URL to replay the request to.
    @Getter
    private final String remoteEndpoint;

    @Getter
    private final boolean syncTopic;
    @Getter
    private final boolean replayTopic;

    // State tracking.
    @Getter
    private final String stateFile;

    // Kafka consumer setup.
    @Getter
    private final Properties kafkaConsumerProps;

    public KConnectorDesc(List<String> topics, String bootstrapServers, String fusekiDispatchName, String remoteEndpoint,
                          String stateFile, boolean syncTopic, boolean replayTopic, Properties kafkaConsumerProps) {
        this.topics = Objects.requireNonNull(topics, "topics cannot be null");
        if (this.topics.isEmpty()) {
            throw new IllegalArgumentException("topics cannot be empty");
        }
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers cannot be null");
        this.fusekiDispatchPath = fusekiDispatchName;
        this.remoteEndpoint = remoteEndpoint;
        this.syncTopic = syncTopic;
        this.replayTopic = replayTopic;
        this.stateFile = stateFile;
        this.kafkaConsumerProps = kafkaConsumerProps;

        boolean hasLocalFusekiService = StringUtils.isNotBlank(fusekiDispatchName);
        boolean hasRemoteEndpoint = StringUtils.isNotBlank(remoteEndpoint);

        if (hasRemoteEndpoint && hasLocalFusekiService) {
            Log.warn(this, "ConnectorFK built with both a local dispatch path and remote endpoint URL");
        }
        if (!hasRemoteEndpoint && !hasLocalFusekiService) {
            Log.warn(this, "ConnectorFK built with no local dispatch path nor remote endpoint URL");
        }
    }

    /**
     * The destination of events on the Kafka topic.
     * <p>
     * Either they are dispatched to Fuseki, in the same JVM, and the connector destination is given by as a dataset
     * name by this method, or replayed on to a "remote" endpoint URL given by {@link #getRemoteEndpoint}.
     *
     * @return HTTP path for local dispatch.
     */
    public String getLocalDispatchPath() {
        return fusekiDispatchPath;
    }

    public boolean isLocalDispatch() {
        return !StringUtils.isNotBlank(fusekiDispatchPath);
    }

}
