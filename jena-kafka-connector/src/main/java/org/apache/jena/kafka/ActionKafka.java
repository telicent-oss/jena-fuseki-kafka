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

import java.io.InputStream;
import java.util.Map;

/**
 * Record of a message with it's topic.
 */
public class ActionKafka {
    private final Map<String, String> headers;
    private final InputStream bytes;
    private final String topic;

    protected ActionKafka(String topic, Map<String, String> headers, InputStream bytes) {
        this.topic = topic;
        this.headers = headers;
        this.bytes = bytes;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getContentType() {
        return headers.get(FusekiKafka.hContentType);
    }

    public InputStream getBytes() {
        return bytes;
    }

}
