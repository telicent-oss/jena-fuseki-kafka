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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

/**
 * Record of a message with it's topic.
 */
public class ActionKafka {
    private final Map<String, String> headers;
    private final String topic;
    // Either-or
    private final byte[] bytes;
    private final InputStream bytesInput;

    protected ActionKafka(String topic, Map<String, String> headers, InputStream bytesInput) {
        this.topic = topic;
        this.headers = headers;
        this.bytes = null;
        this.bytesInput = Objects.requireNonNull(bytesInput);

    }

    protected ActionKafka(String topic, Map<String, String> headers, byte[] bytes) {
        this.topic = topic;
        this.headers = headers;
        this.bytes = Objects.requireNonNull(bytes);
        this.bytesInput = null;
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

    public long getByteCount() {
        return bytes == null ? -1 : bytes.length;
    }

    /* Get bytes - this may be null, meaning there is an input stream instead */
    public byte[] getBytes() {
        return bytes;
    }

    public InputStream getInputStream() {
        if ( hasInputStream() )
            return bytesInput;
        return new ByteArrayInputStream(bytes);
    }

    public boolean hasInputStream() {
        return bytesInput != null;
    }

    public boolean hasBytes() {
        return bytes != null;
    }
}
