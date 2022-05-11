/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.kafka;

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserialize to an internal "request object"
 */
public class DeserializerActionFK implements Deserializer<ActionFK> {

    private static String defaultContentType = WebContent.contentTypeNQuads;

    public DeserializerActionFK() {}

    @Override
    public ActionFK deserialize(String topic, Headers headers, byte[] data) {
        Map<String, String> requestHeaders = FK.headerToMap(headers);

        // Default Content-Type to NQuads
        if ( ! requestHeaders.containsKey(HttpNames.hContentType) ) {
            Log.warn(FusekiKafka.LOG, "No Content-Type - defaulting to "+defaultContentType);
            requestHeaders.put(HttpNames.hContentType, defaultContentType);
        }

        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        return new ActionFK(topic, requestHeaders, bytesIn);
    }

    @Override
    public ActionFK deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }
}
