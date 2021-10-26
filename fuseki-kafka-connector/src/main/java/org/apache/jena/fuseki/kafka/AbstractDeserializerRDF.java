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

package org.apache.jena.fuseki.kafka;

import static org.apache.jena.fuseki.kafka.FusekiKafka.hContentType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Receive and dispatch incoming to SPARQL Update or RDF data.
 */
public abstract class AbstractDeserializerRDF implements Deserializer<Void> {

    protected AbstractDeserializerRDF() {}

    protected void action(String contentType, String topic, InputStream data) {
        try {
            if ( WebContent.contentTypeSPARQLUpdate.equals(contentType) ) {
                actionSparqlUpdate(topic, data);
                return;
            }
            Lang lang = RDFLanguages.contentTypeToLang(contentType);
            if ( lang != null ) {
                actionData(topic, lang, data);
                return;
            }
            FmtLog.warn(FusekiKafka.LOG, "Topic = %s : Failed to handle '%s'",  topic, contentType);
        } catch (RuntimeException ex) {
            actionFailed(ex, contentType, topic, data);
        }
    }

    protected void actionFailed(RuntimeException ex, String contentType, String topic, InputStream data) {
        ex.printStackTrace();
    }

    protected abstract void actionSparqlUpdate(String topic, InputStream data);

    protected abstract void actionData(String topic, Lang lang, InputStream data);

    @Override
    final public Void deserialize(String topic, Headers headers, byte[] data) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        //ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        String contentType = Bytes.bytes2string(headers.lastHeader(hContentType).value());
        action(contentType, topic, bytesIn);
        return null;
    }

    @Override
    final public Void deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }
}
