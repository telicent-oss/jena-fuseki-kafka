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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.kafka.common.header.Headers;

/**
 * Process incoming to SPARQL Update or RDF data.
 * <p>
 * This is the simplified version of what Fuseki would do for an operation sent to
 * the dataset URL. By looking at the {@code Content-Type}, it splits incoming Kafka messages into:
 * <ul>
 * <li>SPARQL Updates</li>
 * <li>RDF Patch</li>
 * <li>RDF Data</li>
 * <ul>
 *
 * <p>
 * The Fuseki dispatch is {@link FKRequestProcessor} which uses Fuseki dispatch on operation type.
 */
public abstract class FKProcessor {

    protected FKProcessor() {}

    public void process(String topic, Headers headers, byte[] data) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        String contentType = Bytes.bytes2string(headers.lastHeader(HttpNames.hContentType).value());
        action(topic, contentType, bytesIn);
    }

    /**
     * Split SPARQL Update from RDF data.
     */
    public void action(String topic, String contentType, InputStream data) {
        try {
            if ( WebContent.contentTypeSPARQLUpdate.equals(contentType) ) {
                actionSparqlUpdate(topic, data);
                return;
            }
            if ( WebContent.contentTypePatch.equals(contentType) ) {
                actionRDFPatch(topic, data);
                return;
            }

            Lang lang = RDFLanguages.contentTypeToLang(contentType);
            if ( lang != null ) {
                actionData(topic, lang, data);
                return;
            }
            FmtLog.warn(FusekiKafka.LOG, "Failed to handle '%s'",  contentType);
        } catch (RuntimeException ex) {
            actionFailed(topic, ex, contentType, data);
        }
    }

    protected void actionFailed(String topic, RuntimeException ex, String contentType, InputStream data) {
        ex.printStackTrace();
    }

    protected abstract void actionSparqlUpdate(String topic, InputStream data);

    protected abstract void actionRDFPatch(String topic, InputStream data);

    protected abstract void actionData(String topic, Lang lang, InputStream data);

}
