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

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.ResponseFK;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;

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
 */
public abstract class FKProcessorBase implements FKProcessor {

    private static AtomicLong requestId = new AtomicLong(0);

    protected FKProcessorBase() {}

    @Override
    public ResponseFK process(RequestFK request) {
        //String id = String.format("%s:%d", request.getTopic(), requestId.incrementAndGet());
        String id = request.getTopic();
        try {
            ResponseFK response = action(id, request);
            if ( response == null )
                response = ResponseFK.success(request.getTopic());
            return response;
        } catch (Throwable th) {
            return ResponseFK.success(request.getTopic());
        }
    }

    private ResponseFK action(String id, RequestFK request) {
        try {
            InputStream data = request.getInputStream();
            String contentType = request.getContentType();

            if ( WebContent.contentTypeSPARQLUpdate.equals(contentType) ) {
                actionSparqlUpdate(id, request, data);
                return null;
            }
            if ( WebContent.contentTypePatch.equals(contentType) ) {
                actionRDFPatch(id, request, data);
                return null;
            }

            Lang lang = RDFLanguages.contentTypeToLang(contentType);
            if ( lang != null ) {
                actionData(id, request, lang, data);
                return null;
            }
            FmtLog.warn(FusekiKafka.LOG, "Failed to handle '%s'",  contentType);
            return null;
        } catch (RuntimeException ex) {
            actionFailed(id, request, ex);
            return null;
        }
    }

    protected void actionFailed(String id, RequestFK request, String message) {
        throw new FusekiKafkaException(message);
    }

    protected void actionFailed(String id, RequestFK request, RuntimeException ex) {
        ex.printStackTrace();
    }

    protected abstract void actionSparqlUpdate(String id, RequestFK request, InputStream data);

    protected abstract void actionRDFPatch(String id, RequestFK request, InputStream data);

    protected abstract void actionData(String id, RequestFK requestd, Lang lang, InputStream data);

}
