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
import java.io.PrintStream;
import java.util.Map;
import java.util.function.Function;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserialize to an internal "request object"
 */
public class DeserializerActionFK implements Deserializer<RequestFK> {

    /*
     * Verbose mode - dumps incoming Kafka event to an output stream.
     * The purpose is to be able to capture events
     *
     */

    private final static String defaultContentType = WebContent.contentTypeNQuads;
    private final Function<Integer, PrintStream> output;
    private boolean verbose = false;

    /**
     * New DeserializerActionFK
     * @param verbose
     *      Dump events.
     * @param output
     *      Supply a {@code PrintStream} for dump output.
     *      The output is thread-safe.
     *      The output is flushed after each event.
     */
    public DeserializerActionFK(boolean verbose, Function<Integer, PrintStream> output) {
        this.verbose = verbose;
        this.output = output;
    }

    public DeserializerActionFK() {
        this(false, null);
    }

    private int counter = 0;

    @Override
    public RequestFK deserialize(String topic, Headers headers, byte[] data) {
        Map<String, String> requestHeaders = JK.headerToMap(headers);

        if ( verbose && output != null ) {
            synchronized(this) {
                counter++;
                PrintStream out = output.apply(counter);
                out.printf("## %d ##\n", counter);
                headers.forEach(h->out.println(h.key()+": "+StrUtils.fromUTF8bytes(h.value())));
                out.println();
                String x = StrUtils.fromUTF8bytes(data);
                out.print(x);
                if ( ! x.endsWith("\n") )
                    out.println();
                out.flush();
            }
        }

        // Default Content-Type to NQuads
        if ( ! requestHeaders.containsKey(HttpNames.hContentType) ) {
            //Log.warn(FusekiKafka.LOG, "No Content-Type - defaulting to "+defaultContentType);
            requestHeaders.put(HttpNames.hContentType, defaultContentType);
        }

        // Content-Length
        if ( ! requestHeaders.containsKey(HttpNames.hContentLength) ) {
            String contentLengthStr = Integer.toString(data.length);
            requestHeaders.put(HttpNames.hContentLength, contentLengthStr);
        }

        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        return new RequestFK(topic, requestHeaders, bytesIn);
    }

    @Override
    public RequestFK deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }
}
