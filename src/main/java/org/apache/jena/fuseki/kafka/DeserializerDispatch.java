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
import java.io.ByteArrayOutputStream;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.fuseki.kafka.lib.HttpServletRequestMinimal;
import org.apache.jena.fuseki.kafka.lib.HttpServletResponseMinimal;
import org.apache.jena.riot.web.HttpNames;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserialize and dispatch to a RequestDispatcher.
 * This goes though the Fuseki dispatch-logging cycle.
 *
 * @see DeserializerAction
 */
public class DeserializerDispatch implements Deserializer<Void> {

    private final RequestDispatcher dispatcher;
    private final ServletContext servletContext;
    private final String requestURI;

    // ServletContext
    // -- to get the process request URI (null acceptable) DataAccessPoint Registry (null not acceptable)
    public DeserializerDispatch(RequestDispatcher dispatcher, String requestURI, ServletContext servletContext) {
        this.dispatcher = dispatcher;
        this.requestURI = requestURI;
        this.servletContext = servletContext;
    }

    @Override
    public Void deserialize(String topic, Headers headers, byte[] data) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

        String contentType = Bytes.bytes2string(headers.lastHeader(hContentType).value());
        Map<String, String> requestHeaders = Map.of(HttpNames.hContentType, contentType);
        Map<String, String> requestParameters = Map.of();

        HttpServletRequest req = new HttpServletRequestMinimal(requestURI, requestHeaders, requestParameters, bytesIn, servletContext);
        HttpServletResponse resp = new HttpServletResponseMinimal(bytesOut);
        dispatcher.dispatch(req, resp);
        return null;
    }

    @Override
    public Void deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }
}
