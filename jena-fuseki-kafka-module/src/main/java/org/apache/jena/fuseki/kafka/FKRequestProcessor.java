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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.fuseki.kafka.lib.HttpServletRequestMinimal;
import org.apache.jena.fuseki.kafka.lib.HttpServletResponseMinimal;
import org.apache.jena.kafka.ActionFK;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.kafka.common.DataState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * The engine for the Kafka-Fuseki connector.
 */
public class FKRequestProcessor {
    // --> rename!

    private final RequestDispatcher dispatcher;
    private final ServletContext servletContext;
    private final String requestURI;

    public FKRequestProcessor(RequestDispatcher dispatcher, String requestURI, ServletContext servletContext) {
        this.dispatcher = dispatcher;
        this.requestURI = requestURI;
        // ServletContext
        // -- to get the process request URI (null acceptable); DataAccessPoint Registry (null not acceptable)
        this.servletContext = servletContext;
    }

    /**
     * Once round the polling loop, updating the record.
     * Return true if some processing happened.
     */
    public boolean receiver(Consumer<String, ActionFK> consumer, DataState dataState, Duration pollingDuration) {
        Objects.requireNonNull(consumer);
        Objects.requireNonNull(dataState);
        if ( pollingDuration == null )
            pollingDuration = Duration.ofSeconds(5000);

        final long lastOffsetState = dataState.getOffset();
        try {
            long newOffset = receiverStep(dataState.getOffset(), consumer, pollingDuration);
            if ( newOffset == lastOffsetState )
                return false;
            dataState.setOffset(newOffset);
            return true;
        } catch (RuntimeException ex) {
            Log.error(FusekiKafka.LOG, ex.getMessage(), ex);
            return false;
        }
    }

    /** Do one Kafka consumer poll step. */
    private long receiverStep(long lastOffsetState, Consumer<String, ActionFK> consumer, Duration pollingDuration) {
        Objects.requireNonNull(pollingDuration);

        ConsumerRecords<String, ActionFK> cRec = consumer.poll(pollingDuration);
        long lastOffset = lastOffsetState;
        int count = cRec.count();

        if ( count != 0 )
            FmtLog.info(FusekiKafka.LOG, "receiver: from %d , count = %d", lastOffset, count);

        for ( ConsumerRecord<String, ActionFK> rec : cRec ) {
            lastOffset = processRequest(rec);
        }
        return lastOffset;
    }

    private long processRequest(ConsumerRecord<String, ActionFK> rec) {
        String key = rec.key();
        ActionFK action = rec.value();
        long offset = rec.offset();
        FmtLog.info(FusekiKafka.LOG, "Record Offset %s", offset);

        dispatch(dispatcher, rec.topic(), requestURI, action, servletContext);

        // This happens in replay or catch up. Not a warning.
//        if ( offset != lastOffset+1)
//            FmtLog.warn(FusekiKafka.LOG, "WARNING: Inconsistent offsets: offset=%d, lastOffset = %d\n", offset, lastOffset);
        return offset;
    }

    /**
     * The logic to send an {@link ActionFK} to a {@link RequestDispatcher} which handled {@code HttpServlet} style operations.
     */
    public static ActionFK dispatch(RequestDispatcher dispatcher, String topic, String requestURI, ActionFK request, ServletContext servletContext) {
        Map<String, String> requestParameters = Map.of();

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        HttpServletRequest req = new HttpServletRequestMinimal(requestURI, request.getHeaders(), requestParameters,
                                                               request.getBytes(), servletContext);
        HttpServletResponseMinimal resp = new HttpServletResponseMinimal(bytesOut);

        // Do it!
        dispatcher.dispatch(req, resp);

        InputStream respBytes;
        if ( bytesOut.size() != 0 ) {
            respBytes = new ByteArrayInputStream(bytesOut.toByteArray());
        } else
            respBytes = InputStream.nullInputStream();
        ActionFK result = new ActionFK(topic, resp.headers(), respBytes);
        return result;
    }
}
