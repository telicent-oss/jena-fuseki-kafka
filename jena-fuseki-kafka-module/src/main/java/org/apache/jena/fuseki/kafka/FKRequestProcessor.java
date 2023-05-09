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

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.fuseki.kafka.lib.HttpServletRequestMinimal;
import org.apache.jena.fuseki.kafka.lib.HttpServletResponseMinimal;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.ResponseFK;
import org.apache.jena.kafka.common.DataState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * The engine for the Kafka-Fuseki connector.
 * <p>
 * This creates a minimal HTTP request-response pair and sends the message to a
 * {@link RequestDispatcher}, an interface for servlet-like operation.
 * <p>
 * See {@link FKProcessor} for code-level dispatch.
 */
public class FKRequestProcessor {

    /**
     * Kafka has a default message of 500 for consumer.poll
     * <p>
     * This setting is in addition to this, and is the number of times to loop
     * per receiver cycle calling {@link #receiverStep}.
     * <p>
     * Fuseki request do not span the {@link #receiverStep}.
     */
    private final int MAX_LOOPS_PER_CYCLE = 1;

    private final RequestDispatcher dispatcher;
    private final ServletContext servletContext;
    private final String requestURI;
    private boolean skippingPolling = false;

    public FKRequestProcessor(RequestDispatcher dispatcher, String requestURI, ServletContext servletContext) {
        this.dispatcher = dispatcher;
        this.requestURI = requestURI;
        // ServletContext
        // -- to get the process request URI (null acceptable); DataAccessPoint Registry (null not acceptable)
        this.servletContext = servletContext;
    }

    /**
     * Round the polling loop, updating the record.
     * Return true if some processing happened.
     */
    public boolean receiver(Consumer<String, RequestFK> consumer, DataState dataState, Duration pollingDuration) {
        Objects.requireNonNull(consumer);
        Objects.requireNonNull(dataState);

        if ( pollingDuration == null )
            pollingDuration = Duration.ofSeconds(1000);
        final long lastOffsetState = dataState.getOffset();
        try {
            boolean rtn = false;
            long commitedState = lastOffsetState;
            for ( int i = 0 ; i < MAX_LOOPS_PER_CYCLE ; i++ ) {
                long newOffset = receiverStep(dataState.getTopic(), dataState.getOffset(), consumer, pollingDuration);
                if ( newOffset == commitedState )
                    break;
                dataState.setOffset(newOffset);
                commitedState = newOffset;
                rtn = true;
            }
            return rtn;
        } catch (RuntimeException ex) {
            String x = String.format("[%s] %s", dataState.getTopic(), ex.getMessage());
            Log.error(FusekiKafka.LOG, x, ex);
            skippingPolling = true;
            return false;
        }
    }

    /** Do one Kafka consumer poll step. */
    private long receiverStep(String topic, long lastOffsetState, Consumer<String, RequestFK> consumer, Duration pollingDuration) {
        Objects.requireNonNull(pollingDuration);

        ConsumerRecords<String, RequestFK> cRecords = consumer.poll(pollingDuration);

        if ( cRecords.isEmpty() )
            return lastOffsetState ;

        int count = cRecords.count();

        // -- Batch start
        batchStart(topic, lastOffsetState, count);

        List<RequestFK> batch = FKRequestBatcher.batcher(cRecords);
        batchProcess(batch);

        // Check expectation.
        long newOffsetState = lastOffsetState + count;
        batchFinish(topic, lastOffsetState, newOffsetState);
        // -- Batch finish

        return newOffsetState;
    }

    private void batchStart(String topic, long lastOffsetState, long count) {
        FmtLog.info(FusekiKafka.LOG, "[%s] Receiver: from %d, count=%d", topic, lastOffsetState, count);
    }

    private void batchFinish(String topic, long lastOffsetState, long newOffsetState) {
        FmtLog.info(FusekiKafka.LOG, "[%s] Processed [%d, %d)", topic, lastOffsetState, newOffsetState);
    }

    private void batchProcess(List<RequestFK> requests) {
        for ( RequestFK action : requests ) {
            try {
                //FmtLog.info(FusekiKafka.LOG, "[%s] Record Offset %s", action.getTopic(), offset);
                dispatch(dispatcher, action.getTopic(), requestURI, action, servletContext);
            } catch(Throwable ex) {
                // Something unexpected went wrong.
                // Polling is asynchronous to the server.
                // When shutting down, various things can go wrong.
                // Log and ignore!
                FmtLog.warn(FusekiKafka.LOG, ex, "Exception in dispatch: %s", ex.getMessage());
            }
        }
    }

    // Alternative - send the batched request to a FKProcessor.
    private void batchProcess(List<RequestFK> requests, FKProcessor proc) {
        for ( RequestFK action : requests ) {
            try {
                proc.action(action.getTopic(), action.getContentType(), action.getInputStream());
            } catch(Throwable ex) {
                // Something unexpected went wrong.
                // Polling is asynchronous to the server.
                // When shutting down, various things can go wrong.
                // Log and ignore!
                FmtLog.warn(FusekiKafka.LOG, ex, "Exception in processing: %s", ex.getMessage());
            }
        }
    }

    private static byte[] emptyBytes = new byte[0];

    /**
     * The logic to send an {@link RequestFK} to a {@link RequestDispatcher} which handles {@code HttpServlet} style operations.
     */
    private static ResponseFK dispatch(RequestDispatcher dispatcher, String topic, String requestURI, RequestFK request, ServletContext servletContext) {
        Map<String, String> requestParameters = Map.of();

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        HttpServletRequest req = new HttpServletRequestMinimal(requestURI, request.getHeaders(), requestParameters,
                                                               request.getInputStream(), servletContext);
        HttpServletResponseMinimal resp = new HttpServletResponseMinimal(bytesOut);

        dispatcher.dispatch(req, resp);

        byte[] responseBytes = ( bytesOut.size() != 0 ) ? bytesOut.toByteArray() : emptyBytes;
        ResponseFK result = new ResponseFK(topic, resp.headers(), responseBytes);
        return result;
    }
}
