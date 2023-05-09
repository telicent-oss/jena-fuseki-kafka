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
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;

import org.apache.jena.atlas.io.IOX;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.kafka.ActionKafka;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.web.HttpNames;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Consolidating {@code ConsumerRecords<String, RequestFK>} into a
 * {@code List<RequestFK>}. Moved out of FKRequestProcessor for convenience.
 * <p>
 * {@link RequestFK} is the request object from Kafka (extracted so that the uing
 * code is not depending on Kafka classes).
 * <p>
 * The batching process combines adjacent {@link RequestFK RequestFK's} that are compatible.
 * Currently, N-Triples, N-Quads, Turtle, Trig are each combineable.
 * Other consolidation may be added.
 */
public class FKRequestBatcher {
    final static private boolean DEBUG = false;
    private static boolean BATCHING_ACTIVE = true;
    public static final String ENV_BATCHING = "FK_BATCHING";

    /**
     * Maximum items to consolidate into a batch.
     * This is within a {@link ConsumerRecords} - this process does not consolidate across calls of {@code consumer.poll}.
     * Default Kafka per poll size is 500.
     * Set to -1 to turn off batching within {@code ConsumerRecords}
     */
    private static int MAX_BATCH_SIZE = -1;
    public static final String ENV_MAX_BATCH_SIZE = "FK_MAX_BATCH_SIZE";

    /**
     * Limit the size in bytes of message accumulation
     * If a ConsumerRecords/RequestFK would go over this limit,
     * the batch is ended and and batching starts again.
     * Set to -1 to turn off.
     */
    private static int MAX_BATCH_BYTES = 500_000;
    public static final String ENV_MAX_BATCH_BYTES = "FK_MAX_BATCH_BYTES";

    /**
     * Limit on the size of items to merge.
     * Don't consider merging a RequestFK large than this.
     * Set to -1 to turn off.
     */
    private static int LARGE_ITEM_BYTES = 100_000;
    public static final String ENV_LARGE_ITEM_BYTES = "FK_LARGE_ITEM_BYTES";

    /*package*/ static void setFromEnvironment() {
        boolean hasBeenSet = false;
        hasBeenSet |= safeSet(ENV_BATCHING,         (x)->BATCHING_ACTIVE = Boolean.parseBoolean(x));
        hasBeenSet |= safeSet(ENV_MAX_BATCH_SIZE,   (x)->MAX_BATCH_SIZE = Integer.parseInt(x));
        hasBeenSet |= safeSet(ENV_MAX_BATCH_BYTES,  (x)->MAX_BATCH_BYTES = Integer.parseInt(x));
        hasBeenSet |= safeSet(ENV_LARGE_ITEM_BYTES, (x)->LARGE_ITEM_BYTES = Integer.parseInt(x));

        if ( hasBeenSet ) {
            FmtLog.info(FKRequestBatcher.class, "Env:%s, var:BATCHING_ACTIVE = %s",  ENV_BATCHING, BATCHING_ACTIVE);
            FmtLog.info(FKRequestBatcher.class, "Env:%s, var:MAX_BATCH_SIZE = %s",   ENV_MAX_BATCH_SIZE, MAX_BATCH_SIZE);
            FmtLog.info(FKRequestBatcher.class, "Env:%s, var:MAX_BATCH_BYTES = %s",  ENV_MAX_BATCH_BYTES, MAX_BATCH_BYTES);
            FmtLog.info(FKRequestBatcher.class, "Env:%s, var:LARGE_ITEM_BYTES = %s", ENV_LARGE_ITEM_BYTES, LARGE_ITEM_BYTES);
        }
    }

    private static boolean safeSet(String envVar, java.util.function.Consumer<String> action) {
        // Get from system property or environment variable (in that order).
        String x = Lib.getenv(envVar);
        if ( x == null )
            return false;
        try {
            FmtLog.info(FKRequestBatcher.class, "Env:%s = %s", envVar, x);
            action.accept(x);
        } catch (Throwable th) {
            FmtLog.warn(FKRequestBatcher.class, "Failed to set "+envVar+": "+th.getMessage());
        }
        return true;
    }

    /**
     * Convert ConsumerRecords in to a list of RequestFKs.
     * This may include merging adjacent, compatible RequestFK objects.
     */
    /*package*/ static List<RequestFK> batcher(ConsumerRecords<String, RequestFK> cRecords) {
        if ( ! BATCHING_ACTIVE ) {
            // Bypass
            List<RequestFK> x = new ArrayList<>();
            cRecords.iterator().forEachRemaining(cr->x.add(cr.value()));
            return x;
        }

        if ( cRecords.isEmpty() )
            return List.of();

        // Accumulate
        List<RequestFK> batch = new ArrayList<>();
        batcher(cRecords, action->batch.add(action));
        if ( DEBUG )
            print(batch);
        return batch;
    }

    /*package*/ static void batcher(ConsumerRecords<String, RequestFK> cRecords, java.util.function.Consumer<RequestFK> sink) {

        if ( cRecords.isEmpty() ) {
            if ( DEBUG ) System.out.printf("Nothing\n");
            return;
        }

        if ( cRecords.count() == 1 ) {
            ConsumerRecord<String, RequestFK> rec = cRecords.iterator().next();
            if ( DEBUG ) System.out.printf("Singleton: %d\n", rec.offset());
            RequestFK request = rec.value();
            sink.accept(request);
            return;
        }

        Accumulator acc = null;

        if ( DEBUG ) System.out.printf("Start batching: %d\n", cRecords.count()) ;

        for ( ConsumerRecord<String, RequestFK> rec : cRecords ) {
            if ( DEBUG ) System.out.printf("Item: %d\n", rec.offset());
            RequestFK action = rec.value();

            if ( LARGE_ITEM_BYTES > 0 && action.getByteCount() > LARGE_ITEM_BYTES ) {
                if ( DEBUG ) System.out.printf("Large item: %d\n", rec.offset());
                // Flush sink.
                if ( acc != null ) {
                    if ( DEBUG ) System.out.printf("Finish acc: %d\n", rec.offset());
                    acc.complete(sink);
                    acc = null;
                }
                if ( DEBUG ) System.out.printf("Add singleton\n");
                // Single large item.
                sink.accept(action);
                continue;
            }

            if ( DEBUG ) {
                System.out.printf("Item:   [%s] Content-type=%s Headers=%s\n", action.getTopic(), action.getContentType(), action.getHeaders());
                if ( acc != null )
                    System.out.printf("Acc   [%s] Content-type=%s Headers=%s\n", acc.topic, acc.contentType, acc.headers);
                else
                    System.out.printf("Acc   No accumulator\n");
            }

            if ( acc != null ) {
                // Already accumulating. Is this message compatible with the last?
                if ( mergeable(acc, action) ) {
                    if ( DEBUG ) System.out.printf("Accumulate: %d\n", rec.offset());
                    acc.merge(action);
                    continue;
                }
            }
            // Didn't merge, and not a large item, incompatible in some way (e.g.
            if ( acc != null ) {
                if ( DEBUG ) System.out.printf("Finish acc: %d\n", rec.offset());
                // Finish outstanding batch.
                acc.complete(sink);
                acc = null;
                // Drop to reset the accumulator
            }

            if ( DEBUG ) System.out.printf("Start acc: %d\n", rec.offset());
            // Set start of batch
            var accHeaders = new HashMap<>(action.getHeaders());
            // Drop the length.
            accHeaders.remove(HttpNames.hContentLength);
            // Start new accumulator.
            acc = new Accumulator(accHeaders, action.getContentType(), action.getTopic());
            // First item.
            acc.merge(action);
        } // for each ConsumerRecord

        // Finish any outstanding batch.
        if ( acc != null ) {
            if ( DEBUG ) System.out.printf("Complete\n");
            acc.complete(sink);
        }
    }

    /** Test whether mergeable */
    private static boolean mergeable(Accumulator acc, RequestFK action) {
        if ( acc == null )
            return false;
        Map<String, String> headers = action.getHeaders();
        String contentType = action.getContentType();
        String topic = action.getTopic();

        if ( ! Objects.equals(acc.topic, topic) ) {
            if ( DEBUG ) System.out.printf("Not mergable: different topic (%s,%s)\n", acc.topic, topic);
            return false;
        }
        if ( ! Objects.equals(acc.contentType, contentType) ) {
            if ( DEBUG ) System.out.printf("Not mergable: different content type (%s,%s)\n", acc.contentType, contentType);
            return false;
        }
        if ( ! headerCompatible(acc.headers, headers) ) {
            if ( DEBUG ) System.out.printf("Not mergable: headers no compatible (%s,%s)\n", acc.headers, headers);
            return false;
        }
        if ( ! canConcatenate(contentType) ) {
            if ( DEBUG ) System.out.printf("Not mergable: not a suitable content type %s\n", contentType);
            return false;
        }
        // Maximum batch size.
        if ( MAX_BATCH_SIZE > 0 && acc.numBatch >= MAX_BATCH_SIZE ) {
            if ( DEBUG ) System.out.printf("Not mergable: exceeds MAX_BATCH_SIZE (Limit=%,d, actual=%d)\n", MAX_BATCH_SIZE, acc.numBatch);
            return false;
        }
        long x = action.getByteCount();
        if ( x >= 0  && MAX_BATCH_BYTES > 0 && acc.accumulatedSize + x >= MAX_BATCH_BYTES ) {
            if ( DEBUG ) System.out.printf("Not mergable: exceeds MAX_BATCH_BYTES (Limit=%,d, actual=%,d)\n", MAX_BATCH_BYTES, acc.accumulatedSize + x);
            return false;
        }
        return true;
    }

    /** Compare header maps - some fields don't matter */
    private static boolean headerCompatible(Map<String, String> headers1, Map<String, String> headers2) {
        // Are the entries of headers1 in headers2?
        // Assumes no null values.
        for ( Entry<String, String> entry : headers1.entrySet() ) {
            String key = entry.getKey();
            String value = entry.getValue();
            //if (value == null) {}
            String value2 = headers2.get(key);
            if ( value2 == null )
                return false;
            if ( ! value.equals(value2) )
                return false;
        }
        return true;
    }

    private static long copy(ActionKafka action, OutputStream output) {
        try {
        if ( action.hasBytes() ) {
            output.write(action.getBytes());
            return action.getByteCount();
        }
        else
            return action.getInputStream().transferTo(output);
        } catch (IOException e) {
            throw IOX.exception(e);
        }
    }

    private static RequestFK newRequestFK(String topic, Map<String, String> headers, byte[] byteArray) {
        int length = byteArray.length;
        headers.put(HttpNames.hContentLength, Integer.toString(length));
        RequestFK request = new RequestFK(topic, headers, byteArray);
        return request;
    }

    private static void print(List<RequestFK> batch) {
        System.out.printf("Batch: %d\n", batch.size());
        batch.forEach(req ->{
            System.out.printf("    [%s] Content-type=%s Headers=%s\n", req.getTopic(), req.getContentType(), req.getHeaders());
        });

    }

    /** Language that can be accumulated (their syntax allows concatenation */
    private static Set<Lang> isConcatLang = Set.of(Lang.NTRIPLES, Lang.NQUADS, Lang.TURTLE, Lang.TRIG);

    private static boolean canConcatenate(String contentType) {
        Lang lang = RDFLanguages.contentTypeToLang(contentType);
        if ( lang == null )
            return false;
        return isConcatLang.contains(lang);
    }

    /** Helper class for batching ConsumerRecords */
    private static class Accumulator {
        private boolean completed = false;
        final ByteArrayOutputStream bout;
        int numBatch = 0;
        long accumulatedSize = 0;
        final Map<String, String> headers;
        final String contentType;
        final String topic;

        Accumulator(Map<String, String> headers, String contentType, String topic) {
            super();
            this.bout = new ByteArrayOutputStream(1024);
            this.numBatch = 0;
            this.accumulatedSize = 0;
            this.headers = headers;
            this.contentType = contentType;
            this.topic = topic;
        }

        public void merge(RequestFK action) {
            if ( completed )
                throw new IllegalStateException("FKRequestProcessor.Accumulator already completed");
            // Copy the bytes
            long x = copy(action, bout);
            numBatch++;
            accumulatedSize+= x;
        }

        public void complete(java.util.function.Consumer<RequestFK> sink) {
            RequestFK batchedRequest = toRequestFK();
            sink.accept(batchedRequest);
            numBatch = 0;
            accumulatedSize = 0;
        }

        // Call once per batched item
        private RequestFK toRequestFK() {
            if ( completed )
                throw new IllegalStateException("FKRequestProcessor.Accumulator already completed");
            completed = true;
            RequestFK request = newRequestFK(topic, headers, bout.toByteArray());
            return request;
        }
    }
}
