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
class FKRequestBatcher {
    final static private boolean DEBUG = false;
    private static boolean BATCHING_ACTIVE = true;
    private static String ENV_BATCHING = "FK_BATCHING";

    /**
     * Maximum items to consolidate into a batch.
     * This is within a {@link ConsumerRecords} - this process does not consolidate across calls of {@code consumer.poll}.
     * Default Kafka per poll size is 500.
     * Set to -1 to turn off batching within {@code ConsumerRecords}
     */
    private static int MAX_BATCH_SIZE = -1;
    private static String ENV_MAX_BATCH_SIZE = "FK_MAX_BATCH_SIZE";

    /**
     * Limit the size in bytes of message accumulation
     * If a ConsumerRecords/RequestFK would go over this limit,
     * the batch is ended and and batching starts again.
     * Set to -1 to turn off.
     */
    private static int MAX_BATCH_BYTES = 500_000;
    private static String ENV_MAX_BATCH_BYTES = "MAX_BATCH_BYTES";

    /**
     * Limit on the size of items to merge.
     * Don't consider merging a RequestFK large than this.
     * Set to -1 to turn off.
     */
    private static int LARGE_ITEM_BYTES = 100_000;
    private static String ENV_LARGE_ITEM_BYTES = "FK_LARGE_ITEM_BYTES";

    // -- Copied from jena 4.8.0
    /** Get an environment variable value; if not found try in the system properties. */
    public static String getenv(String name) {
        String x = System.getenv(name);
        if ( x == null )
            x = System.getProperty(name);
        return x;
    }
    // -- Copied

    /*package*/ static void setFromEnvironment() {

        String x0 = getenv(ENV_BATCHING);
        if ( x0 != null )
            safeSet(ENV_BATCHING, ()->BATCHING_ACTIVE = Boolean.parseBoolean(x0));


        String x1 = getenv(ENV_MAX_BATCH_SIZE);
        if ( x1 != null )
            safeSet(ENV_MAX_BATCH_SIZE,()->MAX_BATCH_SIZE = Integer.parseInt(x1));

        String x2 = getenv(ENV_MAX_BATCH_BYTES);
        if ( x2 != null )
            safeSet(ENV_MAX_BATCH_BYTES, ()->MAX_BATCH_BYTES = Integer.parseInt(x2));

        String x3 = getenv(ENV_LARGE_ITEM_BYTES);
        if ( x3 != null )
            safeSet(ENV_LARGE_ITEM_BYTES, ()->LARGE_ITEM_BYTES = Integer.parseInt(x3));
    }

    private static void safeSet(String name, Runnable action) {
        try {
            action.run();
        } catch (Throwable th) {
            FmtLog.warn(FKRequestBatcher.class, "Failed to set "+name+": "+th.getMessage());
        }
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
                if ( acc != null ) {
                    if ( DEBUG ) System.out.printf("Finish acc: %d\n", rec.offset());
                    acc.complete(sink);
                }
                if ( DEBUG ) System.out.printf("Add singleton");
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
                // Accumulating. Is this message compatible with the last?
                if ( mergeable(acc, action) ) {
                    if ( DEBUG ) System.out.printf("Accumulate: %d\n", rec.offset());
                    acc.merge(action);
                    continue;
                }
            }
            // Didn't merge, and not a large item.
            if ( acc != null ) {
                // Finish outstanding batch.
                if ( DEBUG ) System.out.printf("Finish acc: %d\n", rec.offset());
                acc.complete(sink);
                acc = null;
            }

            if ( DEBUG ) System.out.printf("Start acc: %d\n", rec.offset());
            // Set start of batch
            var accHeaders = new HashMap<>(action.getHeaders());
            // Drop the length.
            accHeaders.remove(HttpNames.hContentLength);
            // Start accumulator.
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

//    private boolean accHasSpace(Accumulator acc, RequestFK action) {
//        long x = action.getByteCount();
//        if ( MAX_BATCH_BYTES > 0 && acc.accumulatedSize + x > MAX_BATCH_BYTES )
//            return false;
//        return true;
//    }

    /** Whether mergable */
    private static boolean mergeable(Accumulator acc, RequestFK action) {
        if ( acc == null )
            return false;
        Map<String, String> headers = action.getHeaders();
        String contentType = action.getContentType();
        String topic = action.getTopic();

        if ( ! Objects.equals(acc.topic, topic) )
            return false;
        if ( ! Objects.equals(acc.contentType, contentType) )
            return false;
        if ( ! headerCompatible(acc.headers, headers) )
            return false;
        if ( ! canConcatenate(contentType) )
            return false;
        // Maximum batch size.
        if ( MAX_BATCH_SIZE > 0 && acc.numBatch >= MAX_BATCH_SIZE )
            return false;
        long x = action.getByteCount();
        if ( x >= 0  && acc.accumulatedSize + x >= MAX_BATCH_BYTES )
            return false;
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

    private static void print(List<RequestFK> batch) {
        System.out.printf("Batch: %d\n", batch.size());
        batch.forEach(req ->{
            System.out.printf("    [%s] Content-type=%s Headers=%s\n", req.getTopic(), req.getContentType(), req.getHeaders());
        });

    }

    private static RequestFK newRequestFK(String topic, Map<String, String> headers, byte[] byteArray) {
        int length = byteArray.length;
        headers.put(HttpNames.hContentLength, Integer.toString(length));
        RequestFK request = new RequestFK(topic, headers, byteArray);
        return request;
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
