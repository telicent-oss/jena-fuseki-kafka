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

package org.apache.jena.fuseki.kafka.lib;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.kafka.common.DeserializerDump;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.util.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class FKLib {

    // -- send file
    public static String  ctForFile(String fn) {
        String ct = null;
        if ( ct == null ) {
            String ext = FileUtils.getFilenameExt(fn);
            if ( Lib.equals("ru", ext) )
                ct = WebContent.contentTypeSPARQLUpdate;
            else {
                Lang lang = RDFLanguages.filenameToLang(fn);
                if ( lang != null )
                    ct = lang.getContentType().getContentTypeStr();
            }
        }
        return ct;
    }

    /** Send files */
    public static Producer<String, String> producer(Properties props) {
        StringSerializer serString1 = new StringSerializer();
        StringSerializer serString2 = new StringSerializer();
        Producer<String, String> producer = new KafkaProducer<>(props, serString1, serString2);
        return producer;
    }

    public static void send(Properties props, String topic, List<String> files) {
        try ( StringSerializer serString1 = new StringSerializer();
              StringSerializer serString2 = new StringSerializer();
              Producer<String, String> producer = producer(props) ) {
            send(producer, null, topic, files);
        }
    }

    private static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void send(Producer<String, String> producer, Integer partition, String topic, List<String> files) {
        for ( String fn : files ) {
            RecordMetadata res = sendFile(producer, partition, topic, fn);
            if ( res == null )
                System.out.println("Error");
            else if ( ! res.hasOffset() )
                System.out.println("No offset");
            else
                System.out.println("Send: Offset = "+res.offset());
        }
    }

    public static RecordMetadata sendFile(Producer<String, String> producer, Integer partition, String topic, String fn) {
        String ct = ctForFile(fn);
        String body = IO.readWholeFileAsUTF8(fn);
        List<Header> headers = ( ct != null ) ? List.of(header(HttpNames.hContentType, ct)) : List.of();
        RecordMetadata res = sendBody(producer, partition, topic, headers, body);
        return res;
    }

    public static RecordMetadata sendBody(Producer<String, String> producer, Integer partition, String topic, List<Header> headers, String body) {
        try {
            ProducerRecord<String, String> pRec = new ProducerRecord<>(topic, partition, null, null, body, headers);
            Future<RecordMetadata> f = producer.send(pRec);
            RecordMetadata res = f.get();
            return res;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void receive(DataState dataState, Properties cProps, String topic, BiConsumer<ConsumerRecord<String, String>, Long> handler) {
        try ( StringDeserializer strDeser = new StringDeserializer();
              DeserializerDump deSer = new DeserializerDump();
              Consumer<String, String> consumer = new KafkaConsumer<String, String>(cProps, strDeser, deSer)){
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            consumer.assign(Arrays.asList(topicPartition));
            long initialOffset = dataState.getOffset();
            if ( initialOffset < 0 )
                consumer.seekToBeginning(Arrays.asList(topicPartition));
            receiverLoop(consumer, dataState, handler);
        }
    }

    // Receive until empty.
    private static void receiverLoop(Consumer<String, String> consumer,
                                DataState dataState, BiConsumer<ConsumerRecord<String, String>, Long> handler) {
        for ( ;; ) {
            boolean somethingReceived = receiver(consumer, dataState, handler);
            if ( ! somethingReceived )
                break;
        }
    }

    // Once round the polling loop.

    private static boolean receiver(Consumer<String, String> consumer, DataState dataState,
                                    BiConsumer<ConsumerRecord<String, String>, Long> handler) {
        final long lastOffsetState = dataState.getOffset();
        long newOffset = receiverStep(dataState.getOffset(), consumer, handler);
        //System.out.println("Batch end");
        if ( newOffset == lastOffsetState )
            return false;
        //FmtLog.info(LOG, "Offset: %d -> %d", lastOffsetState, newOffset);
        dataState.setOffset(newOffset);
        return true;
    }

    // Do the Kafka-poll/wait.
    private static long receiverStep(final long lastOffsetState, Consumer<String, String> consumer,
                                     BiConsumer<ConsumerRecord<String, String>, Long> handler) {
        ConsumerRecords<String, String> cRec = consumer.poll(Duration.ofMillis(5000));
        long lastOffset = lastOffsetState;
        int count = cRec.count();
        for ( ConsumerRecord<String, String> rec : cRec ) {
            handler.accept(rec, rec.offset());
            long offset = rec.offset();
            if ( offset != lastOffset+1 )
                FmtLog.warn(FusekiKafka.LOG, "WARNING: Inconsistent offsets: offset=%d, lastOffset = %d\n", offset, lastOffset);
            lastOffset = offset;
        }
        return lastOffset;
    }
}
