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

package org.apache.jena.fuseki.kafka.lib;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.util.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

public class FKLib {

    private static final Logger LOG = FusekiKafka.LOG;

    // -- send file
    public static String ctForFile(String fn) {
        String ct = null;
        String ext = FileUtils.getFilenameExt(fn);
        if (Lib.equals("ru", ext)) {
            ct = WebContent.contentTypeSPARQLUpdate;
        } else if (Lib.equals("rdfp", ext)) {
            ct = WebContent.contentTypePatch;
        } else {
            Lang lang = RDFLanguages.filenameToLang(fn);
            if (lang != null) {
                ct = lang.getContentType().getContentTypeStr();
            }
        }
        return ct;
    }

    /**
     * Send files
     */
    public static Producer<String, String> producer(Properties props) {
        StringSerializer serString1 = new StringSerializer();
        StringSerializer serString2 = new StringSerializer();
        return new KafkaProducer<>(props, serString1, serString2);
    }

    public static void sendFiles(Properties props, String topic, List<String> files) {
        try (Producer<String, String> producer = producer(props)) {
            sendFiles(producer, null, topic, files);
        }
    }

    public static void sendString(Properties props, String topic, String contentType, String content) {
        try (StringSerializer serString1 = new StringSerializer(); StringSerializer serString2 = new StringSerializer(); Producer<String, String> producer = producer(
                props)) {
            sendString(producer, null, topic, contentType, content);
        }
    }

    private static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void sendFiles(Producer<String, String> producer, Integer partition, String topic,
                                  List<String> files) {
        for (String fn : files) {
            RecordMetadata res = sendFile(producer, partition, topic, fn);
            if (res == null) {
                FmtLog.error(LOG, "[%s] Error: sendFile %s", topic, fn);
            } else if (!res.hasOffset()) {
                FmtLog.info(LOG, "[%s] No offset", topic);
            } else {
                FmtLog.info(LOG, "[%s] Send: %s: Offset = %s", topic, fn, res.offset());
            }
        }
    }

    private static void sendString(Producer<String, String> producer, Integer partition, String topic,
                                   String contentType, String content) {
        List<Header> headers = (contentType != null) ? List.of(header(HttpNames.hContentType, contentType)) : List.of();
        RecordMetadata res = sendBody(producer, partition, topic, headers, content);
        if (res == null) {
            FmtLog.error(LOG, "[%s] Error: sendString", topic);
        } else if (!res.hasOffset()) {
            FmtLog.info(LOG, "[%s] sendString: No offset", topic);
        } else {
            FmtLog.info(LOG, "[%s] sendString: Offset = %s", topic, res.offset());
        }
    }

    private static RecordMetadata sendFile(Producer<String, String> producer, Integer partition, String topic,
                                           String fn) {
        String ct = ctForFile(fn);
        String body = IO.readWholeFileAsUTF8(fn);
        List<Header> headers = (ct != null) ? List.of(header(HttpNames.hContentType, ct)) : List.of();
        return sendBody(producer, partition, topic, headers, body);
    }

    private static RecordMetadata sendBody(Producer<String, String> producer, Integer partition, String topic,
                                           List<Header> headers, String body) {
        try {
            ProducerRecord<String, String> pRec = new ProducerRecord<>(topic, partition, null, null, body, headers);
            Future<RecordMetadata> f = producer.send(pRec);
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }
}
