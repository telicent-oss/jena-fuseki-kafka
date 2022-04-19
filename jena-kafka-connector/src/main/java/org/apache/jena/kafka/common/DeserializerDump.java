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

package org.apache.jena.kafka.common;

import static org.apache.jena.kafka.FusekiKafka.hContentType;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.riot.*;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/** Print out the events. */
public class DeserializerDump implements Deserializer<String> {

    public DeserializerDump() {}

    @Override
    public String deserialize(String topic, Headers headers, byte[] data) {
        StringBuilder sbuff = new StringBuilder();
        AtomicBoolean a = new AtomicBoolean(false);
        headers.forEach(h->{
            String k = h.key();
            String v = Bytes.bytes2string(h.value());
            sbuff.append(String.format("%-10s %s\n", k+":", v));
            a.set(true);
        });

        if ( a.get() )
            sbuff.append("\n");

        try {
            // Library
            String contentType = Bytes.bytes2string(headers.lastHeader(hContentType).value());
            if ( WebContent.contentTypeSPARQLUpdate.equals(contentType) ) {
                bodyUpdate(sbuff, topic, data);
                return sbuff.toString();
            } else {
                Lang lang = RDFLanguages.contentTypeToLang(contentType);
                if ( lang != null ) {
                    bodyLang(sbuff, topic, lang, data);
                    return sbuff.toString();
                }
            }
        } catch (RuntimeException ex) {}
        return Bytes.bytes2string(data);
    }

    private void bodyLang(StringBuilder sbuff, String topic, Lang lang, byte[] data) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        RDFDataMgr.read(dsg, bytesIn, lang);
        StringWriter sw = new StringWriter();
        RDFDataMgr.write(sw, dsg, RDFFormat.TRIG_BLOCKS);
        sbuff.append(sw.toString());
    }

    private void bodyUpdate(StringBuilder sbuff, String topic, byte[] data) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        UpdateRequest req = UpdateFactory.read(bytesIn);
        sbuff.append(req.toString());
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }
}
