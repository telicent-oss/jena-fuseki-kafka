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

package org.apache.jena.kafka.cmd;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.cmd.CmdException;
import org.apache.jena.cmd.CmdGeneral;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RIOT;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.util.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

/** Send one file. */
public class FK_Send extends CmdGeneral {

    static final ArgDecl argServer      = new ArgDecl(ArgDecl.HasValue, "server", "s") ;
    static final ArgDecl argTopic       = new ArgDecl(ArgDecl.HasValue, "topic", "t") ;
    static final ArgDecl argContentType = new ArgDecl(ArgDecl.HasValue, "content-type", "ct") ;
    static final ArgDecl argKafkaHeader = new ArgDecl(ArgDecl.HasValue, "header", "H") ;

    static {
        LogCtl.setLog4j2();
        JenaSystem.init();
        RIOT.getContext().set(RIOT.symTurtleDirectiveStyle, "sparql");
    }

    public static void main(String... args) {
        new FK_Send(args).mainRun() ;
    }

    private String server = null;
    private String topic = null;
    private String contentType = null;
    private List<String> kafkaHeadersStr = null;
    private List<Header> kafkaHeaders = null;

    public FK_Send(String... args) {
        super(args) ;
        super.add(argServer) ;
        super.add(argTopic) ;
        super.add(argContentType) ;
        super.add(argKafkaHeader);
    }

    @Override
    protected String getCommandName() {
        return "fksend";
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server BOOTSTRAP [-ct MIMETYPE] FILE...";
    }

    @Override
    protected void processModulesAndArgs() {
        if ( ! contains(argServer) )
            throw new CmdException("No --server (-s) argument");
        server = getValue(argServer);

        if ( ! super.contains(argTopic) )
            throw new CmdException("No --topic (-t) argument");
        topic = getValue(argTopic);

        kafkaHeadersStr = super.getValues(argKafkaHeader);
        kafkaHeaders = kafkaHeadersStr.stream().map(h->kafkaHeader(h)).collect(Collectors.toList());
        // Add a content type header.
        contentType = super.getValue(argContentType);
        if ( contentType != null )
            kafkaHeaders.add(kafkaHeader(HttpNames.hContentType, contentType));
    }

    private static RecordMetadata send(Producer<String, String> producer, Integer partition, String topic, List<Header> headers, String body) {
        try {
            ProducerRecord<String, String> pRec = new ProducerRecord<>(topic, partition, null, null, body, headers);
            Future<RecordMetadata> f = producer.send(pRec);
            RecordMetadata res = f.get();
            return res;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void exec() {
        if ( getPositional().isEmpty() )
            throw new CmdException("Nothing to send") ;
        Properties props = new Properties();
        props.put("bootstrap.servers", server);
        try ( StringSerializer serString1 = new StringSerializer();
              StringSerializer serString2 = new StringSerializer();
              Producer<String, String> producer = new KafkaProducer<>(props, serString1, serString2)) {

            for ( String fn : getPositional() ) {
                exec1(producer, fn);
            }
        }
    }

    protected void exec1(Producer<String, String> producer, String fn) {
        List<Header> sendHeaders = new ArrayList<>(kafkaHeaders);
        boolean hasContentType = kafkaHeaders.stream().anyMatch(h->h.key().equalsIgnoreCase(HttpNames.hContentType));
        if ( ! hasContentType ) {
            String ct = chooseContentType(fn);
            if ( ct == null )
                throw new CmdException("Failed to determine the Content-type for '"+fn+"'");
            sendHeaders.add(kafkaHeader(HttpNames.hContentType, ct));
        }

        String body = IO.readWholeFileAsUTF8(fn);
        RecordMetadata res = send(producer, null, topic, sendHeaders, body);
        if ( res == null )
            System.out.println("Error");
        else if ( ! res.hasOffset() )
            System.out.println("No offset");
        else
            System.out.println("Send: Offset = "+res.offset());
    }


    private String chooseContentType(String fn) {
        String ext = FileUtils.getFilenameExt(fn);
        if ( Lib.equals("ru", ext) )
            return WebContent.contentTypeSPARQLUpdate;
        Lang lang = RDFLanguages.filenameToLang(fn);
        if ( lang != null )
            return lang.getContentType().getContentTypeStr();
        return null;
    }

    private static Header kafkaHeader(String key_value) {
        String[] a = key_value.split(":",2);
        if ( a.length != 2 )
            throw new CmdException("Bad header (format is \"name: value\"): "+key_value);
        String key = a[0].trim();
        String value = a[1].trim();
        return kafkaHeader(key, value);
    }

    static Header kafkaHeader(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
}

