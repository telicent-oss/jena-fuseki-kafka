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

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.io.IOX;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.cmd.CmdException;
import org.apache.jena.cmd.CmdGeneral;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.riot.RIOT;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

// Dump topic.
public class FK_DumpTopic extends CmdGeneral {

    static final ArgDecl argServer = new ArgDecl(ArgDecl.HasValue, "server", "s");
    static final ArgDecl argTopic = new ArgDecl(ArgDecl.HasValue, "topic", "t");
    static final ArgDecl argMessages = new ArgDecl(ArgDecl.HasValue, "messages", "msgs");

    static {
        LogCtl.setLog4j2();
        JenaSystem.init();
        RIOT.getContext().set(RIOT.symTurtleDirectiveStyle, "sparql");
    }

    public static void main(String...args) {
        new FK_DumpTopic(args).mainRun();
    }

    private String server = null;
    private String topic = null;
    private String msgDirectory = null;

    public FK_DumpTopic(String...args) {
        super(args);
        super.add(argServer);
        super.add(argTopic);
        super.add(argMessages);
    }

    @Override
    protected String getCommandName() {
        return "fksend";
    }

    @Override
    protected String getSummary() {
        return getCommandName() + " --server BOOTSTRAP [-msgs DIR]";
    }

    @Override
    protected void processModulesAndArgs() {
        if ( !contains(argServer) )
            throw new CmdException("No --server (-s) argument");
        server = getValue(argServer);

        if ( !super.contains(argTopic) )
            throw new CmdException("No --topic (-t) argument");
        topic = getValue(argTopic);

        if ( contains(argMessages) )
            msgDirectory = getValue(argMessages);
    }

    private static RecordMetadata send(Producer<String, String> producer, Integer partition, String topic, List<Header> headers,
                                       String body) {
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

    @Override
    protected void exec() {
        if ( msgDirectory != null ) {
            FileOps.ensureDir(msgDirectory);
            FileOps.clearAll(msgDirectory);
        }
        Properties cProps = new Properties();
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put("bootstrap.servers", server);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);
        dumpTopic(consumer, topic);
    }

    private void dumpTopic(KafkaConsumer<String, String> consumer, String topic) {
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        consumer.seekToBeginning(Arrays.asList(topicPartition));

        DataState dState = DataState.createEphemeral(topic);
        for ( ;; ) {
            boolean somethingReceived = receiver(consumer, dState);
            System.out.println(dState.toString());
            if ( !somethingReceived ) {
                break;
            }
        }
    }

    private boolean receiver(Consumer<String, String> consumer, DataState dState) {
        final long lastOffsetState = dState.getLastOffset();
        long newOffset = receiverStep(dState.getLastOffset(), consumer);
        if ( newOffset == lastOffsetState )
            return false;
        // FmtLog.info(LOG, "Offset: %d -> %d", lastOffsetState, newOffset);
        dState.setLastOffset(newOffset);
        return true;
    }

    private long receiverStep(long lastOffsetState, Consumer<String, String> consumer) {
        if ( msgDirectory == null )
            return receiverStepConsole(System.out, lastOffsetState, consumer);
        return receiverStepDumpMessages(msgDirectory, lastOffsetState, consumer);
    }

    // ---- Write to console
    private final static AtomicBoolean seenFirst = new AtomicBoolean(false);

    // Do the Kafka-poll/wait.
    private long receiverStepConsole(PrintStream output, long lastOffsetState, Consumer<String, String> consumer) {
        ConsumerRecords<String, String> cRec = consumer.poll(Duration.ofMillis(1000));
        long lastOffset = lastOffsetState;
        int count = cRec.count();
        if ( seenFirst.get() ) {
            output.println();
            seenFirst.set(true);
        }

        boolean seenFirstInBatch = seenFirst.get();
        for ( ConsumerRecord<String, String> rec : cRec ) {
            if ( seenFirstInBatch )
                output.println();
            else
                seenFirstInBatch = true;
            long offset = rec.offset();
            output.printf("==--== Offset: %d ==--------==\n", offset);
            Headers headers = rec.headers();
            boolean seenHeader = false;
            for ( Header header : headers ) {
                output.printf("%-20s : %s\n", header.key(), StrUtils.fromUTF8bytes(header.value()));
                seenHeader = true;
            }
            if ( seenHeader )
                output.println();

            output.print(rec.value());
            if ( offset != lastOffset + 1 )
                output.printf("WARNING: Inconsistent offsets: offset=%d, lastOffset = %d\n", offset, lastOffset);
            lastOffset = offset;
        }

        return lastOffset;
    }

    // ---- Write to replayabale files.
    private static long receiverStepDumpMessages(String msgDirectory, long lastOffsetState, Consumer<String, String> consumer) {
        ConsumerRecords<String, String> cRec = consumer.poll(Duration.ofMillis(1000));
        long lastOffset = lastOffsetState;
        int count = cRec.count();
        for ( ConsumerRecord<String, String> rec : cRec ) {
            long offset = rec.offset();
            String msgFile = String.format("%s/data-%04d.msg", msgDirectory, offset);

            System.out.printf("==--== Write: %d ==--------==\n", offset);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrintWriter pw = IO.asPrintWriterUTF8(out);

            Headers headers = rec.headers();
            boolean seenHeader = false;
            for ( Header header : headers ) {
                pw.printf("%s: %s\n", header.key(), StrUtils.fromUTF8bytes(header.value()));
                seenHeader = true;
            }
            if ( seenHeader )
                pw.println();
            pw.print(rec.value());
            pw.flush();

            try ( OutputStream msgOutput = IO.openOutputFile(msgFile) ) {
                msgOutput.write(out.toByteArray());
            } catch (IOException ex) {
                IOX.exception(ex);
            }
            lastOffset = offset;
        }

        return lastOffset;
    }
}
