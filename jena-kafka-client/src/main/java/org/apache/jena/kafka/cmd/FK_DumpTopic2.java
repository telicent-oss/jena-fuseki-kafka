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

import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.kafka.common.DeserializerDump;
import org.apache.jena.riot.RIOT;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


// Old(er) code - uses connector.ttl
public class FK_DumpTopic2 {

    static {
        LogCtl.setLog4j2();
        JenaSystem.init();
        RIOT.getContext().set(RIOT.symTurtleDirectiveStyle, "sparql");
    }

    public static void main(String... args) {
        // No args - assumes FK_Defaults.connectorFile
        LogCtl.setLog4j2();
        RIOT.getContext().set(RIOT.symTurtleDirectiveStyle, "sparql");

        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
        KConnectorDesc conn = (KConnectorDesc)AssemblerUtils.build(FK_Defaults.connectorFile, KafkaConnectorAssembler.getType());

        if ( conn == null ) {
            System.err.flush();
            System.out.println();
            System.out.println("FAILED");
            return;
        }

        String topic = conn.getTopic();

        // Client-side state management.
        DataState dState = DataState.createEphemeral(conn.getTopic());
        long lastOffset = dState.getOffset();

        // -- Props
        Properties cProps = conn.getKafkaConsumerProps();
        StringDeserializer strDeser = new StringDeserializer();
        DeserializerDump deSer = new DeserializerDump();
        Consumer<String, String> consumer = new KafkaConsumer<>(cProps, strDeser, deSer);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        // Resume.
        long initialOffset = dState.getOffset();
        if ( initialOffset < 0 )
            consumer.seekToBeginning(Arrays.asList(topicPartition));
        else {
            System.err.println("Should be replay");
        }

        for ( ;; ) {
            boolean somethingReceived = receiver(consumer, dState);
            if ( ! somethingReceived )
                break;
        }

        System.exit(0);
    }

    // Once round the polling loop.
    private static boolean receiver(Consumer<String, String> consumer, DataState dState) {
        final long lastOffsetState = dState.getOffset();
        long newOffset = receiverStep(dState.getOffset(), consumer);
        //System.out.println("Batch end");
        if ( newOffset == lastOffsetState )
            return false;
        //FmtLog.info(LOG, "Offset: %d -> %d", lastOffsetState, newOffset);
        dState.setOffset(newOffset);
        return true;
    }

    private final static AtomicBoolean seenFirst = new AtomicBoolean(false);
    private final static PrintStream output = System.out;

    // Do the Kafka-poll/wait.
    private static long receiverStep(final long lastOffsetState, Consumer<String, String> consumer) {
        ConsumerRecords<String, String> cRec = consumer.poll(Duration.ofMillis(5000));
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
            output.print(rec.value());
            if ( offset != lastOffset+1 )
                output.printf("WARNING: Inconsistent offsets: offset=%d, lastOffset = %d\n", offset, lastOffset);
            lastOffset = offset;
        }

        return lastOffset;
    }
}
