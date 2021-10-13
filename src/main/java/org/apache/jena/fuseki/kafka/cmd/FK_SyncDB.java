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

package org.apache.jena.fuseki.kafka.cmd;

import java.util.Arrays;
import java.util.Properties;

import org.apache.jena.fuseki.kafka.*;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Sync a database.
 */
public class FK_SyncDB {

    public static void main(String... args) {
        FusekiLogging.setLogging();

        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
        ConnectorFK conn = (ConnectorFK)AssemblerUtils.build("assembler.ttl", KafkaConnectorAssembler.getType());

        if ( conn == null ) {
            System.err.flush();
            System.out.println();
            System.out.println("FAILED");
            return;
        }

        String topic = conn.getTopic();
        String dsName = "/ds";
        String DIR = "Databases";
        String tdbDatabase = DIR+"/TDB2";
        String stateFile = DIR+"/state-file";

        // Arguments. ?? Assembler?
        DatasetGraph dsg = DatabaseMgr.connectDatasetGraph(tdbDatabase);
        PersistentState state = new PersistentState(stateFile);
        DataState dState = new DataState(state, dsName, conn.getTopic());
        {
            long offset = dState.getOffset();
            String x = (offset<0) ? "<empty>" : "Offset: "+Long.toString(offset);
            System.out.println(x);
        }

        // LIBRARY
        // -- Kafka Props
        Properties cProps = conn.getKafkaProps();
        StringDeserializer strDeser = new StringDeserializer();

        // Do work in deserializer or return a function that is the handler.
        Deserializer<Void> reqDer = new DeserializerAction(dsg);

        Consumer<String, Void> consumer = new KafkaConsumer<String, Void>(cProps, strDeser, reqDer);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        // Resume or start from the beginning.
        long initialOffset = dState.getOffset();
        if ( initialOffset < 0 )
            consumer.seekToBeginning(Arrays.asList(topicPartition));
        else
            consumer.seek(topicPartition, initialOffset+1);

        for ( ;; ) {
            boolean somethingReceived = FK.receiver(consumer, dState);
            if ( somethingReceived ) {
                System.out.println("Offset: "+dState.getOffset());
//                System.out.println("----");
//                RDFDataMgr.write(System.out, dsg,  Lang.TRIG);
//                System.out.println("----");
            }
        }
    }

}
