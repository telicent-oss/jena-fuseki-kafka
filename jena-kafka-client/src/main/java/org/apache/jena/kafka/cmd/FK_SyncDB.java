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

import java.util.Arrays;
import java.util.Properties;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.DeserializerActionFK;
import org.apache.jena.kafka.KafkaConnectorAssembler;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.common.DataState;
import org.apache.jena.kafka.common.PersistentState;
import org.apache.jena.riot.RIOT;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Sync a database.
 * Not with Fuseki running.
 */
public class FK_SyncDB {
    static {
        LogCtl.setLog4j2();
        JenaSystem.init();
        RIOT.getContext().set(RIOT.symTurtleDirectiveStyle, "sparql");
    }
    public static void main(String... args) {
        // No args - assumes FK_Defaults.connectorFile

        AssemblerUtils.registerAssembler(null, KafkaConnectorAssembler.getType(), new KafkaConnectorAssembler());
        KConnectorDesc conn = (KConnectorDesc)AssemblerUtils.build(FK_Defaults.connectorFile, KafkaConnectorAssembler.getType());

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
        DataState dState = DataState.create(state);
        {
            long offset = dState.getLastOffset();
            String x = (offset<0) ? "<empty>" : "Offset: "+Long.toString(offset);
            System.out.println(x);
        }

        // LIBRARY
        // -- Kafka Props
        Properties cProps = conn.getKafkaConsumerProps();
        StringDeserializer strDeser = new StringDeserializer();

        Deserializer<RequestFK> reqDer = new DeserializerActionFK(false, null);

        try ( Consumer<String, RequestFK> consumer = new KafkaConsumer<>(cProps, strDeser, reqDer) ) {
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            consumer.assign(Arrays.asList(topicPartition));

            // Resume or start from the beginning.
            long initialOffset = dState.getLastOffset();
            if ( initialOffset < 0 )
                consumer.seekToBeginning(Arrays.asList(topicPartition));
            else
                consumer.seek(topicPartition, initialOffset+1);

            /// XXX Unfinished.
            for ( ;; ) {
//                boolean somethingReceived = false;
//                if ( somethingReceived ) {
//                    System.out.println("Offset: "+dState.getOffset());
//                    System.out.println("----");
//                    RDFDataMgr.write(System.out, dsg,  Lang.TRIG);
//                    System.out.println("----");
//                }
            }
        }
    }

}
