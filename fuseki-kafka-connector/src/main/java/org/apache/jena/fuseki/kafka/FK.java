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

package org.apache.jena.fuseki.kafka;

import java.time.Duration;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/** Cmd support library */
public class FK {

    private FK() {}

    /**
     * Once round the polling loop, updating the record.
     * Return true if some processing happened.
     */
    public static boolean receiver(Consumer<String, Void> consumer, DataState dState) {
        final long lastOffsetState = dState.getOffset();
        try {
            long newOffset = FK.receiverStep(dState.getOffset(), consumer);
            if ( newOffset == lastOffsetState )
                return false;
            dState.setOffset(newOffset);
            return true;
        } catch (RuntimeException ex) {
            Log.error(FusekiKafka.LOG, ex.getMessage(), ex);
            return false;
        }
    }

    /** Do one Kafka consumer poll step. */
    public static long receiverStep(long lastOffsetState, Consumer<String, Void> consumer) {
        ConsumerRecords<String, Void> cRec = consumer.poll(Duration.ofMillis(1000));
        long lastOffset = lastOffsetState;
        int count = cRec.count();

        for ( ConsumerRecord<String, Void> rec : cRec ) {
            long offset = rec.offset();
            //FmtLog.info(FusekiKafka.LOG, "Record Offset %s", offset);
            if ( offset != lastOffset+1)
                FmtLog.warn(FusekiKafka.LOG, "WARNING: Inconsistent offsets: offset=%d, lastOffset = %d\n", offset, lastOffset);

//            rec.headers().forEach(h->{
//                System.out.println("H: "+h.key()+" = "+Bytes.bytes2string(h.value()));
//            });
            lastOffset = offset;
        }
        return lastOffset;
    }
}
