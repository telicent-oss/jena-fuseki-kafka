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

package org.apache.jena.kafka.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Objects;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.refs.RefBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Track the state of data ingested from Kafka.  */
public class DataState {
    static Logger LOG = LoggerFactory.getLogger(DataState.class);

    // Rename as "localDispatch" and "remoteEndpoint"
    private static String fDataset = "dataset";
    private static String fEndpoint = "endpoint";
    private static String fTopic = "topic";
    private static String fOffset = "offset";

    private final String dispatchPath;
    private final String remoteEndpoint;
    private final String topic;
    private final RefBytes state;
    // This is the offset of the next state to write.
    // -1 : for uninitialized.
    //  0 : first to be written
    //  X : Last Kafka offset read is X-1.
    private long offset;

    /** Minimal dummy DataState */
    public static DataState createEphemeral(String topic) {
        PersistentState state = PersistentState.createEphemeral();
        return new DataState(state, "", "", topic);
    }

    public static DataState create(PersistentState state) {
        return fromJson(state);
    }

    private DataState(RefBytes state, String localDispatchPath, String remoteEndpoint, String topic) {
        this.state = state;
        this.dispatchPath = localDispatchPath;
        this.remoteEndpoint = (remoteEndpoint != null) ? remoteEndpoint : "" ;
        this.topic = topic;
        this.offset = -1;
    }

    public static DataState restoreOrCreate(RefBytes state, String localDispatchPath, String remoteEndpoint, String topic) {
        Objects.requireNonNull(state);
        Objects.requireNonNull(topic);
        Objects.requireNonNull(localDispatchPath);

        if ( state.getBytes().length == 0 ) {
            DataState dataState = new DataState(state, localDispatchPath, remoteEndpoint, topic);
            return dataState;
        }

        // Existing.
        InputStream bout = new ByteArrayInputStream(state.getBytes());
        JsonObject obj = JSON.parse(bout);
        DataState dataState = fromJson(state);
        checkExpectedSettings(dataState, localDispatchPath, remoteEndpoint, topic);
        return dataState;
    }

    private static void checkExpectedSettings(DataState dataState, String localDispatchPath, String remoteEndpoint, String topic) {
        if ( ! dataState.dispatchPath.equals(localDispatchPath) )
            throw new JenaKafkaException("Dataset name does not match: loaded="+dataState.dispatchPath+ " / expected=" +localDispatchPath);
        if ( ! Objects.equals(dataState.remoteEndpoint, remoteEndpoint) )
            throw new JenaKafkaException("Endpoint name does not match: loaded="+dataState.remoteEndpoint+ " / expected=" +remoteEndpoint);
        if ( ! dataState.topic.equals(topic) )
            throw new JenaKafkaException("Topic does not match: loaded="+dataState.dispatchPath+ " / expected=" +topic);
    }

    private void writeState() {
        if ( state == null )
            return;
        // Via JSON.
        JsonObject obj = asJson();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try ( IndentedWriter b = new IndentedWriter(output) ) {
            //b.setFlatMode(true);
            JSON.write(b, obj);
            b.println();
        }

        //FmtLog.info(LOG, "[%s] DataState new offset = %d", getTopic(), offset);
        state.setBytes(output.toByteArray());
    }

    @Override
    public String toString() {
        JsonObject obj = asJson();
        return JSON.toStringFlat(obj);
    }

    private JsonObject asJson() {
        return JSON.buildObject(builder-> {
            if ( dispatchPath != null )
                builder.pair(fDataset, dispatchPath);
            if ( remoteEndpoint != null )
                builder.pair(fEndpoint, remoteEndpoint);
            builder.pair(fTopic,   topic);
            builder.pair(fOffset,  offset);
            });
    }

    private void setFromJson(JsonObject obj) {
        String datasetName = obj.getString(fDataset);
        if ( datasetName == null )
            throw new JenaKafkaException("No dataset name: "+JSON.toStringFlat(obj));
        String endpoint = obj.getString(fEndpoint);
        String topic  = obj.getString(fTopic);
        if ( topic == null )
            throw new JenaKafkaException("No topic name: "+JSON.toStringFlat(obj));
        Number offsetNum = obj.getNumber(fOffset);
        if ( offsetNum == null )
            throw new JenaKafkaException("No offset: "+JSON.toStringFlat(obj));
        long offset = offsetNum.longValue();

        if ( ! this.dispatchPath.equals(datasetName) )
            throw new JenaKafkaException("Dataset name does not match: this="+this.dispatchPath+ " / read=" +datasetName);
        if ( ! Objects.equals(this.remoteEndpoint, endpoint) )
            throw new JenaKafkaException("Endpoint name does not match: this="+this.remoteEndpoint+ " / read=" +endpoint);
        if ( ! this.topic.equals(topic) )
            throw new JenaKafkaException("Topic does not match: this="+this.dispatchPath+ " / read=" +topic);
        this.offset = offset;
    }



    private static DataState fromJson(RefBytes state) {
        InputStream bout = new ByteArrayInputStream(state.getBytes());
        JsonObject obj = JSON.parse(bout);

        String datasetName = obj.getString(fDataset);
        if ( datasetName == null )
            throw new JenaKafkaException("No dataset name: "+JSON.toStringFlat(obj));
        String endpoint = obj.hasKey(fEndpoint) ? obj.getString(fEndpoint) : null;
        String topic  = obj.getString(fTopic);
        if ( topic == null )
            throw new JenaKafkaException("No topic name: "+JSON.toStringFlat(obj));
        Number offsetNum = obj.getNumber(fOffset);
        if ( offsetNum == null )
            throw new JenaKafkaException("No offset: "+JSON.toStringFlat(obj));
        long offset = offsetNum.longValue();

        DataState dataState = new DataState(state, datasetName, endpoint, topic);
        dataState.offset = offset;
        return dataState;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
        writeState();
    }

    public String getDatasetName() {
        return dispatchPath;
    }

    public String getTopic() {
        return topic;
    }
}
