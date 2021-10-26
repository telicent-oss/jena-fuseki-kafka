/*
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
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.apache.jena.fuseki.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Objects;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.fuseki.kafka.refs.RefBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Track the state of data ingested from Kafka.  */
public class DataState {
    static Logger LOG = LoggerFactory.getLogger(DataState.class);

    private static String fDataset = "dataset";
    private static String fTopic = "topic";
    private static String fOffset = "offset";

    private final String datasetName;
    private final String topic;
    private final RefBytes state;
    private long offset;

    public DataState(RefBytes state, String datasetName, String topic) {
        Objects.requireNonNull(state);
        Objects.requireNonNull(topic);
        Objects.requireNonNull(datasetName);
        this.state = state;
        this.offset = 0;
        this.datasetName = datasetName;
        this.topic = topic;
        this.offset = -1;
//        if ( offset < 0 )
//            throw new IllegalArgumentException("Negative offset");
        if ( state.getBytes().length != 0 ) {
            // Existing.
            InputStream bout = new ByteArrayInputStream(state.getBytes());
            JsonObject obj = JSON.parse(bout);
            setFromJson(obj);
        } else {
            // New.
            writeState();
        }
    }

    // Protobuf?!

    private void writeState() {
        // Via JSON.
        JsonObject obj = asJson();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try ( IndentedWriter b = new IndentedWriter(output) ) {
            b.setFlatMode(true);
            JSON.write(b, obj);
            b.println();
        }
        state.setBytes(output.toByteArray());
    }

    @Override
    public String toString() {
        JsonObject obj = asJson();
        return JSON.toStringFlat(obj);
    }

    private JsonObject asJson() {
        return JSON.buildObject(builder->builder
                                .pair(fDataset, datasetName)
                                .pair(fTopic,   topic)
                                .pair(fOffset,  offset)
        );
    }

    private void setFromJson(JsonObject obj) {
        String datasetName = obj.getString(fDataset);
        if ( datasetName == null )
            throw new FusekiKafkaException("No dataset name: "+JSON.toStringFlat(obj));
        String topic  = obj.getString(fTopic);
        if ( topic == null )
            throw new FusekiKafkaException("No topic name: "+JSON.toStringFlat(obj));
        Number offsetNum = obj.getNumber(fOffset);
        if ( offsetNum == null )
            throw new FusekiKafkaException("No offset: "+JSON.toStringFlat(obj));
        long offset = offsetNum.longValue();

        if ( ! this.datasetName.equals(datasetName) )
            throw new FusekiKafkaException("Dataset name does not match: this="+this.datasetName+ " / read=" +datasetName);
        if ( ! this.topic.equals(topic) )
            throw new FusekiKafkaException("Topic does not match: this="+this.datasetName+ " / read=" +topic);
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
        writeState();
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getTopic() {
        return topic;
    }

}

//    private String name;
//    private String uri;
//    private RefString stateStr;
//    private PersistentState state;
//
//    // State is:
//    //   version - at least an int
//    //   dsRef - id
//    //   (lastpatchId).
//    // Stored as JSON:
//    // {
//    //   version:    int
//    //   datasource: string/uuid
//    //   patch:      string/uuid
//    // }
//
//    /** Create from existing state */
//    /*package*/ DataState(Zone zone, PersistentState state) {
//        this.zone = zone;
//        this.state = state;
//        this.stateStr = state;
//        setStateFromString(this, state.getString());
//        //FmtLog.info(LOG, "%s", this);
//    }
//
//    /** Create new, initialize state. */
//    /*package*/ DataState(Zone zone, Path stateFile, LocalStorageType storage, Id dsRef, String name, String uri, Version version, Id patchId) {
//        this.zone = zone;
//        this.datasource = dsRef;
//        if ( stateFile != null )
//            this.state = new PersistentState(stateFile);
//        else
//            this.state = PersistentState.createEphemeral();
//        this.stateStr = state;
//        this.version = version;
//        this.patchId = patchId;
//        this.name = name;
//        this.uri = uri;
//        this.storage = storage;
//        writeState(this);
//    }
//
//    public void refresh() {
//        if ( state != null )
//            load(getStatePath());
//    }
//
//    private void load(Path stateFile) {
//        state = new PersistentState(stateFile);
//        stateStr = state;
//        readState(stateStr);
//    }
//
//    public Version version() {
//        return version;
//    }
//
//    public Id latestPatchId() {
//        return patchId;
//    }
//
//    public Zone zone() {
//        return zone;
//    }
//
//    @Override
//    public String toString() {
//        return String.format("[DataState: %s version=%s patch=%s]", datasource, version(), latestPatchId());
//    }
//
//    public synchronized void updateState(Version newVersion, Id patchId) {
//        // Update the shadow data first. Replaying patches is safe.
//        // Update on disk.
//        writeState(this.stateStr, this.datasource, this.name, this.uri, this.storage, newVersion, patchId);
//        // Update local
//        this.version = newVersion;
//        this.patchId = patchId;
//    }
//
//    public Id getDataSourceId() {
//        return datasource;
//    }
//
////    public DataSourceDescription asDataSourceDescription() {
////        return new DataSourceDescription(datasource, name, uri);
////    }
//
//    /** Place on-disk where the state is stored. Use with care. */
//    public Path getStatePath() {
//        return state.getPath();
//    }
//
//    public String getDatasourceName() {
//        return name ;
//    }
//
//    public String getUri() {
//        return uri ;
//    }
//
//    public LocalStorageType getStorageType() {
//        return storage ;
//    }
//
//    private void readState(RefString state) {
//        setStateFromString(this, state.getString());
//    }
//
//
//    private void writeState(DataState dataState) {
//        writeState(dataState.stateStr, dataState.datasource, dataState.name, dataState.uri, dataState.storage, dataState.version, dataState.patchId);
//    }
//
//    /** Allow a different version so we can write the state ahead of changing in-memory */
//    private static void writeState(RefString state, Id datasource, String name, String uri, LocalStorageType storage, Version version, Id patchId) {
//        if ( state == null )
//            // Not persisted.
//            return ;
//        String x = stateToString(datasource, name, uri, storage, version, patchId);
//        if ( ! x.endsWith("\n") )
//            x = x+"\n";
//        state.setString(x);
//    }
//
//    private static String stateToString(Id datasource, String name, String uri, LocalStorageType storage, Version version, Id patchId) {
//        JsonObject json = stateToJson(datasource, name, uri, storage, version, patchId);
//        return JSON.toString(json);
//    }
//
//    private static JsonObject stateToJson(Id datasource, String name, String uri,  LocalStorageType storage, Version version, Id patchId) {
//        String x = "";
//        if ( patchId != null )
//            x = patchId.asPlainString();
//        String patchStr = x;
//        return
//            JSONX.buildObject(builder->{
//                builder
//                    .pair(F_VERSION, version.asJson())
//                    .pair(F_ID, patchStr)
//                    .pair(F_NAME, name)
//                    .pair(F_DATASOURCE, datasource.asPlainString());
//
//                if ( storage != null )
//                    builder.pair(F_STORAGE, storage.typeName());
//                if ( uri != null )
//                    builder.pair(F_URI, uri);
//            });
//    }
//
//    /** Set version and datasource id from a string which is JOSN */
//    private static void setStateFromString(DataState state, String string) {
//        JsonObject obj = JSON.parse(string);
//        setFromJsonObject(state, obj);
//    }
//
//    /** JsonObject -> DataState */
//    private static void setFromJsonObject(DataState dataState, JsonObject sourceObj) {
//        Version version = Version.fromJson(sourceObj, F_VERSION, Version.UNSET);
//        if ( ! Version.isValid(version) ) {
//            if ( ! version.equals(Version.INIT) )
//                LOG.warn("Bad version: "+JSON.toStringFlat(sourceObj));
//        }
//        dataState.version = version;
//
//        String patchStr = JSONX.getStrOrNull(sourceObj, F_ID);
//        if ( patchStr == null || patchStr.isEmpty() ) {
//            dataState.patchId = null;
//        } else {
//            dataState.patchId = Id.fromString(patchStr);
//        }
//
//        String dsStr = JSONX.getStrOrNull(sourceObj, F_DATASOURCE);
//        if ( dsStr != null )
//            dataState.datasource = Id.fromString(dsStr);
//        else {
//            LOG.error("No datasource: "+JSON.toStringFlat(sourceObj));
//            throw new DeltaException("No datasource: "+JSON.toStringFlat(sourceObj));
//        }
//
//        String name = JSONX.getStrOrNull(sourceObj, F_NAME);
//        if ( name != null )
//            dataState.name = name;
//        else {
//            LOG.error("No datasource name: "+JSON.toStringFlat(sourceObj));
//            throw new DeltaException("No datasource name: "+JSON.toStringFlat(sourceObj));
//        }
//
//        String uri = JSONX.getStrOrNull(sourceObj, F_URI);
//        if ( uri != null )
//            dataState.uri = uri;
//
//        String typeName = JSONX.getStrOrNull(sourceObj, F_STORAGE);
//        LocalStorageType storage = LocalStorageType.fromString(typeName);
////        if ( storage == null )
////            throw new DeltaException("No storage type: "+JSON.toStringFlat(sourceObj));
//        dataState.storage = storage;
//    }
// }
