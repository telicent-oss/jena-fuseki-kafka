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

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.slf4j.Logger;

/**
 * Deserialize and directly call perform an action on a dataset.
 * This does not go though the Fuseki dispatch-logging cycle.
 *
 * @see DeserializerDispatch
 */
public class DeserializerAction extends AbstractDeserializerRDF {

    private static Logger LOG = FusekiKafka.LOG;
    private final DatasetGraph dataset;
    private static AtomicLong counter = new AtomicLong(0);

    public DeserializerAction(DatasetGraph dataset) {
        this.dataset = dataset;
    }

    @Override
    protected void actionSparqlUpdate(String topic, InputStream data) {
        String id = actionId();
        try {
            FmtLog.info(LOG,"[%s] SPARQL Update",id);
            dataset.executeWrite(()->{
                UpdateRequest req = UpdateFactory.read(data);
                UpdateExec.dataset(dataset).update(req).execute();
            });
        } catch (QueryParseException ex) {
            ex.printStackTrace();
        } catch (QueryException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    protected void actionData(String topic, Lang lang, InputStream data) {
        String id = actionId();
        try {
            dataset.executeWrite(()->{
                FmtLog.info(LOG,"[%s] RDF data (%s)",id, lang.getLabel());
                RDFParser.source(data).lang(lang).parse(dataset);
            });
        } catch (RiotParseException ex) {
            ex.printStackTrace();
        }
    }

    private static String actionId() {
        long x = counter.incrementAndGet();
        return String.format("Event %d", x);
    }
}

