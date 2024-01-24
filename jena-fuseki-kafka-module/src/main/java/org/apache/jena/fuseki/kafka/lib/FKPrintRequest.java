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

package org.apache.jena.fuseki.kafka.lib;

import java.io.InputStream;
import java.io.StringWriter;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.fuseki.kafka.FKProcessorBaseAction;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.ResponseFK;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;

public class FKPrintRequest extends FKProcessorBaseAction {
    /** Print details of the request on stdout. */
    public static void parsePrint(RequestFK action) {
        ResponseFK response = new FKPrintRequest().process(action);
    }

    public FKPrintRequest() {}

    @Override
    public void startBatch(int batchSize, long startOffset) {}

    @Override
    public void finishBatch(int processedCount, long finishOffset, long startOffset) {}

    @Override
    protected void actionSparqlUpdate(String id, RequestFK request, InputStream data) {
        UpdateRequest up = UpdateFactory.read(data);
        String dataStr = up.toString();
        print(id, request, dataStr);
    }

    @Override
    protected void actionRDFPatch(String id, RequestFK request, InputStream data) {
        //printRaw(topic, data);
        RDFPatch patch = RDFPatchOps.read(data);
        String dataStr = patch.toString();
        print(id, request, dataStr);
    }

    @Override
    protected void actionData(String id, RequestFK request, Lang lang, InputStream data) {
        StringWriter sw = new StringWriter();
        StreamRDF stream = StreamRDFLib.writer(sw);
        RDFParser.source(data).lang(lang).parse(stream);
        String dataStr = sw.toString();
        print(id, request, dataStr);
    }

    private void printRaw(String id, RequestFK request, InputStream data) {
        System.out.println("== Request: "+id);
        String dataStr = IO.readWholeFileAsUTF8(data);
        System.out.print(dataStr);
        if ( ! dataStr.endsWith("\n") )
            System.out.println();
        System.out.println();
    }

    private void print(String id, RequestFK request, String dataStr) {
        System.out.println("== Request: "+id);
        System.out.print(dataStr);
        if ( ! dataStr.endsWith("\n") )
            System.out.println();
        System.out.println("--");
    }
}
