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

package org.apache.jena.fuseki.kafka.lib;

import java.io.InputStream;
import java.io.StringWriter;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.fuseki.kafka.FKProcessor;
import org.apache.jena.kafka.ActionFK;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;

public class FKPrintRequest {
    /**
     * Parse the action according to Content-Type and print the outcome parsing.
     */
    public static void parsePrint(ActionFK action) {
        // If this is removed, change FKProcessor.action to protected

        FKProcessor proc = new FKProcessor() {
            @Override
            protected void actionSparqlUpdate(String topic, InputStream data) {
                //printRaw(topic, data);
                UpdateRequest up = UpdateFactory.read(data);
                String dataStr = up.toString();
                print(topic, dataStr);
            }

            @Override
            protected void actionRDFPatch(String topic, Lang lang, InputStream data) {
                //printRaw(topic, data);
                RDFPatch patch = RDFPatchOps.read(data);
                String dataStr = patch.toString();
                print(topic, dataStr);
            }

            @Override
            protected void actionData(String topic, Lang lang, InputStream data) {
                //printRaw(topic, data);
                StringWriter sw = new StringWriter();
                StreamRDF stream = StreamRDFLib.writer(sw);
                RDFParser.source(data).lang(lang).parse(stream);
                String dataStr = sw.toString();
                print(topic, dataStr);
            }

            private void printRaw(String topic, InputStream data) {
                System.out.println("== Topic: "+topic);
                String dataStr = IO.readWholeFileAsUTF8(data);
                System.out.print(dataStr);
                if ( ! dataStr.endsWith("\n") )
                    System.out.println();
                System.out.println();
            }

            private void print(String topic, String dataStr) {
                System.out.println("== Topic: "+topic);
                System.out.print(dataStr);
                if ( ! dataStr.endsWith("\n") )
                    System.out.println();
                System.out.println("--");
            }

        };
        proc.action(action.getContentType(), action.getTopic(), action.getBytes());
    }

}
