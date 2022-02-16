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

import java.util.Objects;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.other.G;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.graph.NodeConst;

/**
 * This class is the beginnings of assembler-like functionality working at the Graph level.
 * Very WIP experimentation.
 */
class Assem2 {

    // Get required String
    static String getString(Graph graph, Node node, Node property) {
        Node x = G.getOneSP(graph, node, property);
        if ( Util.isSimpleString(x) )
            return x.getLiteralLexicalForm();
        throw error(node, property, "Not a string");
    }

    static String getStringOrDft(Graph graph, Node node, Node property, String defaultString) {
        Node x = G.getZeroOrOneSP(graph, node, property);
        if ( x == null )
            return defaultString;
        if ( Util.isSimpleString(x) )
            return x.getLiteralLexicalForm();
        throw error(node, property, "Not a string");
    }

    static Boolean getBooleanOrDft(Graph graph, Node node, Node property, Boolean dftValue) {
        Node x = G.getZeroOrOneSP(graph, node, property);
        if ( x == null )
            return dftValue;
        if ( Objects.equals(x, NodeConst.FALSE) )
            return false;
        if ( Objects.equals(x, NodeConst.TRUE) )
            return true;
        throw error(node, property, "Not a boolean literal");
    }

    static FusekiKafkaException error(Node node, String msg) {
        return new FusekiKafkaException(NodeFmtLib.displayStr(node)+" : "+msg);
    }

    static FusekiKafkaException error(Node node, Node property, String msg) {
        return new FusekiKafkaException(NodeFmtLib.displayStr(node)+" "+NodeFmtLib.displayStr(property)+" : "+msg);
    }
}
