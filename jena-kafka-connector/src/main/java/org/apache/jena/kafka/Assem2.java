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

package org.apache.jena.kafka;

import java.util.List;
import java.util.Objects;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.graph.NodeConst;
import org.apache.jena.system.G;

/**
 * This class is the beginnings of assembler-like functionality working at the Graph level. Very WIP experimentation.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class Assem2 {

    /**
     * Generator of exceptions for operations.
     */
    public interface OnError {
        RuntimeException exception(String errorMsg);
    }

    /**
     * Get a required String from an object that is {@code xsd:string}.
     * <p>
     * If absent, multivalued or not a {@code xsd:string}, then throw a custom runtime exception.
     */
    public static String getString(Graph graph, Node node, Node property, OnError onError) {
        Objects.requireNonNull(graph);
        Objects.requireNonNull(onError);
        Node x = G.getOneSP(graph, node, property);
        if (Util.isSimpleString(x)) {
            return x.getLiteralLexicalForm();
        }
        throw onError(node, property, "Not a string", onError);
    }

    public static RuntimeException onError(Node node, Node property, String errorMsg, OnError onError) {
        String eMsg = NodeFmtLib.displayStr(node) + " " + NodeFmtLib.displayStr(property) + " : " + errorMsg;
        return onError.exception(eMsg);
    }

    /**
     * Get a String from a {@code xsd:string} or return a default value if no such subject-property. Error if the object
     * is not a string or multivalued.
     */
    public static String getStringOrDft(Graph graph, Node node, Node property, String defaultString, OnError onError) {
        Node x = G.getZeroOrOneSP(graph, node, property);
        if (x == null) {
            return defaultString;
        }
        if (Util.isSimpleString(x)) {
            return x.getLiteralLexicalForm();
        }
        throw onError(node, property, "Not a single-valued string for subject-property", onError);
    }

    public static List<String> getStrings(Graph graph, Node node, Node property, OnError onError) {
        List<String> values = graph.stream(node, property, Node.ANY)
                                   .map(Triple::getObject)
                                   .filter(Util::isSimpleString)
                                   .map(Node::getLiteralLexicalForm)
                                   .toList();
        if (!values.isEmpty()) {
            return values;
        }
        throw onError(node, property, "No simple string values found for subject-property", onError);
    }

    /**
     * Get a boolean. Return null for no such subject-property. Error if the object is not a string.
     */
    public static boolean getBooleanOrDft(Graph graph, Node node, Node property, boolean dftValue, OnError onError) {
        Node x = G.getZeroOrOneSP(graph, node, property);
        if (x == null) {
            return dftValue;
        }
        if (Objects.equals(x, NodeConst.FALSE)) {
            return false;
        }
        if (Objects.equals(x, NodeConst.TRUE)) {
            return true;
        }
        throw onError(node, property, "Not a single-valued boolean for subject-property", onError);
    }
}
