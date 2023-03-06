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

import java.util.Objects;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.other.G;
import org.apache.jena.riot.other.RDFDataException;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.graph.NodeConst;

/**
 * This class is the beginnings of assembler-like functionality working at the Graph level.
 * Very WIP experimentation.
 */
class Assem2 {

    /** Generator of exceptions for operations. */
    public interface OnError {
        RuntimeException exception(String errorMsg);
    }

    private static OnError dftErrorException = errorMsg -> new RDFDataException(errorMsg);

//    /**
//     * Get a required String from a object that is xsd:string.
//     * <p>
//     * If absent, multi-valued or not an xsd:string, then throw {@link RDFDataException}.
//     */
//    static String getString(Graph graph, Node node, Node property) {
//        return getString(graph, node, property, dftErrorException);
//    }

    /**
     * Get a required String from a object that is xsd:string.
     * <p>
     * If absent, multi-valued or not an xsd:string, then throw a custom runtime
     * exception.
     */
    public static String getString(Graph graph, Node node, Node property, OnError onError) {
        Objects.requireNonNull(graph);
        Objects.requireNonNull(onError);
        Node x = G.getOneSP(graph, node, property);
        if ( Util.isSimpleString(x) )
            return x.getLiteralLexicalForm();
        throw onError(node, property, "Not a string", onError);
    }

    public static RuntimeException onError(Node node, Node property, String errorMsg, OnError onError) {
        String eMsg = NodeFmtLib.displayStr(node)+" "+NodeFmtLib.displayStr(property)+" : "+errorMsg;
        return onError.exception(eMsg);
    }

    public static RuntimeException onError(Node node, String errorMsg, OnError onError) {
        String eMsg = NodeFmtLib.displayStr(node)+" : "+errorMsg;
        return onError.exception(eMsg);
    }


//    /**
//     * Get a string from a URI or an xsd:string.
//     * Otherwise throw {@link RDFDataException}.
//     */
//    public static String getAsString(Graph graph, Node node, Node property) {
//        return getAsString(graph, node, property, dftErrorException);
//    }

    /**
     * Get a string from a URI or an xsd:string.
     * Otherwise throw a custom runtime exception.
     */
    public static String getAsString(Graph graph, Node node, Node property, OnError onError) {
        Objects.requireNonNull(graph);
        Objects.requireNonNull(onError);
        Node obj = G.getOneSP(graph, node, property);
        if ( obj == null )
            return null;
        if ( obj.isURI() )
            return obj.getURI() ;
        if ( Util.isSimpleString(obj) )
            return obj.getLiteralLexicalForm();
        throw onError(node, property, "Not a URI or a string", onError);
    }

    /**
     * Get a String from an xsd:string or return a default value if no such subject-property.
     * Error if the object is not a string or multi-valued.
     */
    public static String getStringOrDft(Graph graph, Node node, Node property, String defaultString, OnError onError) {
        Node x = G.getZeroOrOneSP(graph, node, property);
        if ( x == null )
            return defaultString;
        if ( Util.isSimpleString(x) )
            return x.getLiteralLexicalForm();
        throw onError(node, property, "Not a single-valued string for subject-property", onError);
    }

    /**
     * Get a boolean.
     * Return null for no such subject-property.
     * Error if the object is not a string.
     */
    public static boolean getBooleanOrDft(Graph graph, Node node, Node property, boolean dftValue, OnError onError) {
        Node x = G.getZeroOrOneSP(graph, node, property);
        if ( x == null )
            return dftValue;
        if ( Objects.equals(x, NodeConst.FALSE) )
            return false;
        if ( Objects.equals(x, NodeConst.TRUE) )
            return true;
        throw onError(node, property, "Not a single-valued boolean for subject-property", onError);
    }
}
