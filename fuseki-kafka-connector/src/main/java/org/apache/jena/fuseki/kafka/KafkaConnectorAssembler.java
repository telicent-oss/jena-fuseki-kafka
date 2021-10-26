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

import java.util.Properties;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.JA;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.FusekiVocab;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.other.G;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Assembler for a Fuseki-Kakfa connector that takes Kafka events and executes them on
 * a Fuseki server.
 * This is an update stream, not publishing data to Kafka.
 */
public class KafkaConnectorAssembler extends AssemblerBase implements Assembler {

    private static String NS = "http://jena.apache.org/fuseki/kafka#";
    public static String getNS() { return NS; }

    private static Resource tKafkaConnector = ResourceFactory.createResource(NS+"Connector");
    private static Node pServiceName     = NodeFactory.createURI(NS+"serviceName");
    private static Node pService         = NodeFactory.createURI(NS+"service");
    private static Node pKafkaTopic      = NodeFactory.createURI(NS+"topic");
    private static Node pStateFile       = NodeFactory.createURI(NS+"stateFile");
    private static Node pKafkaProperty   = NodeFactory.createURI(NS+"config");
    private static Node pKafkaBootstrapServers = NodeFactory.createURI(NS+"bootstrapServers");
    private static Node pKafkaGroupId = NodeFactory.createURI(NS+"groupId");

    public static Resource getType() {
        return tKafkaConnector;
    }

    @Override
    public Object open(Assembler a, Resource root, Mode mode) {
        return create(root.getModel().getGraph(), root.asNode(), tKafkaConnector.asNode());
    }

    private ConnectorFK create(Graph graph, Node node, Node type) {
        try {
            return createSub(graph, node, type);
        } catch (RuntimeException ex) {
            System.err.println(ex.getMessage());
            return null;
        }
    }

    private ConnectorFK createSub(Graph graph, Node node, Node type) {
        // Required!
        String topic = getString(graph, node, pKafkaTopic);

        String serviceName = serviceName(graph, node);
        serviceName = DataAccessPoint.canonical(serviceName);

        String bootstrapServers = getString(graph, node, pKafkaBootstrapServers);
        String stateFile = getString(graph, node, pStateFile);

        String groupId = getStringOrDft(graph, node, pKafkaGroupId, "TelicentApp1");

        // ----
        Properties kafkaProps = new Properties();
        // "bootstrap.servers"
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // "group.id"
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Optional Kafka configuration as pairs of (key-value) as RDF lists.
        String queryString = StrUtils.strjoinNL
                    ( "PREFIX ja: <"+JA.getURI()+">"
                    , "SELECT ?k ?v { ?X ?P (?k ?v) }"
                    );

        QueryExec.graph(graph)
                .query(queryString)
                .substitution("X", node)
                .substitution("P", pKafkaProperty)
                .build().select()
                .forEachRemaining(row->{
                    Node nk = row.get("k");
                    String key = nk.getLiteralLexicalForm();
                    Node nv = row.get("v");
                    String value = nv.getLiteralLexicalForm();
                    kafkaProps.setProperty(key, value);
                });

        // These are ignored if the deserializers are in the Kafka consumer constructor.
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DeserializerDispatch.class.getName());

        return new ConnectorFK(topic, serviceName, stateFile, kafkaProps);
    }

    private String serviceName(Graph graph, Node node) {
        String PREFIXES = StrUtils.strjoinNL("PREFIX ja:     <"+JA.getURI()+">"
                                            ,"PREFIX fk:     <"+NS+">"
                                            ,"PREFIX fuseki: <"+FusekiVocab.NS+">"
                                            ,""
                                            );
        String queryString = StrUtils.strjoinNL
                ( PREFIXES
                , "SELECT ?n { "
                , "   OPTIONAL { ?X fk:service/fuseki:name ?N1 }"
                , "   OPTIONAL { ?X fk:serviceName ?N2 }"
                , "   BIND(COALESCE(?N1, ?N2, '') AS ?n)"
                , "}"
                );
        RowSet rowSet = QueryExec.graph(graph)
                .query(queryString)
                .substitution("X", node)
                .build()
                .select();
        if ( !rowSet.hasNext() ) {
            /* error */
        }
        Binding row = rowSet.next();
        if ( rowSet.hasNext() ) {
            /* error */
        }
        Node n = row.get("n");
        if ( !n.isLiteral() ) {/* error */}
        String name = n.getLiteralLexicalForm();
        if ( name.isEmpty() ) {
            /* error */
        }
        return name;
    }

    private static String getString(Graph graph, Node node, Node property) {
        Node x = G.getOneSP(graph, node, property);
        if ( Util.isSimpleString(x) )
            return x.getLiteralLexicalForm();
        throw new FusekiKafkaException("Not a string for: "+property);
    }

    private static String getStringOrDft(Graph graph, Node node, Node property, String defaultString) {
        Node x = G.getZeroOrOneSP(graph, node, property);
        if ( x == null )
            return defaultString;
        if ( Util.isSimpleString(x) )
            return x.getLiteralLexicalForm();
        throw new FusekiKafkaException("Not a string for: "+property);
    }
}

