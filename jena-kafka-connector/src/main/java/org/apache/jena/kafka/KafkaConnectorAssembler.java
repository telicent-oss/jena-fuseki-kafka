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

package org.apache.jena.kafka;

import static org.apache.jena.kafka.Assem2.onError;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.JA;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.atlas.lib.IRILib;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.other.G;
import org.apache.jena.riot.other.RDFDataException;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Assembler for a Fuseki-Kafka connector that takes Kafka events and executes them on
 * a Fuseki server.
 * This is an update stream, not publishing data to Kafka.
 */
public class KafkaConnectorAssembler extends AssemblerBase implements Assembler {

    private static String NS = "http://jena.apache.org/fuseki/kafka#";
    public static String getNS() { return NS; }

    private static Resource tKafkaConnector = ResourceFactory.createResource(NS+"Connector");
    private static Node pDatabaseName       = NodeFactory.createURI(NS+"datasetName");
    private static Node pEndpointName       = NodeFactory.createURI(NS+"service");          // Optional

    private static Node pKafkaTopic         = NodeFactory.createURI(NS+"topic");
    private static Node pStateFile          = NodeFactory.createURI(NS+"stateFile");

    private static Node pSyncTopic          = NodeFactory.createURI(NS+"syncTopic");
    private static Node pReplayTopic        = NodeFactory.createURI(NS+"replayTopic");

    private static Node pKafkaProperty      = NodeFactory.createURI(NS+"config");
    private static Node pKafkaBootstrapServers = NodeFactory.createURI(NS+"bootstrapServers");
    private static Node pKafkaGroupId       = NodeFactory.createURI(NS+"groupId");
    private static String noServiceName     = "";

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
            ex.printStackTrace();
            return null;
        }
    }

    static RDFDataException dataException(Node node, String msg) {
        return new RDFDataException(NodeFmtLib.displayStr(node)+" : "+msg);
    }

    static RDFDataException dataException(Node node, Node property, String msg) {
        return new RDFDataException(NodeFmtLib.displayStr(node)+" "+NodeFmtLib.displayStr(property)+" : "+msg);
    }


    private static Assem2.OnError errorException = errorMsg -> new FusekiKafkaException(errorMsg);

    static FusekiKafkaException error(Node node, String msg) {
        return new FusekiKafkaException(NodeFmtLib.displayStr(node)+" : "+msg);
    }

    static FusekiKafkaException error(Node node, Node property, String msg) {
        return new FusekiKafkaException(NodeFmtLib.displayStr(node)+" "+NodeFmtLib.displayStr(property)+" : "+msg);
    }

    private ConnectorFK createSub(Graph graph, Node node, Node type) {
        /*
         * PREFIX fk: <http://jena.apache.org/fuseki/kafka#>
         *
         * [] rdf:type jenakafka:Connector ;
         *     fk:topic             "TOPIC";
         *     fk:bootstrapServers  "localhost:9092";
         *     fk:stateFile         "dir/filename.state" ;
         *     fk:databaseName      "/ds";
         * ## Optional - with defaults
         *
         *     ## false means don't sync on startup.
         *     fk:syncTopic         true;
         *
         *     ## false means replay from the start (ignore sync)
         *     fk:replayTopic       false;
         *
         * ??  fk:restart
         *
         *     ## Datasets service name
         *     fk:serviceName       "upload"";
         *
         *     fk:groupId
         */

        // Required!
        String topic = Assem2.getString(graph, node, pKafkaTopic, errorException);

        String datasetName = datasetName(graph, node);
        datasetName = /*DataAccessPoint.*/canonical(datasetName);

        String endpoint = endpointName(graph, node);

        String bootstrapServers = Assem2.getString(graph, node, pKafkaBootstrapServers, errorException);

        Boolean topicSync = Assem2.getBooleanOrDft(graph, node, pSyncTopic, true, errorException);
        Boolean replayTopic = Assem2.getBooleanOrDft(graph, node, pReplayTopic, false, errorException);

        String stateFile = Assem2.getAsString(graph, node, pStateFile, errorException);
        // The file name can be a relative file name as a string or a
        // file: can URL place the area next to the configuration file.
        // Turn "file:/" to a filename.
        if ( stateFile.startsWith("file:") )
            stateFile = IRILib.IRIToFilename(stateFile);

        String groupId = Assem2.getStringOrDft(graph, node, pKafkaGroupId, "JenaFusekiKafka", errorException);

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
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DeserializerActionFK.class.getName());

        return new ConnectorFK(topic, datasetName, endpoint, stateFile, topicSync.booleanValue(), replayTopic.booleanValue(), kafkaProps);
    }

    private static String PREFIXES = StrUtils.strjoinNL("PREFIX ja:     <"+JA.getURI()+">"
                                                        ,"PREFIX fk:     <"+NS+">"
                                                        //OLD,"PREFIX fuseki: <http://jena.apache.org/fuseki#>"; //+FusekiVocab.NS+">"
                                                        ,"" );

    private String datasetName(Graph graph, Node node) {
        String queryString = StrUtils.strjoinNL
                ( PREFIXES
                , "SELECT ?n { "
                //OLD , "   OPTIONAL { ?X fk:service/fuseki:name ?N1 }"
                , "   OPTIONAL { ?X fk:datasetName ?N2 }"
                , "   BIND(COALESCE(?N1, ?N2, '') AS ?n)"
                , "}"
                );
        RowSet rowSet = QueryExec.graph(graph)
                .query(queryString)
                .substitution("X", node)
                .build()
                .select();

        if ( !rowSet.hasNext() )
            throw new FusekiKafkaException("Can't find the datasetName: "+NodeFmtLib.displayStr(node));
        Binding row = rowSet.next();
        if ( rowSet.hasNext() )
            throw new FusekiKafkaException("Multiple datasetNames: "+NodeFmtLib.displayStr(node));

        Node n = row.get("n");
        if ( n == null )
            throw new FusekiKafkaException("Can't find the datasetName: "+NodeFmtLib.displayStr(node));

        if ( ! Util.isSimpleString(n) )
            throw new FusekiKafkaException("Dataset name is not a string: "+NodeFmtLib.displayStr(node));
        String name = n.getLiteralLexicalForm();
        if ( StringUtils.isBlank(name) )
            throw new FusekiKafkaException("Dataset name is blank: "+NodeFmtLib.displayStr(node));
        return name;
    }

    private String endpointName(Graph graph, Node node) {
        List<Node> x = G.listSP(graph, node, pEndpointName);
        if ( x.isEmpty() )
            return noServiceName;
        if ( x.size() > 1 )
            throw onError(node, "Multiple service names", errorException);
        Node n = x.get(0);
        if ( ! Util.isSimpleString(n) )
            throw onError(node, "Service name is not a string", errorException);
        String epName = n.getLiteralLexicalForm();
        if ( StringUtils.isBlank(epName) )
            return noServiceName;
        if ( epName.contains("/") )
            throw onError(node, "Service name can not contain \"/\"", errorException);
        if ( epName.contains(" ") )
            throw onError(node, "Service name can not contain spaces", errorException);
        return epName;
    }

    // Copy of DataAccessPoint.canonical.
    public static String canonical(String datasetPath) {
        if ( datasetPath == null )
            return datasetPath;
        if ( datasetPath.equals("/") )
            return datasetPath;
        if ( datasetPath.equals("") )
            return "/";
        if ( !datasetPath.startsWith("/") )
            datasetPath = "/" + datasetPath;
        if ( datasetPath.endsWith("/") )
            datasetPath = datasetPath.substring(0, datasetPath.length() - 1);
        return datasetPath;
    }
}

