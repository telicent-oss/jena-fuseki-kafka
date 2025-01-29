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

import static org.apache.jena.kafka.Assem2.onError;
import static org.apache.jena.kafka.utils.EnvVariables.checkForEnvironmentVariableValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

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
import org.apache.jena.graph.Triple;
import org.apache.jena.kafka.utils.EnvVariables;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.system.G;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assembler for a Fuseki-Kafka connector that takes Kafka events and executes them on a Fuseki server.
 * <p>
 * The Kafka event has a header "Content-type" and acts the same as HTTP.
 * <p>
 * This is an update stream, not publishing data to Kafka.
 * <p>
 * Illustration, required settings for use in Fuseki:
 * <pre>
 * &lt;#connector&gt; rdf:type fk:Connector ;
 *   # Destination Fuseki service name (when used with Fuseki).
 *   fk:fusekiServiceName   "ds";
 *
 *   # Using Kafka-RAFT
 *   fk:bootstrapServers    "localhost:9092";
 *
 *   # Kafka topic
 *   fk:topic               "RDF";
 *
 *   # File used to track the state (the last offset processes)
 *   # Used across Fuseki restarts.
 *   fk:stateFile           "Databases/RDF.state";
 *    .
 * </pre>
 */
public class KafkaConnectorAssembler extends AssemblerBase implements Assembler {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectorAssembler.class);

    private static String NS = "http://jena.apache.org/fuseki/kafka#";

    public static String getNS() {
        return NS;
    }

    /**
     * Type of a connector description
     */
    public static Resource tKafkaConnector = ResourceFactory.createResource(NS + "Connector");

    // Preferred:   "fusekiServiceName"
    // Alternative: "datasetName"

    /**
     * Destination dataset and endpoint for dispatching Kafka events.
     */
    public static Node pFusekiServiceName = NodeFactory.createURI(NS + "fusekiServiceName");
    /**
     * @deprecated Use {@link #pFusekiServiceName}
     */
    @Deprecated
    private static Node pFusekiDatasetName = NodeFactory.createURI(NS + "datasetName");       // Old name.

    /**
     * Kafka topic to listen to
     */
    public static Node pKafkaTopic = NodeFactory.createURI(NS + "topic");
    /**
     * File used to record topic and last read offset
     */
    public static Node pStateFile = NodeFactory.createURI(NS + "stateFile");

    /**
     * Sync on startup?
     */
    public static Node pSyncTopic = NodeFactory.createURI(NS + "syncTopic");
    /**
     * Replay whole topic on startup?
     */

    private static Node pReplayTopic = NodeFactory.createURI(NS + "replayTopic");

    // Kafka cluster
    public static Node pKafkaProperty = NodeFactory.createURI(NS + "config");
    public static Node pKafkaPropertyFile = NodeFactory.createURI(NS + "configFile");
    public static Node pKafkaBootstrapServers = NodeFactory.createURI(NS + "bootstrapServers");
    public static Node pKafkaGroupId = NodeFactory.createURI(NS + "groupId");

    // Default values.
    private static boolean dftSyncTopic = true;
    private static boolean dftReplayTopic = false;
    public static String dftKafkaGroupId = "JenaFusekiKafka";

    public static Resource getType() {
        return tKafkaConnector;
    }

    @Override
    public Object open(Assembler a, Resource root, Mode mode) {
        return create(root.getModel().getGraph(), root.asNode(), tKafkaConnector.asNode());
    }

    private KConnectorDesc create(Graph graph, Node node, Node type) {
        try {
            return createSub(graph, node, type);
        } catch (RuntimeException ex) {
            FusekiKafka.LOG.error(ex.getMessage());
            return null;
        }
    }

    private static Assem2.OnError errorException = JenaKafkaException::new;

    private KConnectorDesc createSub(Graph graph, Node node, Node type) {
        /*
         * PREFIX fk: <http://jena.apache.org/fuseki/kafka#>
         *
         * [] rdf:type fk:Connector ;
         *     ## Required
         *     fk:topic             "TOPIC";
         *     fk:bootstrapServers  "localhost:9092";
         *     fk:stateFile         "dir/filename.state" ;
         *     fk:fusekiServiceName "/ds"; ## Or a "/ds/service"
         *
         *     ## Optional - with defaults
         *     ## Root of group name - this is made globally unique
         *     ## so every message is seen by every connector.
         *     fk:groupId           "JenaFusekiKafka";
         *
         *     ## false means don't sync on startup.
         *     fk:syncTopic         true;
         *
         *     ## false means replay from the start (ignore sync)
         *     fk:replayTopic       false;
         *
         *     ## Relay to a remote triplestore.
         *     fk:remoteEndpoint    "http://host/triplestore";
         *     .
         */

        // Required!
        String topic = getConfigurationValue(graph, node, pKafkaTopic, errorException);

        String datasetName = datasetName(graph, node);
        datasetName = /*DataAccessPoint.*/canonical(datasetName);

        String bootstrapServers = getConfigurationValue(graph, node, pKafkaBootstrapServers, errorException);

        boolean syncTopic = Assem2.getBooleanOrDft(graph, node, pSyncTopic, dftSyncTopic, errorException);
        boolean replayTopic = Assem2.getBooleanOrDft(graph, node, pReplayTopic, dftReplayTopic, errorException);

        String stateFile = getConfigurationValue(graph, node, pStateFile, errorException);
        // The file name can be a relative file name as a string or a
        // file: can URL place the area next to the configuration file.
        // Turn "file:/" to a filename.
        if (stateFile.startsWith("file:")) {
            stateFile = IRILib.IRIToFilename(stateFile);
        }

        String groupIdAssembler = Assem2.getStringOrDft(graph, node, pKafkaGroupId, dftKafkaGroupId, errorException);
        // We need the group id to be unique so multiple servers will
        // see all the messages topic partition.
        String groupId = groupIdAssembler + "-" + UUID.randomUUID();

        // ----
        Properties kafkaConsumerProps = kafkaConsumerProps(graph, node, topic, bootstrapServers, groupId);
        return new KConnectorDesc(List.of(topic), bootstrapServers, datasetName, stateFile, syncTopic, replayTopic,
                                  kafkaConsumerProps);
    }

    private Properties kafkaConsumerProps(Graph graph, Node node, String topic, String bootstrapServers,
                                          String groupId) {
        Properties props = SysJenaKafka.consumerProperties(bootstrapServers);
        // "group.id"
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Optional Kafka configuration as pairs of (key-value) as RDF lists.
        String queryString = StrUtils.strjoinNL("PREFIX ja: <" + JA.getURI() + ">", "SELECT ?k ?v { ?X ?P (?k ?v) }");

        QueryExec.graph(graph)
                 .query(queryString)
                 .substitution("X", node)
                 .substitution("P", pKafkaProperty)
                 .build()
                 .select()
                 .forEachRemaining(row -> {
                     Node nk = row.get("k");
                     String key = nk.getLiteralLexicalForm();
                     Node nv = row.get("v");
                     String value = nv.getLiteralLexicalForm();
                     props.setProperty(key, value);
                 });

        // External Kafka Properties File
        graph.stream(node, pKafkaPropertyFile, Node.ANY).map(Triple::getObject).forEach(propertyFile -> {
            if (propertyFile.isURI()) {
                if (propertyFile.getURI().startsWith("file:")) {
                    loadKafkaPropertiesFile(node, props, new File(URI.create(propertyFile.getURI())));
                } else if (propertyFile.getURI().startsWith(EnvVariables.ENV_PREFIX)) {
                    resolveKafkaPropertiesFile(propertyFile.getURI(), node, props);
                } else {
                    throw onError(node, pKafkaPropertyFile,
                                  "Properties file MUST be specified as a file URI or a literal", errorException);
                }
            } else if (propertyFile.isLiteral()) {
                resolveKafkaPropertiesFile(propertyFile.getLiteralLexicalForm(), node, props);
            }
        });

        return props;
    }

    private static void resolveKafkaPropertiesFile(String propertyFile, Node node, Properties props) {
        String propertyFileName = checkForEnvironmentVariableValue(pKafkaPropertyFile.getURI(), propertyFile);
        if (StringUtils.isNotBlank(propertyFileName)) {
            loadKafkaPropertiesFile(node, props, new File(propertyFileName));
        } else {
            LOGGER.warn("Ignored fk:configFile expression '{}' as it resolved to an empty value", propertyFile);
        }
    }

    private static void loadKafkaPropertiesFile(Node node, Properties props, File file) {
        Properties externalProperties = new Properties();
        try (FileInputStream input = new FileInputStream(file)) {
            externalProperties.load(input);

            if (!externalProperties.isEmpty()) {
                LOGGER.info("Loaded {} properties from Kafka properties file {}", externalProperties.size(),
                            file.getAbsoluteFile());
                props.putAll(externalProperties);
            }
        } catch (FileNotFoundException e) {
            throw onError(node, pKafkaPropertyFile, "Properties file '" + file.getAbsolutePath() + "' not found",
                          errorException);
        } catch (IOException e) {
            throw onError(node, pKafkaPropertyFile, "Error reading properties file '" + file.getAbsolutePath() + "'",
                          errorException);
        }
    }

    private static final String PREFIXES =
            StrUtils.strjoinNL("PREFIX ja:     <" + JA.getURI() + ">", "PREFIX fk:     <" + NS + ">", "");

    private String datasetName(Graph graph, Node node) {
        String queryString = StrUtils.strjoinNL(PREFIXES, "SELECT ?n { ", "   OPTIONAL { ?X ?fusekiServiceName ?N1 }",
                                                "   OPTIONAL { ?X ?fusekiDatasetName ?N2 }" // Old name.
                , "   BIND(COALESCE( ?N1, ?N2, '' ) AS ?n)", "}");
        RowSet rowSet = QueryExec.graph(graph)
                                 .query(queryString)
                                 .substitution("X", node)
                                 .substitution("fusekiServiceName", pFusekiServiceName)
                                 .substitution("fusekiDatasetName", pFusekiDatasetName)
                                 .build()
                                 .select();

        if (!rowSet.hasNext()) {
            throw new JenaKafkaException("Can't find the datasetName: " + NodeFmtLib.displayStr(node));
        }
        Binding row = rowSet.next();
        if (rowSet.hasNext()) {
            throw new JenaKafkaException("Multiple datasetNames: " + NodeFmtLib.displayStr(node));
        }

        Node n = row.get("n");
        if (n == null) {
            throw new JenaKafkaException("Can't find the datasetName: " + NodeFmtLib.displayStr(node));
        }

        if (!Util.isSimpleString(n)) {
            throw new JenaKafkaException("Dataset name is not a string: " + NodeFmtLib.displayStr(node));
        }
        String name = n.getLiteralLexicalForm();
        if (StringUtils.isBlank(name)) {
            throw new JenaKafkaException("Dataset name is blank: " + NodeFmtLib.displayStr(node));
        }
        return name;
    }

    // Copy of DataAccessPoint.canonical.
    public static String canonical(String datasetPath) {
        if (datasetPath == null) {
            return datasetPath;
        }
        if (datasetPath.isEmpty()) {
            return "/";
        }
        if (datasetPath.equals("/")) {
            return datasetPath;
        }
        if (!datasetPath.startsWith("/")) {
            datasetPath = "/" + datasetPath;
        }
        if (datasetPath.endsWith("/")) {
            datasetPath = datasetPath.substring(0, datasetPath.length() - 1);
        }
        return datasetPath;
    }

    static String getConfigurationValue(Graph graph, Node node, Node configNode, Assem2.OnError errorException) {
        String configurationValue = Assem2.getString(graph, node, configNode, errorException);
        configurationValue = checkForEnvironmentVariableValue(configNode.getURI(), configurationValue);
        return configurationValue;
    }
}
