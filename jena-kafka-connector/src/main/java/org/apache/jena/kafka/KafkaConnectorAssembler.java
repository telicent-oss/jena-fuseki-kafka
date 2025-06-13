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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import lombok.Getter;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assembler for a Fuseki-Kafka connector that takes Kafka events and executes them on a Fuseki server.
 * <p>
 * The Kafka event has a header "Content-type" which is used to detect the incoming event format.
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
 *   # May be multiple topics e.g.
 *   # fk:topic             "Other";
 *
 *   # Optional DLQ Topic to which malformed events are sent
 *   # fk:dlqTopic          "dlq";
 *
 *   # File used to track the state (the last offset processes)
 *   # Used across Fuseki restarts.
 *   fk:stateFile           "Databases/RDF.state";
 *    .
 * </pre>
 */
public class KafkaConnectorAssembler extends AssemblerBase implements Assembler {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectorAssembler.class);

    @Getter
    private static final String NS = "http://jena.apache.org/fuseki/kafka#";

    /**
     * RDF Type for Kafka connectors
     */
    public static Resource tKafkaConnector = ResourceFactory.createResource(NS + "Connector");

    // Preferred:   "fusekiServiceName"

    /**
     * Destination dataset and endpoint for dispatching Kafka events.
     */
    public static Node pFusekiServiceName = NodeFactory.createURI(NS + "fusekiServiceName");
    /*

    /**
     * Kafka topic(s) to listen to
     */
    public static Node pKafkaTopic = NodeFactory.createURI(NS + "topic");
    /**
     * Kafka DLQ topic to which malformed events are forwarded
     */
    public static Node pDlqTopic = NodeFactory.createURI(NS + "dlqTopic");
    /**
     * File used to record topic and partitions offsets
     */
    public static Node pStateFile = NodeFactory.createURI(NS + "stateFile");

    /**
     * Sync on startup?
     */
    public static Node pSyncTopic = NodeFactory.createURI(NS + "syncTopic");
    /**
     * Replay whole topic on startup?
     */
    private static final Node pReplayTopic = NodeFactory.createURI(NS + "replayTopic");

    // Kafka cluster
    public static Node pKafkaProperty = NodeFactory.createURI(NS + "config");
    public static Node pKafkaPropertyFile = NodeFactory.createURI(NS + "configFile");
    public static Node pKafkaBootstrapServers = NodeFactory.createURI(NS + "bootstrapServers");
    public static Node pKafkaGroupId = NodeFactory.createURI(NS + "groupId");

    // Default values.
    private static final boolean DEFAULT_SYNC_TOPIC = true;
    private static final boolean DEFAULT_REPLAY_TOPIC = false;
    public static final String DEFAULT_CONSUMER_GROUP_ID = "JenaFusekiKafka";

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

    private static final Assem2.OnError errorException = JenaKafkaException::new;

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
         *     .
         */

        // Required!
        List<String> topics = getConfigurationValues(graph, node, pKafkaTopic, errorException);

        String datasetName = datasetName(graph, node);
        datasetName = /*DataAccessPoint.*/canonical(datasetName);

        String bootstrapServers = getConfigurationValue(graph, node, pKafkaBootstrapServers, errorException);

        boolean syncTopic = Assem2.getBooleanOrDft(graph, node, pSyncTopic, DEFAULT_SYNC_TOPIC, errorException);
        boolean replayTopic = Assem2.getBooleanOrDft(graph, node, pReplayTopic, DEFAULT_REPLAY_TOPIC, errorException);

        String stateFile = getConfigurationValue(graph, node, pStateFile, errorException);
        // The file name can be a relative file name as a string or a
        // file: can URL place the area next to the configuration file.
        // Turn "file:/" to a filename.
        if (stateFile.startsWith("file:")) {
            stateFile = IRILib.IRIToFilename(stateFile);
        }

        // The Kafka Consumer Group, used both to balance assignments of partitions to a consumer and to track our
        // offsets in our state file.  If you want to run multiple instances of Fuseki with the Kafka module then each
        // MUST have a unique Group ID otherwise only one instance will be assigned the partitions.
        String groupId =
                getConfigurationValueOrDefault(graph, node, pKafkaGroupId, DEFAULT_CONSUMER_GROUP_ID, errorException);

        // Optional
        // DLQ topic to which malformed events are forwarded
        String dlqTopic = getConfigurationValueOrDefault(graph, node, pDlqTopic, null, errorException);

        // ----
        Properties kafkaConsumerProps = kafkaConsumerProps(graph, node, bootstrapServers, groupId);
        return new KConnectorDesc(topics, bootstrapServers, datasetName, stateFile, syncTopic, replayTopic, dlqTopic,
                                  kafkaConsumerProps);
    }

    private Properties kafkaConsumerProps(Graph graph, Node node, String bootstrapServers, String groupId) {
        Properties props = SysJenaKafka.consumerProperties(bootstrapServers);
        // "group.id"
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Optional Kafka configuration as pairs of (key-value) as RDF lists.
        String queryString = StrUtils.strjoinNL("PREFIX ja: <" + JA.getURI() + ">", "SELECT ?k ?v { ?X ?P (?k ?v) }");

        try (QueryExec exec = QueryExec.graph(graph)
                                       .query(queryString)
                                       .substitution("X", node)
                                       .substitution("P", pKafkaProperty)
                                       .build()) {
            exec.select()
                .forEachRemaining(row -> {
                    Node nk = row.get("k");
                    String key = nk.getLiteralLexicalForm();
                    Node nv = row.get("v");
                    String value = nv.getLiteralLexicalForm();
                    props.setProperty(key, value);
                });
        }

        // External Kafka Properties File
        graph.stream(node, pKafkaPropertyFile, Node.ANY).map(Triple::getObject).forEach(propertyFile -> {
            if (propertyFile.isURI()) {
                if (propertyFile.getURI().startsWith("file:")) {
                    loadKafkaPropertiesFile(node, props, new File(URI.create(propertyFile.getURI())));
                } else if (propertyFile.getURI().startsWith(EnvVariables.ENV_PREFIX)) {
                    resolveKafkaPropertiesFile(propertyFile.getURI(), node, props);
                } else {
                    badPropertiesFileValue(node);
                }
            } else if (propertyFile.isLiteral()) {
                resolveKafkaPropertiesFile(propertyFile.getLiteralLexicalForm(), node, props);
            } else {
                badPropertiesFileValue(node);
            }
        });

        return props;
    }

    private static void badPropertiesFileValue(Node node) {
        throw onError(node, pKafkaPropertyFile,
                      "Properties file MUST be specified as a file URI or a literal", errorException);
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
        String queryString = """
                SELECT ?n {
                  OPTIONAL { ?X ?fusekiServiceName ?N1 }
                  BIND(COALESCE( ?N1, ?N2, '' ) AS ?n)
                }
                """;
        try (QueryExec exec = QueryExec.graph(graph).query(queryString)
                                       .substitution("X", node)
                                       .substitution("fusekiServiceName", pFusekiServiceName)
                                       .build()) {
            RowSet rowSet = exec
                    .select();

            // NB - Because we've done a BIND expression in the query over the OPTIONAL we're guaranteed to always
            //      produce at least one row
            Binding row = rowSet.next();
            if (rowSet.hasNext()) {
                throw new JenaKafkaException("Multiple datasetNames: " + NodeFmtLib.displayStr(node));
            }

            // NB - It's also guaranteed that the BIND expression always returns a non-null value since it's using
            //      COALESCE() which given the parameters is guaranteed to return a non-null value
            Node n = row.get("n");
            if (!Util.isSimpleString(n)) {
                throw new JenaKafkaException("Dataset name is not a string: " + NodeFmtLib.displayStr(node));
            }
            String name = n.getLiteralLexicalForm();
            if (StringUtils.isBlank(name)) {
                throw new JenaKafkaException("Dataset name is blank: " + NodeFmtLib.displayStr(node));
            }
            return name;
        }
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

    static String getConfigurationValue(Graph graph, Node node, Node property, Assem2.OnError errorException) {
        String configurationValue = Assem2.getString(graph, node, property, errorException);
        configurationValue = checkForEnvironmentVariableValue(property.getURI(), configurationValue);
        return configurationValue;
    }

    static String getConfigurationValueOrDefault(Graph graph, Node node, Node property, String defaultValue,
                                                 Assem2.OnError errorException) {
        String configurationValue = Assem2.getStringOrDft(graph, node, property, defaultValue, errorException);
        configurationValue = checkForEnvironmentVariableValue(property.getURI(), configurationValue);
        return configurationValue;
    }

    static List<String> getConfigurationValues(Graph graph, Node node, Node property, Assem2.OnError errorException) {
        List<String> configurationValues =
                new ArrayList<>(Assem2.getStrings(graph, node, property, errorException));
        for (int i = 0; i < configurationValues.size(); i++) {
            configurationValues.set(i, checkForEnvironmentVariableValue(property.getURI(),
                                                                        configurationValues.get(i)));
        }
        return configurationValues;
    }
}
