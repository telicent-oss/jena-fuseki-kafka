package org.apache.jena.kafka;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.stream.Stream;

public class TestKafkaConnectorAssembler {

    private static final String TEST_URI = "https://example.org/connector#1";
    private static final String TEST_CLUSTER_URI = "https://example.org/cluster#1";
    private static final String TOPIC = "test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SERVICE_NAME = "/ds";
    private static final String STATE_FILE = "test.state";
    /**
     * The default Kafka properties size if no extra properties are provided
     */
    private static final int DEFAULT_CONFIG_SIZE = 6;

    private final KafkaConnectorAssembler assembler = new KafkaConnectorAssembler();

    @Test
    public void givenNoConfig_whenAssemblingConnector_thenNotLoaded() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        config.add(resource, RDF.type, KafkaConnectorAssembler.tKafkaConnector);

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNull(assembled);
    }

    @ValueSource(strings = {
            "bad-assem-no-dataset-name.ttl",
            "bad-assem-multiple-dataset-names.ttl",
            "bad-assem-uri-dataset-name.ttl",
            "bad-assem-multi-value-string-property.ttl",
            "bad-assem-multi-value-boolean-property.ttl",
            "bad-assem-mistyped-string-property.ttl",
            "bad-assem-mistyped-boolean-property.ttl",
            "bad-assem-mistyped-mandatory-string-property.ttl",
            "bad-assem-dlq-topic-also-input-topic.ttl"
    })
    @ParameterizedTest
    public void givenMalformedConfig_whenAssemblingConnector_thenNotLoaded(String filename) {
        // Given and When
        KConnectorDesc desc = TestConnectorDescriptor.connectorByType(filename);

        // Then
        Assertions.assertNull(desc);
    }

    @Test
    public void givenMinimalConfig_whenAssemblingConnector_thenSuccess_andConfigAsExpected() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNotNull(assembled);

        // And
        verifyMinimalConfig(assembled);
    }

    private static KConnectorDesc verifyMinimalConfig(Object assembled) {
        Assertions.assertInstanceOf(KConnectorDesc.class, assembled);
        KConnectorDesc connector = (KConnectorDesc) assembled;
        Assertions.assertEquals(TOPIC, connector.getTopics().get(0));
        Assertions.assertEquals(BOOTSTRAP_SERVERS, connector.getBootstrapServers());
        Assertions.assertEquals(SERVICE_NAME, connector.getDatasetName());
        Assertions.assertEquals(STATE_FILE, connector.getStateFile());
        Assertions.assertFalse(connector.isCheckTopicAtStartUp());
        return connector;
    }

    private static void createMinimalConfiguration(Model config, Resource resource) {
        config.add(resource, RDF.type, KafkaConnectorAssembler.tKafkaConnector);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaTopic.getURI()),
                   config.createLiteral(TOPIC));
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaBootstrapServers.getURI()),
                   config.createLiteral(BOOTSTRAP_SERVERS));
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pFusekiServiceName.getURI()),
                   config.createLiteral(SERVICE_NAME));
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pStateFile.getURI()),
                   config.createLiteral(STATE_FILE));
    }

    @Test
    public void givenStrictStartupChecksConfig_whenAssemblingConnector_thenEnabled() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pStartupTopicCheck.getURI()),
                   config.createTypedLiteral(true));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNotNull(assembled);
        Assertions.assertTrue(((KConnectorDesc) assembled).isCheckTopicAtStartUp());
    }

    @Test
    public void givenExtraConfig_whenAssemblingConnector_thenSuccess_andConfigAsExpected() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        Properties props = createTestProperties();
        for (String key : props.stringPropertyNames()) {
            config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaProperty.getURI()),
                       config.createList(config.createLiteral(key), config.createLiteral(props.getProperty(key))));
        }

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNotNull(assembled);

        // And
        KConnectorDesc desc = verifyMinimalConfig(assembled);
        verifyTestProperties(desc);
    }

    @Test
    public void givenExtraExternalConfigUri_whenAssemblingConnector_thenSuccess_andConfigAsExpected() throws
            IOException {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        Properties props = createTestProperties();
        File propsFile = prepareExternalPropertiesFile(props);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createResource(propsFile.toURI().toString()));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNotNull(assembled);

        // And
        KConnectorDesc desc = verifyMinimalConfig(assembled);
        verifyTestProperties(desc);
    }

    private static void verifyTestProperties(KConnectorDesc desc) {
        Assertions.assertTrue(desc.getKafkaConsumerProps().size() > 5, "Should be some extra properties present");
        Assertions.assertEquals("bar", desc.getKafkaConsumerProps().get("foo"));
        Assertions.assertEquals("other", desc.getKafkaConsumerProps().get("an"));
    }

    private static File prepareExternalPropertiesFile(Properties props) throws IOException {
        File propsFile = Files.createTempFile("kafka", ".properties").toFile();
        try (FileOutputStream output = new FileOutputStream(propsFile)) {
            props.store(output, null);
        }
        return propsFile;
    }

    @Test
    public void givenExtraExternalConfigLiteral_whenAssemblingConnector_thenSuccess_andConfigAsExpected() throws
            IOException {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        Properties props = createTestProperties();
        File propsFile = prepareExternalPropertiesFile(props);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createLiteral(propsFile.getAbsolutePath()));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNotNull(assembled);

        // And
        KConnectorDesc desc = verifyMinimalConfig(assembled);
        verifyTestProperties(desc);
    }

    private static Properties createTestProperties() {
        Properties props = new Properties();
        props.put("foo", "bar");
        props.put("an", "other");
        return props;
    }

    @ValueSource(strings = { "KAFKA_CONFIG_FILE_PATH", "KAFKA_PROPERTIES_FILE", "SOME_RANDOM_VAR" })
    @ParameterizedTest
    public void givenExtraExternalConfigEnvVar_whenAssemblingConnector_thenSuccess_andConfigAsExpected(
            String envVar) throws
            IOException {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        Properties props = createTestProperties();
        File propsFile = prepareExternalPropertiesFile(props);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createLiteral("env:" + envVar));
        try {
            System.setProperty(envVar, propsFile.getAbsolutePath());

            // When
            Object assembled = assembler.open(resource);

            // Then
            Assertions.assertNotNull(assembled);

            // And
            KConnectorDesc desc = verifyMinimalConfig(assembled);
            verifyTestProperties(desc);
        } finally {
            System.clearProperty(envVar);
        }
    }

    @ValueSource(strings = {
            "https://example.org/kafka.properties",
            "file:/no/such/kafka.properties",
            "env:NO_SUCH_VAR"
    })
    @ParameterizedTest
    public void givenExtraExternalConfigBadUri_whenAssemblingConnector_thenNotLoaded(String source) {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createResource(source));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNull(assembled);
    }

    @ValueSource(strings = {
            "/no/such/kafka.properties",
            "env:NO_SUCH_ENV_VAR",
            "env:{NO_SUCH_ENV_VAR:/no/such/default.properties}"
    })
    @ParameterizedTest
    public void givenExtraExternalConfigBadLiteral_whenAssemblingConnector_thenNotLoaded(String source) {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createResource(source));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNull(assembled);
    }

    @ValueSource(strings = {
            "env:{NO_SUCH_ENV_VAR:}",
            "env:{NO_SUCH_ENV_VAR: }"
    })
    @ParameterizedTest
    public void givenExtraExternalConfigEmptyPath_whenAssemblingConnector_thenLoaded_andNoExtraConfig(String source) {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createResource(source));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNotNull(assembled);

        // And
        KConnectorDesc desc = verifyMinimalConfig(assembled);
        Assertions.assertEquals(DEFAULT_CONFIG_SIZE, desc.getKafkaConsumerProps().size());
    }

    @Test
    public void givenExtraExternalConfigBadBlankNode_whenAssemblingConnector_thenNotLoaded() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createResource());

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNull(assembled);
    }

    @Test
    public void givenExtraExternalConfigEmptyFile_whenAssemblingConnector_thenLoaded_andNoExtraConfig() throws
            IOException {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        String source = Files.createTempFile("kafka",".properties").toFile().getAbsolutePath();
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createLiteral(source));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNotNull(assembled);

        // And
        KConnectorDesc desc = verifyMinimalConfig(assembled);
        Assertions.assertEquals(DEFAULT_CONFIG_SIZE, desc.getKafkaConsumerProps().size());
    }

    @Test
    public void givenExtraExternalConfigNonReadableFile_whenAssemblingConnector_thenNotLoaded() throws IOException {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource resource = config.createResource(TEST_URI);
        createMinimalConfiguration(config, resource);
        File propertiesFile = Files.createTempFile("kafka",".properties").toFile();
        propertiesFile.setReadable(false);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createLiteral(propertiesFile.getAbsolutePath()));

        // When
        Object assembled = assembler.open(resource);

        // Then
        Assertions.assertNull(assembled);
    }

    public static Stream<Arguments> datasetNames() {
        //@formatter:off
        return Stream.of(Arguments.of((String)null, (String)null),
                         Arguments.of("", "/"),
                         Arguments.of("/", "/"),
                         Arguments.of("ds", "/ds"),
                         Arguments.of("ds/", "/ds"));
        //@formatter:on
    }

    @ParameterizedTest
    @MethodSource("datasetNames")
    public void givenDatasetName_whenCanonicalising_thenExpectedResult(String name, String expected) {
        // Given and When
        String canonical = KafkaConnectorAssembler.canonical(name);

        // Then
        Assertions.assertEquals(expected, canonical);
    }

    private static void createConnectorWithoutBootstrap(Model config, Resource resource) {
        config.add(resource, RDF.type, KafkaConnectorAssembler.tKafkaConnector);
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaTopic.getURI()),
                   config.createLiteral(TOPIC));
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pFusekiServiceName.getURI()),
                   config.createLiteral(SERVICE_NAME));
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pStateFile.getURI()),
                   config.createLiteral(STATE_FILE));
    }

    private static Resource createCluster(Model config) {
        Resource cluster = config.createResource(TEST_CLUSTER_URI);
        config.add(cluster, RDF.type, KafkaConnectorAssembler.tKafkaCluster);
        return cluster;
    }

    private static void addBootstrapServers(Model config, Resource resource, String value) {
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaBootstrapServers.getURI()),
                   config.createLiteral(value));
    }

    private static void addConfigPair(Model config, Resource resource, String key, String value) {
        config.add(resource, config.createProperty(KafkaConnectorAssembler.pKafkaProperty.getURI()),
                   config.createList(config.createLiteral(key), config.createLiteral(value)));
    }

    private static void linkCluster(Model config, Resource connector, Resource cluster) {
        config.add(connector, config.createProperty(KafkaConnectorAssembler.pCluster.getURI()), cluster);
    }

    @Test
    public void givenConnectorReferencingCluster_whenAssembling_thenInheritsBootstrapAndConfig() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource connector = config.createResource(TEST_URI);
        createConnectorWithoutBootstrap(config, connector);
        Resource cluster = createCluster(config);
        addBootstrapServers(config, cluster, BOOTSTRAP_SERVERS);
        addConfigPair(config, cluster, "max.poll.records", "100");
        linkCluster(config, connector, cluster);

        // When
        Object assembled = assembler.open(connector);

        // Then
        Assertions.assertNotNull(assembled);
        KConnectorDesc desc = (KConnectorDesc) assembled;
        Assertions.assertEquals(BOOTSTRAP_SERVERS, desc.getBootstrapServers());
        Assertions.assertEquals("100", desc.getKafkaConsumerProps().getProperty("max.poll.records"));
    }

    @Test
    public void givenBootstrapOnConnectorAndCluster_whenAssembling_thenConnectorValueWins() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource connector = config.createResource(TEST_URI);
        createMinimalConfiguration(config, connector);
        Resource cluster = createCluster(config);
        addBootstrapServers(config, cluster, "cluster-host:9092");
        linkCluster(config, connector, cluster);

        // When
        Object assembled = assembler.open(connector);

        // Then
        Assertions.assertNotNull(assembled);
        Assertions.assertEquals(BOOTSTRAP_SERVERS, ((KConnectorDesc) assembled).getBootstrapServers());
    }

    @Test
    public void givenConfigPropertyOnConnectorAndCluster_whenAssembling_thenConnectorValueWins() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource connector = config.createResource(TEST_URI);
        createMinimalConfiguration(config, connector);
        addConfigPair(config, connector, "max.poll.records", "200");
        Resource cluster = createCluster(config);
        addConfigPair(config, cluster, "max.poll.records", "100");
        linkCluster(config, connector, cluster);

        // When
        Object assembled = assembler.open(connector);

        // Then
        Assertions.assertNotNull(assembled);
        Assertions.assertEquals("200", ((KConnectorDesc) assembled).getKafkaConsumerProps()
                                                                   .getProperty("max.poll.records"));
    }

    @Test
    public void givenClusterConfigFile_whenAssembling_thenInheritsFileProperties() throws IOException {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource connector = config.createResource(TEST_URI);
        createConnectorWithoutBootstrap(config, connector);
        Resource cluster = createCluster(config);
        addBootstrapServers(config, cluster, BOOTSTRAP_SERVERS);
        File propsFile = prepareExternalPropertiesFile(createTestProperties());
        config.add(cluster, config.createProperty(KafkaConnectorAssembler.pKafkaPropertyFile.getURI()),
                   config.createLiteral(propsFile.getAbsolutePath()));
        linkCluster(config, connector, cluster);

        // When
        Object assembled = assembler.open(connector);

        // Then
        Assertions.assertNotNull(assembled);
        verifyTestProperties((KConnectorDesc) assembled);
    }

    @Test
    public void givenNoBootstrapOnConnectorOrCluster_whenAssembling_thenNotLoaded() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource connector = config.createResource(TEST_URI);
        createConnectorWithoutBootstrap(config, connector);
        Resource cluster = createCluster(config);
        linkCluster(config, connector, cluster);

        // When
        Object assembled = assembler.open(connector);

        // Then
        Assertions.assertNull(assembled);
    }

    @Test
    public void givenGroupIdOnCluster_whenAssembling_thenNotInherited() {
        // Given
        Model config = ModelFactory.createDefaultModel();
        Resource connector = config.createResource(TEST_URI);
        createConnectorWithoutBootstrap(config, connector);
        Resource cluster = createCluster(config);
        addBootstrapServers(config, cluster, BOOTSTRAP_SERVERS);
        config.add(cluster, config.createProperty(KafkaConnectorAssembler.pKafkaGroupId.getURI()),
                   config.createLiteral("shared-group"));
        linkCluster(config, connector, cluster);

        // When
        Object assembled = assembler.open(connector);

        // Then
        Assertions.assertNotNull(assembled);
        // The group id is deliberately per-connector and must not be inherited from the cluster
        Assertions.assertEquals(KafkaConnectorAssembler.DEFAULT_CONSUMER_GROUP_ID,
                                ((KConnectorDesc) assembled).getConsumerGroupId());
    }
}
