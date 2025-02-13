package org.apache.jena.fuseki.kafka;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.fuseki.FusekiException;
import org.apache.jena.fuseki.kafka.lib.FKLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.http.QueryExecHTTP;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TimeoutException;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.jena.fuseki.kafka.DockerTestConfigFK.configuration;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class TestKafkaDelays {

    private static final String DIR = "src/test/files";
    private static final String STATE_DIR = "target/state";

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.7.1");
    private KafkaContainer kafkaContainer;
    private ToxiproxyContainer toxiproxyContainer;
    private ToxiproxyContainer.ContainerProxy kafkaProxy;
    public Network network = Network.newNetwork();
    boolean debugLogging = false;

    @BeforeClass
    public void setup() throws Exception {
        debugLogging("Starting Kafka container...");
        kafkaContainer = new KafkaContainer(KAFKA_IMAGE).withNetwork(network);
        kafkaContainer.start();
        debugLogging("Kafka container started: " + kafkaContainer.getBootstrapServers());

        debugLogging("Starting Toxiproxy container...");
        toxiproxyContainer = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0")).withNetwork(network);
        toxiproxyContainer.start();
        debugLogging("Toxiproxy container started.");

        kafkaProxy = toxiproxyContainer.getProxy(kafkaContainer, 9093);
        debugLogging("Kafka proxy configured: IP " + kafkaProxy.getContainerIpAddress() + ", Port " + kafkaProxy.getProxyPort());

        debugLogging("Waiting for Kafka broker to be ready...");
        waitForKafkaBroker();
        debugLogging("Kafka broker is ready.");

        debugLogging("Creating test topic...");
        try (AdminClient adminClient = AdminClient.create(getAdminConfig())) {
            adminClient.createTopics(Collections.singletonList(new NewTopic("RDF0", 1, (short) 1))).all().get();
            debugLogging("Topic 'test-topic' created successfully.");
        } catch (Exception e) {
            debugLogging("Failed to create topic: " + e.getMessage());
            throw e;
        }
    }

    @AfterClass
    public void tearDown() {
        debugLogging("Stopping containers...");
        if (toxiproxyContainer != null) {
            toxiproxyContainer.stop();
            debugLogging("Toxiproxy container stopped.");
        }
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            debugLogging("Kafka container stopped.");
        }
    }
    private Properties getAdminConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        return props;
    }

    private void waitForKafkaBroker() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            try (AdminClient adminClient = AdminClient.create(getAdminConfig())) {
                adminClient.listTopics().names().get();
                return;
            } catch (Exception e) {
                debugLogging("Kafka broker not ready. Retrying... (" + (i + 1) + "/10)");
                Thread.sleep(1000);
            }
        }
        throw new IllegalStateException("Kafka broker did not start in time");
    }

    private void debugLogging(String message) {
        if (debugLogging) {
            System.out.println(message);
        }
    }

    Properties producerProps() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaProxy.getContainerIpAddress() + ":" + kafkaProxy.getProxyPort());
        producerProps.putAll(new Properties());
        return producerProps;
    }

    /**
     * This tests that everything works correctly with the ToxiProxy in place.
     */
    @Test
    public void test_happy_path() {
        String TOPIC = "RDF0";
        Graph graph = configuration(DIR+"/config-connector.ttl", kafkaProxy.getContainerIpAddress() + ":" + kafkaProxy.getProxyPort(), new Properties());
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);

        // Configuration knows the topic name.
        FusekiServer server = FusekiServer.create()
                .port(0)
                .verbose(true)
                .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                .parseConfig(ModelFactory.createModelForGraph(graph))
                .build();
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR+"/data.ttl"));
        server.start();
        try {
            String URL = "http://localhost:"+server.getHttpPort()+"/ds";
            RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
            int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
            Assert.assertEquals(count, 1);
        } finally { server.stop(); }
    }

    /**
     * This tests that when a timeout takes place, an error message is logged
     */
    @Test
    public void test_timeout() throws IOException {
        debugLogging("set up error logging capture");
        PrintStream standardErr = System.err;
        ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
        System.setErr(new PrintStream(outputStreamCaptor));

        debugLogging("Set up timeout of 100ms");
        kafkaProxy.toxics()
                .timeout("timeout", ToxicDirection.DOWNSTREAM, 100L);

        debugLogging("Configure Graph with a kakfa time out of 50ms");
        Properties consumerProps = new Properties();
        consumerProps.put("max-poll-interval", "50");
        Graph graph = configuration(DIR+"/config-connector.ttl", kafkaProxy.getContainerIpAddress() + ":" + kafkaProxy.getProxyPort(), consumerProps);
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);
        // Configuration knows the topic name.
        debugLogging("Configure Server");
        FusekiServer server = FusekiServer.create()
                .port(0)
                .verbose(true)
                .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                .parseConfig(ModelFactory.createModelForGraph(graph))
                .build();
        debugLogging("Start Server");
        Throwable actualException = null;
        try {
            server.start();
        }catch (FusekiException exception) {
            actualException = exception;
        } finally { server.stop(); }

        debugLogging("Clear timeout");
        kafkaProxy.toxics().get("timeout").remove();

        debugLogging("Assert timeout exception thrown");
        assertNotNull(actualException);
        assertTrue(actualException.getCause() instanceof TimeoutException);

        debugLogging("Assert error message");
        System.setErr(standardErr);
        debugLogging(outputStreamCaptor.toString().trim());
        assertEquals("org.apache.kafka.common.errors.TimeoutException: Timeout expired while fetching topic metadata", outputStreamCaptor.toString().trim());
    }


    /**
     * This tests that we can handle a little latency without issue.
     */
    @Test
    public void test_latency() throws IOException {
        debugLogging("Set up latency of 100ms");
        kafkaProxy.toxics()
                .latency("latency", ToxicDirection.DOWNSTREAM, 100);

        String TOPIC = "RDF0";
        Properties consumerProps = new Properties();
        consumerProps.put("max-poll-interval", "100");
        Graph graph = configuration(DIR+"/config-connector.ttl", kafkaProxy.getContainerIpAddress() + ":" + kafkaProxy.getProxyPort(), consumerProps);
        FileOps.ensureDir(STATE_DIR);
        FileOps.clearDirectory(STATE_DIR);
        debugLogging("Configure server");
        // Configuration knows the topic name.
        FusekiServer server = FusekiServer.create()
                .port(0)
                .verbose(true)
                .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                .parseConfig(ModelFactory.createModelForGraph(graph))
                .build();
        FKLib.sendFiles(producerProps(), TOPIC, List.of(DIR+"/data.ttl"));
        debugLogging("Start server");
        server.start();
        try {
            String URL = "http://localhost:"+server.getHttpPort()+"/ds";
            RowSet rowSet = QueryExecHTTP.service(URL).query("SELECT (count(*) AS ?C) {?s ?p ?o}").select();
            int count = ((Number)rowSet.next().get("C").getLiteralValue()).intValue();
            debugLogging("Assert query worked");
            Assert.assertEquals(count, 1);
        } finally { server.stop(); }
        debugLogging("Clear latency");
        kafkaProxy.toxics().get("latency").remove();
    }

}
