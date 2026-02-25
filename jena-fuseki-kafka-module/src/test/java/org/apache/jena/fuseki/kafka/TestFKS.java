package org.apache.jena.fuseki.kafka;

import io.telicent.smart.cache.sources.kafka.TopicExistenceChecker;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFKS {

    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    @AfterMethod
    public void cleanup() {
        FKRegistry.get().reset();
    }

    @Test(dataProvider = "paths")
    public void givenEmptyDapRegistry_whenFindingDataset_thenNull(String path) {
        // Given
        DataAccessPointRegistry registry = mock(DataAccessPointRegistry.class);
        when(registry.get(any(String.class))).thenReturn(null);
        FusekiServer server = mock(FusekiServer.class);
        when(server.getDataAccessPointRegistry()).thenReturn(registry);

        // When
        Optional<DatasetGraph> dsg = FKS.findDataset(server, path);

        // Then
        Assert.assertFalse(dsg.isPresent());
    }

    @DataProvider(name = "paths")
    private Object[][] paths() {
        return new Object[][] {
                { "/ds" },
                { "/ds/" },
                { "/ds/upload" },
                { "/ds/upload/"}
        };
    }

    @Test(dataProvider = "paths")
    public void givenNonEmptyDapRegistry_whenFindingDataset_thenFound(String path) {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.empty();
        DataAccessPointRegistry registry = new  DataAccessPointRegistry();
        DataService service = mock(DataService.class);
        when(service.getDataset()).thenReturn(dsg);
        registry.register(new DataAccessPoint("ds", service));
        FusekiServer server = mock(FusekiServer.class);
        when(server.getDataAccessPointRegistry()).thenReturn(registry);

        // When
        Optional<DatasetGraph> found = FKS.findDataset(server, path);

        // Then
        Assert.assertTrue(found.isPresent());
        Assert.assertEquals(found.get(), dsg);
    }

    @Test
    public void givenNoRegisteredConnectors_whenFindingTopics_thenEmptyList() {
        Assert.assertTrue(FKS.findTopics("/ds").isEmpty());
    }

    @Test
    public void givenRegisteredConnectors_whenFindingTopics_thenMatchingTopicsReturned() {
        // Given
        KConnectorDesc dsConnector =
                new KConnectorDesc(List.of("topic-a"), "localhost:9092", "/ds", "target/test.state",
                                   false, false, false, null, new Properties());
        KConnectorDesc nestedConnector =
                new KConnectorDesc(List.of("topic-b"), "localhost:9092", "/ds/upload", "target/test.state",
                                   false, false, false, null, new Properties());
        FKRegistry.get().register(dsConnector.getTopics(), dsConnector);
        FKRegistry.get().register(nestedConnector.getTopics(), nestedConnector);

        // When
        List<String> topics = FKS.findTopics("/ds");

        // Then
        Assert.assertEquals(topics, List.of("topic-a", "topic-b"));
    }

    @Test
    public void givenStartupTopicChecksDisabled_whenCheckingTopics_thenNoCheckerCreated() {
        // Given
        KConnectorDesc conn =
                new KConnectorDesc(List.of("topic-a"), "localhost:9092", "/ds", "target/test.state",
                                   false, false, false, null, new Properties());
        AtomicBoolean checkerCreated = new AtomicBoolean(false);

        // When
        FKS.checkTopicsExistAtStartup(conn, "topic-a", Duration.ofMillis(1), 1, props -> {
            checkerCreated.set(true);
            return new TopicExistenceChecker(null, conn.getBootstrapServers(), conn.getTopics(), null);
        });

        // Then
        Assert.assertFalse(checkerCreated.get());
    }

    @Test
    public void givenTopicChecksEnabledAndTopicsExist_whenCheckingTopics_thenSucceeds() {
        // Given
        KConnectorDesc conn =
                new KConnectorDesc(List.of("topic-a"), "localhost:9092", "/ds", "target/test.state",
                                   false, false, true, null, new Properties());

        // When/Then
        FKS.checkTopicsExistAtStartup(conn, "topic-a", Duration.ofMillis(10), 1,
                                      props -> new TopicExistenceChecker(null, conn.getBootstrapServers(),
                                                                         conn.getTopics(), null));
    }

    @Test
    public void givenTopicChecksEnabledAndTopicsMissing_whenCheckingTopics_thenFails() {
        // Given
        KConnectorDesc conn =
                new KConnectorDesc(List.of("topic-a"), "localhost:9092", "/ds", "target/test.state",
                                   false, false, true, null, new Properties());
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.describeTopics(anyCollection())).thenThrow(new UnknownTopicOrPartitionException("missing"));
        Function<Properties, TopicExistenceChecker> checkerFactory =
                props -> new TopicExistenceChecker(adminClient, conn.getBootstrapServers(), conn.getTopics(), null);

        // When/Then
        FusekiKafkaException ex = Assert.expectThrows(FusekiKafkaException.class, () -> FKS.checkTopicsExistAtStartup(
                conn, "topic-a", Duration.ofMillis(10), 1, checkerFactory));
        Assert.assertTrue(ex.getMessage().contains("Strict startup checks are enabled"));
    }

    @Test
    public void givenTopicChecksEnabledAndCheckerCreationFailsWithRuntime_whenCheckingTopics_thenRuntimeWrapped() {
        // Given
        KConnectorDesc conn =
                new KConnectorDesc(List.of("topic-a"), "localhost:9092", "/ds", "target/test.state",
                                   false, false, true, null, new Properties());

        // When/Then
        FusekiKafkaException ex = Assert.expectThrows(FusekiKafkaException.class, () -> FKS.checkTopicsExistAtStartup(
                conn, "topic-a", Duration.ofMillis(10), 1, props -> {
                    throw new RuntimeException("boom");
                }));
        Assert.assertTrue(ex.getMessage().contains("Failed while performing strict startup topic checks"));
    }

    @Test
    public void givenTopicChecksEnabledAndCheckerCreationFailsWithFusekiException_whenCheckingTopics_thenOriginalExceptionPropagated() {
        // Given
        KConnectorDesc conn =
                new KConnectorDesc(List.of("topic-a"), "localhost:9092", "/ds", "target/test.state",
                                   false, false, true, null, new Properties());

        // When/Then
        FusekiKafkaException ex = Assert.expectThrows(FusekiKafkaException.class, () -> FKS.checkTopicsExistAtStartup(
                conn, "topic-a", Duration.ofMillis(10), 1, props -> {
                    throw new FusekiKafkaException("pre-existing failure");
                }));
        Assert.assertEquals(ex.getMessage(), "pre-existing failure");
    }

    @Test
    public void givenTopicChecksEnabled_whenCheckingTopics_thenCheckerIsClosed() {
        // Given
        KConnectorDesc conn =
                new KConnectorDesc(List.of("topic-a"), "localhost:9092", "/ds", "target/test.state",
                                   false, false, true, null, new Properties());
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.describeTopics(anyCollection())).thenThrow(new UnknownTopicOrPartitionException("missing"));
        TopicExistenceChecker checker = new TopicExistenceChecker(adminClient, conn.getBootstrapServers(),
                                                                  conn.getTopics(), null);

        // When
        Assert.expectThrows(FusekiKafkaException.class, () -> FKS.checkTopicsExistAtStartup(
                conn, "topic-a", Duration.ofMillis(10), 1, props -> checker));

        // Then
        verify(adminClient, times(1)).close();
    }
}
