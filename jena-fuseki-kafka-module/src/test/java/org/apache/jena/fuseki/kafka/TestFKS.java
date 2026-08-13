package org.apache.jena.fuseki.kafka;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.sources.kafka.TopicExistenceChecker;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import io.telicent.smart.cache.projectors.driver.ProjectorDriver;
import io.telicent.smart.cache.projectors.sinks.NullSink;
import io.telicent.smart.cache.sources.Event;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.common.FusekiOffsetStore;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFKS {

    private ExecutorService originalExecutor;

    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    @AfterMethod
    public void cleanup() {
        FKRegistry.get().reset();
        drivers().clear();
        activeDrivers().clear();
        if (this.originalExecutor != null) {
            setExecutor(this.originalExecutor);
            this.originalExecutor = null;
        }
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

    @Test
    public void givenInterruptedThread_whenResettingPollThreads_thenInterruptStatusPreserved() {
        Thread.currentThread().interrupt();
        try {
            FKS.resetPollThreads();
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
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

    @Test
    public void givenConnectorWithUnknownDataset_whenAddingConnectorToServer_thenFails() {
        // Given
        KConnectorDesc conn = connector("/missing");
        FusekiServer server = mock(FusekiServer.class);
        when(server.getDataAccessPointRegistry()).thenReturn(new DataAccessPointRegistry());
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName("/missing").build();

        // When/Then
        FusekiKafkaException ex = Assert.expectThrows(FusekiKafkaException.class,
                                                      () -> FKS.addConnectorToServer(conn, server, offsets, dsg -> NullSink.of()));
        Assert.assertTrue(ex.getMessage().contains("No dataset found"));
    }

    @Test
    public void givenConnectorAndKnownDataset_whenAddingConnectorToServer_thenDriverRegistered() {
        // Given
        RecordingExecutor executor = replaceExecutor(new FakeFuture(FakeFutureMode.TIMEOUT));
        KConnectorDesc conn = connector("/ds");
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName("/ds").build();

        // When
        FKS.addConnectorToServer(conn, serverForDataset("/ds", DatasetGraphFactory.empty()), offsets, null);

        // Then
        Assert.assertEquals(executor.submittedTasks.size(), 1);
        Assert.assertEquals(drivers().get("/ds").size(), 1);
        Assert.assertEquals(activeDrivers().size(), 1);
    }

    @Test
    public void givenConnectorStartupInterrupted_whenAddingConnectorToServer_thenInterruptPreservedAndDriverRemoved() {
        // Given
        replaceExecutor(new FakeFuture(FakeFutureMode.INTERRUPTED));
        KConnectorDesc conn = connector("/ds");
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName("/ds").build();

        // When/Then
        try {
            FusekiKafkaException ex = Assert.expectThrows(FusekiKafkaException.class,
                                                          () -> FKS.addConnectorToServer(conn, serverForDataset("/ds", DatasetGraphFactory.empty()),
                                                                                         offsets, dsg -> NullSink.of()));
            Assert.assertTrue(ex.getMessage().contains("Interrupted while waiting for connector to start up"));
            Assert.assertTrue(Thread.currentThread().isInterrupted());
            Assert.assertTrue(drivers().getOrDefault("/ds", Collections.emptyList()).isEmpty());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void givenConnectorStartupFailure_whenAddingConnectorToServer_thenDriverRemovedAndErrorWrapped() {
        // Given
        replaceExecutor(new FakeFuture(FakeFutureMode.EXECUTION));
        KConnectorDesc conn = connector("/ds");
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName("/ds").build();

        // When/Then
        FusekiKafkaException ex = Assert.expectThrows(FusekiKafkaException.class,
                                                      () -> FKS.addConnectorToServer(conn, serverForDataset("/ds", DatasetGraphFactory.empty()),
                                                                                     offsets, dsg -> NullSink.of()));
        Assert.assertTrue(ex.getMessage().contains("Connector failed to start up"));
        Assert.assertTrue(drivers().getOrDefault("/ds", Collections.emptyList()).isEmpty());
    }

    @Test
    public void givenRegisteredKafkaDriver_whenRestoringOffsets_thenKafkaSourceResetWithDecodedOffsets() {
        // Given
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName("/ds").build();
        offsets.saveOffset("topica-0-group-1", 3L);
        offsets.saveOffset("topica-0-group-2", 8L);
        offsets.saveOffset("topicb-1-group-1", 5L);
        KafkaEventSource<Bytes, RdfPayload> kafkaSource = mock(KafkaEventSource.class);
        @SuppressWarnings("unchecked")
        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver = mock(ProjectorDriver.class);
        when(driver.getSource()).thenReturn(kafkaSource);
        drivers().put("/ds", new ArrayList<>(List.of(driver)));

        // When
        FKS.restoreOffsetForDataset("/ds", offsets);

        // Then
        verify(kafkaSource, times(1)).resetOffsets(Map.of(new TopicPartition("topica", 0), 8L,
                                                          new TopicPartition("topicb", 1), 5L));
    }

    @Test
    public void givenNoDriversForDataset_whenRestoringOffsets_thenNoOp() {
        // Given
        FusekiOffsetStore offsets = FusekiOffsetStore.builder().datasetName("/ds").build();

        // When/Then
        FKS.restoreOffsetForDataset("/unknown", offsets);
    }

    private static KConnectorDesc connector(String datasetName) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        return new KConnectorDesc(List.of("topica"), "localhost:9092", datasetName, "target/test.state",
                                  false, false, false, "topica-dlq", properties);
    }

    private static FusekiServer serverForDataset(String datasetPath, DatasetGraph dataset) {
        DataAccessPointRegistry registry = new DataAccessPointRegistry();
        DataService service = mock(DataService.class);
        when(service.getDataset()).thenReturn(dataset);
        registry.register(new DataAccessPoint(datasetPath.substring(1), service));
        FusekiServer server = mock(FusekiServer.class);
        when(server.getDataAccessPointRegistry()).thenReturn(registry);
        return server;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>>>> drivers() {
        return (Map<String, List<ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>>>>) getStaticField("DRIVERS");
    }

    @SuppressWarnings("unchecked")
    private static Collection<Future<?>> activeDrivers() {
        return (Collection<Future<?>>) getStaticField("ACTIVE_DRIVERS");
    }

    private RecordingExecutor replaceExecutor(FakeFuture future) {
        this.originalExecutor = (ExecutorService) getStaticField("EXECUTOR");
        RecordingExecutor executor = new RecordingExecutor(future);
        setExecutor(executor);
        return executor;
    }

    private static Object getStaticField(String fieldName) {
        try {
            Field field = FKS.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to access FKS." + fieldName, e);
        }
    }

    private static void setExecutor(ExecutorService executor) {
        try {
            Field field = FKS.class.getDeclaredField("EXECUTOR");
            field.setAccessible(true);
            field.set(null, executor);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to replace FKS.EXECUTOR", e);
        }
    }

    private enum FakeFutureMode {
        TIMEOUT,
        EXECUTION,
        INTERRUPTED
    }

    private static final class FakeFuture implements Future<Object> {
        private final FakeFutureMode mode;
        private boolean cancelled;

        private FakeFuture(FakeFutureMode mode) {
            this.mode = mode;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            this.cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return this.cancelled;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public Object get() {
            throw new UnsupportedOperationException("Unused by these tests");
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            switch (this.mode) {
                case TIMEOUT:
                    throw new TimeoutException("still running");
                case EXECUTION:
                    throw new ExecutionException(new IllegalStateException("boom"));
                case INTERRUPTED:
                    throw new InterruptedException("interrupted");
                default:
                    throw new IllegalStateException("Unhandled fake future mode");
            }
        }
    }

    private static final class RecordingExecutor extends AbstractExecutorService {
        private final Future<?> future;
        private final List<Runnable> submittedTasks = new ArrayList<>();
        private boolean shutdown;

        private RecordingExecutor(Future<?> future) {
            this.future = future;
        }

        @Override
        public void shutdown() {
            this.shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            this.shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return this.shutdown;
        }

        @Override
        public boolean isTerminated() {
            return this.shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            this.submittedTasks.add(command);
        }

        @Override
        public Future<?> submit(Runnable task) {
            this.submittedTasks.add(task);
            return this.future;
        }
    }
}
