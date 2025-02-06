package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.projectors.SinkException;
import io.telicent.smart.cache.projectors.sinks.NullSink;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import io.telicent.smart.cache.sources.kafka.KafkaEvent;
import io.telicent.smart.cache.sources.memory.InMemoryEventSource;
import io.telicent.smart.cache.sources.memory.SimpleEvent;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestFusekiProjector {
    static {
        JenaSystem.init();
    }

    /**
     * Creates a mock {@link DatasetGraph} that exhibits the normal Jena transaction behaviour and can be used to verify
     * interactions created by the projector and sink
     *
     * @return Mock dataset graph
     */
    public static DatasetGraph mockDatasetGraph() {
        DatasetGraph dsg = Mockito.mock(DatasetGraph.class);
        AtomicBoolean inTransaction = new AtomicBoolean(false);
        when(dsg.isInTransaction()).thenAnswer(x -> inTransaction.get());
        doAnswer(x -> {
            if (inTransaction.get()) {
                throw new JenaTransactionException("Already in transaction");
            }
            inTransaction.set(true);
            return null;
        }).when(dsg).begin(any(TxnType.class));
        doAnswer(x -> {
            if (inTransaction.get()) {
                throw new JenaTransactionException("Already in transaction");
            }
            inTransaction.set(true);
            return null;
        }).when(dsg).begin(any(ReadWrite.class));
        doAnswer(x -> {
            inTransaction.set(false);
            return null;
        }).when(dsg).commit();
        doAnswer(x -> {
            inTransaction.set(false);
            return null;
        }).when(dsg).abort();
        return dsg;
    }

    /**
     * Verifies projection using the real {@link FusekiSink}, this is necessary for verifying handling of RDF patch
     * events
     *
     * @param source                            Event source
     * @param projector                         Projector
     * @param dsg                               Dataset graph
     * @param expectedExternalBeginTransactions Expected number of external begin transaction calls
     * @param expectedPatchBeginTransactions    Expected number of RDF patch begin transaction calls
     * @param expectedCommits                   Expected number of commits
     */
    private static void verifyFusekiSinkProjection(EventSource<Bytes, RdfPayload> source, FusekiProjector projector,
                                                   DatasetGraph dsg, int expectedExternalBeginTransactions,
                                                   int expectedPatchBeginTransactions, int expectedCommits) {
        projectToFusekiSink(source, projector, dsg);

        // Then
        verify(dsg, times(expectedExternalBeginTransactions)).begin((TxnType) any());
        verify(dsg, times(expectedPatchBeginTransactions)).begin((ReadWrite) any());
        verify(dsg, times(expectedCommits)).commit();
    }

    private static void projectToFusekiSink(EventSource<Bytes, RdfPayload> source, FusekiProjector projector,
                                            DatasetGraph dsg) {
        // When
        try (FusekiSink sink = FusekiSink.builder().dataset(dsg).build();) {
            while (!source.isExhausted()) {
                projector.project(source.poll(Duration.ZERO), sink);
            }
        }
    }

    private static SimpleEvent<Bytes, RdfPayload> createTestDatasetEvent() {
        return new SimpleEvent<>(Collections.emptyList(), null,
                                 RdfPayload.of(TestFusekiSink.createSimpleDatasetPayload()));
    }

    public static FusekiProjector buildProjector(KConnectorDesc connector, EventSource<Bytes, RdfPayload> source,
                                                  DatasetGraph dsg, int batchSize) {
        return FusekiProjector.builder().connector(connector).source(source).dataset(dsg).batchSize(batchSize).build();
    }

    public static FusekiProjector buildProjector(KConnectorDesc connector, EventSource<Bytes, RdfPayload> source,
                                                  DatasetGraph dsg, int batchSize, Sink<Event<Bytes, RdfPayload>> dlq) {
        return FusekiProjector.builder()
                              .connector(connector)
                              .source(source)
                              .dataset(dsg)
                              .batchSize(batchSize)
                              .dlq(dlq)
                              .build();
    }

    private static KConnectorDesc createTestConnector() {
        return new KConnectorDesc(List.of("test"), "localhost:9092", "/ds", null, true, false, null, new Properties());
    }

    public static void verifyProjection(EventSource<Bytes, RdfPayload> source, FusekiProjector projector,
                                         DatasetGraph dsg, long expectedEvents, int expectedBeginTransactions,
                                         int expectedCommitTransactions) {
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            while (!source.isExhausted()) {
                projector.project(source.poll(Duration.ZERO), sink);
            }

            // Then
            Assertions.assertEquals(expectedEvents, sink.count());
            verify(dsg, times(expectedBeginTransactions)).begin((TxnType) any());
            verify(dsg, times(expectedCommitTransactions)).commit();
        }
    }

    @Test
    public void givenNoParameters_whenBuildingProjector_thenNPE() {
        // Given, When and Then
        Assertions.assertThrows(NullPointerException.class, () -> FusekiProjector.builder().build());
    }

    @Test
    public void givenOnlyConnector_whenBuildingProjector_thenNPE() {
        // Given
        KConnectorDesc connector = createTestConnector();

        // When and Then
        Assertions.assertThrows(NullPointerException.class,
                                () -> FusekiProjector.builder().connector(connector).build());
    }

    @Test
    public void givenConnectorAndSource_whenBuildingProjector_thenNPE() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());

        // When and Then
        Assertions.assertThrows(NullPointerException.class,
                                () -> FusekiProjector.builder().connector(connector).source(source).build());
    }

    @Test
    public void givenMinimalConfig_whenBuildingProjector_thenOk() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();

        // When
        FusekiProjector projector = FusekiProjector.builder().connector(connector).source(source).dataset(dsg).build();

        // Then
        Assertions.assertNotNull(projector);
        Assertions.assertEquals(FusekiProjector.DEFAULT_BATCH_SIZE, projector.getBatchSize());
    }

    @Test
    public void givenFullConfig_whenBuildingProjector_thenOk() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();

        // When
        FusekiProjector projector = buildProjector(connector, source, dsg, 100);

        // Then
        Assertions.assertNotNull(projector);
        Assertions.assertEquals(100, projector.getBatchSize());
    }

    @Test
    public void givenNonBatchingProjector_whenProjectingSingleEvent_thenProjectedWithSingleTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(createTestDatasetEvent()));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        verifyProjection(source, projector, dsg, 1L, 1, 1);
    }

    @Test
    public void givenNonBatchingProjector_whenProjectingMultipleEvents_thenProjectedWithTransactionPerEvent() {
        // Given
        KConnectorDesc connector = createTestConnector();
        SimpleEvent<Bytes, RdfPayload> event = createTestDatasetEvent();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(event, event, event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        verifyProjection(source, projector, dsg, 3, 3, 3);
    }

    @Test
    public void givenBatchingProjector_whenProjectingMultipleEvents_thenProjectedWithSingleTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        SimpleEvent<Bytes, RdfPayload> event = createTestDatasetEvent();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(event, event, event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 10);

        // When and Then
        verifyProjection(source, projector, dsg, 3, 1, 1);
    }

    @Test
    public void givenBatchingProjector_whenProjectingEventsEquallingBatchSize_thenProjectedWithSingleTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        SimpleEvent<Bytes, RdfPayload> event = createTestDatasetEvent();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(event, event, event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 3);

        // When and Then
        verifyProjection(source, projector, dsg, 3, 1, 1);
    }

    private List<Event<Bytes, RdfPayload>> createKafkaTestEvents(int size) {
        List<Event<Bytes, RdfPayload>> events = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ConsumerRecord<Bytes, RdfPayload> record = new ConsumerRecord<>("test", 0, i, Bytes.wrap(new byte[0]),
                                                                            RdfPayload.of(
                                                                                    TestFusekiSink.createSimpleDatasetPayload()));
            events.add(new KafkaEvent<>(record, null));
        }
        return events;
    }

    @Test
    public void givenBatchingProjector_whenProjectingKafkaEventsEquallingBatchSize_thenProjectedWithSingleTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        List<Event<Bytes, RdfPayload>> kafkaEvents = createKafkaTestEvents(3);
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(kafkaEvents);
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 3);

        // When and Then
        verifyProjection(source, projector, dsg, 3, 1, 1);
    }

    @Test
    public void givenProjector_whenProjectingPatchThatCommitsTheTransaction_thenNoAdditionalCommits() {
        // Given
        KConnectorDesc connector = createTestConnector();
        RDFChangesCollector collector = new RDFChangesCollector();
        collector.txnCommit();
        SimpleEvent<Bytes, RdfPayload> event =
                new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(collector.getRDFPatch()));
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        verifyFusekiSinkProjection(source, projector, dsg, 1, 0, 1);
    }

    @Test
    public void givenProjector_whenProjectingPatchThatLeavesTransactionOpen_thenCommitted() {
        // Given
        KConnectorDesc connector = createTestConnector();
        RDFChangesCollector collector = new RDFChangesCollector();
        collector.txnBegin();
        collector.txnCommit();
        collector.txnBegin();
        SimpleEvent<Bytes, RdfPayload> event =
                new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(collector.getRDFPatch()));
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        verifyFusekiSinkProjection(source, projector, dsg, 1, 2, 3);
    }

    @Test
    public void givenProjector_whenProjectingMalformedPatch_thenErrorThrown_andTransactionAborted() {
        // Given
        KConnectorDesc connector = createTestConnector();
        RDFChangesCollector collector = badNestedTransactionPatch();
        SimpleEvent<Bytes, RdfPayload> event =
                new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(collector.getRDFPatch()));
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        Assertions.assertThrows(JenaKafkaException.class,
                                () -> verifyFusekiSinkProjection(source, projector, dsg, 1, 1, 1));

        // And
        verify(dsg, times(1)).abort();
    }

    @Test
    public void givenProjectorWithDlq_whenProjectingMalformedPatch_thenErrorThrown_andTransactionAborted() {
        // Given
        KConnectorDesc connector = createTestConnector();
        RDFChangesCollector collector = badNestedTransactionPatch();
        SimpleEvent<Bytes, RdfPayload> event =
                new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(collector.getRDFPatch()));
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, NullSink.of());

        // When and Then
        verifyFusekiSinkProjection(source, projector, dsg, 1, 2, 1);

        // And
        verify(dsg, times(1)).abort();
    }

    private static RDFChangesCollector badNestedTransactionPatch() {
        RDFChangesCollector collector = new RDFChangesCollector();
        collector.txnBegin();
        collector.txnBegin();
        return collector;
    }

    @Test
    public void givenProjector_whenProjectingMalformedRdfPayload_thenNoTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(
                List.of(new SimpleEvent<>(Collections.emptyList(), null,
                                          RdfPayload.of("text/unrecognized", new byte[100]))));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        Assertions.assertThrows(JenaKafkaException.class,
                                () -> verifyFusekiSinkProjection(source, projector, dsg, 0, 0, 0));
        verifyNoTransactions(dsg);
    }

    @Test
    public void givenProjector_whenProjectingMalformedRdfPayloadKafkaEvent_thenNoTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(
                List.of(new KafkaEvent<>(new ConsumerRecord<>("test", 0, 0, Bytes.wrap(new byte[0]),
                                                              RdfPayload.of("text/unrecognized", new byte[100])),
                                         null)));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        Assertions.assertThrows(JenaKafkaException.class,
                                () -> verifyFusekiSinkProjection(source, projector, dsg, 0, 0, 0));
        verifyNoTransactions(dsg);
    }

    @Test
    public void givenProjectorWithBrokenDlq_whenProjectingMalformedRdfPayload_thenNoTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(
                List.of(new SimpleEvent<>(Collections.emptyList(), null,
                                          RdfPayload.of("text/unrecognized", new byte[100]))));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, x -> {
            throw new SinkException("DLQ unavailable");
        });

        // When and Then
        Assertions.assertThrows(JenaKafkaException.class,
                                () -> verifyFusekiSinkProjection(source, projector, dsg, 0, 0, 0));
        verifyNoTransactions(dsg);
    }

    private static void verifyNoTransactions(DatasetGraph dsg) {
        verify(dsg, never()).begin((TxnType) any());
        verify(dsg, never()).begin((ReadWrite) any());
        verify(dsg, never()).commit();
    }

    @Test
    public void givenProjectorWithDlq_whenProjectingMalformedRdfPayload_thenNoTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(
                List.of(new SimpleEvent<>(Collections.emptyList(), null,
                                          RdfPayload.of("text/unrecognized", new byte[100]))));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, NullSink.of());

        // When and Then
        verifyFusekiSinkProjection(source, projector, dsg, 0, 0, 0);
        verifyNoTransactions(dsg);
    }

    @Test
    public void givenProjectorWithDlq_whenProjectingValidFollowedByInvalidEvent_thenValidEventsAreCommitted() {
        // Given
        KConnectorDesc connector = createTestConnector();
        RDFChangesCollector collector = new RDFChangesCollector();
        collector.txnAbort();
        collector.txnAbort();
        SimpleEvent<Bytes, RdfPayload> badEvent =
                new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(collector.getRDFPatch()));
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(createTestDatasetEvent(), badEvent));
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        FusekiProjector projector = buildProjector(connector, source, dsg, 10, NullSink.of());

        // When
        projectToFusekiSink(source, projector, dsg);

        // Then
        Assertions.assertEquals(1L, dsg.stream().count());
    }
}
