package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.projectors.SinkException;
import io.telicent.smart.cache.projectors.sinks.NullSink;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import io.telicent.smart.cache.sources.TelicentHeaders;
import io.telicent.smart.cache.sources.kafka.KafkaEvent;
import io.telicent.smart.cache.sources.memory.InMemoryEventSource;
import io.telicent.smart.cache.sources.memory.SimpleEvent;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;

class TestFusekiProjector extends AbstractFusekiProjectorTests {

    @Test
    void givenNoParameters_whenBuildingProjector_thenNPE() {
        // Given, When and Then
        FusekiProjector.FusekiProjectorBuilder builder = FusekiProjector.builder();
        Assertions.assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void givenOnlyConnector_whenBuildingProjector_thenNPE() {
        // Given
        KConnectorDesc connector = createTestConnector();

        // When and Then
        FusekiProjector.FusekiProjectorBuilder builder = FusekiProjector.builder().connector(connector);
        Assertions.assertThrows(NullPointerException.class,
                                builder::build);
    }

    @Test
    void givenConnectorAndSource_whenBuildingProjector_thenNPE() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());

        // When and Then
        FusekiProjector.FusekiProjectorBuilder builder = FusekiProjector.builder()
                                                                        .connector(connector)
                                                                        .source(source);
        Assertions.assertThrows(NullPointerException.class,
                                builder::build);
    }

    @Test
    void givenMinimalConfig_whenBuildingProjector_thenOk() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();

        // When
        FusekiProjector projector = FusekiProjector.builder().connector(connector).source(source).dataset(dsg).build();

        // Then
        Assertions.assertNotNull(projector);
        Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE, projector.getBatchSize());
    }


    private static Stream<Arguments> badMaxDurations() {
        return Stream.of(() -> new Object[] { null }, Arguments.of(Duration.ZERO), Arguments.of(Duration.ofMinutes(-10)));
    }

    @ParameterizedTest
    @MethodSource(value = "badMaxDurations")
    void givenMinimalConfigAndBadMaxTransactionDuration_whenBuildingProjector_thenDefaultMaxTransactionDurationIsUsed(
            Duration badMax) {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();

        // When
        FusekiProjector projector = FusekiProjector.builder()
                                                   .connector(connector)
                                                   .source(source)
                                                   .dataset(dsg)
                                                   .maxTransactionDuration(badMax)
                                                   .build();

        // Then
        Assertions.assertNotNull(projector);
        Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE, projector.getBatchSize());
        Assertions.assertEquals(SysJenaKafka.DEFAULT_MAX_TRANSACTION_DURATION,
                                projector.getMaxTransactionDuration());
    }

    @Test
    void givenFullConfig_whenBuildingProjector_thenOk() {
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
    void givenNonBatchingProjector_whenProjectingSingleEvent_thenProjectedWithSingleTransaction() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of(createTestDatasetEvent()));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1);

        // When and Then
        verifyProjection(source, projector, dsg, 1L, 1, 1);
    }

    private static Stream<Arguments> projectionBatchingScenarios() {
        return Stream.of(Arguments.of(List.<Event<Bytes, RdfPayload>>of(createTestDatasetEvent(), createTestDatasetEvent(),
                                                                         createTestDatasetEvent()), 1, 3, 3, 3),
                         Arguments.of(List.<Event<Bytes, RdfPayload>>of(createTestDatasetEvent(), createTestDatasetEvent(),
                                                                         createTestDatasetEvent()), 10, 3, 1, 1),
                         Arguments.of(List.<Event<Bytes, RdfPayload>>of(createTestDatasetEvent(), createTestDatasetEvent(),
                                                                         createTestDatasetEvent()), 3, 3, 1, 1),
                         Arguments.of(List.<Event<Bytes, RdfPayload>>of(createTestDatasetEvent(), createTestDatasetEvent(),
                                                                         createTestDatasetEvent()), 100, 3, 1, 1));
    }

    @ParameterizedTest
    @MethodSource("projectionBatchingScenarios")
    void givenProjector_whenProjectingEvents_thenProjectedUsingExpectedTransactionCount(
            List<Event<Bytes, RdfPayload>> events, int batchSize, long projectedEventCount, int expectedTransactionCount,
            int expectedCommitCount) {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(events);
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, batchSize);

        // When and Then
        verifyProjection(source, projector, dsg, projectedEventCount, expectedTransactionCount, expectedCommitCount);
    }

    @Test
    void givenBatchingProjector_whenProjectingFewerEventsThanBatchSizeFromSourceWithNullRemaining_thenNoCommit() {
        // Given
        KConnectorDesc connector = createTestConnector();
        SimpleEvent<Bytes, RdfPayload> event = createTestDatasetEvent();
        EventSource<Bytes, RdfPayload> source = new RemainingNullEventSource<>(List.of(event, event, event));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 100);

        // When and Then
        verifyProjection(source, projector, dsg, 3, 1, 0);
    }

    @Test
    void givenProjector_whenProjectingPatchThatCommitsTheTransaction_thenNoAdditionalCommits() {
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
    void givenProjector_whenProjectingPatchThatLeavesTransactionOpen_thenCommitted() {
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
    void givenProjector_whenProjectingMalformedPatch_thenErrorThrown_andTransactionAborted() {
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
    void givenProjectorWithDlq_whenProjectingMalformedPatch_thenErrorThrown_andTransactionAborted() {
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
    void givenProjector_whenProjectingMalformedRdfPayload_thenNoTransaction() {
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
    void givenProjector_whenProjectingMalformedRdfPayloadKafkaEvent_thenNoTransaction() {
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
    void givenProjectorWithBrokenDlq_whenProjectingMalformedRdfPayload_thenNoTransaction() {
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

    @Test
    void givenProjectorWithDlq_whenProjectingMalformedRdfPayload_thenNoTransaction() {
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
    void givenProjectorWithDlq_whenSinkFails_thenDlqIncludesRootCauseHeaders() {
        // Given
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = mockDatasetGraph();
        List<Event<Bytes, RdfPayload>> dlqEvents = new ArrayList<>();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, dlqEvents::add);
        Event<Bytes, RdfPayload> event =
                new KafkaEvent<>(new ConsumerRecord<>("test", 0, 42L, Bytes.wrap(new byte[0]),
                                                      RdfPayload.of(TestFusekiSink.createSimpleDatasetPayload())),
                                 null);
        Sink<Event<Bytes, RdfPayload>> sink = x -> {
            throw new JenaKafkaException("Failed to apply RDF Patch payload",
                                         new IllegalStateException("No space left on device"));
        };

        // When
        projector.project(event, sink);

        // Then
        Assertions.assertEquals(1, dlqEvents.size());
        Event<Bytes, RdfPayload> dlqEvent = dlqEvents.get(0);
        Assertions.assertEquals("Failed to apply RDF Patch payload: IllegalStateException: No space left on device",
                                dlqEvent.lastHeader(TelicentHeaders.DEAD_LETTER_REASON));
        Assertions.assertEquals("org.apache.jena.kafka.JenaKafkaException",
                                dlqEvent.lastHeader("Dead-Letter-Exception-Class"));
        Assertions.assertEquals("No space left on device", dlqEvent.lastHeader("Dead-Letter-Root-Cause"));
        Assertions.assertEquals("java.lang.IllegalStateException",
                                dlqEvent.lastHeader("Dead-Letter-Root-Cause-Class"));
    }

    @Test
    void givenProjectorWithDlq_whenSinkFails_thenWriteTransactionIsClosedBeforeDlqSend() {
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = mockDatasetGraph();
        List<Event<Bytes, RdfPayload>> dlqEvents = new ArrayList<>();
        AtomicBoolean transactionOpenDuringDlqSend = new AtomicBoolean();
        Sink<Event<Bytes, RdfPayload>> dlq = event -> {
            transactionOpenDuringDlqSend.set(dsg.isInTransaction());
            dlqEvents.add(event);
        };
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, dlq);
        Event<Bytes, RdfPayload> event = createTestDatasetEvent();
        Sink<Event<Bytes, RdfPayload>> failingSink = x -> {
            throw new JenaKafkaException("Failed to apply Dataset payload",
                                         new IllegalStateException("Distribution lifecycle state unavailable"));
        };

        projector.project(event, failingSink);

        Assertions.assertEquals(1, dlqEvents.size());
        Assertions.assertFalse(transactionOpenDuringDlqSend.get(),
                               "A slow DLQ send must not hold Graph's write transaction open");
        verify(dsg).abort();
        Assertions.assertFalse(dsg.isInTransaction());
    }

    @Test
    void givenProjectorWithBrokenDlq_whenSinkFails_thenTransactionIsClosedAndOriginalErrorIsRethrown() {
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = mockDatasetGraph();
        AtomicBoolean transactionOpenDuringDlqSend = new AtomicBoolean();
        Sink<Event<Bytes, RdfPayload>> dlq = event -> {
            transactionOpenDuringDlqSend.set(dsg.isInTransaction());
            throw new SinkException("DLQ unavailable");
        };
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, dlq);
        JenaKafkaException originalError = new JenaKafkaException("Failed to apply Dataset payload");
        Sink<Event<Bytes, RdfPayload>> failingSink = x -> {
            throw originalError;
        };
        Event<Bytes, RdfPayload> event = createTestDatasetEvent();

        JenaKafkaException thrown = Assertions.assertThrows(JenaKafkaException.class,
                () -> projector.project(event, failingSink));

        Assertions.assertSame(originalError, thrown);
        Assertions.assertFalse(transactionOpenDuringDlqSend.get());
        verify(dsg).abort();
        Assertions.assertFalse(dsg.isInTransaction());
    }

    @Test
    void givenAbortFails_whenSinkFails_thenOriginalErrorIsPreservedAndEventIsNotDeadLettered() {
        KConnectorDesc connector = createTestConnector();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        DatasetGraph dsg = mockDatasetGraph();
        doThrow(new JenaTransactionException("Cannot abort transaction")).when(dsg).abort();
        List<Event<Bytes, RdfPayload>> dlqEvents = new ArrayList<>();
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, dlqEvents::add);
        JenaKafkaException originalError = new JenaKafkaException("Failed to apply Dataset payload");
        Sink<Event<Bytes, RdfPayload>> failingSink = x -> {
            throw originalError;
        };
        Event<Bytes, RdfPayload> event = createTestDatasetEvent();

        JenaKafkaException thrown = Assertions.assertThrows(JenaKafkaException.class,
                () -> projector.project(event, failingSink));

        Assertions.assertSame(originalError, thrown,
                              "The original projection failure must propagate, not the abort failure");
        Assertions.assertEquals(1, thrown.getSuppressed().length);
        Assertions.assertInstanceOf(JenaTransactionException.class, thrown.getSuppressed()[0]);
        Assertions.assertEquals("Cannot abort transaction", thrown.getSuppressed()[0].getMessage());
        Assertions.assertTrue(dlqEvents.isEmpty());
    }

    @Test
    void givenProjectorWithDlq_whenProjectingValidFollowedByInvalidEvent_thenValidEventsAreCommitted() {
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

    @Test
    void givenIdleProjector_whenStalling_thenNothingIsCommited() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        KConnectorDesc connector = createTestConnector();
        Sink<Event<Bytes, RdfPayload>> sink = NullSink.of();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(Collections.emptyList());
        FusekiProjector projector = buildProjector(connector, source, dsg, 10, sink);

        // When
        projector.stalled(sink);

        // Then
        verifyNoTransactions(dsg);
    }
}
