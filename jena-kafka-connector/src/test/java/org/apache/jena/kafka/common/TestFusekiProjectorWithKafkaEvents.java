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
import org.apache.jena.query.TxnType;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
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
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestFusekiProjectorWithKafkaEvents extends AbstractFusekiProjectorTests {


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
    public void givenProjectorInTransaction_whenStalling_thenCommits() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        KConnectorDesc connector = createTestConnector();
        NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(createKafkaTestEvents(10));
        FusekiProjector projector = buildProjector(connector, source, dsg, 10, sink);

        // When
        projector.project(createTestDatasetEvent(), sink);
        projector.stalled(sink);

        // Then
        Assertions.assertEquals(1L, sink.count());
        verify(dsg, times(1)).begin((TxnType) any());
        verify(dsg, times(1)).commit();
    }

    @Test
    public void givenNonBatchingProjector_whenStalling_thenNoAdditionalCommits() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        KConnectorDesc connector = createTestConnector();
        NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(createKafkaTestEvents(10));
        FusekiProjector projector = buildProjector(connector, source, dsg, 1, sink);

        // When
        projector.project(createTestDatasetEvent(), sink);
        projector.stalled(sink);

        // Then
        Assertions.assertEquals(1L, sink.count());
        verify(dsg, times(1)).begin((TxnType) any());
        verify(dsg, times(1)).commit();
    }

    @Test
    public void givenProjectorWithMaxTransactionDuration_whenReceivingEventsSlowly_thenCommitsTriggeredByTimeThreshold() throws
            InterruptedException {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        KConnectorDesc connector = createTestConnector();
        NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(createKafkaTestEvents(10));
        FusekiProjector projector = buildProjector(connector, source, dsg, 10, Duration.ofMillis(100), sink);

        // When
        projector.project(createTestDatasetEvent(), sink);
        Thread.sleep(500);
        // NB - We slept longer than our max transaction duration of 100 milliseconds, next projected event should
        //      trigger a commit due to exceeding the duration
        projector.project(createTestDatasetEvent(), sink);

        // Then
        Assertions.assertEquals(2L, sink.count());
        verify(dsg, times(1)).begin((TxnType) any());
        verify(dsg, times(1)).commit();
    }

    @Test
    public void givenProjectorWithMaxTransactionDuration_whenReceivingEventsFasterThanMaxDuration_thenNoCommits() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        KConnectorDesc connector = createTestConnector();
        NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of();
        EventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(createKafkaTestEvents(10));
        FusekiProjector projector = buildProjector(connector, source, dsg, 1_000, Duration.ofMillis(100), sink);

        // When
        // NB - Intentionally 1 fewer than batch size, and source will have remaining events so shouldn't trigger
        //      a commit, plus no sleeps so max duration should not be exceeded
        for (int i = 1; i <= 999; i++) {
            projector.project(createTestDatasetEvent(), sink);
        }

        // Then
        Assertions.assertEquals(999L, sink.count());
        verify(dsg, never()).commit();
    }
}
