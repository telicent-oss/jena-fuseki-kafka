package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.projectors.sinks.NullSink;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import io.telicent.smart.cache.sources.memory.SimpleEvent;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AbstractFusekiProjectorTests {
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
    protected static void verifyFusekiSinkProjection(EventSource<Bytes, RdfPayload> source, FusekiProjector projector,
                                                     DatasetGraph dsg, int expectedExternalBeginTransactions,
                                                     int expectedPatchBeginTransactions, int expectedCommits) {
        AbstractFusekiProjectorTests.projectToFusekiSink(source, projector, dsg);

        // Then
        verify(dsg, times(expectedExternalBeginTransactions)).begin((TxnType) any());
        verify(dsg, times(expectedPatchBeginTransactions)).begin((ReadWrite) any());
        verify(dsg, times(expectedCommits)).commit();
    }

    protected static void projectToFusekiSink(EventSource<Bytes, RdfPayload> source, FusekiProjector projector,
                                              DatasetGraph dsg) {
        // When
        try (FusekiSink<DatasetGraph> sink = FusekiSink.builder().dataset(dsg).build();) {
            while (!source.isExhausted()) {
                projector.project(source.poll(Duration.ZERO), sink);
            }
        }
    }

    protected static SimpleEvent<Bytes, RdfPayload> createTestDatasetEvent() {
        return new SimpleEvent<>(Collections.emptyList(), null,
                                 RdfPayload.of(TestFusekiSink.createSimpleDatasetPayload()));
    }

    public static FusekiProjector buildProjector(KConnectorDesc connector, EventSource<Bytes, RdfPayload> source,
                                                 DatasetGraph dsg, int batchSize) {
        return AbstractFusekiProjectorTests.buildProjector(connector, source, dsg, batchSize, null, null);
    }

    public static FusekiProjector buildProjector(KConnectorDesc connector, EventSource<Bytes, RdfPayload> source,
                                                 DatasetGraph dsg, int batchSize, Sink<Event<Bytes, RdfPayload>> dlq) {
        return AbstractFusekiProjectorTests.buildProjector(connector, source, dsg, batchSize, null, dlq);
    }

    public static FusekiProjector buildProjector(KConnectorDesc connector, EventSource<Bytes, RdfPayload> source,
                                                 DatasetGraph dsg, int batchSize, Duration maxTransactionDuration,
                                                 Sink<Event<Bytes, RdfPayload>> dlq) {
        return FusekiProjector.builder()
                              .connector(connector)
                              .source(source)
                              .dataset(dsg)
                              .batchSize(batchSize)
                              .maxTransactionDuration(maxTransactionDuration)
                              .dlq(dlq)
                              .build();
    }

    protected static KConnectorDesc createTestConnector() {
        return createTestConnector(new Properties());
    }

    protected static KConnectorDesc createTestConnector(Properties properties) {
        return new KConnectorDesc(List.of("test"), "localhost:9092", "/ds", null, true, false, null, properties);
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

    protected static void verifyNoTransactions(DatasetGraph dsg) {
        verify(dsg, never()).begin((TxnType) any());
        verify(dsg, never()).begin((ReadWrite) any());
        verify(dsg, never()).commit();
    }

    protected static void sendEvents(FusekiProjector projector, EventSource<Bytes, RdfPayload> source,
                                     Sink<Event<Bytes, RdfPayload>> sink, int count) {
        for (int i = 0; i < count; i++) {
            if (source.isExhausted()) return;
            Event<Bytes, RdfPayload> event = source.poll(Duration.ofSeconds(3));
            if (event == null) return;
            projector.project(event, sink);
        }
    }
}
