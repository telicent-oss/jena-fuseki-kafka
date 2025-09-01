package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.sinks.NullSink;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import io.telicent.smart.cache.sources.Header;
import io.telicent.smart.cache.sources.TelicentHeaders;
import io.telicent.smart.cache.sources.memory.SimpleEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFWriter;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestFusekiProjectorHighLag extends AbstractFusekiProjectorTests {

    private static final String NTRIPLES = Lang.NTRIPLES.getContentType().getContentTypeStr();

    private EventSource<Bytes, RdfPayload> createHighVolumeSource(long maxRemaining, double multiplier) {
        return createHighVolumeSource(maxRemaining, multiplier, AbstractFusekiProjectorTests::createTestDatasetEvent);
    }

    private EventSource<Bytes, RdfPayload> createHighVolumeSource(long maxRemaining, double multiplier,
                                                                  Supplier<Event<Bytes, RdfPayload>> eventSupplier) {
        return new RemainingVolumeSource(maxRemaining, multiplier, eventSupplier);
    }


    @Test
    public void givenHighLagSource_whenProjectingLessThanBatchSize_thenHighLagDetected_andNoCommit() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createHighVolumeSource(50_000, 1);
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 5_000);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            sendEvents(projector, source, sink, 2_500);
        }

        // Then
        Assertions.assertTrue(projector.isHighLagDetected());
        verify(dsg, times(1)).begin((TxnType) any());

        // And
        verify(dsg, never()).commit();
    }

    @Test
    public void givenHighLagSource_whenProjectingBatchSizeEvents_thenHighLagDetected_andNoCommit() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createHighVolumeSource(50_000, 1);
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 5_000);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            sendEvents(projector, source, sink, 5_000);
        }

        // Then
        Assertions.assertTrue(projector.isHighLagDetected());
        verify(dsg, times(1)).begin((TxnType) any());

        // And
        verify(dsg, never()).commit();
    }

    @Test
    public void givenHighLagSourceWithAdvancedConnectorConfiguration_whenProjectingBatchSizeEvents_thenHighLagNotDetected_andCommitsAsNormal() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createHighVolumeSource(50_000, 1);
        Properties properties = new Properties();
        properties.put(SysJenaKafka.FUSEKI_KAFKA_HIGH_LAG_THRESHOLD, "100000");
        FusekiProjector projector = buildProjector(createTestConnector(properties), source, dsg, 50_000);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            sendEvents(projector, source, sink, 50_000);
        }

        // Then
        Assertions.assertFalse(projector.isHighLagDetected());
        verify(dsg, times(1)).begin((TxnType) any());

        // And
        verify(dsg, times(1)).commit();
    }

    @Test
    public void givenSourceWhoseLagEventuallyReachesZero_whenProjecting_thenHighLagDetectedInitially_andEventuallyDisabled() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createHighVolumeSource(15_000, 0.8);
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 5_000);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            sendEvents(projector, source, sink, 5_000);

            // Then
            Assertions.assertTrue(projector.isHighLagDetected());

            // And
            while (!source.isExhausted()) {
                sendEvents(projector, source, sink, 5_000);
            }
            Assertions.assertFalse(projector.isHighLagDetected());
            verify(dsg, atLeastOnce()).begin((TxnType) any());
            verify(dsg, atLeastOnce()).commit();
        }
    }

    @Test
    public void givenHighLagSource_whenProjectingLargeEvents_thenHighLagDetected_andCommitTriggeredByEventSize() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        Graph graph = GraphFactory.createGraphMem();
        graph.add(NodeFactory.createURI("https://example.org/s"), NodeFactory.createURI("https://example.org/p"),
                  NodeFactory.createLiteralString(RandomStringUtils.insecure().nextAlphanumeric(256 * 1024)));
        byte[] graphData = RDFWriter.create()
                                    .source(graph)
                                    .lang(Lang.NTRIPLES)
                                    .build()
                                    .asString()
                                    .getBytes(StandardCharsets.UTF_8);
        Event<Bytes, RdfPayload> event =
                new SimpleEvent<>(List.of(new Header(TelicentHeaders.CONTENT_TYPE, NTRIPLES)), null,
                                  RdfPayload.of(NTRIPLES, graphData));
        EventSource<Bytes, RdfPayload> source = createHighVolumeSource(50_000, 1, () -> event);
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 5_000);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            sendEvents(projector, source, sink, 10_000);
        }

        // Then
        Assertions.assertTrue(projector.isHighLagDetected());
        verify(dsg, atLeastOnce()).begin((TxnType) any());

        // And
        verify(dsg, atLeastOnce()).commit();
    }
}
