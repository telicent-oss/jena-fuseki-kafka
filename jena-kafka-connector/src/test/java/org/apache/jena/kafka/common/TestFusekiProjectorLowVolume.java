package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.sinks.NullSink;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestFusekiProjectorLowVolume extends AbstractFusekiProjectorTests {

    private EventSource<Bytes, RdfPayload> createLowVolumeSource(long maxRemaining, double multiplier) {
        return new RemainingVolumeSource(maxRemaining, multiplier,
                                         AbstractFusekiProjectorTests::createTestDatasetEvent);
    }

    @Test
    public void givenLowVolumeSource_whenProjecting_thenLowVolumeModeIsEngaged_andSubsequentEventsAreBatchedAppropriately() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createLowVolumeSource(1, 1);
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 100);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            sendEvents(projector, source, sink, SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW);

            // Then
            Assertions.assertTrue(projector.isLowVolumeDetected());
            Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW, sink.count());
            verify(dsg, times(25)).begin((TxnType) any());
            verify(dsg, times(25)).commit();

            // And
            sendEvents(projector, source, sink, 100);
            Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW + 100, sink.count());
            verify(dsg, times(26)).begin((TxnType) any());
            verify(dsg, times(26)).commit();
        }
    }

    @Test
    public void givenLowVolumeSource_whenProjectingWithShortMaxTransactionDuration_thenLowVolumeModeIsEngaged_andSubsequentEventsAreBatchedAppropriately() throws
            InterruptedException {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createLowVolumeSource(1, 1);
        FusekiProjector projector =
                buildProjector(createTestConnector(), source, dsg, 100, Duration.ofMillis(300), null);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            sendEvents(projector, source, sink, SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW);

            // Then
            Assertions.assertTrue(projector.isLowVolumeDetected());
            Assertions.assertEquals(SysJenaKafka.DEFAULT_BATCH_SIZE_TRACKING_WINDOW, sink.count());
            verify(dsg, times(25)).begin((TxnType) any());
            verify(dsg, times(25)).commit();

            // And
            sendEvents(projector, source, sink, 1);
            Thread.sleep(500);
            sendEvents(projector, source, sink, 1);
            verify(dsg, times(26)).begin((TxnType) any());
            verify(dsg, times(26)).commit();
        }
    }

    @Test
    public void givenLowVolumeSourceThatEventuallyGetsHighVolume_whenProjecting_thenLowVolumeModeIsEngagedInitially_andSubsequentlyDisengaged() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createLowVolumeSource(1, 1.1);
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 100);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            while (!projector.isLowVolumeDetected()) {
                sendEvents(projector, source, sink, Math.max(1, source.remaining().intValue()));
            }

            // Then
            verify(dsg, atLeastOnce()).begin((TxnType) any());
            verify(dsg, atLeastOnce()).commit();

            // And
            while (projector.isLowVolumeDetected()) {
                sendEvents(projector, source, sink, Math.max(1, source.remaining().intValue()));
            }
        }
    }

    @Test
    public void givenSourceWhoseVolumeEventuallyBecomesLow_whenProjecting_thenLowVolumeModeEventuallyEngaged() {
        // Given
        DatasetGraph dsg = mockDatasetGraph();
        EventSource<Bytes, RdfPayload> source = createLowVolumeSource(10_000, 0.95);
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 100);

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            while (!source.isExhausted()) {
                sendEvents(projector, source, sink, Math.max(1, source.remaining().intValue()));
            }

            // Then
            verify(dsg, atLeastOnce()).begin((TxnType) any());
            verify(dsg, atLeastOnce()).commit();
            Assertions.assertTrue(projector.isLowVolumeDetected());

        }
    }

}
