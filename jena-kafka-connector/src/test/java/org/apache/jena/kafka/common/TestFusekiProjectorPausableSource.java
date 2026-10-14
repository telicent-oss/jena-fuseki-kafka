/*
 *  Copyright (c) Telicent Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.driver.ProjectorDriver;
import io.telicent.smart.cache.projectors.sinks.NullSink;
import io.telicent.smart.cache.sources.Event;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * Pausing a projector whose event source is a {@link io.telicent.smart.cache.sources.PausableEventSource}, e.g. a
 * Kafka source: the source is paused and the projector thread is not blocked, so the driver keeps polling the source
 * (which keeps a Kafka consumer in its consumer group) for as long as the pause lasts
 */
class TestFusekiProjectorPausableSource extends AbstractFusekiProjectorTests {

    private static List<Event<Bytes, RdfPayload>> events(int count) {
        List<Event<Bytes, RdfPayload>> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(createTestDatasetEvent());
        }
        return events;
    }

    @Test
    void givenPausableSource_whenPauseAndResumeRequested_thenSourceIsPausedAndResumed() {
        // Given
        PausableTestEventSource source = new PausableTestEventSource(events(1));
        FusekiProjector projector = buildProjector(createTestConnector(), source, mockDatasetGraph(), 100);

        // When and Then
        projector.requestPause();
        Assertions.assertTrue(source.isPaused());
        projector.requestResume();
        Assertions.assertFalse(source.isPaused());
    }

    @Test
    @Timeout(10)
    void givenPausableSourceAndInflightTransaction_whenIdleWhilePaused_thenCommitsAndReturnsAtPausePoint() {
        // Given -- one event projected, leaving an open transaction as more events remain
        PausableTestEventSource source = new PausableTestEventSource(events(2));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 100);
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            projector.project(source.poll(Duration.ZERO), sink);
            Assertions.assertTrue(dsg.isInTransaction(), "Pre-condition: open transaction");

            // When
            projector.requestPause();
            Assertions.assertFalse(projector.isAtPausePoint(), "Not at the pause point until the projector says so");
            projector.idle(sink);

            // Then -- returned rather than blocking, having committed
            Assertions.assertFalse(dsg.isInTransaction());
            verify(dsg, atLeastOnce()).commit();
            Assertions.assertTrue(projector.isAtPausePoint());

            // And staying idle keeps it there; resuming clears it
            projector.idle(sink);
            Assertions.assertTrue(projector.isAtPausePoint());
            projector.requestResume();
            Assertions.assertFalse(projector.isAtPausePoint());
        }
    }

    @Test
    @Timeout(10)
    void givenPausableSource_whenEventDeliveredAfterPauseRequested_thenProjectedWithoutBlocking() {
        // Given -- an event was polled before the pause took effect on the source
        PausableTestEventSource source = new PausableTestEventSource(events(2));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 100);
        Event<Bytes, RdfPayload> event = source.poll(Duration.ZERO);
        projector.requestPause();

        // When
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> projector.project(event, sink));

            // Then -- it was projected, and only once idle is the projector at its pause point
            Assertions.assertFalse(projector.isAtPausePoint());
            projector.idle(sink);
            Assertions.assertTrue(projector.isAtPausePoint());
            Assertions.assertFalse(dsg.isInTransaction());
        }
    }

    @Test
    @Timeout(10)
    void givenSourceNotYetPaused_whenProjectorGoesIdle_thenDoesNotReportPausePoint() throws Exception {
        // Given -- a pause request that is part way through, the source not yet paused
        PausableTestEventSource source = new PausableTestEventSource(events(1));
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        source.holdPauseUntil(release);
        FusekiProjector projector = buildProjector(createTestConnector(), source, mockDatasetGraph(), 100);
        CompletableFuture<Void> request = CompletableFuture.runAsync(projector::requestPause);
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            // When -- the driver goes idle while the source could still deliver an event
            projector.idle(sink);

            // Then -- acknowledging the pause now would let waitForPause() succeed before delivery stops
            Assertions.assertFalse(projector.isAtPausePoint());
            Assertions.assertFalse(source.isPaused());

            // And once the source is paused, the next idle() reaches the pause point
            release.countDown();
            request.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(source.isPaused());
            projector.idle(sink);
            Assertions.assertTrue(projector.isAtPausePoint());
        }
    }

    @Test
    void givenPausedAndResumed_whenPausedAgain_thenNotAtPausePointUntilProjectorReachesIt() {
        // Given
        PausableTestEventSource source = new PausableTestEventSource(events(1));
        FusekiProjector projector = buildProjector(createTestConnector(), source, mockDatasetGraph(), 100);
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            projector.requestPause();
            projector.idle(sink);
            Assertions.assertTrue(projector.isAtPausePoint());
            projector.requestResume();

            // When
            projector.requestPause();

            // Then -- a stale flag here would let FKS.waitForPause() succeed before anything was committed
            Assertions.assertFalse(projector.isAtPausePoint());
            projector.idle(sink);
            Assertions.assertTrue(projector.isAtPausePoint());
        }
    }

    @Test
    @Timeout(30)
    void givenRunningDriver_whenPaused_thenDriverKeepsPollingSourceAndNothingIsProjectedUntilResumed()
            throws Exception {
        // Given -- a real driver projecting from a pausable source
        PausableTestEventSource source = new PausableTestEventSource(events(5));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(createTestConnector(), source, dsg, 100);
        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver =
                ProjectorDriver.<Bytes, RdfPayload, Event<Bytes, RdfPayload>>create()
                               .source(source)
                               .projector(projector)
                               .destination(NullSink.of())
                               .unlimited()
                               .pollTimeout(Duration.ofMillis(100))
                               .build();
        CompletableFuture<Void> driverRun = CompletableFuture.runAsync(driver);
        try {
            waitFor(() -> driver.getConsecutiveStalls() >= 2, "Driver did not catch up");

            // When
            projector.requestPause();
            waitFor(projector::isAtPausePoint, "Projector did not reach its pause point");
            int pollsAtPause = source.getPausedPolls();

            // Then -- the driver thread isn't blocked, it keeps polling the paused source
            waitFor(() -> source.getPausedPolls() >= pollsAtPause + 5, "Driver stopped polling the paused source");
            Assertions.assertFalse(driverRun.isDone(), "Driver should still be running");
            Assertions.assertTrue(projector.isAtPausePoint());
            Assertions.assertFalse(dsg.isInTransaction(), "Nothing should be in progress while paused");

            // And after resuming it carries on
            projector.requestResume();
            Assertions.assertFalse(source.isPaused());
            waitFor(() -> !projector.isAtPausePoint() && driver.getConsecutiveStalls() >= 1, "Driver did not resume");
        } finally {
            projector.requestResume();
            driver.cancel();
            driverRun.get(10, TimeUnit.SECONDS);
        }
    }

    private static void waitFor(BooleanSupplier condition, String failureMessage) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
        }
        Assertions.fail(failureMessage);
    }
}
