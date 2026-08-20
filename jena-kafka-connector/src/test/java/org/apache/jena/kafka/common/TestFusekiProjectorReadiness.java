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
import io.telicent.smart.cache.sources.memory.InMemoryEventSource;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

class TestFusekiProjectorReadiness extends AbstractFusekiProjectorTests {

    // -----------------------------------------------------------------------------------
    // requestPause / requestResume / isAtPausePoint
    // -----------------------------------------------------------------------------------

    @Test
    void givenFreshProjector_whenInspecting_thenNotPaused() {
        // Given
        FusekiProjector projector = newProjectorWithSingleEvent();

        // When and Then
        Assertions.assertFalse(projector.isAtPausePoint(),
                               "Fresh projector should not report being at the pause point");
    }

    @Test
    void givenNoPause_whenRequestResumeCalled_thenIdempotent() {
        // Given
        FusekiProjector projector = newProjectorWithSingleEvent();

        // When -- resume without prior pause
        projector.requestResume();
        projector.requestResume();

        // Then -- no exception, still not paused
        Assertions.assertFalse(projector.isAtPausePoint());
    }

    @Test
    @Timeout(10)
    void givenPauseRequestedBeforeProject_whenProjectCalled_thenItBlocksUntilResume()
            throws Exception {
        // Given -- pause is requested before any events are processed
        KConnectorDesc connector = createTestConnector();
        InMemoryEventSource<Bytes, RdfPayload> source =
                new InMemoryEventSource<>(List.of(createTestDatasetEvent()));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 100);
        projector.requestPause();

        // When -- run project() on a worker thread. It should block at the pause check at the
        // top of project() because we already requested pause.
        Event<Bytes, RdfPayload> event = source.poll(Duration.ZERO);
        CompletableFuture<Void> projectCall = CompletableFuture.runAsync(() -> {
            try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
                projector.project(event, sink);
            }
        });

        // Wait until the worker thread reports it has reached the pause point.
        waitFor(projector::isAtPausePoint, Duration.ofSeconds(5),
                "projector did not reach pause point");
        // While paused, the worker must still be blocked -- the project call has not returned.
        Assertions.assertFalse(projectCall.isDone(),
                               "project() should still be blocked while pause is in effect");

        // Then -- requesting resume should release the worker.
        projector.requestResume();
        projectCall.get(5, TimeUnit.SECONDS);
        Assertions.assertFalse(projector.isAtPausePoint(),
                               "After resume, projector should not be at the pause point");
    }

    @Test
    @Timeout(10)
    void givenInflightTransaction_whenPauseRequested_thenTransactionCommittedBeforePauseBlocks()
            throws Exception {
        // Given -- a projector that has processed one event (so it's mid-batch with an open
        // Jena transaction) but the latch has flipped because remaining==0 after that event.
        // We use a 2-event source plus batch size 100, then process the first event, leaving
        // the second un-polled. Actually simpler: process one event with a non-empty source
        // afterwards.
        KConnectorDesc connector = createTestConnector();
        InMemoryEventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(
                List.of(createTestDatasetEvent(), createTestDatasetEvent()));
        DatasetGraph dsg = mockDatasetGraph();
        FusekiProjector projector = buildProjector(connector, source, dsg, 100);
        try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
            // Process one event -- this leaves remaining()==1 so commitTransactionIfNeeded
            // does NOT commit (no zero-lag trigger, batch not full, max duration not reached).
            // After this call, the projector is inside an open Jena transaction.
            projector.project(source.poll(Duration.ZERO), sink);
        }
        Assertions.assertTrue(dsg.isInTransaction(),
                              "Pre-condition: projector should still hold an open transaction");

        // When -- request pause and then call project() with the second event. The pause-check
        // at the top of project() should fire awaitResumeIfPaused, which must commit the
        // in-flight transaction BEFORE blocking.
        projector.requestPause();
        Event<Bytes, RdfPayload> secondEvent = source.poll(Duration.ZERO);
        CompletableFuture<Void> projectCall = CompletableFuture.runAsync(() -> {
            try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
                projector.project(secondEvent, sink);
            }
        });

        waitFor(projector::isAtPausePoint, Duration.ofSeconds(5),
                "projector did not reach pause point");

        // Then -- the commit must have happened before the projector blocked. Restore handlers
        // depend on this: when waitForPause() returns true, the dataset must be idle with no
        // open Jena transaction.
        Assertions.assertFalse(dsg.isInTransaction(),
                               "Pause must commit the in-flight transaction before blocking");
        verify(dsg, times(1)).commit();

        // Cleanup -- release the worker so it doesn't hang the test JVM.
        projector.requestResume();
        projectCall.get(5, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(10)
    void givenPauseRequested_whenStalledCalled_thenItAlsoBlocksUntilResume() throws Exception {
        // Given -- pause is requested. The projector is idle (no events flowing). The
        // driver's stalled() callback must also observe the pause -- this is the path that
        // matters for quiet/low-volume topics where project() is rarely called.
        FusekiProjector projector = newProjectorWithEmptySource();
        projector.requestPause();

        // When -- simulate the driver calling stalled() on the worker thread
        CompletableFuture<Void> stalledCall = CompletableFuture.runAsync(() -> {
            try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
                projector.stalled(sink);
            }
        });

        waitFor(projector::isAtPausePoint, Duration.ofSeconds(5),
                "stalled() did not reach pause point");
        Assertions.assertFalse(stalledCall.isDone(),
                               "stalled() should remain blocked while pause is in effect");

        // Then -- resume releases stalled()
        projector.requestResume();
        stalledCall.get(5, TimeUnit.SECONDS);
        Assertions.assertFalse(projector.isAtPausePoint());
    }

    @Test
    @Timeout(10)
    void givenPauseRequested_whenIdleCalled_thenItAlsoBlocksUntilResume() throws Exception {
        // Given -- pause is requested. The projector is idle (no events flowing) and, crucially,
        // stalled some time ago, so the driver will not call stalled() again. idle() is called on
        // every poll that yields no events and is therefore the only path by which a long quiet
        // projector can observe the pause request. Without it a restore would wait in vain.
        final FusekiProjector projector = newProjectorWithEmptySource();
        projector.requestPause();

        // When -- simulate the driver calling idle() on the worker thread
        final CompletableFuture<Void> idleCall = CompletableFuture.runAsync(() -> {
            try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
                projector.idle(sink);
            }
        });

        waitFor(projector::isAtPausePoint, Duration.ofSeconds(5),
                "idle() did not reach pause point");
        Assertions.assertFalse(idleCall.isDone(),
                               "idle() should remain blocked while pause is in effect");

        // Then -- resume releases idle()
        projector.requestResume();
        idleCall.get(5, TimeUnit.SECONDS);
        Assertions.assertFalse(projector.isAtPausePoint());
    }

    @Test
    @Timeout(10)
    void givenNoPause_whenIdleCalled_thenReturnsImmediately() {
        // Given -- no pause requested, i.e. the steady state for every poll of a quiet topic
        final FusekiProjector projector = newProjectorWithEmptySource();

        // When and Then -- idle() must not block, it is called on every poll
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            try (NullSink<Event<Bytes, RdfPayload>> sink = NullSink.of()) {
                projector.idle(sink);
            }
        });
        Assertions.assertFalse(projector.isAtPausePoint());
    }

    @Test
    @Timeout(30)
    void givenLongStalledDriver_whenPauseRequested_thenProjectorReachesPausePointWithoutAnyEvents() throws Exception {
        // Given -- a real driver polling a quiet source with a real projector, i.e. the state a
        // dataset is in when a restore is run on a caught up system. The driver only reports the
        // first of a run of consecutive stalls, so by the time we request the pause below the
        // stalled() notification is long gone and only idle() can deliver it.
        final QuietEventSource source = new QuietEventSource();
        final FusekiProjector projector =
                buildProjector(createTestConnector(), source, mockDatasetGraph(), 100);
        final ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver =
                ProjectorDriver.<Bytes, RdfPayload, Event<Bytes, RdfPayload>>create()
                               .source(source)
                               .projector(projector)
                               .destination(NullSink.of())
                               .unlimited()
                               .pollTimeout(Duration.ofMillis(200))
                               .build();
        final CompletableFuture<Void> driverRun = CompletableFuture.runAsync(driver);
        waitFor(() -> driver.getConsecutiveStalls() >= 3, Duration.ofSeconds(10),
                "Driver did not stall repeatedly");

        try {
            // When
            projector.requestPause();

            // Then -- this is what FKS.waitForPause() polls for on behalf of a restore
            waitFor(projector::isAtPausePoint, Duration.ofSeconds(5),
                    "Projector did not reach its pause point while stalled");
        } finally {
            projector.requestResume();
            driver.cancel();
            driverRun.get(10, TimeUnit.SECONDS);
        }
    }

    // -----------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------

    private static FusekiProjector newProjectorWithSingleEvent() {
        KConnectorDesc connector = createTestConnector();
        InMemoryEventSource<Bytes, RdfPayload> source =
                new InMemoryEventSource<>(List.of(createTestDatasetEvent()));
        DatasetGraph dsg = mockDatasetGraph();
        return buildProjector(connector, source, dsg, 100);
    }

    private static FusekiProjector newProjectorWithEmptySource() {
        KConnectorDesc connector = createTestConnector();
        InMemoryEventSource<Bytes, RdfPayload> source = new InMemoryEventSource<>(List.of());
        DatasetGraph dsg = mockDatasetGraph();
        return buildProjector(connector, source, dsg, 100);
    }

    /**
     * Polls a condition at short intervals until it becomes true, or fails the test on
     * timeout. Used to wait for the projector thread to reach the pause point without
     * having to instrument the projector itself.
     */
    private static void waitFor(java.util.function.BooleanSupplier condition, Duration timeout,
                                String failureMessage) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
        }
        Assertions.fail(failureMessage + " (within " + timeout + ")");
    }
}
