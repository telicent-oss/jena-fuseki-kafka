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

package org.apache.jena.fuseki.kafka;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Projector;
import io.telicent.smart.cache.projectors.driver.ProjectorDriver;
import io.telicent.smart.cache.sources.Event;
import org.apache.jena.kafka.common.FusekiProjector;
import org.apache.kafka.common.utils.Bytes;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the dataset-scoped pause/resume/wait/initial-load API on {@link FKS}.
 * <p>
 * {@code FKS} keeps registered drivers in a {@code private static final Map} that is
 * populated by {@code startTopicPoll} (which requires a full Fuseki + Kafka harness).
 * For unit-level coverage we instead inject mock {@link ProjectorDriver}s into that
 * map via reflection, exercise the public API, and clear the map after each test.
 * <p>
 * This deliberately does NOT spin up real projector threads — those interactions are
 * verified by {@link org.apache.jena.kafka.common.TestFusekiProjectorReadiness} and the
 * SCG end-to-end integration test.
 */
public class TestFKSPauseResume {

    private static final String DATASET = "/test-ds";

    @AfterMethod
    public void cleanup() {
        drivers().clear();
    }

    // -----------------------------------------------------------------------------------
    // pauseProjectors / resumeProjectors
    // -----------------------------------------------------------------------------------

    @Test
    public void givenUnknownDataset_whenPauseProjectorsCalled_thenNoOp() {
        // Given/When -- no exception expected
        FKS.pauseProjectors("/unknown-dataset");
        // Then -- nothing to assert beyond non-failure
    }

    @Test
    public void givenUnknownDataset_whenResumeProjectorsCalled_thenNoOp() {
        FKS.resumeProjectors("/unknown-dataset");
    }

    @Test
    public void givenRegisteredProjectors_whenPauseProjectorsCalled_thenRequestPauseInvokedOnEach() {
        // Given -- two projectors registered against the dataset
        FusekiProjector p1 = mock(FusekiProjector.class);
        FusekiProjector p2 = mock(FusekiProjector.class);
        registerDrivers(DATASET, mockDriverFor(p1), mockDriverFor(p2));

        // When
        FKS.pauseProjectors(DATASET);

        // Then -- the pause request is propagated to every projector for that dataset
        verify(p1, times(1)).requestPause();
        verify(p2, times(1)).requestPause();
    }

    @Test
    public void givenRegisteredProjectors_whenResumeProjectorsCalled_thenRequestResumeInvokedOnEach() {
        // Given
        FusekiProjector p1 = mock(FusekiProjector.class);
        FusekiProjector p2 = mock(FusekiProjector.class);
        registerDrivers(DATASET, mockDriverFor(p1), mockDriverFor(p2));

        // When
        FKS.resumeProjectors(DATASET);

        // Then
        verify(p1, times(1)).requestResume();
        verify(p2, times(1)).requestResume();
    }

    @Test
    public void givenDriverWithNonFusekiProjector_whenPauseProjectorsCalled_thenSkippedSilently() {
        // Given -- a driver whose projector is some other Projector implementation. The FKS
        // API only knows how to drive FusekiProjector; other types must be skipped without
        // error.
        Projector<?, ?> nonFuseki = mock(Projector.class);
        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver = mockDriverWithProjector(nonFuseki);
        registerDrivers(DATASET, driver);

        // When / Then -- no exception
        FKS.pauseProjectors(DATASET);
        FKS.resumeProjectors(DATASET);
    }

    // -----------------------------------------------------------------------------------
    // waitForPause
    // -----------------------------------------------------------------------------------

    @Test
    public void givenUnknownDataset_whenWaitForPause_thenReturnsTrueImmediately() {
        // Given/When
        boolean result = FKS.waitForPause("/unknown-dataset", Duration.ofMillis(50));

        // Then -- no drivers means nothing to wait for; vacuously satisfied
        Assert.assertTrue(result);
    }

    @Test
    public void givenEmptyDriverList_whenWaitForPause_thenReturnsTrueImmediately() {
        // Given -- an explicit empty entry in the drivers map (edge case distinct from missing)
        registerDrivers(DATASET);  // varargs with no drivers => empty list

        // When
        boolean result = FKS.waitForPause(DATASET, Duration.ofMillis(50));

        // Then
        Assert.assertTrue(result);
    }

    @Test
    public void givenAllProjectorsAtPausePoint_whenWaitForPause_thenReturnsTrue() {
        // Given
        FusekiProjector p1 = mock(FusekiProjector.class);
        when(p1.isAtPausePoint()).thenReturn(true);
        FusekiProjector p2 = mock(FusekiProjector.class);
        when(p2.isAtPausePoint()).thenReturn(true);
        registerDrivers(DATASET, mockDriverFor(p1), mockDriverFor(p2));

        // When
        boolean result = FKS.waitForPause(DATASET, Duration.ofSeconds(5));

        // Then
        Assert.assertTrue(result);
    }

    @Test
    public void givenProjectorNotAtPausePoint_whenWaitForPause_thenTimesOut() {
        // Given -- one projector reports it has reached the pause point, the other has not.
        // waitForPause must wait for ALL projectors (AND) and time out when one of them
        // never gets there.
        FusekiProjector p1 = mock(FusekiProjector.class);
        when(p1.isAtPausePoint()).thenReturn(true);
        FusekiProjector p2 = mock(FusekiProjector.class);
        when(p2.isAtPausePoint()).thenReturn(false);
        registerDrivers(DATASET, mockDriverFor(p1), mockDriverFor(p2));

        // When -- short timeout so the test stays fast
        long startMillis = System.currentTimeMillis();
        boolean result = FKS.waitForPause(DATASET, Duration.ofMillis(150));
        long elapsedMillis = System.currentTimeMillis() - startMillis;

        // Then
        Assert.assertFalse(result, "Should report timeout when not all projectors paused");
        Assert.assertTrue(elapsedMillis >= 100,
                          "Should have actually waited approximately the timeout duration; " +
                          "instead returned in " + elapsedMillis + "ms");
    }

    @Test
    public void givenDriverWithNonFusekiProjector_whenWaitForPause_thenReturnsTrueImmediately() {
        // Given -- non-FusekiProjector drivers can't be paused via this API and are treated
        // as already paused so they don't block the wait.
        Projector<?, ?> nonFuseki = mock(Projector.class);
        registerDrivers(DATASET, mockDriverWithProjector(nonFuseki));

        // When
        boolean result = FKS.waitForPause(DATASET, Duration.ofMillis(50));

        // Then
        Assert.assertTrue(result);
    }

    @Test
    public void givenInterrupted_whenWaitForPause_thenReturnsFalseAndPreservesInterruptFlag() throws InterruptedException {
        // Given -- a projector that will never pause, plus an interrupted thread state.
        // Sleep inside waitForPause will throw InterruptedException, which the method
        // converts into a "false" return and re-asserts the interrupt flag.
        FusekiProjector p = mock(FusekiProjector.class);
        when(p.isAtPausePoint()).thenReturn(false);
        registerDrivers(DATASET, mockDriverFor(p));

        Thread.currentThread().interrupt();

        // When -- give it a long timeout; we expect interrupt to short-circuit immediately
        boolean result;
        try {
            result = FKS.waitForPause(DATASET, Duration.ofSeconds(10));
        } finally {
            // Always swallow the interrupt before assertions/teardown so the rest of the
            // test framework doesn't see a stale flag.
            boolean wasInterrupted = Thread.interrupted();
            // Then -- the interrupt flag should still be set after waitForPause returns
            Assert.assertTrue(wasInterrupted,
                              "waitForPause must re-assert the interrupt flag before returning");
        }
        Assert.assertFalse(result, "Should return false on interrupt");
    }

    // -----------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------

    /**
     * Reflectively reaches into {@code FKS.DRIVERS} so we can populate it for tests. This is
     * how the existing TestFKS-style tests would normally test methods that depend on that
     * private static state — the alternative is a full Fuseki + Kafka harness which is
     * overkill for unit coverage of these helpers.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, List<ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>>>> drivers() {
        try {
            Field f = FKS.class.getDeclaredField("DRIVERS");
            f.setAccessible(true);
            return (Map<String, List<ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>>>>) f.get(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to reflect FKS.DRIVERS — has the field been renamed?", e);
        }
    }

    @SafeVarargs
    private static void registerDrivers(String datasetName,
                                        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>>... drivers) {
        drivers().put(datasetName, new ArrayList<>(List.of(drivers)));
    }

    @SuppressWarnings("unchecked")
    private static ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> mockDriverFor(FusekiProjector projector) {
        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver = mock(ProjectorDriver.class);
        when(driver.getProjector()).thenReturn((Projector) projector);
        return driver;
    }

    @SuppressWarnings("unchecked")
    private static ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> mockDriverWithProjector(Projector<?, ?> projector) {
        ProjectorDriver<Bytes, RdfPayload, Event<Bytes, RdfPayload>> driver = mock(ProjectorDriver.class);
        when(driver.getProjector()).thenReturn((Projector) projector);
        return driver;
    }
}
