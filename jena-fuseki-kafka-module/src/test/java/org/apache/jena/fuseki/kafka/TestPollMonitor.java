package org.apache.jena.fuseki.kafka;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class TestPollMonitor {

    private ExecutorService executor;

    @BeforeMethod
    public void freshExecutor() {
        if (this.executor != null) {
            this.executor.shutdownNow();
        }
        this.executor = Executors.newCachedThreadPool();
    }

    @AfterMethod
    public void cleanup() {
        this.executor.shutdownNow();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void givenNullThreadsToMonitor_whenCreatingMonitor_thenNPE() {
        // Given, When and Then
        new FKS.PollThreadMonitor(null, 3);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void givenNegativeCheckInterval_whenCreatingMonitor_thenIllegalArgument() {
        // Given, When and Then
        new FKS.PollThreadMonitor(Collections.emptyList(), -1);
    }

    private static final class Sleepy implements Runnable {

        @Override
        public void run() {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private static final class Breaky implements Runnable {
        @Override
        public void run() {
            throw new RuntimeException("Breaks");
        }
    }

    private static final class Completey implements Runnable {
        @Override
        public void run() {
            return;
        }
    }

    @Test
    public void givenThreadsToMonitor_whenRunningPollMonitor_thenMonitored_andCanBePromptlyCancelled() throws
            InterruptedException {
        // Given
        List<Future<?>> futures = new ArrayList<>();
        futures.add(executor.submit(new Sleepy()));
        futures.add(executor.submit(new Breaky()));
        futures.add(executor.submit(new Completey()));

        // When
        FKS.PollThreadMonitor monitor = new FKS.PollThreadMonitor(futures, 1);
        Future<?> f = executor.submit(monitor);

        // Then
        verifyMonitorStillRunning(f);

        // And
        verifyPromptCancellation(monitor, f);
    }

    private static void verifyMonitorStillRunning(Future<?> f) {
        Assert.assertThrows("Monitor thread should be running", TimeoutException.class,
                            () -> f.get(3, TimeUnit.SECONDS));
    }

    @Test
    public void givenCancelledThreadsToMonitor_whenRunningPollMonitor_thenMonitored_andCanBePromptlyCancelled() throws
            InterruptedException {
        // Given
        List<Future<?>> futures = new ArrayList<>();
        futures.add(executor.submit(new Sleepy()));
        futures.add(executor.submit(new Breaky()));
        for (Future<?> f : futures) {
            f.cancel(true);
        }

        // When
        FKS.PollThreadMonitor monitor = new FKS.PollThreadMonitor(futures, 1);
        Future<?> f = executor.submit(monitor);

        // Then
        verifyMonitorStillRunning(f);

        // And
        verifyPromptCancellation(monitor, f);
    }

    private static void verifyPromptCancellation(FKS.PollThreadMonitor monitor, Future<?> future) throws InterruptedException {
        monitor.cancel();
        Thread.sleep(100);
        Assert.assertTrue(future.isDone());
    }
}
