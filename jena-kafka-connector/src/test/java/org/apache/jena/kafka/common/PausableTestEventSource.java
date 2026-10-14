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
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.PausableEventSource;
import io.telicent.smart.cache.sources.memory.InMemoryEventSource;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pausable in-memory event source that, like a paused Kafka source, delivers nothing while paused but counts the
 * polls it receives, so tests can check the caller kept polling it
 */
public class PausableTestEventSource implements PausableEventSource<Bytes, RdfPayload> {

    private final InMemoryEventSource<Bytes, RdfPayload> events;
    private volatile boolean paused = false;
    private volatile boolean lastPollPaused = false;
    private volatile java.util.concurrent.CountDownLatch holdPause = null;
    private final AtomicInteger pausedPolls = new AtomicInteger();

    public PausableTestEventSource(Collection<Event<Bytes, RdfPayload>> events) {
        this.events = new InMemoryEventSource<>(events);
    }

    @Override
    public void pause() {
        java.util.concurrent.CountDownLatch hold = this.holdPause;
        if (hold != null) {
            try {
                // Simulates a slow pause() so tests can act while a pause is half way through being requested
                hold.await(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        this.paused = true;
    }

    /**
     * Makes {@link #pause()} wait for the given latch before taking effect
     *
     * @param latch Latch
     */
    public void holdPauseUntil(java.util.concurrent.CountDownLatch latch) {
        this.holdPause = latch;
    }

    @Override
    public boolean wasPausedOnLastPoll() {
        return this.lastPollPaused;
    }

    @Override
    public void resume() {
        this.paused = false;
    }

    @Override
    public boolean isPaused() {
        return this.paused;
    }

    /**
     * @return How many times {@link #poll(Duration)} has been called while paused
     */
    public int getPausedPolls() {
        return this.pausedPolls.get();
    }

    @Override
    public boolean availableImmediately() {
        return !this.paused && this.events.availableImmediately();
    }

    @Override
    public boolean isExhausted() {
        // Like Kafka, never exhausted while open, so a driver keeps polling once caught up
        return this.events.isClosed();
    }

    @Override
    public void close() {
        this.events.close();
    }

    @Override
    public boolean isClosed() {
        return this.events.isClosed();
    }

    @Override
    public Event<Bytes, RdfPayload> poll(Duration timeout) {
        this.lastPollPaused = this.paused;
        if (this.lastPollPaused) {
            this.pausedPolls.incrementAndGet();
            sleep(Math.min(timeout.toMillis(), 20));
            return null;
        }
        Event<Bytes, RdfPayload> event = this.events.poll(timeout);
        if (event == null) {
            // Model a poll of a caught up topic, which waits out a (short) timeout
            sleep(Math.min(timeout.toMillis(), 20));
        }
        return event;
    }

    @Override
    public Long remaining() {
        return this.paused ? 0L : this.events.remaining();
    }

    @Override
    public void processed(Collection<Event<?, ?>> processedEvents) {
        this.events.processed(processedEvents);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
