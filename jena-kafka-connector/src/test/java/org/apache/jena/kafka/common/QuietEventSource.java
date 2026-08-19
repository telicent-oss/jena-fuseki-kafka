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
import io.telicent.smart.cache.sources.EventSource;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.Collection;

/**
 * An event source that is caught up but not exhausted, i.e. every poll blocks for the timeout and then yields nothing.
 * <p>
 * This models a quiet Kafka topic, the state in which a projector is left stalled indefinitely in the driver's poll
 * loop.
 * </p>
 */
public class QuietEventSource implements EventSource<Bytes, RdfPayload> {

    private volatile boolean closed = false;

    @Override
    public boolean availableImmediately() {
        // Nothing is ever immediately available, so the driver expects poll() to block
        return false;
    }

    @Override
    public boolean isExhausted() {
        // Caught up is not the same as exhausted, the topic may receive more events later
        return this.closed;
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public Event<Bytes, RdfPayload> poll(Duration timeout) {
        try {
            Thread.sleep(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    @Override
    public Long remaining() {
        return 0L;
    }

    @Override
    public void processed(Collection<Event<?, ?>> processedEvents) {
        // No-op
    }
}
