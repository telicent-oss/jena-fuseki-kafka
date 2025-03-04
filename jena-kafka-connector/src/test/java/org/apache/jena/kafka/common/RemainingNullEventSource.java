package org.apache.jena.kafka.common;

import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.memory.InMemoryEventSource;

import java.util.Collection;

public class RemainingNullEventSource<TKey, TValue> extends InMemoryEventSource<TKey, TValue> {
    /**
     * Creates a new in-memory event source
     *
     * @param events Events that the source will provide
     */
    public RemainingNullEventSource(Collection<Event<TKey, TValue>> events) {
        super(events);
    }

    @Override
    public Long remaining() {
        return null;
    }
}
