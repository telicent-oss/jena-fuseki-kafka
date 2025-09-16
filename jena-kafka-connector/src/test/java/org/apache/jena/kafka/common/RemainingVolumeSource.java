package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * A test event source where the remaining value is configurable and can ramp up over time
 */
public class RemainingVolumeSource implements EventSource<Bytes, RdfPayload> {

    private final double maxRemainingMultiplier;
    private double maxRemaining;
    private long currentRemaining;
    private final Supplier<Event<Bytes, RdfPayload>> eventSupplier;

    /**
     * Creates a new test event source
     *
     * @param initialMaxRemaining    Initial number of remaining events to report
     * @param maxRemainingMultiplier The multiplier that increases/decreases the remaining events over time
     * @param supplier               Supplier for generating the actual events
     */
    public RemainingVolumeSource(long initialMaxRemaining, double maxRemainingMultiplier,
                                 Supplier<Event<Bytes, RdfPayload>> supplier) {
        this.maxRemainingMultiplier = maxRemainingMultiplier;
        this.maxRemaining = initialMaxRemaining;
        this.currentRemaining = initialMaxRemaining;
        this.eventSupplier = supplier;
    }

    @Override
    public boolean availableImmediately() {
        return this.currentRemaining > 0;
    }

    @Override
    public boolean isExhausted() {
        return this.maxRemaining <= 0.0;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public Event<Bytes, RdfPayload> poll(Duration duration) {
        if (this.isExhausted()) {
            return null;
        }

        if (this.currentRemaining == 0) {
            // Reset our remaining, scaling up the max remaining if a multiplier > 1 has been configured
            double prevMaxRemaining = this.maxRemaining;
            this.maxRemaining = prevMaxRemaining * this.maxRemainingMultiplier;
            if (this.maxRemaining < 1) {
                this.maxRemaining = 0;
            }
            this.currentRemaining = Math.max(0, Math.round(this.maxRemaining));
            if (this.currentRemaining == 0) return null;
        }
        this.currentRemaining--;
        return eventSupplier.get();
    }

    @Override
    public Long remaining() {
        return this.currentRemaining;
    }

    @Override
    public void processed(Collection<Event> collection) {

    }
}
