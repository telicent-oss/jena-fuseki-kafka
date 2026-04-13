package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.sources.Event;
import lombok.*;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.utils.RDFChangesApplyExternalTransaction;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.utils.Bytes;

/**
 * A sink that applies incoming events to the target {@link DatasetGraph}.
 * <p>
 * This is intended for use in conjunction with the {@link FusekiProjector}.
 * </p>
 * <p>
 * If your use case requires applying a Kafka event to have additional side effects beyond just updating the
 * {@link DatasetGraph} then you should extend this class and override the {@link #applyDatasetEvent(Event)} and/or
 * {@link #applyRdfPatchEvent(Event)} as appropriate.
 * </p>
 */
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@Builder
@ToString
public class FusekiSink<T extends DatasetGraph> implements Sink<Event<Bytes, RdfPayload>> {

    static final String ROOT_CAUSE_SEPARATOR = ": ";

    /**
     * The dataset this sink is writing to
     */
    @NonNull
    @ToString.Exclude
    protected final T dataset;

    @Override
    public final void send(Event<Bytes, RdfPayload> event) {
        // Apply the payload to the dataset
        try {
            if (event.value().isDataset()) {
                applyDatasetEvent(event);
            } else {
                applyRdfPatchEvent(event);
            }

        } catch (Throwable e) {
            Throwable rootCause = rootCause(e);
            String payloadType = event.value().isPatch() ? "RDF Patch" : "Dataset";
            StringBuilder message = new StringBuilder("Failed to apply ").append(payloadType).append(" payload");
            appendRootCause(message, rootCause);
            throw new JenaKafkaException(message.toString(), e);
        }
    }

    /**
     * Applies a RDF Patch event
     *
     * @param event RDF Patch event
     */
    protected void applyRdfPatchEvent(Event<Bytes, RdfPayload> event) {
        // NB - A RDF Patch might have transaction boundaries in it, so we use our derived applicator class as
        //      that handles those sensibly.  Transaction boundaries can still lead to failures if the
        //      transaction boundaries in the patch are not valid.
        RDFChangesApplyExternalTransaction apply = new RDFChangesApplyExternalTransaction(this.dataset);
        event.value().getPatch().apply(apply);
    }

    /**
     * Applies a Dataset event
     *
     * @param event Dataset event
     */
    protected void applyDatasetEvent(Event<Bytes, RdfPayload> event) {
        event.value().getDataset().stream().forEach(this.dataset::add);

    }

    @Override
    public void close() {
        if (this.dataset.isInTransaction()) {
            this.dataset.commit();
        }
    }

    static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    static void appendRootCause(StringBuilder message, Throwable rootCause) {
        if (rootCause == null) {
            return;
        }

        String rootCauseClass = rootCause.getClass().getSimpleName();
        String rootCauseMessage = rootCause.getMessage();
        if (rootCauseMessage == null || rootCauseMessage.isBlank()) {
            if (!rootCauseClass.isBlank()) {
                message.append(ROOT_CAUSE_SEPARATOR).append(rootCauseClass);
            }
            return;
        }

        if (!rootCauseClass.isBlank()) {
            message.append(ROOT_CAUSE_SEPARATOR).append(rootCauseClass).append(ROOT_CAUSE_SEPARATOR);
        } else {
            message.append(ROOT_CAUSE_SEPARATOR);
        }
        message.append(rootCauseMessage);
    }
}
