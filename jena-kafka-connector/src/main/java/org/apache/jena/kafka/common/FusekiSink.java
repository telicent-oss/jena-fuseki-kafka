package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.sources.Event;
import lombok.*;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.utils.RDFChangesApplyExternalTransaction;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.utils.Bytes;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString
public class FusekiSink implements Sink<Event<Bytes, RdfPayload>> {

    @NonNull
    private final DatasetGraph dataset;

    @Override
    public void send(Event<Bytes, RdfPayload> event) {
        // Apply the payload to the dataset
        try {
            if (event.value().isDataset()) {
                applyDatasetEvent(event);
            } else {
                applyRdfPatchEvent(event);
            }

        } catch (Throwable e) {
            throw new JenaKafkaException(
                    "Failed to apply " + (event.value().isPatch() ? "RDF Patch" : "Dataset") + " payload", e);
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
}
