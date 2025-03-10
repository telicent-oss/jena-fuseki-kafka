package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.sources.memory.SimpleEvent;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sys.JenaSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class TestFusekiSink {
    static {
        JenaSystem.init();
    }

    @Test
    public void givenNullDataset_whenBuildingSink_thenNPE() {
        // Given
        DatasetGraph dsg = null;

        // When and Then
        assertThrows(NullPointerException.class, () -> FusekiSink.builder().dataset(dsg).build());
    }

    @Test
    public void givenDatasetNotInTransaction_whenClosing_thenNoOp() {
        // Given
        DatasetGraph dsg = Mockito.mock(DatasetGraph.class);
        when(dsg.isInTransaction()).thenReturn(false);

        // When
        FusekiSink sink = FusekiSink.builder().dataset(dsg).build();
        sink.close();

        // Then
        verify(dsg, never()).commit();
    }

    @Test
    public void givenDatasetPayload_whenUsingSink_thenDestinationDatasetIsPopulated() {
        // Given
        DatasetGraph destination = DatasetGraphFactory.createTxnMem();
        DatasetGraph data = createSimpleDatasetPayload();

        // When
        try (FusekiSink sink = FusekiSink.builder().dataset(destination).build()) {
            destination.begin(TxnType.WRITE);
            sink.send(new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(data)));
        }

        // Then
        Assertions.assertEquals(1L, destination.stream().count());
    }

    @Test
    public void givenPatchPayload_whenUsingSink_thenDestinationDatasetIsPopulated() {
        // Given
        DatasetGraph destination = DatasetGraphFactory.createTxnMem();
        RDFChangesCollector patch = new RDFChangesCollector();
        patch.add(Quad.defaultGraphIRI, NodeFactory.createURI("https://example.org/s"),
                  NodeFactory.createURI("https://example.org/p"), NodeFactory.createLiteralString("object"));

        // When
        try (FusekiSink sink = FusekiSink.builder().dataset(destination).build()) {
            destination.begin(TxnType.WRITE);
            sink.send(new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(patch.getRDFPatch())));
        }

        // Then
        Assertions.assertEquals(1L, destination.stream().count());
    }

    @Test
    public void givenFailingPatch_whenUsingSink_thenFails() {
        // Given
        RDFChangesCollector patch = new RDFChangesCollector();
        patch.txnBegin();
        patch.txnCommit();
        patch.txnCommit();

        // When
        try (FusekiSink sink = FusekiSink.builder().dataset(DatasetGraphFactory.createTxnMem()).build()) {
            Assertions.assertThrows(JenaKafkaException.class, () -> sink.send(
                    new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(patch.getRDFPatch()))));
        }
    }

    @Test
    public void givenNonWritableDataset_whenUsingSink_thenFails() {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.empty();
        DatasetGraph data = createSimpleDatasetPayload();

        // When
        try (FusekiSink sink = FusekiSink.builder().dataset(dsg).build()) {
            Assertions.assertThrows(JenaKafkaException.class, () -> sink.send(
                    new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(data))));
        }
    }

    public static DatasetGraph createSimpleDatasetPayload() {
        DatasetGraph data = DatasetGraphFactory.create();
        data.add(Quad.defaultGraphIRI, NodeFactory.createURI("https://example.org/s"),
                 NodeFactory.createURI("https://example.org/p"), NodeFactory.createLiteralString("object"));
        return data;
    }
}
