package org.apache.jena.kafka.utils;

import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRDFChangesApplyExternalTransaction {

    @Test
    public void givenNoExternalTransaction_whenUsingTransactionBegin_thenOk() {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();

        // When
        RDFChangesApplyExternalTransaction changes = new RDFChangesApplyExternalTransaction(dsg);
        changes.txnBegin();
        changes.txnCommit();

        // Then
        Assertions.assertFalse(dsg.isInTransaction());
    }

    @Test
    public void givenExternalTransaction_whenUsingTransactionBegin_thenOk() {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        dsg.begin(TxnType.WRITE);

        // When
        RDFChangesApplyExternalTransaction changes = new RDFChangesApplyExternalTransaction(dsg);
        changes.txnBegin();

        // Then
        Assertions.assertTrue(dsg.isInTransaction());
        dsg.abort();
    }

    @Test
    public void givenExternalTransaction_whenUsingTransactionAbort_thenOk() {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        dsg.begin(TxnType.WRITE);

        // When
        RDFChangesApplyExternalTransaction changes = new RDFChangesApplyExternalTransaction(dsg);
        changes.txnAbort();

        // Then
        Assertions.assertFalse(dsg.isInTransaction());
    }
}
