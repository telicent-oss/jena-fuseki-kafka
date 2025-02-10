package org.apache.jena.kafka.utils;

import org.apache.jena.rdfpatch.changes.RDFChangesApply;
import org.apache.jena.sparql.core.DatasetGraph;

/**
 * Am RDF Patch applicator that respects an external transaction, committing it on the first {@code TX} (transaction
 * begin) that it encounters
 */
public class RDFChangesApplyExternalTransaction extends RDFChangesApply {
    private boolean inExternalTransaction = false;

    /**
     * Creates a new applicator
     * @param dsg Dataset Graph
     */
    public RDFChangesApplyExternalTransaction(DatasetGraph dsg) {
        super(dsg);
        this.inExternalTransaction = dsg.isInTransaction();
    }

    @Override
    public void txnBegin() {
        if (this.inExternalTransaction) {
            super.txnCommit();
            this.inExternalTransaction = false;
        }
        super.txnBegin();
    }

    @Override
    public void txnCommit() {
        super.txnCommit();
        this.inExternalTransaction = false;
    }

    @Override
    public void txnAbort() {
        super.txnAbort();
        this.inExternalTransaction = false;
    }
}
