package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;

/**
 * Makes a redirection index based on the type provided by the join control.
 */
public class JoinRowRedirection {
    /**
     * A utility function that makes a row redirection based on the type determined by the JoinControl.
     *
     * @param control the JoinControl that determines the redirection type
     * @param leftTable the left table of the join, which the join control examines and determines our result size
     *
     * @return an empty WritableRowRedirection
     */
    static WritableRowRedirection makeRowRedirection(JoinControl control, QueryTable leftTable) {
        final JoinControl.RedirectionType redirectionType = control.getRedirectionType(leftTable);

        final WritableRowRedirection rowRedirection;
        switch (redirectionType) {
            case Contiguous:
                rowRedirection = new ContiguousWritableRowRedirection(leftTable.intSize());
                break;
            case Sparse:
                rowRedirection = new LongColumnSourceWritableRowRedirection(new LongSparseArraySource());
                break;
            case Hash:
                rowRedirection = WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());
                break;
            default:
                throw new IllegalStateException();
        }
        return rowRedirection;
    }
}
