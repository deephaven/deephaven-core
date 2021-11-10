package io.deephaven.engine.v2;

import io.deephaven.engine.v2.sources.LongSparseArraySource;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.engine.v2.utils.ContiguousMutableRowRedirection;
import io.deephaven.engine.v2.utils.MutableRowRedirection;

/**
 * Makes a redirection index based on the type provided by the join control.
 */
public class JoinRowRedirection {
    /**
     * A utility function that makes a redirection rowSet based on the type determined by the JoinControl.
     *
     * @param control the JoinControl that determines the redirection type
     * @param leftTable the left table of the join, which the join control examines and determines our result size
     *
     * @return an empty MutableRowRedirection
     */
    static MutableRowRedirection makeRowRedirection(JoinControl control, QueryTable leftTable) {
        final JoinControl.RedirectionType redirectionType = control.getRedirectionType(leftTable);

        final MutableRowRedirection rowRedirection;
        switch (redirectionType) {
            case Contiguous:
                rowRedirection = new ContiguousMutableRowRedirection(leftTable.intSize());
                break;
            case Sparse:
                rowRedirection = new LongColumnSourceMutableRowRedirection(new LongSparseArraySource());
                break;
            case Hash:
                rowRedirection = MutableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());
                break;
            default:
                throw new IllegalStateException();
        }
        return rowRedirection;
    }
}
