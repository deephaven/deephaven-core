package io.deephaven.engine.v2;

import io.deephaven.engine.v2.sources.LongSparseArraySource;
import io.deephaven.engine.structures.rowredirection.ContiguousRedirectionIndexImpl;
import io.deephaven.engine.v2.utils.LongColumnSourceRedirectionIndex;
import io.deephaven.engine.structures.rowredirection.RedirectionIndex;
import io.deephaven.engine.structures.rowredirection.RedirectionIndexLockFreeImpl;

/**
 * Makes a redirection Index based on the type provided by the join control.
 */
public class JoinRedirectionIndex {
    /**
     * A utility function that makes a redirection index based on the type determined by the JoinControl.
     *
     * @param control the JoinControl that determines the redirection type
     * @param leftTable the left table of the join, which the join control examines and determines our result size
     *
     * @return an empty RedirectionIndex
     */
    static RedirectionIndex makeRedirectionIndex(JoinControl control, QueryTable leftTable) {
        final JoinControl.RedirectionType redirectionType = control.getRedirectionType(leftTable);

        final RedirectionIndex redirectionIndex;
        switch (redirectionType) {
            case Contiguous:
                redirectionIndex = new ContiguousRedirectionIndexImpl(leftTable.intSize());
                break;
            case Sparse:
                redirectionIndex = new LongColumnSourceRedirectionIndex(new LongSparseArraySource());
                break;
            case Hash:
                redirectionIndex = RedirectionIndexLockFreeImpl.FACTORY.createRedirectionIndex(leftTable.intSize());
                break;
            default:
                throw new IllegalStateException();
        }
        return redirectionIndex;
    }
}
