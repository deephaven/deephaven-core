package io.deephaven.db.v2.utils.rsp.container;

public interface ShortAdvanceIterator extends ShortIterator {
    short curr();

    int currAsInt();

    /**
     * <p>
     * Advance the iterator position forward until the current value is smaller or equal to the argument, or the
     * iterator is exhausted.
     * </p>
     *
     * <p>
     * If no satisfying position is found, false is returned, and any subsequent call to hasNext returns false, as the
     * iterator has been exhausted, and the current position is undefined. Otherwise true is returned and the current
     * position is updated.
     * </p>
     *
     * @param v a value to search for starting from the current iterator position, which must be a valid one on entry.
     * @return true if a value satisfying the constraints is found, false if the iterator was exhausted.
     */

    boolean advance(int v);
}
