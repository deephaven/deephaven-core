package io.deephaven.db.v2.utils.rsp.container;

public interface SearchRangeIterator extends RangeIterator {
    /**
     * <p>
     * Advance the iterator position forward until the current range end's (exclusive) position is bigger (not equal)
     * than the argument, or the iterator is exhausted. Note this should find a range that either contains the argument
     * or, if there is no such a range, is the first range in the iterator after the argument. If a range containing the
     * argument is found, and its start position is less than the argument, start is updated to the argument value.
     * </p>
     *
     * <p>
     * If no satisfying range is found, false is returned, and any subsequent call to hasNext returns false. Otherwise
     * true is returned and the current range is updated
     * </p>
     *
     * <p>
     * Note the iterator is invalidated (exhausted) when this method returns false: there is no guarantee as to where
     * the start and end positions are left in this case). Calling hasNext() on an invalidated iterator is guaranteed to
     * return false; any other method call results in undefined behavior.
     * </p>
     *
     * @param v a value to search for starting from the current iterator position, which must be a valid one on entry.
     * @return true if a range satisfying the constraints is found, false if the iterator was exhausted.
     */
    boolean advance(int v);

    /**
     * <p>
     * Advance the current iterator (start) position while the current value maintains comp.directionFrom(v) > 0. If
     * next to the last such value there is a value for which comp.directionFrom(v) < 0, or no further values exist,
     * then that last value satisfying comp,.directionFrom(v) > 0 is left as the current position and true is returned.
     * If there are any elements for which comp.directionFrom(v) == 0, one of such elements, no guarantee which one, is
     * left as the current position and true is returned. If at the call entry, the next range starts at a point where
     * comp.directionFrom(v) < 0, false is returned and the current position is not moved.
     * </p>
     *
     * <p>
     * Note the iterator may not move if at the time of the call, the iterator's current range start position satisfies
     * comp.directionFrom(v) >= 0 and there is no other value in the iterator that does.
     * </p>
     *
     *
     * <p>
     * Part of the contract of this method is that comp.directionFrom will only be called with values that are in the
     * underlying container.
     * </p>
     *
     * @param comp a comparator used to search forward from the current iterator position
     * @return false if the target was to the left of the initial position at the time of the call (iterator not
     *         changed); true otherwise. In the true case the current position is guaranteed to satisfy
     *         comp.directionFrom(v) >= 0 and if also comp.directionFrom(v) > 0, then v is the biggest such value for
     *         which comp.directionFrom(v) > 0. If there are multiple values for which comp.directionFrom(v) == 0, there
     *         is no guarantee as of which one will be left as current iterator position.
     */
    boolean search(ContainerUtil.TargetComparator comp);
}
