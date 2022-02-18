package io.deephaven.engine.rowset;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

/**
 * {@link RowSequence} with additional set and set-like capabilities.
 */
public interface RowSet extends RowSequence, LongSizedDataStructure, SafeCloseable, LogOutputAppendable {

    void close();

    /**
     * Make a new {@link WritableRowSet} with the same row keys as {@code this} that is safe for further mutation. As in
     * other operations that return a {@link WritableRowSet}, the result must be {@link #close() closed} by the caller
     * when it is no longer needed. The result will never be a {@link TrackingRowSet}; use
     * {@link WritableRowSet#toTracking()} on the result as needed.
     *
     * @return The copied {@link WritableRowSet}
     */
    WritableRowSet copy();

    /**
     * How many keys are in this RowSet.
     *
     * @return the number of keys in this RowSet.
     */
    @Override
    long size();

    /**
     * Queries whether this RowSet is empty (i.e. has no keys).
     *
     * @return true if the size() of this RowSet is zero, false if the size is greater than zero
     */
    boolean isEmpty();

    /**
     * Queries whether this RowSet is non-empty (i.e. has at least one key).
     *
     * @return true if the size() of this RowSet greater than zero, false if the size is zero
     */
    default boolean isNonempty() {
        return !isEmpty();
    }

    /**
     * Returns whether or not this RowSet is flat. Unlike a table, this is a mutable property; which may change from
     * step to step.
     *
     * @return true if the RowSet keys are contiguous and start at zero.
     */
    default boolean isFlat() {
        return isEmpty() || (lastRowKey() == size() - 1);
    }

    /**
     * Get the first row key in this RowSet.
     *
     * @return The first row key, or {@link #NULL_ROW_KEY} if there is none.
     */
    long firstRowKey();

    /**
     * Get the last row key in this RowSet.
     *
     * @return The last row key, or {@link #NULL_ROW_KEY} if there is none.
     */
    long lastRowKey();

    /**
     * <p>
     * Returns a {@link WritableRowSet} with the row positions of <i>row keys</i> in this RowSet.
     *
     * <p>
     * This can be thought of as an iterative find() over the values in keys, but <b>all</b> keys <b>must</b> exist
     * within this RowSet, because a RowSet result can not represent negative values.
     *
     * @param keys The keys to find positions for
     * @return A new {@link WritableRowSet} containing the positions of the keys in this RowSet
     */
    default WritableRowSet invert(RowSet keys) {
        return invert(keys, Long.MAX_VALUE);
    }

    /**
     * <p>
     * Returns the row positions of <i>row keys</i> in the current set as a {@link WritableRowSet}, stopping at
     * maximumPosition.
     *
     * <p>
     * This can be thought of as an iterative {@link #find(long)} over the values in keys, but <b>all</b> keys
     * <b>must</b> exist within this RowSet, because a RowSet result can not represent negative values.
     *
     * @param keys The keys to find positions for
     * @param maximumPosition The largest position for which we will find a key
     * @return A new {@link WritableRowSet} containing the positions of the keys in this RowSet
     */
    WritableRowSet invert(RowSet keys, long maximumPosition);

    /**
     * For the given keys RowSet, under the assertion that none of them are present in the current RowSet, return the
     * tentative insertion points in the current RowSet with the count for each of them
     *
     * @param keys the keys to identify insertion locations
     * @return two TLongArrayLists; [0] contains the positions, [1] contains the counts.
     */
    TLongArrayList[] findMissing(RowSet keys);

    /**
     * Returns a new RowSet representing the intersection of the current RowSet with the input RowSet
     */
    @NotNull
    WritableRowSet intersect(@NotNull RowSet range);

    /**
     * Returns true if a RowSet has any overlap.
     */
    default boolean overlaps(@NotNull RowSet rowSet) {
        return intersect(rowSet).isNonempty();
    }

    /**
     * Returns true if this RowSet has any overlap with the provided range.
     *
     * @param start Start of range, inclusive.
     * @param end End of range, inclusive.
     * @return true if any value x in start &lt;= x &lt;= end is contained in this RowSet.
     */
    boolean overlapsRange(long start, long end);

    /**
     * Returns true if this RowSet is a (possibly improper) subset of other.
     *
     * @return true if every element of this exists within other
     */
    boolean subsetOf(@NotNull RowSet other);

    /**
     * Returns a new RowSet representing the keys of the current set not present inside rowSetToRemove. This operation
     * is equivalent to set difference. This RowSet is not modified.
     */
    WritableRowSet minus(RowSet rowSetToRemove);

    /**
     * Returns a new RowSet representing the keys present in both this RowSet and the argument RowSet.
     *
     * @param rowSetToAdd A RowSet whose keys will be joined with our own to produce a new RowSet.
     * @return a new RowSet with the union of the keys in both this RowSet and rowSetToAdd.
     */
    WritableRowSet union(RowSet rowSetToAdd);

    WritableRowSet shift(long shiftAmount);

    interface RangeIterator extends SafeCloseable {
        void close();

        boolean hasNext();

        /**
         * <p>
         * Advance the current iterator position until {@code currentRangeStart()} and {@code currentRangeEnd()} are
         * both greater than or equal to ‘v’. This may or may not move the iterator to the next range: if ‘v’ is inside
         * the current range (but to the right of {@code currentRangeStart()}, this will simply advance
         * {@code currentRangeStart()}. Returns true if the operation was successful. Otherwise, returns false. In this
         * case the iteration is over and the iterator is exhausted (calls to {@code hasNext()} will return false, any
         * other operation is undefined).
         * </p>
         *
         * <p>
         * Although calls to {@code advance()} may be interleaved with calls to {@code hasNext()}/{@code next()} if
         * necessary, this is not the common case, as they are separate protocols having little to do with each other.
         * In particular, when iterating with {@code advance()}, you do not use next() to bring the next range into
         * view, even at the start of the iteration. Many common usages only involve calls to advance().
         * </p>
         *
         * <p>
         * Example:
         * </p>
         *
         * <pre>
         * {
         *     &#64;code
         *     RangeIterator it = rowSet.getRangeIterator();
         *     if (!it.advance(100)) {
         *         return; // iteration done… no ranges at 100 or greater
         *     }
         *     assert (it.currentRangeStart() &gt;= 100 &amp;&amp; it.currentRangeEnd() &gt;= 100);
         *     // do something with range
         *     if (!it.advance(500)) {
         *         return; // iteration done… no ranges at 500 or greater
         *     }
         *     assert (it.currentRangeStart() &gt;= 500 &amp;&amp; it.currentRangeEnd() &gt;= 500);
         *     // do something with range
         * }
         * </pre>
         *
         * @param v a value to search forward from the current iterator position
         * @return false if iteration is exhausted, otherwise true.
         */
        boolean advance(long v);

        /**
         * Given an iterator state with a current range of [start, end], and a value v such that start &lt;= v &lt;=
         * end, postpone(v) makes the iterator current range [v, end]. This call is useful to code that may need to
         * process parts of ranges from different call sites from the site iterator. The results of this call are
         * undefined if the value provided is not contained in the current range.
         *
         * @param v A value contained in the current iterator range
         */
        void postpone(long v);

        long currentRangeStart();

        long currentRangeEnd();

        long next();

        RangeIterator empty = new RangeIterator() {
            @Override
            public void close() {}

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public void postpone(final long v) {
                throw new IllegalStateException("empty iterator.");
            }

            @Override
            public boolean advance(long v) {
                return false;
            }

            @Override
            public long currentRangeStart() {
                return -1;
            }

            @Override
            public long currentRangeEnd() {
                return -1;
            }

            @Override
            public long next() {
                return -1;
            }
        };
    }

    interface Evaluator<T extends Comparable<T>> {
        T value(long key);
    }

    interface TargetComparator {
        int compareTargetTo(long rKey, int direction);
    }

    interface Iterator extends PrimitiveIterator.OfLong, SafeCloseable {
        /**
         * Starting from the current next iterator position, provide each value to the consumer, until either the
         * iterator is exhausted or a call to lc.accept returns false; ie, if the consumer returns false for a value,
         * stops after that value (does not provide any values after that).
         *
         * @param lc the consumer.
         * @return false if the consumer ever returned false, true otherwise.
         */
        default boolean forEachLong(final LongAbortableConsumer lc) {
            while (hasNext()) {
                final long v = nextLong();
                final boolean wantMore = lc.accept(v);
                if (!wantMore) {
                    return false;
                }
            }
            return true;
        }

        void close();
    }

    /**
     * Provide each value contained in this RowSet, in increased sorted order to the consumer. If the consumer returns
     * false for a key, stops after that key (does not provide any keys after that key).
     *
     * @param lc the consumer.
     * @return false if the consumer returned false at some point, true if the consumer always returned true and all
     *         values in the RowSet were consumed.
     */
    boolean forEachRowKey(LongAbortableConsumer lc);

    default void forAllRowKeys(java.util.function.LongConsumer lc) {
        forEachRowKey((final long v) -> {
            lc.accept(v);
            return true;
        });
    }

    // vs has to be big enough to hold every key on this RowSet.
    default void toRowKeyArray(final long[] vs) {
        toRowKeyArray(vs, 0);
    }

    // vs has to be big enough to hold every key on this RowSet starting from offset.
    default void toRowKeyArray(final long[] vs, final int offset) {
        final int[] vi = new int[1];
        vi[0] = offset;
        forEachRowKey((final long v) -> {
            vs[vi[0]++] = v;
            return true;
        });
    }

    interface SearchIterator extends Iterator {
        void close();

        boolean hasNext();

        long currentValue();

        long nextLong();

        /**
         * <p>
         * Advance the current iterator position until {@code currentValue()} is greater than or equal to ‘v’. The
         * operation is a no-op (and returns true) if currentValue() is already >= 'v'. Returns true if the operation
         * was successful. Otherwise, returns false. In this case the iteration is over and the iterator is exhausted;
         * calls to {@code hasNext()} will return false, any other operation is undefined.
         * </p>
         *
         * <p>
         * Although calls to {@code advance()} may be interleaved with calls to {@code hasNext()}/{@code next()} if
         * necessary, this is not the common case, as they are separate protocols having little to do with each other.
         * In particular, when iterating with {@code advance()}, you do not use next() to bring the value you advanced
         * to into view, even at the start of the iteration. Many common usages only involve calls to advance().
         * </p>
         *
         * @param v a value to search forward from the current iterator position
         * @return false if iteration is exhausted, otherwise true.
         */
        boolean advance(long v);

        /**
         * <p>
         * Advance the current iterator (start) position while the current value maintains comp.compareTargetTo(v, dir)
         * &gt; 0. If next to the last such value there is a value for which comp.compareTargetTo(v, dir) &lt; 0, or no
         * further values exist, then that last value satisfying comp,.compareTargetTo(v, dir) &gt; 0 is left as the
         * current position and returned. If there are any elements for which comp.compareTargetTo(v, dir) == 0, one of
         * such elements, no guarantee which one, is left as the current position and returned. If at call entry the
         * iterator was exhausted, -1 is returned. If at call entry the iterator was just constructed and had never been
         * advanced, it is moved to the first element (which becomes the current value). If the current value v is such
         * that comp.compareTargetTo(v, dir) &lt; 0, -1 is returned and the current position is not moved.
         * </p>
         *
         * <p>
         * Part of the contract of this method is that comp.compareTargetTo will only be called with values that are in
         * the underlying container.
         * </p>
         *
         * @param comp a comparator used to search forward from the current iterator position
         * @param dir a direction to search for comp, either +1 for forward or -1 for backward.
         * @return -1 if the iterator was exhausted at entry or the target was to the left of the initial position at
         *         the time of the call, in which case the iterator is not changed; the resulting current position
         *         otherwise. In this later case the current position is guaranteed to satisfy comp.compareTargetTo(v,
         *         dir) >= 0 and if also comp.compareTargetTo(v, dir) &gt; 0, then v is the biggest such value for which
         *         comp.compareTargetTo(v, dir) &gt; 0.
         */
        long binarySearchValue(TargetComparator comp, int dir);
    }

    SearchIterator EMPTY_ITERATOR = new SearchIterator() {
        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public boolean advance(long unused) {
            return false;
        }

        @Override
        public long currentValue() {
            throw new IllegalStateException();
        }

        @Override
        public long nextLong() {
            throw new IllegalStateException();
        }

        @Override
        public long binarySearchValue(TargetComparator targetComparator, int direction) {
            return -1;
        }
    };

    /**
     * Get a subset of this RowSet within the specified half-closed range of row positions.
     *
     * @param startPos The first position to included in the output (inclusive)
     * @param endPos The last position to included in the output (exclusive)
     * @return A new RowSet, containing only positions &gt;= startPos and &lt; endPos
     */
    WritableRowSet subSetByPositionRange(long startPos, long endPos);

    /**
     * Get a subset of this RowSet within the specified closed range of row keys.
     *
     * @param startKey The first key to include in the output.
     * @param endKey The last key (inclusive) to include in the output.
     * @return A new RowSet, containing only values &gt;= startKey and &lt;= endKey.
     */
    WritableRowSet subSetByKeyRange(long startKey, long endKey);

    /**
     * Get a subset of this RowSet according to the supplied sequence of row positions in {@code posRowSequence}.
     *
     * @param posRowSequence The {@link RowSequence} of positions ranges to get (as in {@link #get(long)})
     * @param reversed Whether to treat {@code posRowSet} as offsets relative to {@link #size()} rather than {@code 0}
     * @return A new RowSet, containing the row keys from this RowSet at the row positions in {@code posRowSequence}
     */
    WritableRowSet subSetForPositions(RowSequence posRowSequence, boolean reversed);

    /**
     * Get a subset of this RowSet according to the supplied sequence of row positions in {@code posRowSequence}.
     *
     * @param posRowSequence The {@link RowSequence} of position-based ranges to extract.
     * @return A new RowSet, containing values at the locations in the provided RowSet.
     */
    WritableRowSet subSetForPositions(RowSequence posRowSequence);

    /**
     * Get a subset of this RowSet according to the supplied sequence of row positions relative to {@link #size()} in
     * {@code posRowSequence}.
     *
     * @param posRowSequence The {@link RowSequence} of positions ranges to get (as in {@link #get(long)})
     * @return A new RowSet, containing the row keys from this RowSet at the row positions in {@code posRowSequence}
     */
    WritableRowSet subSetForReversePositions(RowSequence posRowSequence);

    /**
     * Returns the row key at the given row position.
     *
     * @param rowPosition A row position in this RowSet between {@code 0} and {@code size() - 1}.
     * @return The row key at the supplied row position
     */
    long get(long rowPosition);

    /**
     * Returns the sequence of (increasing) keys corresponding to the positions provided as input.
     *
     * @param inputPositions an iterator providing row positions in increasing order.
     * @param outputKeys a consumer of corresponding keys for the positions provided as input.
     */
    void getKeysForPositions(PrimitiveIterator.OfLong inputPositions, LongConsumer outputKeys);

    /**
     * Returns the position in [0..(size-1)] where the key is found. If not found, then return (-(position it would be)
     * - 1), a la Array.binarySearch.
     *
     * @param key the key to search for
     * @return a position from [0..(size-1)] if the key was found. If the key was not found, then (-position - 1) as in
     *         Array.binarySearch.
     */
    long find(long key);

    @NotNull
    Iterator iterator();

    SearchIterator searchIterator();

    SearchIterator reverseIterator();

    RangeIterator rangeIterator();

    /**
     * Queries whether this RowSet contains every element in the range provided.
     *
     * @param start Start of the range, inclusive.
     * @param end End of the range, inclusive.
     * @return true if this RowSet contains every element x in start &lt;= x &lt;= end.
     */
    boolean containsRange(long start, long end);

    void validate(final String failMsg);

    default void validate() {
        validate(null);
    }

    /**
     * @return Whether this RowSet is actually {@link WritableRowSet writable}
     */
    default boolean isWritable() {
        return this instanceof WritableRowSet;
    }

    /**
     * <p>
     * Cast this RowSet reference to a {@link WritableRowSet}.
     *
     * @return {@code this} cast to a {@link WritableRowSet}
     * @throws ClassCastException If {@code this} is not a {@link WritableRowSet}
     */
    default WritableRowSet writableCast() {
        return (WritableRowSet) this;
    }

    /**
     * @return Whether this RowSet is actually {@link TrackingRowSet tracking}
     */
    default boolean isTracking() {
        return this instanceof TrackingRowSet;
    }

    /**
     * <p>
     * Cast this RowSet reference to a {@link TrackingRowSet}.
     *
     * @return {@code this} cast to a {@link TrackingRowSet}
     * @throws ClassCastException If {@code this} is not a {@link TrackingRowSet}
     */
    default TrackingRowSet trackingCast() {
        return (TrackingRowSet) this;
    }
}
