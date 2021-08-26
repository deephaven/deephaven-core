package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongConsumer;

/**
 * Read-only subset of the {@link Index} interface.
 */
public interface ReadOnlyIndex extends OrderedKeys, SafeCloseable {
    long NULL_KEY = -1L;

    void close();

    @VisibleForTesting
    int refCount();

    /**
     * Get the last key in this {@code Index}.
     *
     * @return The last key, or {@link Index#NULL_KEY} if there is none.
     */
    long lastKey();

    /**
     * Get the first key in this {@code Index}.
     *
     * @return The first key, or {@link Index#NULL_KEY} if there is none.
     */
    long firstKey();

    Index clone();


    /**
     * Returns an Index with the positions of <i>keys</i> in this Index.
     *
     * This can be thought of as an iterative find() over the values in keys, but <b>all</b> keys
     * <b>must</b> exist within this index, because an Index result can not represent negative
     * values.
     *
     * @param keys the keys to find positions for
     * @return a new Index containing the positions of the keys in this index
     */
    Index invert(ReadOnlyIndex keys);

    /**
     * Returns the positions of <i>keys</i> in the current set as an Index, stopping at
     * maximumPosition.
     *
     * This can be thought of as an iterative find() over the values in keys, but <b>all</b> keys
     * <b>must</b> exist within this index, because an Index result can not represent negative
     * values.
     *
     * @param keys the keys to find positions for
     * @param maximumPosition the largest position for which we will find a key
     * @return a new Index containing the positions of the keys in this index
     */
    Index invert(ReadOnlyIndex keys, long maximumPosition);

    /**
     * For the given keys Index, under the assertion that none of them are present in the current
     * index, return the tentative insertion points in the current index with the count for each of
     * them
     *
     * @param keys the keys to identify insertion locations
     * @return two TLongArrayLists; [0] contains the positions, [1] contains the counts.
     */
    TLongArrayList[] findMissing(ReadOnlyIndex keys);

    /**
     * Returns a new index representing the intersection of the current index with the input index
     */
    @NotNull
    Index intersect(@NotNull ReadOnlyIndex range);

    /**
     * Returns true if an index has any overlap.
     */
    default boolean overlaps(@NotNull ReadOnlyIndex index) {
        return intersect(index).nonempty();
    }

    /**
     * Returns true if this index has any overlap with the provided range.
     *
     * @param start Start of range, inclusive.
     * @param end End of range, inclusive.
     * @return true if any value x in start <= x <= end is contained in this index.
     */
    boolean overlapsRange(long start, long end);

    /**
     * Returns true if this index is a (possibly improper) subset of other.
     *
     * @return true if every element of this exists within other
     */
    boolean subsetOf(@NotNull ReadOnlyIndex other);

    /**
     * Returns a new index representing the keys of the current set not present inside indexToRemove
     * This operation is equivalent to set difference. This index is not modified.
     */
    Index minus(ReadOnlyIndex indexToRemove);

    /**
     * Returns a new index representing the keys present in both this index and the argument index.
     * 
     * @param indexToAdd an index whose keys will be joined with our own to produce a new index.
     * @return a new index with the union of the keys in both this index and indexToAdd.
     */
    Index union(ReadOnlyIndex indexToAdd);

    Index shift(long shiftAmount);

    Map<Object, Index> getGrouping(TupleSource tupleSource);

    void copyImmutableGroupings(TupleSource source, TupleSource dest);

    Map<Object, Index> getPrevGrouping(TupleSource tupleSource);


    /**
     * Return a grouping that contains keys that match the values in keySet.
     *
     * @param keys a set of values that keyColumns should match. For a single keyColumns, the values
     *        within the set are the values that we would like to find. For multiple keyColumns, the
     *        values are SmartKeys.
     * @param tupleSource the tuple factory for the keyColumns
     * @return an Map from keys to Indices, for each of the keys in keySet and this Index.
     */
    Map<Object, Index> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource);

    /**
     * Return a subIndex that contains indices that match the values in keySet.
     *
     * @param keySet a set of values that keyColumns should match. For a single keyColumns, the
     *        values within the set are the values that we would like to find. For multiple
     *        keyColumns, the values are SmartKeys.
     * @param tupleSource the tuple factory for the keyColumn
     * @return an Index containing only keys that match keySet.
     */
    Index getSubIndexForKeySet(Set<Object> keySet, TupleSource tupleSource);

    boolean hasGrouping(ColumnSource... keyColumns);

    interface RangeIterator extends SafeCloseable {
        void close();

        boolean hasNext();

        /**
         * <p>
         * Advance the current iterator position until {@code currentRangeStart()} and
         * {@code currentRangeEnd()} are both greater than or equal to ‘v’. This may or may not move
         * the iterator to the next range: if ‘v’ is inside the current range (but to the right of
         * {@code currentRangeStart()}, this will simply advance {@code currentRangeStart()}.
         * Returns true if the operation was successful. Otherwise, returns false. In this case the
         * iteration is over and the iterator is exhausted (calls to {@code hasNext()} will return
         * false, any other operation is undefined).
         * </p>
         *
         * <p>
         * Although calls to {@code advance()} may be interleaved with calls to
         * {@code hasNext()}/{@code next()} if necessary, this is not the common case, as they are
         * separate protocols having little to do with each other. In particular, when iterating
         * with {@code advance()}, you do not use next() to bring the next range into view, even at
         * the start of the iteration. Many common usages only involve calls to advance().
         * </p>
         *
         * <p>
         * Example:
         * </p>
         *
         * <pre>
         * {
         *     &#64;code
         *     RangeIterator it = index.getRangeIterator();
         *     if (!it.advance(100)) {
         *         return; // iteration done… no ranges at 100 or greater
         *     }
         *     assert (it.currentRangeStart() >= 100 && it.currentRangeEnd() >= 100);
         *     // do something with range
         *     if (!it.advance(500)) {
         *         return; // iteration done… no ranges at 500 or greater
         *     }
         *     assert (it.currentRangeStart() >= 500 && it.currentRangeEnd() >= 500);
         *     // do something with range
         * }
         * </pre>
         *
         * @param v a value to search forward from the current iterator position
         * @return false if iteration is exhausted, otherwise true.
         *
         */
        boolean advance(long v);

        /**
         * Given an iterator state with a current range of [start, end], and a value v such that
         * start <= v <= end, postpone(v) makes the iterator current range [v, end]. This call is
         * useful to code that may need to process parts of ranges from different call sites from
         * the site iterator. The results of this call are undefined if the value provided is not
         * contained in the current range.
         *
         * @param v A value contained in the current iterator range
         *
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
         * Starting from the current next iterator position, provide each value to the consumer,
         * until either the iterator is exhausted or a call to lc.accept returns false; ie, if the
         * consumer returns false for a value, stops after that value (does not provide any values
         * after that).
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
     * Provide each value contained in this index, in increased sorted order to the consumer. If the
     * consumer returns false for a key, stops after that key (does not provide any keys after that
     * key).
     *
     * @param lc the consumer.
     * @return false if the consumer returned false at some point, true if the consumer always
     *         returned true and all values in the index were consumed.
     */
    boolean forEachLong(LongAbortableConsumer lc);

    default void forAllLongs(java.util.function.LongConsumer lc) {
        forEachLong((final long v) -> {
            lc.accept(v);
            return true;
        });
    }

    // vs has to be big enough to hold every key on this index.
    default void toLongArray(final long[] vs) {
        toLongArray(vs, 0);
    }

    // vs has to be big enough to hold every key on this index starting from offset.
    default void toLongArray(final long[] vs, final int offset) {
        final int[] vi = new int[1];
        vi[0] = offset;
        forEachLong((final long v) -> {
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
         * Advance the current iterator position until {@code currentValue()} is greater than or
         * equal to ‘v’. The operation is a no-op (and returns true) if currentValue() is already >=
         * 'v'. Returns true if the operation was successful. Otherwise, returns false. In this case
         * the iteration is over and the iterator is exhausted; calls to {@code hasNext()} will
         * return false, any other operation is undefined.
         * </p>
         *
         * <p>
         * Although calls to {@code advance()} may be interleaved with calls to
         * {@code hasNext()}/{@code next()} if necessary, this is not the common case, as they are
         * separate protocols having little to do with each other. In particular, when iterating
         * with {@code advance()}, you do not use next() to bring the value you advanced to into
         * view, even at the start of the iteration. Many common usages only involve calls to
         * advance().
         * </p>
         * 
         * @param v a value to search forward from the current iterator position
         * @return false if iteration is exhausted, otherwise true.
         *
         */
        boolean advance(long v);

        /**
         * <p>
         * Advance the current iterator (start) position while the current value maintains
         * comp.compareTargetTo(v, dir) > 0. If next to the last such value there is a value for
         * which comp.compareTargetTo(v, dir) < 0, or no further values exist, then that last value
         * satisfying comp,.compareTargetTo(v, dir) > 0 is left as the current position and
         * returned. If there are any elements for which comp.compareTargetTo(v, dir) == 0, one of
         * such elements, no guarantee which one, is left as the current position and returned. If
         * at call entry the iterator was exhausted, -1 is returned. If at call entry the iterator
         * was just constructed and had never been advanced, it is moved to the first element (which
         * becomes the current value). If the current value v is such that comp.compareTargetTo(v,
         * dir) < 0, -1 is returned and the current position is not moved.
         * </p>
         *
         * <p>
         * Part of the contract of this method is that comp.compareTargetTo will only be called with
         * values that are in the underlying container.
         * </p>
         *
         * @param comp a comparator used to search forward from the current iterator position
         * @param dir a direction to search for comp, either +1 for forward or -1 for backward.
         * @return -1 if the iterator was exhausted at entry or the target was to the left of the
         *         initial position at the time of the call, in which case the iterator is not
         *         changed; the resulting current position otherwise. In this later case the current
         *         position is guaranteed to satisfy comp.compareTargetTo(v, dir) >= 0 and if also
         *         comp.compareTargetTo(v, dir) > 0, then v is the biggest such value for which
         *         comp.compareTargetTo(v, dir) > 0.
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
     * Get a subset of this index within this range of positions
     * 
     * @param startPos The first position to included in the output (inclusive)
     * @param endPos The last position to included in the output (exclusive)
     * @return A new index, containing only positions &gt;= startPos and &lt; endPos
     */
    Index subindexByPos(long startPos, long endPos);

    /**
     * Get a subset of this index within this range of keys.
     * 
     * @param startKey The first key to include in the output.
     * @param endKey The last key (inclusive) to include in the output.
     * @return A new index, containing only values &gt;= startKey and &lt;= endKey.
     */
    Index subindexByKey(long startKey, long endKey);

    /**
     * Get a subset of this index within these range of positions.
     * 
     * @param posIndex The index of position-based ranges to extract.
     * @return A new index, containing values at the locations in the provided index.
     */
    default Index subindexByPos(Index posIndex) {
        final MutableLong currentOffset = new MutableLong();
        final OrderedKeys.Iterator iter = getOrderedKeysIterator();
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        posIndex.forEachLongRange((start, end) -> {
            if (currentOffset.longValue() < start) {
                // skip items until the beginning of this range
                iter.getNextOrderedKeysWithLength(start - currentOffset.longValue());
                currentOffset.setValue(start);
            }

            if (!iter.hasMore()) {
                return false;
            }

            iter.getNextOrderedKeysWithLength(end + 1 - currentOffset.longValue())
                .forAllLongRanges(builder::appendRange);
            currentOffset.setValue(end + 1);
            return iter.hasMore();
        });
        return builder.getIndex();
    }

    /**
     * Returns the key at the given rank position.
     *
     * @param pos a position in this index between 0 and size() - 1
     * @return the key at that rank.
     */
    long get(long pos);

    /**
     * Returns the sequence of (increasing) keys corresponding to the positions provided as input.
     *
     * @param inputPositions an iterator providing index positions in increasing order.
     * @param outputKeys a consumer of corresponding keys for the positions provided as input.
     */
    void getKeysForPositions(PrimitiveIterator.OfLong inputPositions, LongConsumer outputKeys);

    long getPrev(long pos);

    long sizePrev();

    Index getPrevIndex();

    long firstKeyPrev();

    long lastKeyPrev();

    /**
     * Returns the position in [0..(size-1)] where the key is found. If not found, then return
     * (-(position it would be) - 1), a la Array.binarySearch.
     *
     * @param key the key to search for
     * @return a position from [0..(size-1)] if the key was found. If the key was not found, then
     *         (-position - 1) as in Array.binarySearch.
     */
    long find(long key);

    /**
     * Returns the position in [0..(size-1)] where the key is found in the previous index. If not
     * found, then return (-(position it would be) - 1), as in Array.binarySearch.
     *
     * @param key the key to search for
     * @return a position from [0..(size-1)] if the key was found. If the key was not found, then
     *         (-position - 1) as in Array.binarySearch.
     */
    long findPrev(long key);

    boolean isSorted();

    @NotNull
    Iterator iterator();

    SearchIterator searchIterator();

    SearchIterator reverseIterator();

    RangeIterator rangeIterator();

    /**
     * How many keys are in this index.
     * 
     * @return the number of keys in this index.
     */
    @Override
    long size();

    /**
     * Returns whether or not this index is flat. Unlike a table, this is a mutable property; which
     * may change from step to step.
     * 
     * @return true if the index keys are continguous and start at zero.
     */
    boolean isFlat();

    /**
     * Queries whether this index is empty (i.e. has no keys).
     * 
     * @return true if the size() of this Index is zero, false if the size is greater than zero
     */
    boolean empty();

    /**
     * Queries whether this index is empty (i.e. has no keys).
     * 
     * @return true if the size() of this Index is zero, false if the size is greater than zero
     */
    default boolean isEmpty() {
        return empty();
    }

    /**
     * Queries whether this index is non-empty (i.e. has at least one key).
     * 
     * @return true if the size() of this Index greater than zero, false if the size is zero
     */
    default boolean nonempty() {
        return !empty();
    }

    /**
     * Queries whether this index contains every element in the range provided.
     *
     * @param start Start of the range, inclusive.
     * @param end End of the range, inclusive.
     * @return true if this index contains every element x in start <= x <= end.
     */
    boolean containsRange(long start, long end);

    void validate(final String failMsg);

    default void validate() {
        validate(null);
    }
}
