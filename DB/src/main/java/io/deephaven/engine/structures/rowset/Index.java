/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.structures.rowset;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.procedure.TLongProcedure;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.structures.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.engine.structures.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.chunk.util.LongChunkIterator;
import io.deephaven.engine.structures.chunk.util.LongChunkRangeIterator;
import io.deephaven.engine.structures.iterationhelpers.LongRangeConsumer;
import io.deephaven.engine.structures.iterationhelpers.LongRangeIterator;
import io.deephaven.engine.structures.rowsequence.OrderedKeys;
import io.deephaven.engine.structures.rowset.singlerange.SingleRange;
import io.deephaven.engine.structures.rowshiftdata.IndexShiftData;
import io.deephaven.engine.structures.util.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;
import java.util.function.LongUnaryOperator;

/**
 * A set of sorted long keys between 0 and Long.MAX_VALUE
 */
public interface Index extends ReadOnlyIndex, LogOutputAppendable, Iterable<Long>, LongSizedDataStructure {
    boolean USE_PRIORITY_QUEUE_RANDOM_BUILDER =
            Configuration.getInstance().getBooleanWithDefault("Index.usePriorityQueueRandomBuilder", true);

    boolean VALIDATE_COALESCED_UPDATES =
            Configuration.getInstance().getBooleanWithDefault("Index.validateCoalescedUpdates", true);

    boolean BAD_RANGES_AS_ERROR =
            Configuration.getInstance().getBooleanForClassWithDefault(Index.class, "badRangeAsError", true);

    /**
     * Add a single key to this index if it's not already present.
     *
     * @param key The key to add
     */
    void insert(long key);

    /**
     * Add all keys in a closed range to this index if they are not already present.
     *
     * @param startKey The first key to add
     * @param endKey The last key to add (inclusive)
     */
    void insertRange(long startKey, long endKey);

    /**
     * Add all of the (ordered) keys in a slice of {@code keys} to this index if they are not already present.
     *
     * @param keys The {@link LongChunk} of {@link OrderedKeyIndices} to insert
     * @param offset The offset in {@code keys} to begin inserting keys from
     * @param length The number of keys to insert
     */
    void insert(LongChunk<OrderedKeyIndices> keys, int offset, int length);

    /**
     * Add all of the keys in {@code added} to this index if they are not already present.
     *
     * @param added The index to add
     */
    void insert(ReadOnlyIndex added);

    /**
     * Remove a single key from this index if it's present.
     *
     * @param key The key to remove
     */
    void remove(long key);

    /**
     * Remove all keys in a closed range from this index if they are present.
     *
     * @param startKey The first key to remove
     * @param endKey The last key to remove (inclusive)
     */
    void removeRange(long startKey, long endKey);

    /**
     * Remove all of the (ordered) keys in a slice of {@code keys} from this index if they are present.
     *
     * @param keys The {@link LongChunk} of {@link OrderedKeyIndices} to remove
     * @param offset The offset in {@code keys} to begin removing keys from
     * @param length The number of keys to remove
     */
    void remove(LongChunk<OrderedKeyIndices> keys, int offset, int length);

    /**
     * Remove all of the keys in {@code removed} that are present in this index.
     *
     * @param removed The index to remove
     */
    void remove(ReadOnlyIndex removed);

    /**
     * Simultaneously adds the keys from the first index and removes the keys from the second one. API assumption: the
     * intersection of added and removed is empty.
     */
    void update(ReadOnlyIndex added, ReadOnlyIndex removed);

    /**
     * Removes all the keys from <i>other</i> index that are present in the current set.
     * 
     * @return a new index representing the keys removed
     */
    @NotNull
    default Index extract(@NotNull Index other) {
        final Index ret = this.intersect(other);
        this.remove(ret);
        return ret;
    }

    /**
     * Modifies the index by removing any keys not in the indexToIntersect argument.
     *
     * @param indexToIntersect an index with the keys to retain; any other keys not in indexToIntersect will be removed.
     */
    void retain(ReadOnlyIndex indexToIntersect);

    /**
     * Modifies the index by keeping only keys in the interval [start, end]
     * 
     * @param start beginning of interval of keys to keep.
     * @param end end of interval of keys to keep (inclusive).
     */
    void retainRange(long start, long end);

    void clear();

    void shiftInPlace(long shiftAmount);

    /**
     * For each key in the provided index, shift it by shiftAmount and insert it in the current index.
     *
     * @param shiftAmount the amount to add to each key in the index argument before insertion.
     * @param other the index with the keys to shift and insert.
     */
    void insertWithShift(long shiftAmount, ReadOnlyIndex other);

    /**
     * May reclaim some unused memory.
     */
    void compact();

    /**
     * Initializes our previous value from the current value.
     *
     * This call is used by operations that manipulate an Index while constructing it, but need to set the state at the
     * end of the initial operation to the current state.
     *
     * Calling this in other circumstances will yield undefined results.
     */
    void initializePreviousValue();

    interface RandomBuilder extends IndexBuilder {
    }

    interface SequentialBuilder extends TLongProcedure, LongRangeConsumer {
        /**
         * No obligation to call, but if called, (a) should be called before providing any values, and (b) no value
         * should be provided outside of the domain. Implementations may be able to use this information to improve
         * memory utilization. Either of the arguments may be given as Index.NULL_KEY, indicating that the respective
         * value is not known.
         *
         * @param minKey the minimum key to be provided, or Index.NULL_KEY if not known.
         * @param maxKey the maximum key to be provided, or Index.NULL_KEY if not known.
         */
        default void setDomain(final long minKey, final long maxKey) {}

        Index getIndex();

        void appendKey(long key);

        void appendRange(long rangeFirstKey, long rangeLastKey);

        default void appendKeys(PrimitiveIterator.OfLong it) {
            while (it.hasNext()) {
                appendKey(it.nextLong());
            }
        }

        default void appendOrderedKeyIndicesChunk(final LongChunk<OrderedKeyIndices> chunk) {
            appendKeys(new LongChunkIterator(chunk));
        }

        default void appendRanges(final LongRangeIterator it) {
            while (it.hasNext()) {
                it.next();
                appendRange(it.start(), it.end());
            }
        }

        default void appendOrderedKeyRangesChunk(final LongChunk<OrderedKeyRanges> chunk) {
            appendRanges(new LongChunkRangeIterator(chunk));
        }

        @Override
        default boolean execute(long value) {
            appendKey(value);
            return true;
        }

        default void appendIndex(final ReadOnlyIndex idx) {
            idx.forAllLongRanges(this::appendRange);
        }

        /**
         * Appends the index shifted by the provided offset to this builder.
         * 
         * @param idx The index to append.
         * @param offset An offset to apply to every range in the index.
         */
        default void appendIndexWithOffset(final ReadOnlyIndex idx, long offset) {
            idx.forAllLongRanges((s, e) -> appendRange(s + offset, e + offset));
        }

        default void appendOrderedKeys(final OrderedKeys orderedKeys) {
            orderedKeys.forAllLongRanges(this::appendRange);
        }
    }

    interface Factory {
        /**
         * Get an {@link Index} containing the specified keys. All keys must be positive numbers
         *
         * @param keys The keys to add to the index
         * @return An index containing the specified keys
         */
        Index getIndexByValues(long... keys);

        Index getEmptyIndex();

        /**
         * Produce an index containing a single key.
         *
         * @param key the key that will exist in the new Index
         * @return a new Index containing key
         */
        Index getIndexByValues(long key);

        /**
         * Get an {@link Index} containing the specified keys.
         *
         * The provided list is sorted, and then passed to a sequential builder.
         *
         * @param list a Trove long array list; note that this list is mutated within the method
         * @return an Index containing the values from list
         */
        Index getIndexByValues(TLongArrayList list);

        /**
         * Create an {@link Index} containing the continuous range [firstKey, lastKey]
         *
         * @param firstKey The first key in the continuous range
         * @param lastKey The last key in the continuous range
         *
         * @return An index containing the specified range
         */
        Index getIndexByRange(long firstKey, long lastKey);

        /**
         * Get a flat {@link Index} containing the range [0, size), or an {@link #getEmptyIndex() empty index} if the
         * specified size is <= 0.
         *
         * @param size The size of the index to create
         * @return A flat index containing the keys [0, size) or an empty index if the size is <= 0
         */
        Index getFlatIndex(long size);

        /**
         * @return An {@link IndexBuilder} suitable for inserting ranges in no particular order.
         */
        RandomBuilder getRandomBuilder();

        /**
         * @return A builder optimized for inserting ranges sequentially in order.
         */
        SequentialBuilder getSequentialBuilder();

        RandomBuilder getBuilder();
    }

    abstract class AbstractRandomBuilder implements Index.RandomBuilder {
        protected AdaptiveBuilder builder = new AdaptiveBuilder();

        @Override
        public void addKey(final long key) {
            builder.addKey(key);
        }

        @Override
        public void addRange(final long start, final long endInclusive) {
            builder.addRange(start, endInclusive);
        }
    }

    class AdaptiveIndexBuilder extends AbstractRandomBuilder {
        @Override
        public Index getIndex() {
            return new TreeIndex(builder.getTreeIndexImpl());
        }
    }

    Factory FACTORY = new AbstractFactory() {
        @Override
        public RandomBuilder getRandomBuilder() {
            if (USE_PRIORITY_QUEUE_RANDOM_BUILDER) {
                return new AdaptiveIndexBuilder() {};
            } else {
                return TreeIndex.makeRandomBuilder();
            }
        }

        @Override
        public SequentialBuilder getSequentialBuilder() {
            return TreeIndex.makeSequentialBuilder();
        }

        @Override
        public Index getEmptyIndex() {
            return TreeIndex.getEmptyIndex();
        }

        // For backwards compatibility.
        /**
         * @return {@link #getRandomBuilder()}
         */
        public RandomBuilder getBuilder() {
            return getRandomBuilder();
        }
    };

    Factory CURRENT_FACTORY = new AbstractFactory() {
        @Override
        public RandomBuilder getRandomBuilder() {
            if (USE_PRIORITY_QUEUE_RANDOM_BUILDER) {
                return new AbstractRandomBuilder() {
                    @Override
                    public Index getIndex() {
                        return new CurrentOnlyIndex(builder.getTreeIndexImpl());
                    }
                };
            } else {
                return TreeIndex.makeCurrentRandomBuilder();
            }
        }

        @Override
        public SequentialBuilder getSequentialBuilder() {
            return TreeIndex.makeCurrentSequentialBuilder();
        }

        @Override
        public Index getEmptyIndex() {
            return new CurrentOnlyIndex();
        }

        @Override
        public Index getIndexByRange(final long firstKey, final long lastKey) {
            return new CurrentOnlyIndex(SingleRange.make(firstKey, lastKey));
        }

        // For backwards compatibility.
        /**
         * @return {@link #getRandomBuilder()}
         */
        public RandomBuilder getBuilder() {
            return getRandomBuilder();
        }
    };

    class ShiftInversionHelper {
        private int destShiftIdx = 0;
        final IndexShiftData shifted;

        public ShiftInversionHelper(final IndexShiftData shifted) {
            this.shifted = shifted;
        }

        private void advanceDestShiftIdx(long destKey) {
            Assert.geq(destKey, "destKey", 0);
            destShiftIdx = (int) binarySearch(destShiftIdx, shifted.size(), innerShiftIdx -> {
                long destEnd = shifted.getEndRange((int) innerShiftIdx) + shifted.getShiftDelta((int) innerShiftIdx);
                // due to destKey's expected range, we know this subtraction will not overflow
                return destEnd - destKey;
            });
        }

        // Converts post-keyspace key to pre-keyspace key. It expects to be invoked in ascending key order.
        public long mapToPrevKeyspace(long key, boolean isEnd) {
            advanceDestShiftIdx(key);

            final long retval;
            final int idx = destShiftIdx;
            if (idx < shifted.size() && shifted.getBeginRange(idx) + shifted.getShiftDelta(idx) <= key) {
                // inside of a destination shift; this is easy to map to prev
                retval = key - shifted.getShiftDelta(idx);
            } else if (idx < shifted.size() && shifted.getShiftDelta(idx) > 0 && shifted.getBeginRange(idx) <= key) {
                // our key is left of the destination but to right of the shift start
                retval = shifted.getBeginRange(idx) - (isEnd ? 1 : 0);
            } else if (idx > 0 && shifted.getShiftDelta(idx - 1) < 0 && key <= shifted.getEndRange(idx - 1)) {
                // our key is right of the destination but left of the shift start
                retval = shifted.getEndRange(idx - 1) + (isEnd ? 0 : 1);
            } else {
                retval = key;
            }
            return retval;
        }

        private static long binarySearch(int low, int high, final LongUnaryOperator evaluate) {
            while (low < high) {
                final int mid = (low + high) / 2;
                final long res = evaluate.applyAsLong(mid);
                if (res < 0) {
                    low = mid + 1;
                } else if (res > 0) {
                    high = mid;
                } else {
                    return mid;
                }
            }
            return low;
        }
    }
}
