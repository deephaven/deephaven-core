/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.util.LongChunkIterator;
import io.deephaven.db.v2.sources.chunk.util.LongChunkRangeIterator;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.util.SafeCloseableList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.procedure.TLongProcedure;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.*;

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

    class LegacyIndexUpdateCoalescer {
        private Index added, modified, removed;

        public LegacyIndexUpdateCoalescer() {
            reset();
        }

        /**
         * The class assumes ownership of one reference to the indices passed; the caller should ensure to Index.clone()
         * them before passing them if they are shared.
         */
        public LegacyIndexUpdateCoalescer(Index added, Index removed, Index modified) {
            this.added = added;
            this.removed = removed;
            this.modified = modified;
        }

        public void update(final Index addedOnUpdate, final Index removedOnUpdate, final Index modifiedOnUpdate) {
            // Note: extract removes matching ranges from the source index
            try (final Index addedBack = this.removed.extract(addedOnUpdate);
                    final Index actuallyAdded = addedOnUpdate.minus(addedBack)) {
                this.added.insert(actuallyAdded);
                this.modified.insert(addedBack);
            }

            // Things we've added, but are now removing. Do not aggregate these as removed since client never saw them.
            try (final Index additionsRemoved = this.added.extract(removedOnUpdate);
                    final Index actuallyRemoved = removedOnUpdate.minus(additionsRemoved)) {
                this.removed.insert(actuallyRemoved);
            }

            // If we've removed it, it should no longer be modified.
            this.modified.remove(removedOnUpdate);

            // And anything modified, should be added to the modified set; unless we've previously added it.
            try (final Index actuallyModified = modifiedOnUpdate.minus(this.added)) {
                this.modified.insert(actuallyModified);
            }

            if (VALIDATE_COALESCED_UPDATES && (this.added.overlaps(this.modified) || this.added.overlaps(this.removed)
                    || this.removed.overlaps(modified))) {
                final String assertionMessage = "Coalesced overlaps detected: " +
                        "added=" + added.toString() +
                        ", removed=" + removed.toString() +
                        ", modified=" + modified.toString() +
                        ", addedOnUpdate=" + addedOnUpdate.toString() +
                        ", removedOnUpdate=" + removedOnUpdate.toString() +
                        ", modifiedOnUpdate=" + modifiedOnUpdate.toString() +
                        "addedIntersectRemoved=" + added.intersect(removed).toString() +
                        "addedIntersectModified=" + added.intersect(modified).toString() +
                        "removedIntersectModified=" + removed.intersect(modified).toString();
                Assert.assertion(false, assertionMessage);
            }
        }

        public Index takeAdded() {
            final Index r = added;
            added = Index.FACTORY.getEmptyIndex();
            return r;
        }

        public Index takeRemoved() {
            final Index r = removed;
            removed = Index.FACTORY.getEmptyIndex();
            return r;
        }

        public Index takeModified() {
            final Index r = modified;
            modified = Index.FACTORY.getEmptyIndex();
            return r;
        }

        public void reset() {
            added = Index.FACTORY.getEmptyIndex();
            modified = Index.FACTORY.getEmptyIndex();
            removed = Index.FACTORY.getEmptyIndex();
        }
    }

    class IndexUpdateCoalescer {
        public Index added;
        public Index removed;
        public Index modified;
        public IndexShiftData shifted;
        public ModifiedColumnSet modifiedColumnSet;

        // This is an index that represents which keys still exist in prevSpace for the agg update. It is necessary to
        // keep to ensure we make the correct selections when shift destinations overlap.
        private final Index index;

        public IndexUpdateCoalescer(final Index index, final ShiftAwareListener.Update update) {
            this.index = index.clone();
            this.index.remove(update.removed);

            this.added = update.added.clone();
            this.removed = update.removed.clone();
            this.modified = update.modified.clone();
            this.shifted = update.shifted;

            if (modified.isEmpty()) {
                // just to be certain
                this.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else {
                this.modifiedColumnSet = update.modifiedColumnSet.copy();
            }
        }

        public ShiftAwareListener.Update coalesce() {
            return new ShiftAwareListener.Update(added, removed, modified, shifted, modifiedColumnSet);
        }

        public IndexUpdateCoalescer update(final ShiftAwareListener.Update update) {
            // Remove update.remove from our coalesced post-shift added/modified.
            try (final SafeCloseableList closer = new SafeCloseableList()) {
                final Index addedAndRemoved = closer.add(added.extract(update.removed));

                if (update.removed.nonempty()) {
                    modified.remove(update.removed);
                }

                // Aggregate update.remove in coalesced pre-shift removed.
                try (final Index myRemoved = update.removed.minus(addedAndRemoved)) {
                    shifted.unapply(myRemoved);
                    removed.insert(myRemoved);
                    index.remove(myRemoved);
                }

                // Apply new shifts to our post-shift added/modified.
                if (update.shifted.nonempty()) {
                    update.shifted.apply(added);
                    update.shifted.apply(modified);

                    updateShifts(update.shifted);
                }

                // We can't modify rows that did not exist previously.
                try (final Index myModified = update.modified.minus(added)) {
                    updateModified(update.modifiedColumnSet, myModified);
                }

                // Note: adding removed identical indices is allowed.
                added.insert(update.added);
            }

            return this;
        }

        private void updateModified(final ModifiedColumnSet myMCS, final Index myModified) {
            if (myModified.empty()) {
                return;
            }

            modified.insert(myModified);
            if (modifiedColumnSet.empty()) {
                modifiedColumnSet = myMCS.copy();
            } else {
                modifiedColumnSet.setAll(myMCS);
            }
        }

        private void updateShifts(final IndexShiftData myShifts) {
            if (shifted.empty()) {
                shifted = myShifts;
                return;
            }

            final Index.SearchIterator indexIter = index.searchIterator();
            final IndexShiftData.Builder newShifts = new IndexShiftData.Builder();

            // Appends shifts to our builder from watermarkKey to supplied key adding extra delta if needed.
            final MutableInt outerIdx = new MutableInt(0);
            final MutableLong watermarkKey = new MutableLong(0);
            final BiConsumer<Long, Long> fixShiftIfOverlap = (end, ttlDelta) -> {
                long minBegin = newShifts.getMinimumValidBeginForNextDelta(ttlDelta);
                if (ttlDelta < 0) {
                    final Index.SearchIterator revIter = index.reverseIterator();
                    if (revIter.advance(watermarkKey.longValue() - 1)
                            && revIter.currentValue() > newShifts.lastShiftEnd()) {
                        minBegin = Math.max(minBegin, revIter.currentValue() + 1 - ttlDelta);
                    }
                }

                if (end < watermarkKey.longValue() || minBegin < watermarkKey.longValue()) {
                    return;
                }

                // this means the previous shift overlaps this shift; let's figure out who wins
                final long contestBegin = watermarkKey.longValue();
                final boolean currentValid = indexIter.advance(contestBegin);
                if (currentValid && indexIter.currentValue() < minBegin && indexIter.currentValue() <= end) {
                    newShifts.limitPreviousShiftFor(indexIter.currentValue(), ttlDelta);
                    watermarkKey.setValue(indexIter.currentValue());
                } else {
                    watermarkKey.setValue(Math.min(end + 1, minBegin));
                }
            };

            final BiConsumer<Long, Long> consumeUntilWithExtraDelta = (endRange, extraDelta) -> {
                while (outerIdx.intValue() < shifted.size() && watermarkKey.longValue() <= endRange) {
                    final long outerBegin =
                            Math.max(watermarkKey.longValue(), shifted.getBeginRange(outerIdx.intValue()));
                    final long outerEnd = shifted.getEndRange(outerIdx.intValue());
                    final long outerDelta = shifted.getShiftDelta(outerIdx.intValue());

                    // Shift before the outer shift.
                    final long headerEnd = Math.min(endRange, outerBegin - 1 + (outerDelta < 0 ? outerDelta : 0));
                    if (watermarkKey.longValue() <= headerEnd && extraDelta != 0) {
                        fixShiftIfOverlap.accept(headerEnd, extraDelta);
                        newShifts.shiftRange(watermarkKey.longValue(), headerEnd, extraDelta);
                    }
                    final long maxWatermark =
                            endRange == Long.MAX_VALUE ? outerBegin : Math.min(endRange + 1, outerBegin);
                    watermarkKey.setValue(Math.max(watermarkKey.longValue(), maxWatermark));

                    // Does endRange occur before this outerIdx shift? If so pop-out we need to change extraDelta.
                    if (watermarkKey.longValue() > endRange) {
                        return;
                    }

                    final long myEnd = Math.min(outerEnd, endRange);
                    final long ttlDelta = outerDelta + extraDelta;
                    fixShiftIfOverlap.accept(myEnd, ttlDelta);

                    newShifts.shiftRange(watermarkKey.longValue(), myEnd, ttlDelta);
                    watermarkKey.setValue(myEnd + 1);

                    // Is this shift completely used up? If so, let's move on to the next!
                    if (myEnd == outerEnd) {
                        outerIdx.increment();
                    }
                }

                if (outerIdx.intValue() == shifted.size() && watermarkKey.longValue() <= endRange && extraDelta != 0) {
                    fixShiftIfOverlap.accept(endRange, extraDelta);
                    newShifts.shiftRange(watermarkKey.longValue(), endRange, extraDelta);
                }
                watermarkKey.setValue(endRange + 1);
            };

            final ShiftInversionHelper inverter = new ShiftInversionHelper(shifted);

            for (int si = 0; si < myShifts.size(); ++si) {
                final long beginKey = inverter.mapToPrevKeyspace(myShifts.getBeginRange(si), false);
                final long endKey = inverter.mapToPrevKeyspace(myShifts.getEndRange(si), true);
                if (endKey < beginKey) {
                    continue;
                }

                consumeUntilWithExtraDelta.accept(beginKey - 1, 0L);
                consumeUntilWithExtraDelta.accept(endKey, myShifts.getShiftDelta(si));
            }
            consumeUntilWithExtraDelta.accept(Long.MAX_VALUE, 0L);

            shifted = newShifts.build();
        }
    }

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
        };

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
