/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public interface TreeIndexImpl {
    TreeIndexImpl ixCowRef();

    void ixRelease();

    @VisibleForTesting
    int ixRefCount();

    TreeIndexImpl ixInsert(long key);

    TreeIndexImpl ixInsertRange(long startKey, long endKey);

    @FinalDefault
    default TreeIndexImpl ixInsert(final LongChunk<OrderedKeyIndices> keys, final int offset,
        final int length) {
        if (length <= 1) {
            if (length == 0) {
                return this;
            }
            return ixInsert(keys.get(offset));
        }

        final int lastOffsetInclusive = offset + length - 1;
        final long first = keys.get(offset);
        final long last = keys.get(lastOffsetInclusive);
        if (last - first + 1 == length) {
            return ixInsertRange(first, last);
        }

        return ixInsertSecondHalf(keys, offset, length);
    }

    TreeIndexImpl ixInsertSecondHalf(LongChunk<OrderedKeyIndices> keys, int offset, int length);

    TreeIndexImpl ixInsert(TreeIndexImpl added);

    TreeIndexImpl ixAppendRange(long startKey, long endKey);

    TreeIndexImpl ixRemove(long key);

    TreeIndexImpl ixRemoveRange(long startKey, long endKey);

    @FinalDefault
    default TreeIndexImpl ixRemove(final LongChunk<OrderedKeyIndices> keys, final int offset,
        final int length) {
        if (ixIsEmpty()) {
            return this;
        }

        if (length <= 1) {
            if (length == 0) {
                return this;
            }
            return ixRemove(keys.get(offset));
        }

        final int lastOffsetInclusive = offset + length - 1;
        final long first = keys.get(offset);
        final long last = keys.get(lastOffsetInclusive);
        if (last - first + 1 == length) {
            return ixRemoveRange(first, last);
        }

        return ixRemoveSecondHalf(keys, offset, length);
    }

    TreeIndexImpl ixRemoveSecondHalf(LongChunk<OrderedKeyIndices> keys, int offset, int length);

    TreeIndexImpl ixRemove(TreeIndexImpl removed);

    long ixLastKey();

    long ixFirstKey();

    boolean ixForEachLong(LongAbortableConsumer lc);

    boolean ixForEachLongRange(LongRangeAbortableConsumer larc);

    TreeIndexImpl ixSubindexByPosOnNew(long startPos, long endPosExclusive);

    TreeIndexImpl ixSubindexByKeyOnNew(long startKey, long endKey);

    long ixGet(long pos);

    void ixGetKeysForPositions(PrimitiveIterator.OfLong inputPositions, LongConsumer outputKeys);

    long ixFind(long key);

    Index.Iterator ixIterator();

    Index.SearchIterator ixSearchIterator();

    Index.SearchIterator ixReverseIterator();

    Index.RangeIterator ixRangeIterator();

    long ixCardinality();

    boolean ixIsEmpty();

    TreeIndexImpl ixUpdate(TreeIndexImpl added, TreeIndexImpl removed);

    TreeIndexImpl ixRetain(TreeIndexImpl toIntersect);

    TreeIndexImpl ixRetainRange(long start, long end);

    TreeIndexImpl ixIntersectOnNew(TreeIndexImpl range);

    boolean ixContainsRange(long start, long end);

    boolean ixOverlaps(TreeIndexImpl impl);

    boolean ixOverlapsRange(long start, long end);

    boolean ixSubsetOf(TreeIndexImpl impl);

    TreeIndexImpl ixMinusOnNew(TreeIndexImpl set);

    TreeIndexImpl ixUnionOnNew(TreeIndexImpl set);

    TreeIndexImpl ixShiftOnNew(long shiftAmount);

    TreeIndexImpl ixShiftInPlace(long shiftAmount);

    TreeIndexImpl ixInsertWithShift(long shiftAmount, TreeIndexImpl other);

    OrderedKeys ixGetOrderedKeysByPosition(long startPositionInclusive, long length);

    OrderedKeys ixGetOrderedKeysByKeyRange(long startKeyInclusive, long endKeyInclusive);

    OrderedKeys.Iterator ixGetOrderedKeysIterator();

    long ixRangesCountUpperBound();

    long ixGetAverageRunLengthEstimate();

    RspBitmap ixToRspOnNew();

    /**
     * Invert the given index.
     *
     * @param keys Index of keys to invert
     * @param maximumPosition the largest position to add to indexBuilder, inclusive
     *
     * @return the inverse of index
     */
    TreeIndexImpl ixInvertOnNew(TreeIndexImpl keys, long maximumPosition);

    TreeIndexImpl ixCompact();

    void ixValidate(String failMsg);

    default void ixValidate() {
        ixValidate(null);
    }

    /**
     * Produce a {@link TreeIndexImpl} from a slice of a {@link LongChunk} of
     * {@link OrderedKeyIndices}.
     *
     * @param keys The {@link LongChunk} of {@link OrderedKeyIndices} to build from
     * @param offset The offset in {@code keys} to begin building from
     * @param length The number of keys to include
     * @return A new {@link TreeIndexImpl} containing the specified slice of {@code keys}
     */
    static TreeIndexImpl fromChunk(final LongChunk<OrderedKeyIndices> keys, final int offset,
        final int length, final boolean disposable) {
        if (length == 0) {
            return EMPTY;
        }

        final int lastOffsetInclusive = offset + length - 1;
        final long first = keys.get(offset);
        final long last = keys.get(lastOffsetInclusive);
        if (last - first + 1 == length) {
            return SingleRange.make(first, last);
        }

        final TreeIndexImplSequentialBuilder builder =
            new TreeIndexImplSequentialBuilder(disposable);
        builder.appendKey(first);
        for (int ki = offset + 1; ki < lastOffsetInclusive; ++ki) {
            builder.appendKey(keys.get(ki));
        }
        builder.appendKey(last);
        return builder.getTreeIndexImpl();
    }

    TreeIndexImpl EMPTY = new TreeIndexImpl() {
        @Override
        public TreeIndexImpl ixCowRef() {
            return this;
        }

        @Override
        public void ixRelease() {}

        @Override
        public int ixRefCount() {
            return 1;
        }

        @Override
        public TreeIndexImpl ixInsert(final long key) {
            return SingleRange.make(key, key);
        }

        @Override
        public TreeIndexImpl ixInsertRange(final long startKey, final long endKey) {
            return SingleRange.make(startKey, endKey);
        }

        @Override
        public TreeIndexImpl ixInsertSecondHalf(final LongChunk<OrderedKeyIndices> keys,
            final int offset, final int length) {
            return fromChunk(keys, offset, length, false);
        }

        @Override
        public TreeIndexImpl ixRemoveSecondHalf(final LongChunk<OrderedKeyIndices> keys,
            final int offset, final int length) {
            throw new IllegalStateException();
        }

        @Override
        public TreeIndexImpl ixAppendRange(final long startKey, final long endKey) {
            return ixInsertRange(startKey, endKey);
        }

        @Override
        public TreeIndexImpl ixRemove(long key) {
            return this;
        }

        @Override
        public long ixLastKey() {
            return -1;
        }

        @Override
        public long ixFirstKey() {
            return -1;
        }

        @Override
        public boolean ixForEachLong(LongAbortableConsumer lc) {
            return true;
        }

        @Override
        public boolean ixForEachLongRange(LongRangeAbortableConsumer larc) {
            return true;
        }

        @Override
        public TreeIndexImpl ixSubindexByPosOnNew(long startPos, long endPos) {
            return this;
        }

        @Override
        public TreeIndexImpl ixSubindexByKeyOnNew(long startKey, long endKey) {
            return this;
        }

        @Override
        public long ixGet(long pos) {
            return Index.NULL_KEY;
        }

        @Override
        public long ixFind(long key) {
            return Index.NULL_KEY;
        }

        @Override
        public void ixGetKeysForPositions(PrimitiveIterator.OfLong inputPositions,
            LongConsumer outputKeys) {
            while (inputPositions.hasNext()) {
                inputPositions.nextLong();
                outputKeys.accept(Index.NULL_KEY);
            }
        }

        @Override
        public Index.Iterator ixIterator() {
            return Index.EMPTY_ITERATOR;
        }

        @Override
        public Index.SearchIterator ixSearchIterator() {
            return Index.EMPTY_ITERATOR;
        }

        @Override
        public Index.SearchIterator ixReverseIterator() {
            return Index.EMPTY_ITERATOR;
        }

        @Override
        public Index.RangeIterator ixRangeIterator() {
            return Index.RangeIterator.empty;
        }

        @Override
        public long ixCardinality() {
            return 0;
        }

        @Override
        public boolean ixIsEmpty() {
            return true;
        }

        @Override
        public TreeIndexImpl ixUpdate(TreeIndexImpl added, TreeIndexImpl removed) {
            if (added.ixIsEmpty()) {
                return this;
            }
            return added.ixCowRef();
        }

        @Override
        public TreeIndexImpl ixRemove(TreeIndexImpl removed) {
            return this;
        }

        @Override
        public TreeIndexImpl ixRemoveRange(long startKey, long endKey) {
            return this;
        }

        @Override
        public TreeIndexImpl ixRetain(TreeIndexImpl toIntersect) {
            return this;
        }

        @Override
        public TreeIndexImpl ixRetainRange(final long start, final long end) {
            return this;
        }

        @Override
        public TreeIndexImpl ixIntersectOnNew(TreeIndexImpl range) {
            return this;
        }

        @Override
        public boolean ixContainsRange(final long start, final long end) {
            return false;
        }

        @Override
        public boolean ixOverlaps(TreeIndexImpl impl) {
            return false;
        }

        @Override
        public boolean ixOverlapsRange(long start, long end) {
            return false;
        }

        @Override
        public boolean ixSubsetOf(TreeIndexImpl impl) {
            return true;
        }

        @Override
        public TreeIndexImpl ixMinusOnNew(TreeIndexImpl set) {
            return this;
        }

        @Override
        public TreeIndexImpl ixUnionOnNew(final TreeIndexImpl set) {
            return set.ixCowRef();
        }

        @Override
        public TreeIndexImpl ixShiftOnNew(final long shiftAmount) {
            return this;
        }

        @Override
        public TreeIndexImpl ixShiftInPlace(final long shiftAmount) {
            return this;
        }

        @Override
        public TreeIndexImpl ixInsert(final TreeIndexImpl added) {
            return added.ixCowRef();
        }

        @Override
        public TreeIndexImpl ixInsertWithShift(final long shiftAmount, final TreeIndexImpl other) {
            return other.ixShiftOnNew(shiftAmount);
        }

        @Override
        public OrderedKeys ixGetOrderedKeysByPosition(long startPositionInclusive, long length) {
            return OrderedKeys.EMPTY;
        }

        @Override
        public OrderedKeys ixGetOrderedKeysByKeyRange(long startKeyInclusive,
            long endKeyInclusive) {
            return OrderedKeys.EMPTY;
        }

        @Override
        public OrderedKeys.Iterator ixGetOrderedKeysIterator() {
            return OrderedKeys.Iterator.EMPTY;
        }

        @Override
        public long ixRangesCountUpperBound() {
            return 0;
        }

        @Override
        public long ixGetAverageRunLengthEstimate() {
            return 1;
        }

        @Override
        public TreeIndexImpl ixInvertOnNew(TreeIndexImpl keys, long maximumPosition) {
            return this;
        }

        @Override
        public TreeIndexImpl ixCompact() {
            return this;
        }

        @Override
        public void ixValidate(final String failmsg) {}

        @Override
        public RspBitmap ixToRspOnNew() {
            return new RspBitmap();
        }

        @Override
        public String toString() {
            return "EMPTY";
        }
    };

    interface SequentialBuilder extends LongRangeConsumer {
        boolean check =
            Configuration.getInstance().getBooleanForClassWithDefault(
                TreeIndexImpl.class, "sequentialBuilderCheck", true);

        String outOfOrderKeyErrorMsg = "Out of order key(s) in sequential builder: ";

        default void setDomain(long minKey, long maxKey) {}

        TreeIndexImpl getTreeIndexImpl();

        void appendKey(long key);

        void appendRange(long firstKey, long lastKey);

        default void appendTreeIndexImpl(final long shiftAmount, final TreeIndexImpl ix,
            final boolean acquire) {
            ix.ixForEachLongRange((final long start, final long last) -> {
                appendRange(start + shiftAmount, last + shiftAmount);
                return true;
            });
        }

        @Override
        default void accept(final long firstKey, final long lastKey) {
            appendRange(firstKey, lastKey);
        }
    }

    interface RandomBuilder extends SequentialBuilder {
        void addKey(long key);

        void addRange(long firstKey, long lastKey);

        default void appendKey(final long key) {
            addKey(key);
        }

        default void appendRange(final long firstKey, final long lastKey) {
            addRange(firstKey, lastKey);
        }

        default void add(final SingleRange ix) {
            addRange(ix.ixFirstKey(), ix.ixLastKey());
        }

        void add(SortedRanges ix, boolean acquire);

        void add(RspBitmap ix, boolean acquire);

        @Override
        default void accept(final long firstKey, final long lastKey) {
            appendRange(firstKey, lastKey);
        }
    }

    static TreeIndexImpl twoRanges(final long s1, final long e1, final long s2, final long e2) {
        SortedRanges sr = SortedRanges.tryMakeForKnownRangeKnownCount(4, s1, e2);
        if (sr != null) {
            sr = sr.appendRangeUnsafe(s1, e1)
                .appendRangeUnsafe(s2, e2)
                .tryCompactUnsafe(4);
            return sr;
        }
        final RspBitmap ans = new RspBitmap(s1, e1);
        ans.appendRangeUnsafeNoWriteCheck(s2, e2);
        ans.finishMutationsAndOptimize();
        return ans;
    }

    // Note the caller has no way to know if a new reference was created,
    // so it should not release. This method is intended for cases where ixRspOnNew
    // is not desirable since that will increment the refcount for objects that
    // are of type RspBitmap already.
    static RspBitmap asRspBitmap(final TreeIndexImpl t) {
        return (t instanceof RspBitmap)
            ? (RspBitmap) t
            : t.ixToRspOnNew();
    }
}
