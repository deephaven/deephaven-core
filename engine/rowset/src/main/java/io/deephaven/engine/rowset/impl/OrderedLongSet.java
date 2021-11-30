/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.rowset.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

/**
 * An ordered sequence of {@code long} values with set operations.
 */
public interface OrderedLongSet {

    OrderedLongSet ixCowRef();

    void ixRelease();

    @VisibleForTesting
    int ixRefCount();

    OrderedLongSet ixInsert(long key);

    OrderedLongSet ixInsertRange(long startKey, long endKey);

    @FinalDefault
    default OrderedLongSet ixInsert(final LongChunk<OrderedRowKeys> keys, final int offset,
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

    OrderedLongSet ixInsertSecondHalf(LongChunk<OrderedRowKeys> keys, int offset, int length);

    OrderedLongSet ixInsert(OrderedLongSet added);

    OrderedLongSet ixAppendRange(long startKey, long endKey);

    OrderedLongSet ixRemove(long key);

    OrderedLongSet ixRemoveRange(long startKey, long endKey);

    @FinalDefault
    default OrderedLongSet ixRemove(final LongChunk<OrderedRowKeys> keys, final int offset,
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

    OrderedLongSet ixRemoveSecondHalf(LongChunk<OrderedRowKeys> keys, int offset, int length);

    OrderedLongSet ixRemove(OrderedLongSet removed);

    long ixLastKey();

    long ixFirstKey();

    boolean ixForEachLong(LongAbortableConsumer lc);

    boolean ixForEachLongRange(LongRangeAbortableConsumer larc);

    OrderedLongSet ixSubindexByPosOnNew(long startPos, long endPosExclusive);

    OrderedLongSet ixSubindexByKeyOnNew(long startKey, long endKey);

    long ixGet(long pos);

    void ixGetKeysForPositions(PrimitiveIterator.OfLong inputPositions, LongConsumer outputKeys);

    long ixFind(long key);

    RowSet.Iterator ixIterator();

    RowSet.SearchIterator ixSearchIterator();

    RowSet.SearchIterator ixReverseIterator();

    RowSet.RangeIterator ixRangeIterator();

    long ixCardinality();

    boolean ixIsEmpty();

    OrderedLongSet ixUpdate(OrderedLongSet added, OrderedLongSet removed);

    OrderedLongSet ixRetain(OrderedLongSet toIntersect);

    OrderedLongSet ixRetainRange(long start, long end);

    OrderedLongSet ixIntersectOnNew(OrderedLongSet range);

    boolean ixContainsRange(long start, long end);

    boolean ixOverlaps(OrderedLongSet impl);

    boolean ixOverlapsRange(long start, long end);

    boolean ixSubsetOf(OrderedLongSet impl);

    OrderedLongSet ixMinusOnNew(OrderedLongSet set);

    OrderedLongSet ixUnionOnNew(OrderedLongSet set);

    OrderedLongSet ixShiftOnNew(long shiftAmount);

    OrderedLongSet ixShiftInPlace(long shiftAmount);

    OrderedLongSet ixInsertWithShift(long shiftAmount, OrderedLongSet other);

    RowSequence ixGetRowSequenceByPosition(long startPositionInclusive, long length);

    RowSequence ixGetRowSequenceByKeyRange(long startKeyInclusive, long endKeyInclusive);

    RowSequence.Iterator ixGetRowSequenceIterator();

    long ixRangesCountUpperBound();

    long ixGetAverageRunLengthEstimate();

    RspBitmap ixToRspOnNew();

    /**
     * Invert the given OrderedLongSet.
     *
     * @param keys OrderedLongSet of keys to invert
     * @param maximumPosition the largest position to add to indexBuilder, inclusive
     *
     * @return the inverse of {@code keys}
     */
    OrderedLongSet ixInvertOnNew(OrderedLongSet keys, long maximumPosition);

    OrderedLongSet ixCompact();

    void ixValidate(String failMsg);

    default void ixValidate() {
        ixValidate(null);
    }

    /**
     * Produce a {@link OrderedLongSet} from a slice of a {@link LongChunk} of {@link OrderedRowKeys}.
     *
     * @param keys The {@link LongChunk} of {@link OrderedRowKeys} to build from
     * @param offset The offset in {@code keys} to begin building from
     * @param length The number of keys to include
     * @return A new {@link OrderedLongSet} containing the specified slice of {@code keys}
     */
    static OrderedLongSet fromChunk(final LongChunk<OrderedRowKeys> keys, final int offset, final int length,
            final boolean disposable) {
        if (length == 0) {
            return EMPTY;
        }

        final int lastOffsetInclusive = offset + length - 1;
        final long first = keys.get(offset);
        final long last = keys.get(lastOffsetInclusive);
        if (last - first + 1 == length) {
            return SingleRange.make(first, last);
        }

        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential(disposable);
        builder.appendKey(first);
        for (int ki = offset + 1; ki < lastOffsetInclusive; ++ki) {
            builder.appendKey(keys.get(ki));
        }
        builder.appendKey(last);
        return builder.getTreeIndexImpl();
    }

    OrderedLongSet EMPTY = new OrderedLongSet() {
        @Override
        public OrderedLongSet ixCowRef() {
            return this;
        }

        @Override
        public void ixRelease() {}

        @Override
        public int ixRefCount() {
            return 1;
        }

        @Override
        public OrderedLongSet ixInsert(final long key) {
            return SingleRange.make(key, key);
        }

        @Override
        public OrderedLongSet ixInsertRange(final long startKey, final long endKey) {
            return SingleRange.make(startKey, endKey);
        }

        @Override
        public OrderedLongSet ixInsertSecondHalf(final LongChunk<OrderedRowKeys> keys, final int offset,
                final int length) {
            return fromChunk(keys, offset, length, false);
        }

        @Override
        public OrderedLongSet ixRemoveSecondHalf(final LongChunk<OrderedRowKeys> keys, final int offset,
                final int length) {
            throw new IllegalStateException();
        }

        @Override
        public OrderedLongSet ixAppendRange(final long startKey, final long endKey) {
            return ixInsertRange(startKey, endKey);
        }

        @Override
        public OrderedLongSet ixRemove(long key) {
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
        public OrderedLongSet ixSubindexByPosOnNew(long startPos, long endPos) {
            return this;
        }

        @Override
        public OrderedLongSet ixSubindexByKeyOnNew(long startKey, long endKey) {
            return this;
        }

        @Override
        public long ixGet(long pos) {
            return RowSequence.NULL_ROW_KEY;
        }

        @Override
        public long ixFind(long key) {
            return RowSequence.NULL_ROW_KEY;
        }

        @Override
        public void ixGetKeysForPositions(PrimitiveIterator.OfLong inputPositions, LongConsumer outputKeys) {
            while (inputPositions.hasNext()) {
                inputPositions.nextLong();
                outputKeys.accept(RowSequence.NULL_ROW_KEY);
            }
        }

        @Override
        public RowSet.Iterator ixIterator() {
            return RowSet.EMPTY_ITERATOR;
        }

        @Override
        public RowSet.SearchIterator ixSearchIterator() {
            return RowSet.EMPTY_ITERATOR;
        }

        @Override
        public RowSet.SearchIterator ixReverseIterator() {
            return RowSet.EMPTY_ITERATOR;
        }

        @Override
        public RowSet.RangeIterator ixRangeIterator() {
            return RowSet.RangeIterator.empty;
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
        public OrderedLongSet ixUpdate(OrderedLongSet added, OrderedLongSet removed) {
            if (added.ixIsEmpty()) {
                return this;
            }
            return added.ixCowRef();
        }

        @Override
        public OrderedLongSet ixRemove(OrderedLongSet removed) {
            return this;
        }

        @Override
        public OrderedLongSet ixRemoveRange(long startKey, long endKey) {
            return this;
        }

        @Override
        public OrderedLongSet ixRetain(OrderedLongSet toIntersect) {
            return this;
        }

        @Override
        public OrderedLongSet ixRetainRange(final long start, final long end) {
            return this;
        }

        @Override
        public OrderedLongSet ixIntersectOnNew(OrderedLongSet range) {
            return this;
        }

        @Override
        public boolean ixContainsRange(final long start, final long end) {
            return false;
        }

        @Override
        public boolean ixOverlaps(OrderedLongSet impl) {
            return false;
        }

        @Override
        public boolean ixOverlapsRange(long start, long end) {
            return false;
        }

        @Override
        public boolean ixSubsetOf(OrderedLongSet impl) {
            return true;
        }

        @Override
        public OrderedLongSet ixMinusOnNew(OrderedLongSet set) {
            return this;
        }

        @Override
        public OrderedLongSet ixUnionOnNew(final OrderedLongSet set) {
            return set.ixCowRef();
        }

        @Override
        public OrderedLongSet ixShiftOnNew(final long shiftAmount) {
            return this;
        }

        @Override
        public OrderedLongSet ixShiftInPlace(final long shiftAmount) {
            return this;
        }

        @Override
        public OrderedLongSet ixInsert(final OrderedLongSet added) {
            return added.ixCowRef();
        }

        @Override
        public OrderedLongSet ixInsertWithShift(final long shiftAmount, final OrderedLongSet other) {
            return other.ixShiftOnNew(shiftAmount);
        }

        @Override
        public RowSequence ixGetRowSequenceByPosition(long startPositionInclusive, long length) {
            return RowSequenceFactory.EMPTY;
        }

        @Override
        public RowSequence ixGetRowSequenceByKeyRange(long startKeyInclusive, long endKeyInclusive) {
            return RowSequenceFactory.EMPTY;
        }

        @Override
        public RowSequence.Iterator ixGetRowSequenceIterator() {
            return RowSequenceFactory.EMPTY_ITERATOR;
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
        public OrderedLongSet ixInvertOnNew(OrderedLongSet keys, long maximumPosition) {
            return this;
        }

        @Override
        public OrderedLongSet ixCompact() {
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

    interface BuilderSequential extends LongRangeConsumer {
        boolean check =
                Configuration.getInstance().getBooleanForClassWithDefault(
                        OrderedLongSet.class, "sequentialBuilderCheck", true);

        String outOfOrderKeyErrorMsg = "Out of order key(s) in sequential builder: ";

        default void setDomain(long minKey, long maxKey) {}

        OrderedLongSet getTreeIndexImpl();

        void appendKey(long key);

        void appendRange(long firstKey, long lastKey);

        default void appendTreeIndexImpl(final long shiftAmount, final OrderedLongSet ix, final boolean acquire) {
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

    interface BuilderRandom extends BuilderSequential {
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

    static OrderedLongSet twoRanges(final long s1, final long e1, final long s2, final long e2) {
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
    static RspBitmap asRspBitmap(final OrderedLongSet t) {
        return (t instanceof RspBitmap)
                ? (RspBitmap) t
                : t.ixToRspOnNew();
    }
}
