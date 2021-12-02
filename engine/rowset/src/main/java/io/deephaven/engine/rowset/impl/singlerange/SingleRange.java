package io.deephaven.engine.rowset.impl.singlerange;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.OrderedLongSetBuilderSequential;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.impl.RowSetUtils;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public abstract class SingleRange implements OrderedLongSet {
    public abstract long rangeStart();

    public abstract long rangeEnd();

    public abstract long getCardinality();

    public abstract SingleRange copy();

    protected static long unsignedIntToLong(final int unsignedInt) {
        return maxUnsignedInt() & unsignedInt;
    }

    protected static long maxUnsignedInt() {
        return 0xFFFF_FFFFL;
    }

    protected static int lowBitsAsUnsignedInt(final long v) {
        return (int) v;
    }

    protected static long maxUnsignedShort() {
        return 0xFFFFL;
    }

    protected static long unsignedShortToLong(final short unsignedShort) {
        return maxUnsignedShort() & unsignedShort;
    }

    protected static short lowBitsAsUnsignedShort(final long v) {
        return (short) v;
    }

    public static SingleRange make(final long start, final long end) {
        final long delta = end - start;
        if (delta == 0) {
            final int unsignedIntStart = lowBitsAsUnsignedInt(start);
            if (unsignedIntToLong(unsignedIntStart) == start) {
                return new SingleIntSingleRange(unsignedIntStart);
            }
            return new SingleLongSingleRange(start);
        }
        final short unsignedShortStart = lowBitsAsUnsignedShort(start);
        if (unsignedShortToLong(unsignedShortStart) == start) {
            final short unsignedShortDelta = lowBitsAsUnsignedShort(delta);
            if (unsignedShortToLong(unsignedShortDelta) == delta) {
                return new ShortStartShortDeltaSingleRange(unsignedShortStart, unsignedShortDelta);
            }
        }
        final int unsignedIntStart = lowBitsAsUnsignedInt(start);
        final int unsignedIntDelta = lowBitsAsUnsignedInt(delta);
        if (unsignedIntToLong(unsignedIntDelta) == delta) {
            if (unsignedIntToLong(unsignedIntStart) == start) {
                return new IntStartIntDeltaSingleRange(unsignedIntStart, unsignedIntDelta);
            }
            return new LongStartIntDeltaSingleRange(start, unsignedIntDelta);
        }
        if (unsignedIntToLong(unsignedIntStart) == start) {
            return new IntStartLongDeltaSingleRange(unsignedIntStart, delta);
        }
        return new LongStartLongEndSingleRange(start, end);
    }

    @Override
    public final long ixLastKey() {
        return rangeEnd();
    }

    @Override
    public final long ixFirstKey() {
        return rangeStart();
    }

    @Override
    public final long ixCardinality() {
        return getCardinality();
    }

    @Override
    public final boolean ixForEachLong(final LongAbortableConsumer lc) {
        for (long v = rangeStart(); v <= rangeEnd(); ++v) {
            if (!lc.accept(v)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public final boolean ixForEachLongRange(final LongRangeAbortableConsumer larc) {
        return larc.accept(rangeStart(), rangeEnd());
    }

    @Override
    public final SingleRange ixCowRef() {
        return copy();
    }

    @Override
    public final void ixRelease() {}

    @Override
    public final int ixRefCount() {
        return 1;
    }

    @SuppressWarnings("unused")
    private void ifDebugValidate() {
        if (RspArray.debug) {
            ixValidate();
        }
    }

    @Override
    public final OrderedLongSet ixInsert(final long key) {
        if (rangeStart() <= key && key <= rangeEnd()) {
            return this;
        }
        if (key + 1 < rangeStart()) {
            return OrderedLongSet.twoRanges(key, key, rangeStart(), rangeEnd());
        }
        if (key + 1 == rangeStart()) {
            return make(key, rangeEnd());
        }
        if (rangeEnd() + 1 == key) {
            return make(rangeStart(), key);
        }
        return OrderedLongSet.twoRanges(rangeStart(), rangeEnd(), key, key);
    }

    @Override
    public final OrderedLongSet ixInsertRange(final long startKey, final long endKey) {
        if (rangeStart() <= startKey && endKey <= rangeEnd()) {
            return this;
        }
        if (overlapsOrAdjacentToRange(startKey, endKey)) {
            return make(
                    Math.min(startKey, rangeStart()),
                    Math.max(endKey, rangeEnd()));
        }
        if (startKey < rangeStart()) {
            return OrderedLongSet.twoRanges(startKey, endKey, rangeStart(), rangeEnd());
        }
        return OrderedLongSet.twoRanges(rangeStart(), rangeEnd(), startKey, endKey);
    }

    @Override
    public final OrderedLongSet ixInsertSecondHalf(final LongChunk<OrderedRowKeys> keys, final int offset,
            final int length) {
        return OrderedLongSet.fromChunk(keys, offset, length, false).ixInsertRange(rangeStart(), rangeEnd());
    }

    @Override
    public final OrderedLongSet ixRemoveSecondHalf(final LongChunk<OrderedRowKeys> keys, final int offset,
            final int length) {
        return ixRemove(OrderedLongSet.fromChunk(keys, offset, length, true));
    }

    @Override
    public final OrderedLongSet ixAppendRange(final long startKey, final long endKey) {
        if (rangeEnd() + 1 < startKey) {
            return OrderedLongSet.twoRanges(rangeStart(), rangeEnd(), startKey, endKey);
        }
        if (rangeEnd() + 1 == startKey) {
            return make(rangeStart(), endKey);
        }
        throw new IllegalStateException("startKey(=" + startKey + ") < rangeEnd(=" + rangeEnd() + ")");
    }

    @Override
    public final OrderedLongSet ixRemove(final long key) {
        if (key < rangeStart() || key > rangeEnd()) {
            return this;
        }
        if (key == rangeStart()) {
            if (rangeEnd() == rangeStart()) {
                return OrderedLongSet.EMPTY;
            }
            return make(key + 1, rangeEnd());
        }
        if (key == rangeEnd()) {
            return make(rangeStart(), key - 1);
        }
        return OrderedLongSet.twoRanges(rangeStart(), key - 1, key + 1, rangeEnd());
    }

    @Override
    public final OrderedLongSet ixSubindexByPosOnNew(final long startPos, final long endPosExclusive) {
        final long endPos = endPosExclusive - 1; // make inclusive.
        if (endPos < startPos || endPos < 0) {
            return OrderedLongSet.EMPTY;
        }
        final long sz = ixCardinality();
        if (startPos >= sz) {
            return OrderedLongSet.EMPTY;
        }
        final long len = endPos - startPos + 1;
        if (startPos == 0 && len >= sz) {
            return ixCowRef();
        }
        return make(
                Math.max(rangeStart() + startPos, rangeStart()),
                Math.min(rangeStart() + endPos, rangeEnd()));
    }

    @Override
    public final OrderedLongSet ixSubindexByKeyOnNew(final long startKey, final long endKey) {
        if (startKey > rangeEnd() || endKey < rangeStart()) {
            return OrderedLongSet.EMPTY;
        }
        if (startKey == rangeStart() && endKey == rangeEnd()) {
            return ixCowRef();
        }
        return make(
                Math.max(startKey, rangeStart()),
                Math.min(endKey, rangeEnd()));
    }

    @Override
    public final long ixGet(final long pos) {
        if (pos < 0 || pos >= ixCardinality()) {
            return RowSequence.NULL_ROW_KEY;
        }
        return rangeStart() + pos;
    }

    @Override
    public final void ixGetKeysForPositions(final PrimitiveIterator.OfLong inputPositions,
            final LongConsumer outputKeys) {
        final long sz = ixCardinality();
        while (inputPositions.hasNext()) {
            final long pos = inputPositions.nextLong();
            if (pos < 0 || pos >= sz) {
                outputKeys.accept(RowSequence.NULL_ROW_KEY);
                continue;
            }
            outputKeys.accept(rangeStart() + pos);
        }
    }

    @Override
    public final long ixFind(final long key) {
        if (key < rangeStart()) {
            return ~0;
        }
        if (key > rangeEnd()) {
            return ~ixCardinality();
        }
        return key - rangeStart();
    }

    private static class Iterator implements RowSet.Iterator {
        protected long curr;
        protected final long last;

        public Iterator(final SingleRange ix) {
            curr = ix.rangeStart() - 1;
            last = ix.rangeEnd();
        }

        @Override
        public long nextLong() {
            return ++curr;
        }

        @Override
        public boolean hasNext() {
            return curr < last;
        }

        @Override
        public void close() { /* We never held to anything. */ }
    }

    @Override
    public RowSet.Iterator ixIterator() {
        return new Iterator(this);
    }

    private static final class SearchIterator extends Iterator implements RowSet.SearchIterator {
        private final long rangeStart;

        public SearchIterator(final SingleRange ix) {
            super(ix);
            rangeStart = ix.rangeStart();
        }

        @Override
        public long currentValue() {
            return curr;
        }

        @Override
        public boolean advance(final long v) {
            if (curr < rangeStart) {
                curr = rangeStart;
            }
            if (v > last) {
                curr = last;
                return false;
            }
            if (v > curr) {
                curr = v;
            }
            return true;
        }

        @Override
        public long binarySearchValue(RowSet.TargetComparator tc, final int dir) {
            if (curr < rangeStart) {
                if (tc.compareTargetTo(rangeStart, dir) < 0) {
                    return -1;
                }
                curr = rangeStart;
            } else if (tc.compareTargetTo(curr, dir) < 0) {
                return -1;
            }
            return curr = RowSetUtils.rangeSearch(curr, last,
                    (long k) -> tc.compareTargetTo(k, dir));
        }
    }

    @Override
    public final RowSet.SearchIterator ixSearchIterator() {
        return new SearchIterator(this);
    }

    private static final class ReverseIter implements RowSet.SearchIterator {
        private final long start;
        private final long end;
        private long curr;

        public ReverseIter(final long rangeStart, final long rangeEnd) {
            start = rangeStart;
            end = rangeEnd;
            curr = rangeEnd + 1;
        }

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
            return start < curr;
        }

        @Override
        public long currentValue() {
            return curr;
        }

        @Override
        public long nextLong() {
            return --curr;
        }

        @Override
        public boolean advance(long v) {
            if (v < start) {
                curr = start;
                return false;
            }
            curr = Math.min(v, Math.min(curr, end)); // it might not have been started yet.
            return true;
        }

        @Override
        public long binarySearchValue(RowSet.TargetComparator targetComparator, int direction) {
            throw new UnsupportedOperationException("Reverse iterator does not support binary search.");
        }
    }

    @Override
    public final RowSet.SearchIterator ixReverseIterator() {
        return new ReverseIter(rangeStart(), rangeEnd());
    }

    private static final class RangeIter implements RowSet.RangeIterator {
        private long start;
        private final long end;
        private boolean hasNext;

        public RangeIter(final long rangeStart, final long rangeEnd) {
            start = rangeStart;
            end = rangeEnd;
            hasNext = true;
        }

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public boolean advance(long v) {
            hasNext = false;
            if (v <= start) {
                return true;
            }
            if (v > end) {
                return false;
            }
            start = v;
            return true;
        }

        @Override
        public void postpone(final long v) {
            start = v;
        }

        @Override
        public long currentRangeStart() {
            return start;
        }

        @Override
        public long currentRangeEnd() {
            return end;
        }

        @Override
        public long next() {
            hasNext = false;
            return start;
        }
    }

    @Override
    public final RowSet.RangeIterator ixRangeIterator() {
        return new RangeIter(rangeStart(), rangeEnd());
    }

    @Override
    public final boolean ixIsEmpty() {
        return false;
    }

    @Override
    public final OrderedLongSet ixUpdate(final OrderedLongSet added, final OrderedLongSet removed) {
        if (removed.ixIsEmpty() || removed.ixLastKey() < rangeStart() || removed.ixFirstKey() > rangeEnd()) {
            if (added.ixIsEmpty()) {
                return this;
            }
            return ixInsert(added);
        }
        if (removed instanceof SingleRange) {
            if (removed.ixFirstKey() <= ixFirstKey() && ixLastKey() <= removed.ixLastKey()) {
                return added.ixCowRef();
            }
            final OrderedLongSet t = ixRemoveRange(removed.ixFirstKey(), removed.ixLastKey());
            return t.ixInsert(added);
        }
        if (added.ixIsEmpty()) {
            return ixRemove(removed);
        }
        if (added instanceof SingleRange) {
            return ixRemove(removed).ixInsertRange(added.ixFirstKey(), added.ixLastKey());
        }
        if (added instanceof SortedRanges) {
            final SortedRanges ans = toSortedRanges();
            return ans.ixUpdate(added, removed);
        }
        final RspBitmap ans = toRsp();
        ans.updateUnsafeNoWriteCheck(
                OrderedLongSet.asRspBitmap(added),
                OrderedLongSet.asRspBitmap(removed));
        if (ans.isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        ans.finishMutations();
        return ans;
    }

    @Override
    public final OrderedLongSet ixRemove(final OrderedLongSet removed) {
        return minus(removed);
    }

    private OrderedLongSet minus(final OrderedLongSet removed) {
        if (removed.ixIsEmpty() || removed.ixLastKey() < rangeStart() || removed.ixFirstKey() > rangeEnd()) {
            return this;
        }
        if (ixSubsetOf(removed)) {
            return OrderedLongSet.EMPTY;
        }
        if (removed instanceof SingleRange) {
            return ixRemoveRange(removed.ixFirstKey(), removed.ixLastKey());
        }
        final SortedRanges sr = toSortedRanges();
        final OrderedLongSet r = sr.remove(removed);
        if (r != null) {
            return r;
        }
        final RspBitmap ans = toRsp();
        ans.andNotEqualsUnsafeNoWriteCheck(OrderedLongSet.asRspBitmap(removed));
        if (ans.isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        ans.finishMutations();
        return ans;
    }

    @Override
    public final OrderedLongSet ixRemoveRange(final long startKey, final long endKey) {
        if (endKey < rangeStart() || startKey > rangeEnd()) {
            return this;
        }
        if (startKey <= rangeStart() && rangeEnd() <= endKey) {
            return OrderedLongSet.EMPTY;
        }
        if (rangeStart() < startKey && endKey < rangeEnd()) {
            // creates a hole.
            return OrderedLongSet.twoRanges(rangeStart(), startKey - 1, endKey + 1, rangeEnd());
        }
        if (endKey < rangeEnd()) {
            return make(endKey + 1, rangeEnd());
        }
        return make(rangeStart(), startKey - 1);
    }

    @Override
    public final OrderedLongSet ixRetain(final OrderedLongSet other) {
        return intersect(other);
    }

    private OrderedLongSet intersect(final OrderedLongSet other) {
        if (other.ixIsEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        final long otherFirstKey = other.ixFirstKey();
        if (rangeEnd() < otherFirstKey) {
            return OrderedLongSet.EMPTY;
        }
        final long otherLastKey = other.ixLastKey();
        if (otherLastKey < rangeStart()) {
            return OrderedLongSet.EMPTY;
        }
        if (other instanceof SingleRange) {
            return make(Math.max(rangeStart(), otherFirstKey), Math.min(rangeEnd(), otherLastKey));
        }
        return other.ixSubindexByKeyOnNew(rangeStart(), rangeEnd());
    }

    @Override
    public final OrderedLongSet ixRetainRange(final long start, final long end) {
        if (rangeEnd() < start) {
            return OrderedLongSet.EMPTY;
        }
        if (end < rangeStart()) {
            return OrderedLongSet.EMPTY;
        }
        return make(Math.max(rangeStart(), start), Math.min(rangeEnd(), end));
    }

    @Override
    public final OrderedLongSet ixIntersectOnNew(final OrderedLongSet intersected) {
        return intersect(intersected);
    }

    @Override
    public final boolean ixContainsRange(final long start, final long end) {
        return rangeStart() <= start && end <= rangeEnd();
    }

    @Override
    public final boolean ixOverlaps(final OrderedLongSet impl) {
        if (impl.ixIsEmpty()) {
            return false;
        }
        if (impl instanceof SingleRange) {
            return !(impl.ixLastKey() < rangeStart() || impl.ixFirstKey() > rangeEnd());
        }
        return impl.ixOverlapsRange(rangeStart(), rangeEnd());
    }

    @Override
    public final boolean ixOverlapsRange(final long start, final long end) {
        return !(start > rangeEnd() || end < rangeStart());
    }

    private boolean overlapsOrAdjacentToRange(final long start, final long end) {
        return !(rangeEnd() < start - 1 || end < rangeStart() - 1);
    }

    @Override
    public final boolean ixSubsetOf(final OrderedLongSet impl) {
        if (impl.ixIsEmpty()) {
            return false;
        }
        if (impl instanceof SingleRange) {
            return impl.ixFirstKey() <= rangeStart() && rangeEnd() <= impl.ixLastKey();
        }
        return impl.ixContainsRange(rangeStart(), rangeEnd());
    }

    @Override
    public final OrderedLongSet ixMinusOnNew(final OrderedLongSet set) {
        return minus(set);
    }

    @Override
    public final OrderedLongSet ixUnionOnNew(final OrderedLongSet set) {
        return union(set);
    }

    private OrderedLongSet union(OrderedLongSet set) {
        if (set.ixIsEmpty()) {
            return ixCowRef();
        }
        if (rangeStart() <= set.ixFirstKey() && set.ixLastKey() <= rangeEnd()) {
            return ixCowRef();
        }
        if (set instanceof SingleRange) {
            return ixInsertRange(set.ixFirstKey(), set.ixLastKey());
        }
        final RspBitmap rspSet;
        if (set instanceof SortedRanges) {
            final SortedRanges sr = (SortedRanges) set;
            final SortedRanges ans = sr.deepCopy().addRange(rangeStart(), rangeEnd());
            if (ans != null) {
                return ans;
            }
            rspSet = sr.toRsp();
        } else {
            rspSet = (RspBitmap) set;
        }
        RspBitmap ans = rspSet.deepCopy();
        ans.addRangeUnsafeNoWriteCheck(rangeStart(), rangeEnd());
        ans.finishMutations();
        return ans;
    }

    @Override
    public final OrderedLongSet ixShiftOnNew(final long shiftAmount) {
        return make(rangeStart() + shiftAmount, rangeEnd() + shiftAmount);
    }

    @Override
    public final OrderedLongSet ixShiftInPlace(final long shiftAmount) {
        return ixShiftOnNew(shiftAmount);
    }

    @Override
    public final OrderedLongSet ixInsert(final OrderedLongSet added) {
        if (added.ixIsEmpty() ||
                (rangeStart() <= added.ixFirstKey() && added.ixLastKey() <= rangeEnd())) {
            return this;
        }
        if (added instanceof SingleRange) {
            return ixInsertRange(added.ixFirstKey(), added.ixLastKey());
        }
        final OrderedLongSet ix = added.ixCowRef();
        return ix.ixInsertRange(rangeStart(), rangeEnd());
    }

    @Override
    public final OrderedLongSet ixInsertWithShift(final long shiftAmount, final OrderedLongSet other) {
        if (other.ixIsEmpty()) {
            return this;
        }
        final long ansFirst = other.ixFirstKey() + shiftAmount;
        final long ansLast = other.ixLastKey() + shiftAmount;
        if (rangeStart() <= ansFirst && ansLast <= rangeEnd()) {
            return this;
        }
        if (other instanceof SingleRange) {
            return ixInsertRange(ansFirst, ansLast);
        }
        return other.ixShiftOnNew(shiftAmount).ixInsertRange(rangeStart(), rangeEnd());
    }

    @Override
    public final RowSequence ixGetRowSequenceByPosition(final long startPositionInclusive, final long length) {
        if (startPositionInclusive >= ixCardinality() || length == 0) {
            return RowSequenceFactory.EMPTY;
        }
        final long s = rangeStart() + startPositionInclusive;
        final long e = Math.min(s + length - 1, rangeEnd());
        return new SingleRangeRowSequence(s, e);
    }

    @Override
    public final RowSequence ixGetRowSequenceByKeyRange(final long startKeyInclusive, final long endKeyInclusive) {
        if (startKeyInclusive > rangeEnd() ||
                endKeyInclusive < rangeStart() ||
                endKeyInclusive < startKeyInclusive) {
            return RowSequenceFactory.EMPTY;
        }
        return new SingleRangeRowSequence(
                Math.max(startKeyInclusive, rangeStart()),
                Math.min(endKeyInclusive, rangeEnd()));
    }

    @Override
    public final RowSequence.Iterator ixGetRowSequenceIterator() {
        return new SingleRangeRowSequence.Iterator(rangeStart(), rangeEnd());
    }

    @Override
    public final long ixRangesCountUpperBound() {
        return 1;
    }

    @Override
    public final long ixGetAverageRunLengthEstimate() {
        return ixCardinality();
    }

    @Override
    public final OrderedLongSet ixInvertOnNew(final OrderedLongSet keys, final long maximumPosition) {
        final BuilderSequential b = new OrderedLongSetBuilderSequential();
        final RowSet.RangeIterator it = keys.ixRangeIterator();
        final String exStr = "invert for non-existing key:";
        while (it.hasNext()) {
            it.next();
            final long start = it.currentRangeStart();
            final long end = it.currentRangeEnd();
            final long startPos = start - rangeStart();
            if (startPos < 0) {
                throw new IllegalArgumentException(exStr + start);
            }
            if (startPos > maximumPosition) {
                break;
            }
            long endPos = startPos;
            if (start != end) {
                endPos = end - rangeStart();
                if (endPos < 0) {
                    throw new IllegalArgumentException(exStr + end);
                }
            }
            if (endPos > maximumPosition) {
                b.appendRange(startPos, maximumPosition);
                break;
            }
            b.appendRange(startPos, endPos);
        }
        return b.getTreeIndexImpl();
    }

    public final RspBitmap toRsp() {
        return new RspBitmap(rangeStart(), rangeEnd());
    }

    public final SortedRanges toSortedRanges() {
        return SortedRanges.makeSingleRange(rangeStart(), rangeEnd());
    }

    @Override
    public final RspBitmap ixToRspOnNew() {
        return toRsp();
    }

    @Override
    public final SingleRange ixCompact() {
        return this;
    }

    @Override
    public final void ixValidate(final String failMsg) {
        final boolean b = rangeStart() >= 0 && rangeEnd() >= rangeStart();
        if (!b) {
            final String m = failMsg == null ? "" : failMsg + " ";
            Assert.geqZero(rangeStart(), m + "rangeStart");
            Assert.geq(rangeEnd(), m + "rangeEnd", rangeStart(), "rangeStart");
        }
    }
}
