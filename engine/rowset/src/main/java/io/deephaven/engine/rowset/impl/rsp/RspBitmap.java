//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.OrderedLongSetBuilderSequential;
import io.deephaven.engine.rowset.impl.RowSetUtils;
import io.deephaven.engine.rowset.impl.rsp.container.*;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeConsumer;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * See header comment on RspArray for explanation on space partitioning.
 */
public class RspBitmap extends RspArray<RspBitmap> implements OrderedLongSet {
    public RspBitmap() {
        super();
    }

    // Create a bitmap with a single initial range.
    public RspBitmap(final long start, final long end) {
        super(start, end);
    }

    private RspBitmap(final RspBitmap other) {
        super(other);
    }

    public RspBitmap(
            final RspArray src,
            final int startIdx, final long startOffset,
            final int endIdx, final long endOffset) {
        super(src, startIdx, startOffset, endIdx, endOffset);
    }

    public RspBitmap(final RspArray src, final int startIdx, final int endIdx) {
        super(src, startIdx, endIdx);
    }

    public static RspBitmap makeEmpty() {
        return new RspBitmap();
    }

    public static RspBitmap makeSingleRange(final long start, final long end) {
        return new RspBitmap(start, end);
    }

    public static RspBitmap makeSingle(final long v) {
        return makeSingleRange(v, v);
    }

    @Override
    protected final RspBitmap make(final RspArray src,
            final int startIdx, final long startOffset,
            final int endIdx, final long endOffset) {
        return new RspBitmap(src, startIdx, startOffset, endIdx, endOffset);
    }

    @Override
    protected final RspBitmap make() {
        return new RspBitmap();
    }

    // RefCounted
    @Override
    protected RspBitmap self() {
        return this;
    }

    @Override
    public RspBitmap deepCopy() {
        return new RspBitmap(this);
    }

    public RspBitmap writeCheck() {
        return getWriteRef();
    }

    @VisibleForTesting
    RspArray getKvs() {
        return this;
    }

    private static short lowBitsAsShort(final long val) {
        return (short) (val & BLOCK_LAST);
    }

    @VisibleForTesting
    RspBitmap addValues(final long... values) {
        RspBitmap rb = this;
        for (long value : values) {
            rb = rb.add(value);
        }
        return rb;
    }

    public long first() {
        return firstValue();
    }

    public long last() {
        return lastValue();
    }

    private final static class AddCtx {
        long key;
        int index;
        Container c; // The RB Container, or null if key corresponds to a full block span or single key.
    }

    static Container containerForTwoValues(final long v1, final long v2) {
        if (v1 == v2) {
            return null;
        }
        if (v1 < v2) {
            return Container.twoValues(lowBitsAsShort(v1), lowBitsAsShort(v2));
        }
        return Container.twoValues(lowBitsAsShort(v2), lowBitsAsShort(v1));
    }

    public RspBitmap addValuesUnsafe(final LongChunk<OrderedRowKeys> values, final int offset, final int length) {
        final RspBitmap rb = writeCheck();
        rb.addValuesUnsafeNoWriteCheck(values, offset, length);
        return rb;
    }

    /**
     * Add {@code length} values from {@code values}, starting at {@code offset}, to this bitmap.
     *
     * <p>
     * Blocks we do not have yet are collected and inserted in one pass, rather than shifting our tail once per block.
     * Spans marked for removal along the way are recorded by index, so the two are reconciled together at the end, and
     * at any point in between where our arrays have to be settled.
     */
    public void addValuesUnsafeNoWriteCheck(final LongChunk<OrderedRowKeys> values, final int offset,
            final int length) {
        int lengthFromThisSpan;
        final WorkData wd = workDataPerThread.get();
        final MutableObject<SortedRanges> sortedRangesMu = getWorkSortedRangesMutableObject(wd);
        final PendingSpanInserts pending = wd.getPendingSpanInserts();
        // Key of the last full block span left pending, or -1. Two full block spans for adjacent blocks have to be a
        // single span, and a pending one is not in the array yet for fullBlockSpanNeedsNoMerge to notice.
        long pendingFullBlockKey = -1;
        int spanIndex = 0;
        try (SpanView ourView = wd.borrowSpanView()) {
            for (int vi = 0; vi < length; vi += lengthFromThisSpan) {
                final long value = values.get(vi + offset);
                final long highBits = highBits(value);
                lengthFromThisSpan = countContiguousHighBitsMatches(
                        values, vi + offset + 1, length - vi - 1, highBits) + 1;
                final int spanIndexRaw = getSpanIndex(spanIndex, highBits);
                Container container = null;
                boolean existing = false;
                if (spanIndexRaw < 0) {
                    spanIndex = ~spanIndexRaw;
                } else {
                    spanIndex = spanIndexRaw;
                    final Object existingSpan = spans[spanIndex];
                    final long existingSpanInfo = spanInfos[spanIndex];
                    if (getFullBlockSpanLen(existingSpanInfo, existingSpan) >= 1) {
                        continue;
                    }
                    ourView.init(this, spanIndex, existingSpanInfo, existingSpan);
                    container = ourView.getContainer();
                    existing = true;
                }
                final Container result = createOrUpdateContainerForValues(
                        values, vi + offset, lengthFromThisSpan, existing, spanIndex, container);
                if (result != null && result.isAllOnes()) {
                    final boolean adjacentToPendingFullBlockSpan =
                            pendingFullBlockKey != -1 && highBits - pendingFullBlockKey == BLOCK_SIZE;
                    if (!adjacentToPendingFullBlockSpan && existing
                            && fullBlockSpanNeedsNoMerge(spanIndex - 1, spanIndex + 1, highBits, 1)) {
                        // Nothing moves, so whatever is pending stays valid.
                        setFullBlockSpan(spanIndex, highBits, 1);
                    } else if (!adjacentToPendingFullBlockSpan && !existing
                            && fullBlockSpanNeedsNoMerge(spanIndex - 1, spanIndex, highBits, 1)) {
                        pending.pushFullBlockSpan(spanIndex, highBits, 1);
                        pendingFullBlockKey = highBits;
                    } else {
                        // This one has to merge with, or absorb, spans of ours; that is what
                        // setOrInsertFullBlockSpanAtIndex is for, and it needs our arrays settled first.
                        final int idxForFull;
                        if (pending.size() == 0) {
                            idxForFull = spanIndexRaw;
                        } else {
                            // Our spans move here, so the position we searched out above no longer holds. Anything
                            // already marked for removal has to go at the same time, since those marks are recorded by
                            // index too.
                            applyPendingSpanEdits(pending, sortedRangesMu);
                            sortedRangesMu.setValue(wd.getMadeNullSortedRanges());
                            pendingFullBlockKey = -1;
                            idxForFull = getSpanIndex(0, highBits);
                        }
                        spanIndex = setOrInsertFullBlockSpanAtIndex(idxForFull, highBits, 1, sortedRangesMu);
                    }
                } else if (!existing) {
                    if (result == null) {
                        pending.pushSingleton(spanIndex, value);
                    } else {
                        pending.pushContainer(spanIndex, highBits, result);
                    }
                } else {
                    setContainerSpan(container, spanIndex, highBits, result);
                }
            }
        }
        applyPendingSpanEdits(pending, sortedRangesMu);
    }

    private static int countContiguousHighBitsMatches(final LongChunk<OrderedRowKeys> values,
            final int offset, final int length,
            final long highBits) {
        for (int vi = 0; vi < length; ++vi) {
            if (highBits(values.get(vi + offset)) != highBits) {
                return vi;
            }
        }
        return length;
    }

    private Container createOrUpdateContainerForValues(@NotNull final LongChunk<OrderedRowKeys> values,
            final int offset, final int length,
            final boolean existing,
            final int keyIdx,
            Container container) {
        final long firstValue = values.get(offset);
        if (length == 1) {
            // We're adding only one value
            if (!existing) {
                return null;
            }
            if (container == null) {
                final long singletonValue = getSingletonSpanValue(keyIdx);
                if (firstValue == singletonValue) {
                    return null;
                }
                final long left, right;
                if (firstValue < singletonValue) {
                    left = firstValue;
                    right = singletonValue;
                } else {
                    left = singletonValue;
                    right = firstValue;
                }
                if (left + 1 == right) {
                    final int start = lowBitsAsInt(left);
                    final int end = lowBitsAsInt(right);
                    return new SingleRangeContainer(start, end + 1);
                }
                final short leftLow = lowBitsAsShort(left);
                final short rightLow = lowBitsAsShort(right);
                return new TwoValuesContainer(leftLow, rightLow);
            }
            final short firstValueLowBits = lowBitsAsShort(firstValue);
            return container.iset(firstValueLowBits);
        }
        final long lastValue = values.get(offset + length - 1);
        if (lastValue - firstValue + 1 == length) {
            // We know we're adding a contiguous range of values
            if (!existing) {
                return Container.singleRange(lowBitsAsInt(firstValue), lowBitsAsInt(lastValue) + 1);
            }
            if (container == null) {
                return new RunContainer(lowBitsAsInt(firstValue), lowBitsAsInt(lastValue) + 1)
                        .iset(lowBitsAsShort(getSingletonSpanValue(keyIdx)));
            }
            return container.iadd(lowBitsAsInt(firstValue), lowBitsAsInt(lastValue) + 1);
        }
        if (length == 2) {
            // We know we're adding exactly two items, with no contiguous range
            if (!existing) {
                return Container.twoValues(lowBitsAsShort(firstValue), lowBitsAsShort(lastValue));
            }
            if (container == null) {
                return new ArrayContainer(3)
                        .iset(lowBitsAsShort(firstValue))
                        .iset(lowBitsAsShort(lastValue))
                        .iset(lowBitsAsShort(spanInfos[keyIdx]));
            }
            return container.iset(lowBitsAsShort(firstValue)).iset(lowBitsAsShort(lastValue));
        }
        // We're adding more than two non-contiguous values
        if (!existing) {
            return makeValuesContainer(values, offset, length).runOptimize();
        }
        if (container == null) {
            container = Container.singleton(lowBitsAsShort(spanInfos[keyIdx]));
        }
        return addValuesToContainer(values, offset, length, container);
    }

    private static Container makeValuesContainer(final LongChunk<OrderedRowKeys> values,
            final int offset, final int length) {
        if (length <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            final short[] valuesArray = new short[length];
            for (int vi = 0; vi < length; ++vi) {
                valuesArray[vi] = lowBitsAsShort(values.get(vi + offset));
            }
            return new ArrayContainer(valuesArray);
        }
        final BitmapContainer bitmapContainer = new BitmapContainer();
        for (int vi = 0; vi < length; ++vi) {
            bitmapContainer.iset(lowBitsAsShort(values.get(vi + offset)));
        }
        return bitmapContainer;
    }

    private static Container addValuesToContainer(final LongChunk<OrderedRowKeys> values,
            final int offset, final int length,
            Container container) {
        if (container.getCardinality() <= length / 2) {
            return makeValuesContainer(values, offset, length).ior(container);
        }
        for (int vi = 0; vi < length; ++vi) {
            container = container.iset(lowBitsAsShort(values.get(vi + offset)));
        }
        return container;
    }

    public RspBitmap add(final long val) {
        final RspBitmap rb = addUnsafe(val);
        rb.finishMutations();
        return rb;
    }

    // Does not update cardinality cache. Caller must ensure finishMutations() is called before calling
    // any operation depending on the cardinality cache being up to date.
    public RspBitmap addUnsafe(final long val) {
        final RspBitmap rb = writeCheck();
        rb.addUnsafeNoWriteCheck(val);
        return rb;
    }

    public void addUnsafeNoWriteCheck(final long val) {
        int index = getSpanIndex(val);
        if (index < 0) {
            insertSingletonAtIndex(~index, val);
            return;
        }
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, index)) {
            final long flen = view.getFullBlockSpanLen();
            if (flen > 0) {
                // if flen > 0 nothing to do, val is already there.
                return;
            }
            final Container result;
            Container container = null;
            if (view.isSingletonSpan()) {
                final long single = view.getSingletonSpanValue();
                result = containerForTwoValues(single, val);
                if (result == null) {
                    return;
                }
            } else {
                container = view.getContainer();
                result = container.iset(lowBitsAsShort(val));
            }
            final long key = view.getKey();
            if (result.isAllOnes()) {
                setOrInsertFullBlockSpanAtIndex(index, key, 1, null);
            } else {
                setContainerSpan(container, index, key, result);
            }
        }
    }

    // Prerequisite: keyForLastBlock <= sHigh
    // end is inclusive
    public void appendRangeUnsafeNoWriteCheck(final long sHigh, final long start, final long end) {
        appendRangeUnsafeNoWriteCheck(sHigh, start, highBits(end), end);
    }

    private void appendRangeUnsafeNoWriteCheck(final long sHigh, final long start, final long eHigh, final long end) {
        final int sLow = lowBitsAsInt(start);
        final int eLow = lowBitsAsInt(end);
        if (sHigh == eHigh) {
            singleBlockAppendRange(sHigh, start, sLow, eLow);
            return;
        }
        singleBlockAppendRange(sHigh, start, sLow, BLOCK_LAST);
        final long sHighNext = RspArray.nextKey(sHigh);
        if (sHighNext == eHigh) {
            if (eLow == BLOCK_LAST) {
                appendFullBlockSpan(sHighNext, 1);
            } else {
                if (eLow == 0) {
                    appendSingletonSpan(sHighNext);
                } else {
                    appendContainer(sHighNext, Container.rangeOfOnes(0, eLow + 1));
                }
            }
            return;
        }
        if (eLow < BLOCK_LAST) {
            appendFullBlockSpan(sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh));
            if (eLow == 0) {
                appendSingletonSpan(eHigh);
            } else {
                appendContainer(eHigh, Container.rangeOfOnes(0, eLow + 1));
            }
            return;
        }
        appendFullBlockSpan(sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh) + 1);
    }

    // end is inclusive.
    public RspBitmap appendRange(final long start, final long end) {
        final RspBitmap rb = appendRangeUnsafe(start, end);
        rb.finishMutations();
        return rb;
    }

    // end is inclusive.
    // Does not update cardinality cache. Caller must ensure finishMutations() is called before calling
    // any operation depending on the cardinality cache being up to date.
    public RspBitmap appendRangeUnsafe(final long start, final long end) {
        if (start > end) {
            throw new IllegalArgumentException("bad range start=" + start + " > end=" + end + ".");
        }
        final long sHigh = highBits(start);
        final RspBitmap rb = writeCheck();
        rb.appendRangeUnsafeNoWriteCheck(sHigh, start, end);
        return rb;
    }

    public void appendRangeUnsafeNoWriteCheck(final long start, final long end) {
        appendRangeUnsafeNoWriteCheck(highBits(start), start, end);
    }

    public void appendContainerUnsafeNoWriteCheck(final long k, final Container c) {
        if (c != null) {
            if (c.isAllOnes()) {
                appendFullBlockSpan(k, 1);
                return;
            }
            if (c.isSingleElement()) {
                final long value = k | c.first();
                appendSingletonSpan(value);
                return;
            }
        }
        appendContainer(k, c);
    }

    public void appendFullBlockSpanUnsafeNoWriteCheck(final long k, final long slen) {
        appendFullBlockSpan(k, slen);
    }

    public RspBitmap append(final long v) {
        final RspBitmap rb = appendUnsafe(v);
        rb.finishMutations();
        return rb;
    }

    // Does not update cardinality cache. Caller must ensure finishMutations() is called before
    // any operation depending on the cardinality cache being up to date are called.
    public RspBitmap appendUnsafe(final long v) {
        final RspBitmap rb = writeCheck();
        rb.appendUnsafeNoWriteCheck(v);
        return rb;
    }

    public void appendUnsafeNoWriteCheck(final long v) {
        final long sHigh = highBits(v);
        final short low = lowBits(v);
        long keyForLastBlock = 0;
        if (isEmpty() || (keyForLastBlock = keyForLastBlock()) < sHigh) {
            appendSingletonSpan(v);
            return;
        }
        if (keyForLastBlock != sHigh) {
            throw new IllegalArgumentException("Can't append v=" + v + " when keyForLastBlock=" + keyForLastBlock);
        }

        final int lastIndex = size - 1;
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, lastIndex)) {
            if (view.getFullBlockSpanLen() > 0) {
                // if it is a full block span we already have the value.
                return;
            }
            final Container result;
            Container container = null;
            if (view.isSingletonSpan()) {
                final long single = view.getSingletonSpanValue();
                if (single == v) {
                    return;
                }
                if (single < v) {
                    result = Container.twoValues(lowBitsAsShort(single), lowBitsAsShort(v));
                } else {
                    result = Container.twoValues(lowBitsAsShort(v), lowBitsAsShort(single));
                }
            } else {
                container = view.getContainer();
                result = container.iset(low);
            }
            if (result.isAllOnes()) {
                setLastFullBlockSpan(sHigh, 1);
                return;
            }
            setContainerSpan(container, lastIndex, sHigh, result);
        }
    }

    /**
     * Adds the provided (start, end) range, relative to the given key, to this array.
     *
     * @param startPos the initial index from which to start the search for k
     * @param startHighBits the high bits of the start position for the range provided.
     * @param start the start position for the range provided.
     * @param startLowBits the low bits of the start of the range to add. 0 <= start < BLOCK_SIZE
     * @param endLowBits the low bits of the end (inclusive) of the range to add. 0 <= end < BLOCK_SIZE
     * @param madeNullSpansMu when not null, spans absorbed by a full block span are marked for removal and recorded
     *        here instead of being compacted out immediately; the caller then compacts once for the whole batch
     * @return the index of the span where the interval was added.
     */
    private int singleBlockAddRange(final int startPos, final long startHighBits, final long start,
            final int startLowBits, final int endLowBits, final MutableObject<SortedRanges> madeNullSpansMu) {
        final int endExclusive = endLowBits + 1;
        final int i = getSpanIndex(startPos, start);
        if (endExclusive - startLowBits == BLOCK_SIZE) {
            return setOrInsertFullBlockSpanAtIndex(i, startHighBits, 1, madeNullSpansMu);
        }
        if (i < 0) {
            final int j = -i - 1;
            if (startLowBits == endLowBits) {
                insertSingletonAtIndex(j, start);
            } else {
                insertContainerAtIndex(j, startHighBits, Container.rangeOfOnes(startLowBits, endExclusive));
            }
            return j;
        }
        final Object span = spans[i];
        if (RspArray.isFullBlockSpan(span)) {
            return i;
        }
        Container container = null;
        SpanView view = null;
        final Container result;
        if (isSingletonSpan(span)) {
            final long single = getSingletonSpanValue(i);
            final int keyLowAsInt = lowBitsAsInt(single);
            if (startLowBits == endLowBits && startLowBits == keyLowAsInt) {
                return i;
            }
            if (keyLowAsInt + 1 < startLowBits) {
                result = new RunContainer(keyLowAsInt, keyLowAsInt + 1, startLowBits, endExclusive);
            } else if (keyLowAsInt + 1 == startLowBits) {
                if (endExclusive - keyLowAsInt == BLOCK_SIZE) {
                    return setOrInsertFullBlockSpanAtIndex(i, startHighBits, 1, madeNullSpansMu);
                }
                result = Container.singleRange(keyLowAsInt, endExclusive);
            } else if (endLowBits + 1 < keyLowAsInt) {
                result = new RunContainer(startLowBits, endExclusive, keyLowAsInt, keyLowAsInt + 1);
            } else if (endLowBits + 1 == keyLowAsInt) {
                if (keyLowAsInt + 1 - startLowBits == BLOCK_SIZE) {
                    return setOrInsertFullBlockSpanAtIndex(i, startHighBits, 1, madeNullSpansMu);
                }
                result = Container.singleRange(startLowBits, keyLowAsInt + 1);
            } else { // start <= key <= end
                result = Container.singleRange(startLowBits, endExclusive);
            }
        } else {
            view = workDataPerThread.get().borrowSpanView(this, i, spanInfos[i], span);
            container = view.getContainer();
            result = container.iadd(startLowBits, endExclusive);
            if (result.isAllOnes()) {
                view.close();
                return setOrInsertFullBlockSpanAtIndex(i, startHighBits, 1, madeNullSpansMu);
            }
        }
        try (SpanView ensureViewIsClosedIfNotNull = view) {
            setContainerSpan(container, i, startHighBits, result);
            return i;
        }
    }


    /**
     * Appends the provided (start, end) range, relative to the given key, to this array. Prerequisite:
     * keyForLastBlock() <= k
     *
     * @param k the key to use for the range provided.
     * @param start the start of the range to add. 0 <= start < BLOCK_SIZE
     * @param end the end (inclusive) of the range to add. 0 <= end < BLOCK_SIZE
     * @return the index of the span where the interval was added.
     */
    private int singleBlockAppendRange(final long kHigh, final long k, final int start, final int end) {
        final int endExclusive = end + 1;
        long keyForLastBlock = 0;
        if (isEmpty() || (keyForLastBlock = keyForLastBlock()) < kHigh) {
            final int pos = size();
            if (start == end) {
                appendSingletonSpan(k);
            } else {
                if (endExclusive - start == BLOCK_SIZE) {
                    final int insertIdx = -pos - 1;
                    return setOrInsertFullBlockSpanAtIndex(insertIdx, kHigh, 1, null);
                }
                appendContainer(kHigh, Container.rangeOfOnes(start, endExclusive));
            }
            return pos;
        }
        if (keyForLastBlock == kHigh) {
            final int pos = size() - 1;
            final Object span = spans[pos];
            if (!RspArray.isFullBlockSpan(span)) { // if it is a full block span, we already have the range.
                final Container result;
                Container container = null;
                try (SpanView view = workDataPerThread.get().borrowSpanView(this, pos, spanInfos[pos], span)) {
                    if (view.isSingletonSpan()) {
                        final long single = view.getSingletonSpanValue();
                        result = containerForLowValueAndRange(lowBitsAsInt(single), start, end);
                    } else {
                        container = view.getContainer();
                        result = container.iadd(start, endExclusive);
                    }
                    if (result != null && result.isAllOnes()) {
                        return setOrInsertFullBlockSpanAtIndex(pos, kHigh, 1, null);
                    }
                    setContainerSpan(container, pos, kHigh, result);
                }
            }
            return pos;
        }
        throw new IllegalArgumentException("Can't append range (k=" + k + ", start=" + start + ", end=" + end +
                ") when keyForLastBlock=" + keyForLastBlock);
    }

    public static Container containerForLowValueAndRange(final int val, final int start, final int end) {
        if (end == start) {
            return containerForTwoValues(val, start);
        }
        if (val + 1 < start) {
            return new RunContainer(val, val + 1, start, end + 1);
        }
        if (val + 1 == start) {
            return Container.singleRange(val, end + 1);
        }
        if (end + 1 < val) {
            return new RunContainer(start, end + 1, val, val + 1);
        }
        if (end + 1 == val) {
            return Container.singleRange(start, val + 1);
        }
        // start <= val <= end.
        return Container.singleRange(start, end + 1);
    }

    // Note end is exclusive; the range is open on the right.
    public RspBitmap addRangeExclusiveEnd(final long start, final long end) {
        return addRange(start, end - 1);
    }

    // end is inclusive
    public RspBitmap addRange(final long start, final long end) {
        final RspBitmap rb = addRangeUnsafe(start, end);
        rb.finishMutations();
        return rb;
    }

    // Figure out where to insert for k, starting from index i
    private int getSetOrInsertIdx(final int startIdx, final long keyToInsert) {
        final Object startIdxSpan = spans[startIdx];
        final long startIdxSpanInfo = spanInfos[startIdx];
        if (getFullBlockSpanLen(startIdxSpanInfo, startIdxSpan) > 1) {
            return startIdx;
        }
        final int i = startIdx + 1;
        if (i >= size() || getKey(i) > keyToInsert) {
            return -i - 1;
        }
        return i;
    }

    // end is inclusive
    // Does not update cardinality cache. Caller must ensure finishMutations() is called before
    // any operation depending on the cardinality cache being up to date are called.
    public RspBitmap addRangeUnsafe(final long start, final long end) {
        if (start > end) {
            throw new IllegalArgumentException("bad range start=" + start + " > end=" + end + ".");
        }
        final RspBitmap rb = writeCheck();
        rb.addRangeUnsafeNoWriteCheck(0, start, end);
        return rb;
    }

    public void addRangeUnsafeNoWriteCheck(final long first, final long last) {
        addRangeUnsafeNoWriteCheck(0, first, last);
    }

    public int addRangeUnsafeNoWriteCheck(final int fromIdx, final long start, final long end) {
        return addRangeUnsafeNoWriteCheck(fromIdx, start, end, null);
    }

    /**
     * Add a range, searching for its place from {@code fromIdx}. A range that covers whole blocks may merge with, or
     * absorb, spans we already hold; with {@code madeNullSpansMu} null the absorbed spans are compacted out at once,
     * which shifts every later span, so a caller adding many ranges passes a tracker and compacts once at the end via
     * {@link #collectRemovedIndicesIfAny}. Marked spans only ever sit before the index returned, so later searches that
     * start from it never see them.
     *
     * @return the index of the span holding the range's last key, from where the next (higher) range's search can start
     */
    private int addRangeUnsafeNoWriteCheck(final int fromIdx, final long start, final long end,
            final MutableObject<SortedRanges> madeNullSpansMu) {
        if (start > end) {
            throw new IllegalArgumentException("bad range start=" + start + " > end=" + end + ".");
        }
        final long sHigh = highBits(start);
        final boolean kvsIsEmpty = isEmpty();
        if (kvsIsEmpty || sHigh >= keyForLastBlock()) { // append case.
            appendRangeUnsafeNoWriteCheck(sHigh, start, end);
            return size - 1;
        }
        // not an append; need to lookup.
        final long eHigh = highBits(end);
        final int sLow = lowBitsAsInt(start);
        final int eLow = lowBitsAsInt(end);
        if (sHigh == eHigh) {
            return singleBlockAddRange(fromIdx, sHigh, start, sLow, eLow, madeNullSpansMu);
        }
        int i = singleBlockAddRange(fromIdx, sHigh, start, sLow, BLOCK_LAST, madeNullSpansMu);
        final long sHighNext = RspArray.nextKey(sHigh);
        final int idxForFull = getSetOrInsertIdx(i, sHighNext);
        if (sHighNext == eHigh) {
            if (eLow == BLOCK_LAST) {
                i = setOrInsertFullBlockSpanAtIndex(idxForFull, sHighNext, 1, madeNullSpansMu);
            } else {
                i = singleBlockAddRange(i, sHighNext, sHighNext, 0, eLow, madeNullSpansMu);
            }
            return i;
        }
        if (eLow < BLOCK_LAST) {
            final int j = setOrInsertFullBlockSpanAtIndex(
                    idxForFull, sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh), madeNullSpansMu);
            return singleBlockAddRange(j, eHigh, eHigh, 0, eLow, madeNullSpansMu);
        }
        return setOrInsertFullBlockSpanAtIndex(
                idxForFull, sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh) + 1, madeNullSpansMu);

    }

    public void addRangesUnsafeNoWriteCheck(final RowSet.RangeIterator rit) {
        addShiftedRangesUnsafeNoWriteCheck(0, rit);
    }

    /**
     * Add every range of {@code rit}, shifted by {@code shiftAmount}, compacting out the spans absorbed by full block
     * spans once at the end rather than once per range. Closes {@code rit}.
     */
    private void addShiftedRangesUnsafeNoWriteCheck(final long shiftAmount, final RowSet.RangeIterator rit) {
        final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject(workDataPerThread.get());
        try {
            int i = 0;
            while (rit.hasNext()) {
                rit.next();
                i = addRangeUnsafeNoWriteCheck(i, rit.currentRangeStart() + shiftAmount,
                        rit.currentRangeEnd() + shiftAmount, madeNullSpansMu);
            }
        } finally {
            rit.close();
        }
        collectRemovedIndicesIfAny(madeNullSpansMu);
    }

    public boolean contains(final long val) {
        final long key = highBits(val);
        final int i = getSpanIndex(key);
        if (i < 0) {
            return false;
        }
        final Object span = spans[i];
        if (RspArray.isFullBlockSpan(span)) {
            return true;
        }
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, i, spanInfos[i], span)) {
            if (view.isSingletonSpan()) {
                return view.getSingletonSpanValue() == val;
            }
            return view.getContainer().contains(lowBitsAsShort(val));
        }
    }

    public RspBitmap remove(final long val) {
        final RspBitmap rb = removeUnsafe(val);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap removeUnsafe(final long val) {
        final long key = highBits(val);
        final int i = getSpanIndex(key);
        if (i < 0) {
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.removeUnsafeNoWriteCheck(val, key, i);
        return rb;
    }

    public RspBitmap removeUnsafeNoWriteCheck(final long val) {
        final long key = highBits(val);
        final int i = getSpanIndex(key);
        if (i >= 0) {
            removeUnsafeNoWriteCheck(val, key, i);
        }
        return this;
    }

    public void removeUnsafeNoWriteCheck(final long val, final long blockKey, final int i) {
        final Object s = spans[i];
        final long spanInfo = spanInfos[i];
        final long flen = RspArray.getFullBlockSpanLen(spanInfo, s);
        if (flen == 0) {
            if (isSingletonSpan(s)) {
                final long single = spanInfoToSingletonSpanValue(spanInfo);
                if (val == single) {
                    removeSpanAtIndex(i);
                }
            } else {
                try (SpanView view = workDataPerThread.get().borrowSpanView(this, i, spanInfo, s)) {
                    final Container orig = view.getContainer();
                    final Container result = orig.iunset(lowBitsAsShort(val));
                    if (result.isSingleElement()) {
                        setSingletonSpan(i, blockKey | result.first());
                    } else if (result.isEmpty()) {
                        removeSpanAtIndex(i);
                    } else {
                        setContainerSpan(orig, i, blockKey, result);
                    }
                }
            }
            return;
        }
        // flen > 0.
        final long spanStartKey = spanInfoToKey(spanInfo);
        final long spanEndKey = spanStartKey + BLOCK_SIZE * flen; // exclusive
        final int low = lowBitsAsInt(val);
        final Container c;
        long singletonValue = 0;
        if (low == 0) {
            c = Container.rangeOfOnes(1, BLOCK_SIZE);
        } else if (low == BLOCK_LAST) {
            c = Container.rangeOfOnes(0, BLOCK_LAST);
        } else {
            final int preStart = 0;
            final int preEnd = low; // exclusive
            final int posStart = low + 1;
            final int posEnd = BLOCK_SIZE; // exclusive
            // Do the bigger subrange first, to avoid changing the container type unnecessarily.
            Container c2;
            if (posEnd - posStart > preEnd - preStart) {
                c2 = Container.rangeOfOnes(posStart, posEnd);
                c2 = c2.iadd(preStart, preEnd);
            } else {
                c2 = Container.rangeOfOnes(preStart, preEnd);
                c2 = c2.iadd(posStart, posEnd);
            }
            if (c2.isSingleElement()) {
                singletonValue = blockKey | c2.first();
                c = null;
            } else {
                c = c2;
            }
        }
        final long preflen = RspArray.distanceInBlocks(spanStartKey, blockKey);
        final long posSpanFirstKey = RspArray.nextKey(blockKey);
        final long posflen = RspArray.distanceInBlocks(posSpanFirstKey, spanEndKey);
        if (preflen > 0) {
            if (posflen > 0) {
                final ArraysBuf buf = workDataPerThread.get().getArraysBuf(3);
                buf.pushFullBlockSpan(spanStartKey, preflen);
                if (c == null) {
                    buf.pushSingletonSpan(singletonValue);
                } else {
                    buf.pushContainer(blockKey, c);
                }
                buf.pushFullBlockSpan(posSpanFirstKey, posflen);
                replaceSpanAtIndex(i, buf);
                return;
            }
            final ArraysBuf buf = workDataPerThread.get().getArraysBuf(2);
            buf.pushFullBlockSpan(spanStartKey, preflen);
            if (c == null) {
                buf.pushSingletonSpan(singletonValue);
            } else {
                buf.pushContainer(blockKey, c);
            }
            replaceSpanAtIndex(i, buf);
            return;
        }
        if (posflen > 0) {
            final ArraysBuf buf = workDataPerThread.get().getArraysBuf(2);
            if (c == null) {
                buf.pushSingletonSpan(singletonValue);
            } else {
                buf.pushContainer(blockKey, c);
            }
            buf.pushFullBlockSpan(posSpanFirstKey, posflen);
            replaceSpanAtIndex(i, buf);
            return;
        }
        if (c == null) {
            setSingletonSpan(i, singletonValue);
        } else {
            setContainerSpan(i, blockKey, c);
        }
    }

    // end is inclusive.
    public RspBitmap removeRange(final long start, final long end) {
        if (isEmpty() || last() < start || end < first()) {
            return this;
        }
        final RspBitmap rb = removeRangeUnsafe(start, end);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap removeRangeUnsafe(final long start, final long end) {
        final RspBitmap rb = writeCheck();
        rb.removeRangeUnsafeNoWriteCheck(start, end);
        return rb;
    }

    /**
     * Return the logical or of two RspArrays as a new RspArray. The arguments won't be modified.
     *
     * @param r1 an RspArray
     * @param r2 an RspArray
     * @return
     */
    private static RspBitmap orImpl(final RspBitmap r1, final RspBitmap r2) {
        final RspBitmap r;
        if (r1.size > r2.size) {
            r = r1.deepCopy();
            r.orEquals(r2);
        } else {
            r = r2.deepCopy();
            r.orEquals(r1);
        }
        return r;
    }

    /**
     * Return the logical or of two bitmaps as a new bitmap. This is equivalent to the union of the two bitmaps as sets.
     * The arguments won't be modified.
     *
     * @param b1 a bitmap
     * @param b2 a bitmap
     * @return b1 or b2 as a new bitmap.
     */
    public static RspBitmap or(final RspBitmap b1, final RspBitmap b2) {
        final RspBitmap rb = orImpl(b1, b2);
        rb.finishMutations();
        return rb;
    }

    /**
     * Add every element on other to this bitmap.
     */
    public RspBitmap orEquals(final RspBitmap other) {
        final RspBitmap rb = orEqualsUnsafe(other);
        rb.finishMutations();
        return rb;
    }

    /**
     * For every key on other, add (key + shiftAmount) to this bitmap.
     */
    public RspBitmap orEqualsShifted(final long shiftAmount, final RspBitmap other) {
        final RspBitmap rb = orEqualsShiftedUnsafe(shiftAmount, other);
        rb.finishMutations();
        return rb;
    }

    /**
     * Add every element on other to this bitmap. Does not update cardinality cache. Caller must ensure
     * finishMutations() is called before any operation depending on the cardinality cache being up to date are called.
     */
    public RspBitmap orEqualsUnsafe(final RspBitmap other) {
        return orEqualsShiftedUnsafe(0, other);
    }

    /**
     * For every key on other, add (key + shiftAmount) to this bitmap. Note shiftAmount is assumed to be a multiple of
     * BLOCK_SIZE. Does not update cardinality cache. Caller must ensure finishMutations() is called before any
     * operation depending on the cardinality cache being up to date are called.
     */
    public RspBitmap orEqualsShiftedUnsafe(final long shiftAmount, final RspBitmap other) {
        if (other.isEmpty()) {
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.orEqualsShiftedUnsafeNoWriteCheck(shiftAmount, other);
        return rb;
    }

    public void appendShiftedUnsafeNoWriteCheck(final long shiftAmount, final RspArray other) {
        if ((shiftAmount & BLOCK_LAST) == 0 &&
                tryAppendShiftedUnsafeNoWriteCheck(shiftAmount, other)) {
            return;
        }
        if (lastValue() >= other.firstValue() + shiftAmount) {
            throw new IllegalArgumentException(
                    "Cannot append rowSet with shiftAmount=" + shiftAmount + ", firstRowKey=" + other.firstValue() +
                            " when our lastValue=" + lastValue());

        }
        other.forEachLongRange((final long start, final long end) -> {
            appendRangeUnsafeNoWriteCheck(start + shiftAmount, end + shiftAmount);
            return true;
        });
    }

    /**
     * Return the logical and of r1 and r2 as a new RspArray.
     *
     * @param r1 an RspArray.
     * @param r2 an RspArray.
     * @return r1 and r2 as a new RspArray.
     */
    private static RspBitmap andImpl(final RspBitmap r1, final RspBitmap r2) {
        if (r1.isEmpty() || r2.isEmpty()) {
            return new RspBitmap();
        }
        if (r1.size < r2.size) {
            final RspBitmap r = r1.deepCopy();
            r.andEquals(r2);
            return r;
        }
        final RspBitmap r = r2.deepCopy();
        r.andEquals(r1);
        return r;
    }

    /**
     * Return the logical and of two bitmaps as a new bitmap. This is equivalent to the intersection of the two bitmaps
     * as sets.
     *
     * @param b1 a bitmap
     * @param b2 a bitmap
     * @return b1 and b2 as a new bitmap.
     */
    public static RspBitmap and(final RspBitmap b1, final RspBitmap b2) {
        final RspBitmap rb = andImpl(b1, b2);
        rb.finishMutations();
        return rb;
    }

    /**
     * Removes every element from this bitmap that is not in the other bitmap.
     */
    public RspBitmap andEquals(final RspBitmap other) {
        final RspBitmap rb = andEqualsUnsafe(other);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap andEqualsUnsafe(final RspBitmap other) {
        final RspBitmap rb = writeCheck();
        rb.andEqualsUnsafeNoWriteCheck(other);
        return rb;
    }

    /**
     * Return the logical result of r1 and not r2 as a new RspArray. The arguments won't be modified.
     *
     * @param r1 an RspArray
     * @param r2 an RspArray
     * @return r1 and not r2 as a new RspArray.
     */
    public static RspBitmap andNotImpl(final RspBitmap r1, final RspBitmap r2) {
        final int minLen = Math.min(r1.size, r2.size);
        // Detect if there is an "obvious" common prefix.
        int startIndex;
        for (startIndex = 0; startIndex < minLen; ++startIndex) {
            final long r1SpanInfo = r1.spanInfos[startIndex];
            final long r2SpanInfo = r2.spanInfos[startIndex];
            if (r1SpanInfo != r2SpanInfo) {
                // We do not detect the case where a full block span is encoded differently
                // (with a marker object in the spans array and the lower 16 bits of spanInfo in one case,
                // versus a Long object in the other).
                // We also wouldn't detect a singleton container that is encoded as null span object in one
                // case, with the lower 16 bits indicating the singleton value, and with an actual container
                // with a single element in the other.
                // Bottom line we need the exact same optimization applied to both RspBitmap arguments.
                break;
            }
            final Object r1Span = r1.spans[startIndex];
            final Object r2Span = r2.spans[startIndex];

            if (r1Span == r2Span) {
                // r1Span and r2Span are either:
                // (a) Both null, representing singleton spans, so our check for spanInfo equality was enough
                // to guarantee sameness
                // (b) The same object, representing a shared container or full block span (either marker or Long; if
                // marker our check for spanInfo equality was enough to guarantee sameness).
                continue;
            }
            // r1Span != r2Span
            if (r1Span instanceof Long && r2Span instanceof Long) {
                if (((Long) r1Span).longValue() != ((Long) r2Span).longValue()) {
                    break;
                }
            } else {
                // In the case of containers, we only detect same object being shared;
                // we do not try to compare contents of containers otherwise.
                break;
            }
        }
        final RspBitmap r;
        if (startIndex == 0) {
            r = r1.deepCopy();
        } else {
            if (startIndex == r1.size) {
                return makeEmpty();
            }
            r = new RspBitmap(r1, startIndex, r1.size - 1);
        }
        r.andNotEqualsUnsafeNoWriteCheck(r2);
        return r;
    }

    /**
     * Return the logical result of r1 and not r2 as a new bitmap. This is equivalent to removing every element in b2
     * from b1. The arguments won't be modified.
     *
     * @param b1 a bitmap
     * @param b2 a bitmap
     * @return b1 and not b2 as a new bitmap.
     */
    public static RspBitmap andNot(final RspBitmap b1, final RspBitmap b2) {
        final RspBitmap rb = andNotImpl(b1, b2);
        rb.finishMutations();
        return rb;
    }

    /**
     * Updates the bitmap by adding and removing the bitmaps given as parameter.
     *
     * @param added Elements to add. Assumed disjoint with removed.
     * @param removed Elements to remove. Assumed disjoint with added.
     */
    public RspBitmap update(final RspBitmap added, final RspBitmap removed) {
        final RspBitmap rb = updateUnsafe(added, removed);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap updateUnsafe(final RspBitmap added, final RspBitmap removed) {
        if (debug) {
            if (added.overlaps((removed))) {
                throw new IllegalArgumentException(("rowSet update: added overlaps with removed."));
            }
        }
        final RspBitmap rb = writeCheck();
        rb.updateUnsafeNoWriteCheck(added, removed);
        return rb;
    }

    public void updateUnsafeNoWriteCheck(final RspBitmap added, final RspBitmap removed) {
        andNotEqualsUnsafeNoWriteCheck(removed);
        orEqualsUnsafeNoWriteCheck(added);
    }

    public RspBitmap andNotEquals(final RspBitmap other) {
        final RspBitmap rb = andNotEqualsUnsafe(other);
        rb.finishMutations();
        return rb;
    }

    /**
     * Remove every element in other from this bitmap.
     *
     */
    public RspBitmap andNotEqualsUnsafe(final RspBitmap other) {
        if (other.isEmpty()) {
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.andNotEqualsUnsafeNoWriteCheck(other);
        return rb;
    }

    /**
     * Apply an offset to every value in this bitmap, mutating it.
     *
     * @param offset The offset to apply.
     */
    public RspBitmap applyOffset(final long offset) {
        return applyOffsetImpl(offset, this::self, this::writeCheck);
    }

    public RspBitmap applyOffsetNoWriteCheck(final long offset) {
        return applyOffsetImpl(offset, this::self, this::self);
    }

    /**
     * Apply an offset to every value in this bitmap, returning a new bitmap (original is not changed).
     *
     * @param offset The offset to apply.
     */
    public RspBitmap applyOffsetOnNew(final long offset) {
        return applyOffsetImpl(offset, this::cowRef, this::deepCopy);
    }

    public RspBitmap applyOffsetImpl(
            final long offset, final Supplier<RspBitmap> onZeroOffset, final Supplier<RspBitmap> onAlignedOffset) {
        if (offset == 0 || isEmpty()) {
            return onZeroOffset.get();
        }
        if (offset < 0) {
            final long first = firstValue();
            if (first + offset < 0) {
                throw new IllegalArgumentException("offset=" + offset + " when first=" + first);
            }
        } else {
            final long last = lastValue();
            if (last + offset < 0) {
                throw new IllegalArgumentException("offset=" + offset + " when last=" + last);
            }
        }
        if ((offset & BLOCK_LAST) == 0) {
            final RspBitmap ans = onAlignedOffset.get();
            ans.applyKeyOffset(offset);
            ans.ifDebugValidate();
            return ans;
        }
        final RspBitmap rb = new RspBitmap();
        try (final RspRangeIterator it = getRangeIterator()) {
            int i = 0;
            while (it.hasNext()) {
                it.next();
                final long s = it.start();
                final long e = it.end();
                i = rb.addRangeUnsafeNoWriteCheck(i, s + offset, e + offset);
            }
        }
        rb.finishMutations();
        return rb;
    }

    public RspBitmap subrangeByPos(final long firstPos, final long lastPos, final boolean returnNullIfEmptyResult) {
        final RspBitmap rb = subrangeByPosInternal(firstPos, lastPos);
        if (rb == null || rb.isEmpty()) {
            if (returnNullIfEmptyResult) {
                return null;
            }
            return new RspBitmap();
        }
        return rb;
    }

    // lastPos is inclusive
    public RspBitmap subrangeByPos(final long firstPos, final long lastPos) {
        return subrangeByPos(firstPos, lastPos, false);
    }

    public RspBitmap subrangeByValue(final long start, final long end, final boolean returnNullIfEmptyResult) {
        if (isEmpty()) {
            if (returnNullIfEmptyResult) {
                return null;
            }
            return cowRef();
        }
        if (start <= first() && last() <= end) {
            return cowRef();
        }
        final RspBitmap rb = subrangeByKeyInternal(start, end);
        rb.finishMutationsAndOptimize();
        if (rb.isEmpty() && returnNullIfEmptyResult) {
            return null;
        }

        return rb;
    }

    // end is inclusive.
    public RspBitmap subrangeByValue(final long start, final long end) {
        return subrangeByValue(start, end, false);
    }

    public void invert(final LongRangeConsumer builder, final RowSet.RangeIterator it, final long maxPos) {
        if (!it.hasNext()) {
            return;
        }
        int startIndex = 0;
        it.next();
        int knownIdx = 0;
        long knownBeforeCard = 0;
        try (SpanView view = workDataPerThread.get().borrowSpanView()) {
            SPANS_LOOP: while (true) {
                final long startHiBits = highBits(it.currentRangeStart());
                final int i = getSpanIndex(startIndex, startHiBits);
                if (i < 0) {
                    throw new IllegalArgumentException("invert for non-existing key:" + it.currentRangeStart());
                }
                final long prevCap;
                if (acc == null) {
                    prevCap = cardinalityBeforeNoAcc(i, knownIdx, knownBeforeCard);
                    knownIdx = i;
                    knownBeforeCard = prevCap;
                } else {
                    prevCap = cardinalityBeforeWithAcc(i);
                }
                if (prevCap - 1 >= maxPos) {
                    return;
                }
                final Object span = spans[i];
                final long spanInfo = spanInfos[i];
                final long flen = getFullBlockSpanLen(spanInfo, span);
                if (flen > 0) {
                    final long k = spanInfoToKey(spanInfo);
                    final long spanCard = flen * BLOCK_SIZE;
                    final long sLast = k + spanCard - 1;
                    while (true) {
                        final long startPos = prevCap + it.currentRangeStart() - k;
                        if (startPos > maxPos) {
                            return;
                        }
                        final long end = uMin(sLast, it.currentRangeEnd());
                        final long endPos = prevCap + end - k;
                        if (endPos > maxPos) {
                            builder.accept(startPos, maxPos);
                            return;
                        }
                        builder.accept(startPos, endPos);
                        if (uGreater(it.currentRangeEnd(), sLast)) {
                            // Only reached when something lies past sLast, so sLast is below the last key here.
                            it.postpone(sLast + 1);
                            startIndex = i + 1;
                            if (acc == null) {
                                knownIdx = startIndex;
                                knownBeforeCard += spanCard;
                            }
                            continue SPANS_LOOP;
                        }
                        if (!it.hasNext()) {
                            return;
                        }
                        it.next();
                        if (uGreater(it.currentRangeStart(), sLast)) {
                            startIndex = i + 1;
                            if (acc == null) {
                                knownIdx = startIndex;
                                knownBeforeCard += spanCard;
                            }
                            continue SPANS_LOOP;
                        }
                    }
                }
                final Container c;
                if (isSingletonSpan(span)) {
                    final long v = spanInfoToSingletonSpanValue(spanInfo);
                    c = Container.singleton(lowBitsAsShort(v));
                } else {
                    view.init(this, i, spanInfo, span);
                    c = view.getContainer();
                }
                final RangeConsumer rc = (final int rs, final int re) -> {
                    final long start = prevCap + rs;
                    final long end = prevCap + re;
                    builder.accept(start, end - 1);
                };
                final int rMaxPos = (int) uMin(maxPos - prevCap, BLOCK_SIZE);
                final IndexRangeIteratorView rv = new IndexRangeIteratorView(it, startHiBits, startHiBits + BLOCK_SIZE);
                final boolean maxReached = c.findRanges(rc, rv, rMaxPos);
                if (maxReached || rv.underlyingIterFinished()) {
                    return;
                }
                startIndex = i + 1;
                if (acc == null) {
                    knownIdx = startIndex;
                    knownBeforeCard += c.getCardinality();
                }
            }
        }
    }

    private static int long2hash(final long v) {
        return (int) (v ^ (v >>> 32));
    }

    // Simple minded hashCode and equals implementations, intended for testing.
    @Override
    public int hashCode() {
        int r = 17;
        if (!isEmpty()) {
            r = 31 * r + long2hash(getCardinality());
            r = 31 * r + long2hash(last());
        }
        return r;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof RspBitmap)) {
            return false;
        }
        final RspBitmap other = (RspBitmap) o;
        if (getCardinality() != other.getCardinality()) {
            return false;
        }
        // An iterator run to the end gives back the reference it holds by itself; one abandoned at the first
        // difference below has to be closed for that to happen.
        try (final RspRangeIterator it = getRangeIterator();
                final RspRangeIterator oit = other.getRangeIterator()) {
            while (it.hasNext()) {
                if (!oit.hasNext()) {
                    return false;
                }
                it.next();
                oit.next();
                if (it.start() != oit.start()) {
                    return false;
                }
                if (it.end() != oit.end()) {
                    return false;
                }
            }
            // no need to check for oit.hasNext() since we checked for cardinality already.
            return true;
        }
    }

    public void finishMutations() {
        ensureCardinalityCache();
    }

    public void finishMutationsAndOptimize() {
        ensureAccAndOptimize();
    }

    /*
     *
     * ============= OrderedLongSet =============
     *
     */

    @Override
    public RspBitmap ixCowRef() {
        return cowRef();
    }

    @Override
    public RspBitmap ixInsert(final long key) {
        return add(key);
    }

    @Override
    public void ixRelease() {
        release();
    }

    @VisibleForTesting
    @Override
    public int ixRefCount() {
        return refCount();
    }

    @Override
    public RspBitmap ixInsertRange(final long startKey, final long endKey) {
        return addRange(startKey, endKey);
    }

    @Override
    public final OrderedLongSet ixInsertSecondHalf(final LongChunk<OrderedRowKeys> values,
            final int offset, final int length) {
        final RspBitmap ans = addValuesUnsafe(values, offset, length);
        ans.finishMutations();
        return ans;
    }

    @Override
    public final OrderedLongSet ixRemoveSecondHalf(final LongChunk<OrderedRowKeys> values,
            final int offset, final int length) {
        return ixRemove(OrderedLongSet.fromChunk(values, offset, length, true));
    }

    @Override
    public RspBitmap ixAppendRange(final long startKey, final long endKey) {
        return appendRange(startKey, endKey);
    }

    @Override
    public RspBitmap ixRemove(final long key) {
        return remove(key);
    }

    @Override
    public long ixLastKey() {
        return isEmpty() ? RowSequence.NULL_ROW_KEY : last();
    }

    @Override
    public long ixFirstKey() {
        return isEmpty() ? RowSequence.NULL_ROW_KEY : first();
    }

    @Override
    public long ixGet(final long pos) {
        if (pos < 0) {
            return RowSequence.NULL_ROW_KEY;
        }
        return get(pos);
    }

    @Override
    public void ixGetKeysForPositions(final PrimitiveIterator.OfLong inputPositions, final LongConsumer outputKeys) {
        getKeysForPositions(inputPositions, outputKeys);
    }

    @Override
    public long ixFind(final long key) {
        return find(key);
    }

    @Override
    public long ixCardinality() {
        return getCardinality();
    }

    @Override
    public boolean ixIsEmpty() {
        return isEmpty();
    }

    @Override
    public OrderedLongSet ixInvertOnNew(final OrderedLongSet keys, final long maximumPosition) {
        if (keys.ixIsEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        if (keys instanceof SingleRange) {
            final long pos = ixFind(keys.ixFirstKey());
            if (pos < 0) {
                throw new IllegalArgumentException("invert for non-existing key:" + keys.ixFirstKey());
            }
            // The range is wholly present exactly when its last key sits the range's length past its first.
            final long lastPos = ixFind(keys.ixLastKey());
            if (lastPos != pos + keys.ixCardinality() - 1) {
                throw new IllegalArgumentException("invert for non-existing key:" + keys.ixLastKey());
            }
            if (pos > maximumPosition) {
                return OrderedLongSet.EMPTY;
            }
            return SingleRange.make(pos, Math.min(pos + keys.ixCardinality() - 1, maximumPosition));
        }
        try (final RowSet.RangeIterator rit = keys.ixRangeIterator()) {
            final BuilderSequential builder = new OrderedLongSetBuilderSequential();
            invert(builder, rit, maximumPosition);
            return builder.getOrderedLongSet();
        }
    }

    @Override
    public boolean ixForEachLong(final LongAbortableConsumer lc) {
        return forEachLong(lc);
    }

    @Override
    public boolean ixForEachLongRange(final LongRangeAbortableConsumer lc) {
        return forEachLongRange(lc);
    }

    // the range [startPos, endPosExclusive) is closed on the left and open on the right.
    @Override
    public OrderedLongSet ixSubindexByPosOnNew(final long startPos, final long endPosExclusive) {
        final long endPos = endPosExclusive - 1; // make inclusive.
        if (endPos < startPos || endPos < 0) {
            return OrderedLongSet.EMPTY;
        }
        long effectiveStartPos = Math.max(0, startPos);
        final RspBitmap result = subrangeByPos(effectiveStartPos, endPos, true);
        if (result == null) {
            return OrderedLongSet.EMPTY;
        }
        // subSetByPositionRange tends to create small indices, it pays off to check for compacting the result.
        final OrderedLongSet compacted = result.ixCompact();
        if (compacted != result) {
            result.ixRelease();
        }
        return compacted;
    }

    @Override
    public OrderedLongSet ixSubindexByKeyOnNew(long startKey, final long endKey) {
        if (endKey < startKey || endKey < 0) {
            return OrderedLongSet.EMPTY;
        }
        startKey = Math.max(0, startKey);
        final RspBitmap result = subrangeByValue(startKey, endKey, true);
        if (result == null) {
            return OrderedLongSet.EMPTY;
        }
        // subSetByKeyRange tends to create small indices, it pays off to check for compacting the result.
        final OrderedLongSet compacted = result.ixCompact();
        if (compacted != result) {
            result.ixRelease();
        }
        return compacted;
    }

    // API assumption: added and removed are disjoint.
    @Override
    public OrderedLongSet ixUpdate(final OrderedLongSet added, final OrderedLongSet removed) {
        if (added.ixIsEmpty()) {
            if (removed.ixIsEmpty()) {
                return this;
            }
            return ixRemove(removed);
        }
        if (removed.ixIsEmpty()) {
            return ixInsert(added);
        }
        return getWriteRef().ixUpdateNoWriteCheck(added, removed);
    }

    public OrderedLongSet ixUpdateNoWriteCheck(final OrderedLongSet added, final OrderedLongSet removed) {
        if (added instanceof SingleRange) {
            addRangeUnsafeNoWriteCheck(added.ixFirstKey(), added.ixLastKey());
            if (removed instanceof SingleRange) {
                removeRangeUnsafeNoWriteCheck(removed.ixFirstKey(), removed.ixLastKey());
            } else if (removed instanceof SortedRanges) {
                removeRangesUnsafeNoWriteCheck(removed.ixRangeIterator());
            } else {
                andNotEqualsUnsafeNoWriteCheck((RspBitmap) removed);
            }
        } else if (removed instanceof SingleRange) {
            removeRangeUnsafeNoWriteCheck(removed.ixFirstKey(), removed.ixLastKey());
            if (added instanceof SortedRanges) {
                insertOrderedLongSetUnsafeNoWriteCheck((SortedRanges) added);
            } else {
                orEqualsUnsafeNoWriteCheck((RspBitmap) added);
            }
        } else if (added instanceof RspBitmap && removed instanceof RspBitmap) {
            updateUnsafeNoWriteCheck((RspBitmap) added, (RspBitmap) removed);
        } else {
            final OrderedLongSet ans = ixRemoveNoWriteCheck(removed);
            return ans.ixInsert(added);
        }
        if (isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        finishMutations();
        return this;
    }


    @Override
    public RspBitmap ixInsert(final OrderedLongSet other) {
        if (other.ixIsEmpty()) {
            return this;
        }
        return getWriteRef().ixInsertNoWriteCheck(other);
    }

    public RspBitmap ixInsertNoWriteCheck(final OrderedLongSet other) {
        if (other instanceof SingleRange) {
            insertOrderedLongSetUnsafeNoWriteCheck((SingleRange) other);
        } else if (other instanceof SortedRanges) {
            insertOrderedLongSetUnsafeNoWriteCheck((SortedRanges) other);
        } else {
            insertOrderedLongSetUnsafeNoWriteCheck((RspBitmap) other);
        }
        finishMutations();
        return this;
    }

    public void insertOrderedLongSetUnsafeNoWriteCheck(final SingleRange ix) {
        addRangeUnsafeNoWriteCheck(0, ix.ixFirstKey(), ix.ixLastKey());
    }

    public void insertOrderedLongSetUnsafeNoWriteCheck(final SortedRanges sr) {
        makeRoomForPartiallyCoveredBlocks(0, sr);
        addRangesUnsafeNoWriteCheck(sr.getRangeIterator());
    }

    /**
     * Create, in a single pass, a span for each block that {@code sr} only covers part of and that we do not have a
     * span for yet. Adding the ranges afterwards then finds those blocks present and updates them in place, instead of
     * shifting the tail of our spans array once per range to make room -- which is quadratic when both sides are large.
     *
     * <p>
     * A range needs room made for up to three spans: the block it starts in and the block it ends in, when it only
     * covers part of them, and one full block span for the run of complete blocks in between. The partial ones become
     * singletons holding a key the insert is about to add anyway, so our invariants hold throughout. The run is only
     * taken when it can be placed as-is; a run that would have to merge with, or absorb, a span of ours is left to the
     * insert, which is what knows how to do that.
     *
     * @param shiftAmount added to every key in {@code sr} before it is inserted; not necessarily a multiple of the
     *        block size, so a range can land in a different block than the one it came from
     * @param sr the ranges about to be inserted
     */
    private void makeRoomForPartiallyCoveredBlocks(final long shiftAmount, final SortedRanges sr) {
        if (size == 0) {
            // Nothing to make room in; the insert takes its append path.
            return;
        }
        final WorkData wd = workDataPerThread.get();
        final PendingSpanInserts pending = wd.getPendingSpanInserts();
        final long ourLastBlockKey = keyForLastBlock();
        int hint = 0;
        long lastQueuedBlockKey = -1;
        try (final RowSet.RangeIterator it = sr.getRangeIterator()) {
            // Block keys only go up from here on: the ranges ascend and do not overlap, so each range's first block
            // is at or after the previous range's last block. That is what lets the loop stop for good below, rather
            // than walking the rest of the ranges to reject them one at a time.
            ranges: while (it.hasNext()) {
                it.next();
                final long start = it.currentRangeStart() + shiftAmount;
                final long end = it.currentRangeEnd() + shiftAmount;
                final long firstBlockKey = highBits(start);
                final long lastBlockKey = highBits(end);
                final boolean fullyCoversFirstBlock = lowBitsAsInt(start) == 0
                        && (firstBlockKey != lastBlockKey || lowBitsAsInt(end) == BLOCK_LAST);
                final boolean fullyCoversLastBlock = lowBitsAsInt(end) == BLOCK_LAST
                        && (firstBlockKey != lastBlockKey || lowBitsAsInt(start) == 0);
                // The run of blocks the range covers completely, which becomes one full block span. A single block
                // range is its own run when it covers that block fully; computing the run's end by stepping back a
                // block would underflow there.
                final boolean hasFullBlockSpan;
                final long firstFullBlockKey;
                final long lastFullBlockKey;
                if (firstBlockKey == lastBlockKey) {
                    hasFullBlockSpan = fullyCoversFirstBlock;
                    firstFullBlockKey = firstBlockKey;
                    lastFullBlockKey = lastBlockKey;
                } else {
                    firstFullBlockKey = fullyCoversFirstBlock ? firstBlockKey : nextKey(firstBlockKey);
                    lastFullBlockKey = fullyCoversLastBlock ? lastBlockKey : lastBlockKey - BLOCK_SIZE;
                    hasFullBlockSpan = uLessOrEqual(firstFullBlockKey, lastFullBlockKey);
                }
                // In ascending key order: the partial first block, the run of complete blocks, the partial last block.
                for (int blockSegment = 0; blockSegment < 3; ++blockSegment) {
                    final long blockKey;
                    final long keyInBlock;
                    final long flen;
                    if (blockSegment == 0) {
                        if (fullyCoversFirstBlock) {
                            continue;
                        }
                        blockKey = firstBlockKey;
                        keyInBlock = start;
                        flen = 0;
                    } else if (blockSegment == 1) {
                        if (!hasFullBlockSpan) {
                            continue;
                        }
                        blockKey = firstFullBlockKey;
                        keyInBlock = -1;
                        flen = distanceInBlocks(firstFullBlockKey, lastFullBlockKey) + 1;
                    } else {
                        if (firstBlockKey == lastBlockKey || fullyCoversLastBlock) {
                            continue;
                        }
                        blockKey = lastBlockKey;
                        keyInBlock = end;
                        flen = 0;
                    }
                    if (uGreater(blockKey, ourLastBlockKey)) {
                        // This block and every one after it is past our last, and the insert appends those with no
                        // shifting to make room for, so there is nothing left for this pass to do.
                        break ranges;
                    }
                    final int idx = getSpanIndex(hint, blockKey);
                    if (idx >= 0) {
                        hint = idx;
                        continue;
                    }
                    hint = ~idx;
                    if (flen > 0) {
                        // Two runs can never be queued for adjacent blocks -- that would need two adjacent ranges, and
                        // SortedRanges coalesces those -- so unlike the chunk insert, this has no queued run of its own
                        // to check against. What is left is a run that would merge with or absorb a span of ours; the
                        // insert would repair that by walking the same ranges, but we skip it anyway, to keep our
                        // arrays valid the whole way through and to avoid queueing a span only for the insert to take
                        // straight back out.
                        if (!fullBlockSpanNeedsNoMerge(hint - 1, hint, blockKey, flen)) {
                            continue;
                        }
                        pending.pushFullBlockSpan(hint, blockKey, flen);
                        continue;
                    }
                    if (blockKey == lastQueuedBlockKey) {
                        // Another range in the same block already has a span queued for it.
                        continue;
                    }
                    pending.pushSingleton(hint, keyInBlock);
                    lastQueuedBlockKey = blockKey;
                }
            }
        }
        applyPendingSpanInserts(pending);
    }

    public void insertOrderedLongSetUnsafeNoWriteCheck(final RspBitmap rb) {
        orEqualsUnsafeNoWriteCheck(rb);
    }

    @Override
    public OrderedLongSet ixRemove(final OrderedLongSet other) {
        if (isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        if (other.ixIsEmpty()) {
            return this;
        }
        return getWriteRef().ixRemoveNoWriteCheck(other);
    }

    public OrderedLongSet ixRemoveNoWriteCheck(final OrderedLongSet other) {
        if (other instanceof SingleRange) {
            removeRangeUnsafeNoWriteCheck(other.ixFirstKey(), other.ixLastKey());
        } else if (other instanceof SortedRanges) {
            removeRangesUnsafeNoWriteCheck(other.ixRangeIterator());
        } else {
            andNotEqualsUnsafeNoWriteCheck((RspBitmap) other);
        }
        if (isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        finishMutations();
        return this;
    }

    @Override
    public OrderedLongSet ixRetain(final OrderedLongSet other) {
        return retainImpl(other, this::getWriteRef);
    }

    public OrderedLongSet ixRetainNoWriteCheck(final OrderedLongSet other) {
        return retainImpl(other, () -> this);
    }

    private OrderedLongSet retainImpl(final OrderedLongSet other, Supplier<RspBitmap> refSupplier) {
        if (isEmpty() || other.ixIsEmpty() || last() < other.ixFirstKey() || other.ixLastKey() < first()) {
            return OrderedLongSet.EMPTY;
        }
        if (other instanceof SingleRange) {
            return refSupplier.get().ixRetainRange(other.ixFirstKey(), other.ixLastKey());
        }
        if (other instanceof SortedRanges) {
            final SortedRanges sr = (SortedRanges) other;
            final OrderedLongSet ans = sr.intersectOnNew(this);
            return (ans != null) ? ans : retainImpl(sr.toRsp(), refSupplier);
        }
        final RspBitmap o = (RspBitmap) other;
        return retainImpl(o, refSupplier);
    }

    private static OrderedLongSet retainImpl(final RspBitmap other, Supplier<RspBitmap> refSupplier) {
        final RspBitmap ans = refSupplier.get();
        ans.andEqualsUnsafeNoWriteCheck(other);
        if (ans.isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        ans.finishMutations();
        return ans;
    }

    @Override
    public OrderedLongSet ixRetainRange(final long start, final long end) {
        if (ixIsEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        final long myFirstKey = ixFirstKey();
        final long myLastKey = ixLastKey();
        if (myLastKey < start || end < myFirstKey) {
            return OrderedLongSet.EMPTY;
        }
        boolean mayHaveChanged = false;
        RspBitmap ans = this;
        if (end < myLastKey) {
            mayHaveChanged = true;
            ans = ans.removeRangeUnsafe(end + 1, myLastKey);
        }
        if (myFirstKey < start) {
            if (!mayHaveChanged) {
                mayHaveChanged = true;
                // start can't be 0 given the if condition above.
                ans = ans.removeRangeUnsafe(myFirstKey, start - 1);
            } else {
                ans.removeRangeUnsafeNoWriteCheck(myFirstKey, start - 1);
            }
        }
        if (mayHaveChanged) {
            if (ans.isEmpty()) {
                return OrderedLongSet.EMPTY;
            }
            ans.finishMutations();
            return ans;
        }
        return this;
    }

    public OrderedLongSet ixRetainRangeNoWriteCheck(final long start, final long end) {
        boolean mayHaveChanged = false;
        if (end < ixLastKey()) {
            mayHaveChanged = true;
            removeRangeUnsafeNoWriteCheck(end + 1, ixLastKey());
        }
        if (ixFirstKey() < start) {
            mayHaveChanged = true;
            // start can't be 0 given the if condition above.
            removeRangeUnsafeNoWriteCheck(ixFirstKey(), start - 1);
        }
        if (mayHaveChanged) {
            if (isEmpty()) {
                return OrderedLongSet.EMPTY;
            }
            finishMutations();
        }
        return this;
    }

    @Override
    public OrderedLongSet ixRemoveRange(final long startKey, final long endKey) {
        if (isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        final RspBitmap rb = removeRangeUnsafe(startKey, endKey);
        if (rb.isEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        rb.finishMutations();
        return rb;
    }

    @Override
    public OrderedLongSet ixIntersectOnNew(final OrderedLongSet other) {
        if (other.ixIsEmpty()) {
            return OrderedLongSet.EMPTY;
        }
        if (other instanceof SingleRange) {
            return ixSubindexByKeyOnNew(other.ixFirstKey(), other.ixLastKey());
        }
        if (other instanceof SortedRanges) {
            final SortedRanges sr = (SortedRanges) other;
            return sr.intersectOnNew(this);
        }
        return RspBitmap.and(this, (RspBitmap) other);
    }

    @Override
    public boolean ixContainsRange(final long start, final long end) {
        return containsRange(start, end);
    }

    @Override
    public boolean ixOverlaps(final OrderedLongSet other) {
        if (other.ixIsEmpty()) {
            return false;
        }
        if (other instanceof SingleRange) {
            return overlapsRange(other.ixFirstKey(), other.ixLastKey());
        }
        if (other instanceof SortedRanges) {
            final SortedRanges sr = (SortedRanges) other;
            return sr.overlaps(ixRangeIterator());
        }
        final RspBitmap o = (RspBitmap) other;
        return overlaps(o);
    }

    @Override
    public boolean ixOverlapsRange(final long start, final long end) {
        return overlapsRange(start, end);
    }

    public boolean subsetOf(final SortedRanges sr) {
        if (isEmpty()) {
            return true;
        }
        if (sr.isEmpty()) {
            return false;
        }
        // Take the complement sr, and see if we have any elements in it, which would make the return false.
        // If no element of us is in the complement of sr, return true.
        if (first() < sr.first() || sr.last() < last()) {
            return false;
        }
        long pendingLast = -1;
        // The walk stops as soon as one of our keys turns up in a gap, with the rest of sr's ranges unread; closing
        // the iterator is what returns the reference it holds on sr.
        try (final RowSet.RangeIterator it = sr.getRangeIterator()) {
            int i = 0;
            while (it.hasNext()) {
                it.next();
                final long start = it.currentRangeStart();
                if (pendingLast != -1) {
                    i = overlapsRange(i, pendingLast + 1, start - 1);
                    if (i >= 0) {
                        return false;
                    }
                    i = ~i;
                }
                pendingLast = it.currentRangeEnd();
            }
            return true;
        }
    }

    @Override
    public boolean ixSubsetOf(final OrderedLongSet other) {
        if (ixIsEmpty()) {
            return true;
        }
        if (other.ixIsEmpty()) {
            return false;
        }
        if (other instanceof SingleRange) {
            return other.ixFirstKey() <= ixFirstKey() && ixLastKey() <= other.ixLastKey();
        }
        if (other instanceof SortedRanges) {
            return subsetOf((SortedRanges) other);
        }
        return subsetOf((RspBitmap) other);
    }

    @Override
    public OrderedLongSet ixMinusOnNew(final OrderedLongSet other) {
        if (other.ixIsEmpty()) {
            return cowRef();
        }
        if (other instanceof SingleRange) {
            if (other.ixFirstKey() <= ixFirstKey() && ixLastKey() <= other.ixLastKey()) {
                return OrderedLongSet.EMPTY;
            }
            final RspBitmap ans = deepCopy();
            ans.removeRangeUnsafeNoWriteCheck(other.ixFirstKey(), other.ixLastKey());
            ans.finishMutations();
            return ans;
        }
        if (other instanceof SortedRanges) {
            final RspBitmap ans = deepCopy();
            final SortedRanges sr = (SortedRanges) other;
            ans.removeRangesUnsafeNoWriteCheck(sr.getRangeIterator());
            ans.finishMutations();
            return ans;
        }
        return RspBitmap.andNot(this, (RspBitmap) other);
    }

    @Override
    public OrderedLongSet ixUnionOnNew(final OrderedLongSet other) {
        if (isEmpty()) {
            return other.ixCowRef();
        }
        if (other.ixIsEmpty()) {
            return this.cowRef();
        }
        if (other instanceof SingleRange) {
            if (other.ixFirstKey() <= ixFirstKey() && ixLastKey() <= other.ixLastKey()) {
                return other.ixCowRef();
            }
            final RspBitmap b = deepCopy();
            b.addRangeUnsafeNoWriteCheck(0, other.ixFirstKey(), other.ixLastKey());
            b.finishMutations();
            return b;
        }
        if (other instanceof SortedRanges) {
            return other.ixUnionOnNew(this);
        }
        return RspBitmap.or(this, (RspBitmap) other);
    }

    @Override
    public RspBitmap ixShiftOnNew(final long shiftAmount) {
        return applyOffsetOnNew(shiftAmount);
    }

    @Override
    public RspBitmap ixShiftInPlace(final long shiftAmount) {
        return applyOffset(shiftAmount);
    }

    public OrderedLongSet ixInsertWithShift(final long shiftAmount, final SortedRanges sr) {
        final RspBitmap ans = getWriteRef();
        // Same reasoning as the unshifted insert: without this, every range starting a block we lack shifts the tail of
        // our spans array on its own, which is quadratic when both sides are large.
        ans.makeRoomForPartiallyCoveredBlocks(shiftAmount, sr);
        ans.addShiftedRangesUnsafeNoWriteCheck(shiftAmount, sr.getRangeIterator());
        ans.finishMutations();
        return ans;
    }

    @Override
    public OrderedLongSet ixInsertWithShift(final long shiftAmount, final OrderedLongSet other) {
        if (other.ixIsEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other.ixShiftOnNew(shiftAmount);
        }
        if (other instanceof SingleRange) {
            return addRange(other.ixFirstKey() + shiftAmount, other.ixLastKey() + shiftAmount);
        }
        if (other instanceof SortedRanges) {
            return ixInsertWithShift(shiftAmount, (SortedRanges) other);
        }
        if ((shiftAmount & BLOCK_LAST) != 0) {
            RspBitmap rspOther = (RspBitmap) other;
            rspOther = rspOther.applyOffsetOnNew(shiftAmount);
            final RspBitmap ans = getWriteRef();
            ans.insertOrderedLongSetUnsafeNoWriteCheck(rspOther);
            ans.finishMutations();
            return ans;
        }
        return orEqualsShifted(shiftAmount, (RspBitmap) other);
    }

    private static class SearchIteratorImpl implements RowSet.SearchIterator {
        private final RspRangeIterator it;
        private long curr = 0;
        // The first key of the current range not yet produced. It equals curr while curr itself is still to be
        // produced, and steps past the range's end once the range is done -- except at the top of the key space, where
        // stepping past Long.MAX_VALUE wraps to a value below curr. Hence the next >= curr guards below.
        private long next = 0;
        private long currRangeEnd = -1;

        public SearchIteratorImpl(final RspBitmap rb) {
            it = rb.getRangeIterator();
        }

        @Override
        public void close() {
            it.close();
        }

        @Override
        public boolean hasNext() {
            if (next >= curr && next <= currRangeEnd) {
                return true;
            }
            return it.hasNext();
        }

        @Override
        public long currentValue() {
            return curr;
        }

        @Override
        public long nextLong() {
            if (next >= curr && next <= currRangeEnd) {
                curr = next++;
            } else {
                it.next();
                curr = it.start();
                next = curr + 1;
                currRangeEnd = it.end();
            }
            return curr;
        }

        @Override
        public boolean advance(final long v) {
            if (currRangeEnd == -1) { // not-started-yet iterator
                if (!it.hasNext()) {
                    return false;
                }
                it.next();
                curr = it.start();
                next = curr + 1;
                currRangeEnd = it.end();
            }
            if (v <= currRangeEnd) {
                if (v > curr) {
                    curr = v;
                    next = curr + 1;
                }
                return true;
            }
            if (it.advance(v)) {
                if (v < it.start()) {
                    curr = it.start();
                } else {
                    curr = v;
                }
                currRangeEnd = it.end();
                next = curr + 1;
                return true;
            }
            // it.hasNext() == false since it.advance(v) returned false.
            next = 0;
            currRangeEnd = -1;
            return false;
        }

        @Override
        public long binarySearchValue(final RowSet.TargetComparator tc, final int dir) {
            if (currRangeEnd == -1) { // not-started-yet iterator
                if (!it.hasNext()) {
                    return -1;
                }
                it.next();
                curr = next = it.start();
                currRangeEnd = it.end();
            }
            final RowSetUtils.Comparator comp = (long k) -> tc.compareTargetTo(k, dir);
            int c = comp.directionToTargetFrom(curr);
            if (c < 0) {
                return -1;
            }
            it.search(comp);
            curr = it.start();
            next = curr + 1;
            currRangeEnd = it.end();
            return curr;
        }
    }

    @Override
    public RowSet.SearchIterator ixSearchIterator() {
        return new SearchIteratorImpl(this);
    }

    private static class IteratorImpl implements RowSet.Iterator {
        private final RspIterator it;

        public IteratorImpl(final RspBitmap rb) {
            it = rb.getIterator();
        }

        @Override
        public void close() {
            it.release();
        }

        @Override
        public boolean forEachLong(final LongAbortableConsumer lc) {
            return it.forEachLong(lc);
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public long nextLong() {
            return it.nextLong();
        }
    }

    @Override
    public RowSet.Iterator ixIterator() {
        return new IteratorImpl(this);
    }

    @Override
    public RowSet.SearchIterator ixReverseIterator() {
        return new RowSet.SearchIterator() {
            final RspReverseIterator it = getReverseIterator();

            @Override
            public void close() {
                it.release();
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public long currentValue() {
                return it.current();
            }

            @Override
            public long nextLong() {
                it.next();
                return it.current();
            }

            @Override
            public boolean advance(long v) {
                return it.advance(v);
            }

            @Override
            public long binarySearchValue(RowSet.TargetComparator targetComparator, int direction) {
                throw new UnsupportedOperationException("Reverse iterator does not support binary search.");
            }
        };
    }

    @Override
    public RowSet.RangeIterator ixRangeIterator() {
        return new RowSet.RangeIterator() {
            final RspRangeIterator it = getRangeIterator();

            @Override
            public void close() {
                it.close();
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public boolean advance(final long v) {
                return it.advance(v);
            }

            @Override
            public void postpone(final long v) {
                it.postpone(v);
            }

            @Override
            public long currentRangeStart() {
                return it.start();
            }

            @Override
            public long currentRangeEnd() {
                return it.end();
            }

            @Override
            public long next() {
                it.next();
                return it.start();
            }
        };
    }

    @Override
    public OrderedLongSet ixCompact() {
        final OrderedLongSet timpl = tryCompact();
        if (timpl != null) {
            return timpl;
        }
        return this;
    }

    @Override
    public void ixValidate(final String failMsg) {
        validate(failMsg);
    }

    @Override
    public RowSequence ixGetRowSequenceByPosition(final long startPositionInclusive, final long length) {
        return getRowSequenceByPosition(startPositionInclusive, length);
    }

    @Override
    public RowSequence ixGetRowSequenceByKeyRange(final long startKeyInclusive, final long endKeyInclusive) {
        return getRowSequenceByKeyRange(startKeyInclusive, endKeyInclusive);
    }

    @Override
    public RowSequence.Iterator ixGetRowSequenceIterator() {
        return getRowSequenceIterator();
    }

    @Override
    public long ixRangesCountUpperBound() {
        return rangesCountUpperBound();
    }

    @Override
    public long ixGetAverageRunLengthEstimate() {
        return getAverageRunLengthEstimate();
    }

    @Override
    public RspBitmap ixToRspOnNew() {
        return cowRef();
    }

    @Override
    public String toString() {
        return valuesToString();
    }

}
