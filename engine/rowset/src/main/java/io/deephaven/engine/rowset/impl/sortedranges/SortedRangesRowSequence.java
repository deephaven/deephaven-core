/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.TrackingWritableRowSetImpl;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.impl.RowSequenceAsChunkImpl;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;

import java.util.function.BiConsumer;

public class SortedRangesRowSequence extends RowSequenceAsChunkImpl {
    private static final boolean DEBUG = SortedRanges.DEBUG;
    private SortedRanges sar;
    private int startIdx; // end-of-range position index to the first key of this OK.
    private int endIdx; // end-of-range position index to the last key of this OK.
    private long startPos; // position offset of our start in the parent sorted array.
    private long startOffset; // offset into range pointed by startIdx; note this is <= 0.
    private long endOffset; // offset into range pointed by endIdx; note this is <= 0.
    private long size; // cardinality.

    SortedRangesRowSequence(final SortedRanges sar) {
        sar.acquire();
        this.sar = sar;
        if (sar.isEmpty()) {
            throw new IllegalArgumentException("sar=" + sar);
        }
        size = sar.getCardinality();
        endIdx = sar.count - 1;
        endOffset = 0;
        long data1;
        if (size == 1 || (data1 = sar.packedGet(1)) >= 0) {
            startIdx = 0;
            startOffset = 0;
            return;
        }
        final long data0 = sar.packedGet(0);
        startIdx = 1;
        startOffset = data0 + data1;
        ifDebugValidate();
    }

    SortedRangesRowSequence(
            final SortedRanges sar,
            final long startPos,
            final int startIdx, final long startOffset,
            final int endIdx, final long endOffset,
            final long size) {
        sar.acquire();
        this.startPos = startPos;
        this.sar = sar;
        this.startIdx = startIdx;
        this.startOffset = startOffset;
        this.endIdx = endIdx;
        this.endOffset = endOffset;
        this.size = size;
        ifDebugValidate();
    }

    private SortedRangesRowSequence(final SortedRanges sar, final long startPos) {
        this.sar = sar; // note no acquire.
        this.startPos = startPos;
    }

    @Override
    public void close() {
        closeSortedArrayRowSequence();
    }

    protected final void closeSortedArrayRowSequence() {
        if (sar == null) {
            return;
        }
        sar.release();
        sar = null;
        closeRowSequenceAsChunkImpl();
    }

    @Override
    public Iterator getRowSequenceIterator() {
        return new Iterator(this);
    }

    @Override
    public RowSequence getRowSequenceByPosition(final long pos, long length) {
        if (length <= 0 || pos >= size) {
            return RowSequenceFactory.EMPTY;
        }
        final int i;
        final long iPos;
        final long data = sar.packedGet(startIdx);
        if (data < 0) {
            final long startRangeEndValue = -data;
            i = startIdx - 1;
            final long startRangeStartValue = sar.packedGet(i);
            iPos = startPos - startOffset - (startRangeEndValue - startRangeStartValue);
        } else {
            i = startIdx;
            iPos = startPos;
        }
        if (length > size - pos) {
            length = size - pos;
        }
        return sar.getRowSequenceByPositionWithStart(iPos, i, startPos + pos, length);
    }

    @Override
    public RowSequence getRowSequenceByKeyRange(long startRowKeyInclusive, long endRowKeyInclusive) {
        if (size == 0) {
            return RowSequenceFactory.EMPTY;
        }
        final long lastKey = lastRowKey();
        final long firstKey = firstRowKey();
        startRowKeyInclusive = Math.max(startRowKeyInclusive, firstKey);
        endRowKeyInclusive = Math.min(endRowKeyInclusive, lastKey);
        if (endRowKeyInclusive < startRowKeyInclusive) {
            return RowSequenceFactory.EMPTY;
        }
        final int i;
        final long iPos;
        final long data = sar.packedGet(startIdx);
        if (data < 0) {
            final long startRangeEndValue = -data;
            i = startIdx - 1;
            final long startRangeStartValue = sar.packedGet(i);
            iPos = startPos + startRangeEndValue + startOffset - startRangeStartValue;
        } else {
            i = startIdx;
            iPos = 0;
        }
        return sar.getRowSequenceByKeyRangePackedWithStart(iPos, i, sar.pack(startRowKeyInclusive),
                sar.pack(endRowKeyInclusive));
    }

    @Override
    public RowSet asRowSet() {
        if (size == sar.getCardinality()) {
            return new TrackingWritableRowSetImpl(sar.deepCopy());
        }
        if (size <= 0) {
            return RowSetFactory.empty();
        }
        SortedRanges ans = sar.makeMyTypeAndOffset(Math.min(endIdx - startIdx + 2, sar.count));
        ans.cardinality = size;
        final long packedStartValue = sar.absPackedGet(startIdx);
        ans.packedSet(0, packedStartValue + startOffset);
        if (startIdx == endIdx) {
            if (endOffset > startOffset) {
                ans.packedSet(1, -(packedStartValue + endOffset));
                ans.count = 2;
            } else {
                ans.count = 1;
            }
        } else {
            int iSar = (startOffset < 0) ? startIdx : startIdx + 1;
            int iAns = 1;
            long lastData = 0;
            while (iSar < endIdx) {
                ans.packedSet(iAns++, lastData = sar.packedGet(iSar++));
            }
            final long endIdxData = sar.packedGet(endIdx);
            final boolean endIdxIsNeg = endIdxData < 0;
            if (endIdxIsNeg) {
                final long endValue = -endIdxData + endOffset;
                if (endValue > lastData) {
                    ans.packedSet(iAns++, -endValue);
                }
            } else {
                ans.packedSet(iAns++, endIdxData);
            }
            ans.count = iAns;
        }
        return new TrackingWritableRowSetImpl(ans);
    }

    @Override
    public void fillRowKeyChunk(final WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        chunkToFill.setSize(0);
        forEachRowKey((final long key) -> {
            chunkToFill.add(key);
            return true;
        });
    }

    @Override
    public void fillRowKeyRangesChunk(final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        chunkToFill.setSize(0);
        forEachRowKeyRange((final long start, final long end) -> {
            chunkToFill.add(start);
            chunkToFill.add(end);
            return true;
        });
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public long firstRowKey() {
        return sar.absUnpackedGet(startIdx) + startOffset;
    }

    @Override
    public long lastRowKey() {
        return sar.absUnpackedGet(endIdx) + endOffset;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long getAverageRunLengthEstimate() {
        return size / (endIdx - startIdx + 1);
    }

    @Override
    public boolean forEachRowKey(final LongAbortableConsumer lac) {
        if (size == 0) {
            return true;
        }
        int i = startIdx;
        final long startValue = sar.absUnpackedGet(i);
        if (startIdx == endIdx) {
            for (long offset = startOffset; offset <= endOffset; ++offset) {
                if (!lac.accept(startValue + offset)) {
                    return false;
                }
            }
            return true;
        }
        for (long offset = startOffset; offset <= 0; ++offset) {
            if (!lac.accept(startValue + offset)) {
                return false;
            }
        }
        ++i;
        long pendingStart = sar.unpackedGet(i);
        if (i == endIdx) {
            return lac.accept(pendingStart);
        }
        ++i;
        while (i < endIdx) {
            final long iData = sar.unpackedGet(i);
            final boolean iNeg = iData < 0;
            if (iNeg) {
                final long iValue = -iData;
                for (long v = pendingStart; v <= iValue; ++v) {
                    if (!lac.accept(v)) {
                        return false;
                    }
                }
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    if (!lac.accept(pendingStart)) {
                        return false;
                    }
                }
                pendingStart = iData;
            }
            ++i;
        }
        if (pendingStart != -1) {
            if (!lac.accept(pendingStart)) {
                return false;
            }
        }
        final long iData = sar.unpackedGet(endIdx);
        if (iData < 0) {
            final long iValue = -iData;
            for (long v = pendingStart + 1; v <= iValue + endOffset; ++v) {
                if (!lac.accept(v)) {
                    return false;
                }
            }
        } else {
            if (!lac.accept(iData)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachRowKeyRange(final LongRangeAbortableConsumer lrac) {
        if (size == 0) {
            return true;
        }
        int i = startIdx;
        final long startValue = sar.absUnpackedGet(i);
        if (startIdx == endIdx) {
            if (!lrac.accept(startValue + startOffset, startValue + endOffset)) {
                return false;
            }
            return true;
        }
        if (!lrac.accept(startValue + startOffset, startValue)) {
            return false;
        }
        ++i;
        long pendingStart = sar.unpackedGet(i);
        if (i == endIdx) {
            return lrac.accept(pendingStart, pendingStart);
        }
        ++i;
        while (i < endIdx) {
            final long iData = sar.unpackedGet(i);
            final boolean iNeg = iData < 0;
            if (iNeg) {
                if (!lrac.accept(pendingStart, -iData)) {
                    return false;
                }
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    if (!lrac.accept(pendingStart, pendingStart)) {
                        return false;
                    }
                }
                pendingStart = iData;
            }
            ++i;
        }
        final long iData = sar.unpackedGet(endIdx);
        if (iData < 0) {
            final long iValue = -iData;
            if (!lrac.accept(pendingStart, iValue + endOffset)) {
                return false;
            }
        } else {
            if (pendingStart != -1) {
                if (!lrac.accept(pendingStart, pendingStart)) {
                    return false;
                }
            }
            if (!lrac.accept(iData, iData)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long rangesCountUpperBound() {
        return endIdx - Math.max(startIdx - 1, 0) + 1;
    }

    private void reset(final long startPos, final int startIdx, final long startOffset,
            final int endIdx, final long endOffset, final long size) {
        if (sar != null) {
            closeRowSequenceAsChunkImpl();
        }
        this.startPos = startPos;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.size = size;
        ifDebugValidate();
    }

    static class Iterator implements RowSequence.Iterator {
        private static class RSWrapper extends SortedRangesRowSequence {
            RSWrapper(final SortedRanges sar, final long startPos) {
                super(sar, startPos);
            }

            @Override
            public void close() {
                if (SortedRanges.DEBUG) {
                    throw new IllegalStateException();
                }
                // We purposely /do not/ close the RspRowSequence part as it will get reused.
                // The API doc for Iterator states that clients should /never/ call close. So that we eneded up here
                // means
                // there is some kind of bug.
                closeRowSequenceAsChunkImpl();
            }
        }

        private SortedRanges sar;
        // Internal buffer used as a return value for next... methods. Note this object and the
        // position reflected by curr{Start,End}{Idx,Offset} may not be in sync if there
        // were any intermediate calls to advance.
        private final SortedRangesRowSequence currBuf;
        // The following four fields match the last values
        // returned for getNext* methods; when we have not returned any yet,
        // currStart* matches the start of the RowSequence from which we were constructed,
        // and currEndIdx == -1 to signal the iterator has not been started yet.
        private int currStartIdx;
        private int currEndIdx;
        private long currStartOffset;
        private long currEndOffset;

        private long sizeLeft; // number of keys left for subsequent calls to next* methods.
        private final int rsEndIdx;
        private final long rsEndOffset;

        // cached value for the first key on the call to any getNext* method, or -1 if cache has not been populated yet.
        private long nextKey;
        private long pendingAdvanceSize = 0;

        public Iterator(final SortedRangesRowSequence rs) {
            rs.sar.acquire();
            this.sar = rs.sar;
            currStartIdx = rs.startIdx;
            currStartOffset = rs.startOffset;
            rsEndIdx = rs.endIdx;
            rsEndOffset = rs.endOffset;
            currEndIdx = -1;
            currEndOffset = 1;
            currBuf = new RSWrapper(sar, rs.startPos);
            sizeLeft = rs.size;
            nextKey = -1;
        }

        @Override
        public void close() {
            currBuf.closeRowSequenceAsChunkImpl();
            if (sar == null) {
                return;
            }
            sar.release();
            sar = null;
        }

        @Override
        public boolean hasMore() {
            return sizeLeft > 0;
        }

        @Override
        public long peekNextKey() {
            if (!hasMore()) {
                return -1;
            }
            if (nextKey == -1) {
                final int pos;
                final long offset;
                if (currEndIdx == -1) {
                    pos = currStartIdx;
                    offset = currStartOffset;
                } else if (currEndOffset < 0) {
                    pos = currEndIdx;
                    offset = currEndOffset + 1;
                } else {
                    pos = currEndIdx + 1;
                    offset = 0;
                }
                nextKey = sar.absUnpackedGet(pos) + offset;
            }
            return nextKey;
        }

        // Updates curr{Start,End}{Idx,Offset} to a range starting on the position right after the end
        // at the time of the call, to the last position not greater than toKey.
        // Returns the size of the resulting OK.
        private long updateCurrThrough(final long toKey) {
            if (!hasMore()) {
                return 0;
            }
            final int savedStartIdx = currStartIdx;
            final long savedStartOffset = currStartOffset;
            if (currEndIdx != -1) {
                if (currEndOffset < 0) {
                    currStartIdx = currEndIdx;
                    currStartOffset = currEndOffset + 1;
                } else {
                    currStartIdx = currEndIdx + 1;
                    // currEndIdx + 1 < sar.count, otherwise we would have returned on the hasMore() check above.
                    if (SortedRanges.DEBUG) {
                        Assert.lt(currEndIdx + 1, "currEndIdx + 1",
                                sar.count, "sar.count");
                    }
                    if (currStartIdx + 1 < sar.count) {
                        final long nextData = sar.packedGet(currStartIdx + 1);
                        if (nextData < 0) {
                            final long currStartValue = sar.packedGet(currStartIdx);
                            ++currStartIdx;
                            currStartOffset = currStartValue + nextData; // < 0.
                        } else {
                            currStartOffset = 0;
                        }
                    } else {
                        currStartOffset = 0;
                    }
                }
            }
            long packedToKey = sar.pack(toKey);
            final long rangeEndKey = sar.absPackedGet(currStartIdx);
            final long startKey = rangeEndKey + currStartOffset;
            if (startKey > packedToKey) {
                currStartIdx = savedStartIdx;
                currStartOffset = savedStartOffset;
                return 0;
            }
            if (rangeEndKey >= packedToKey) {
                currEndIdx = currStartIdx;
                currEndOffset = packedToKey - rangeEndKey;
                final long sz = currEndOffset - currStartOffset + 1;
                if (sz > sizeLeft) {
                    currEndIdx = rsEndIdx;
                    currEndOffset = rsEndOffset;
                    return sizeLeft;
                }
                return sz;
            }
            long sz = 1 - currStartOffset;
            if (sz > sizeLeft) {
                currEndIdx = rsEndIdx;
                currEndOffset = rsEndOffset;
                return sizeLeft;
            }
            int i = currStartIdx + 1;
            long pendingStart = -1;
            while (i <= rsEndIdx) {
                long iData = sar.packedGet(i);
                boolean neg = iData < 0;
                if (neg) {
                    if (-iData >= packedToKey) {
                        currEndIdx = i;
                        currEndOffset = packedToKey + iData;
                        if (currEndIdx == rsEndIdx && currEndOffset > rsEndOffset) {
                            currEndOffset = rsEndOffset;
                            packedToKey = -iData + rsEndOffset;
                        }
                        sz += packedToKey - pendingStart;
                        if (sz > sizeLeft) {
                            currEndIdx = rsEndIdx;
                            currEndOffset = rsEndOffset;
                            return sizeLeft;
                        }
                        return sz;
                    }
                    sz += -iData - pendingStart;
                    pendingStart = -1;
                } else {
                    if (iData > packedToKey) {
                        currEndIdx = i - 1;
                        currEndOffset = 0;
                        if (sz > sizeLeft) {
                            currEndIdx = rsEndIdx;
                            currEndOffset = rsEndOffset;
                            return sizeLeft;
                        }
                        return sz;
                    }
                    ++sz;
                    if (iData == packedToKey) {
                        if (sz > sizeLeft) {
                            currEndIdx = rsEndIdx;
                            currEndOffset = rsEndOffset;
                            return sizeLeft;
                        }
                        long nextData;
                        if (i + 1 > rsEndIdx || (nextData = sar.packedGet(i + 1)) >= 0) {
                            currEndIdx = i;
                            currEndOffset = 0;
                            return sz;
                        }
                        // nextData < 0.
                        currEndIdx = i + 1;
                        currEndOffset = nextData + packedToKey;
                        return sz;
                    }
                    pendingStart = iData;
                }
                ++i;
            }
            if (sz > sizeLeft) {
                currEndIdx = rsEndIdx;
                currEndOffset = rsEndOffset;
                return sizeLeft;
            }
            currEndIdx = rsEndIdx;
            currEndOffset = 0;
            return sz;
        }

        @Override
        public RowSequence getNextRowSequenceThrough(final long maxKey) {
            if (maxKey < 0) {
                return RowSequenceFactory.EMPTY;
            }
            final long sz = updateCurrThrough(maxKey);
            if (sz == 0) {
                return RowSequenceFactory.EMPTY;
            }
            nextKey = -1;
            currBuf.reset(currBuf.startPos + currBuf.size + pendingAdvanceSize, currStartIdx, currStartOffset,
                    currEndIdx, currEndOffset, sz);
            pendingAdvanceSize = 0;
            sizeLeft -= sz;
            return currBuf;
        }

        private void nextRowSequenceWithLength(final long actualLen) {
            long accumLen = 0;
            int i;
            long pendingStart;
            long iData;
            boolean iNeg;
            if (currEndIdx != -1) {
                if (currEndOffset < 0) {
                    final long delta = actualLen + currEndOffset;
                    if (delta <= 0) {
                        currStartIdx = currEndIdx;
                        currStartOffset = currEndOffset + 1;
                        currEndIdx = currStartIdx;
                        currEndOffset = delta;
                        return;
                    }
                    currStartIdx = currEndIdx;
                    currStartOffset = currEndOffset + 1;
                    accumLen += -currEndOffset;
                    i = currEndIdx + 1;
                    pendingStart = -1;
                    iData = sar.packedGet(i);
                    iNeg = iData < 0;
                } else {
                    i = currEndIdx + 1;
                    if (i == rsEndIdx || i + 1 == sar.count) {
                        currStartIdx = currEndIdx = i;
                        currStartOffset = currEndOffset = 0;
                        return;
                    }
                    final long iNextData = sar.packedGet(i + 1);
                    if (iNextData < 0) {
                        final long iValue = sar.packedGet(i);
                        final long firstRangeLen = -iNextData - iValue + 1;
                        currStartIdx = i + 1;
                        currStartOffset = -firstRangeLen + 1;
                        if (actualLen <= firstRangeLen) {
                            currEndIdx = currStartIdx;
                            currEndOffset = actualLen - firstRangeLen;
                            return;
                        }
                        accumLen += firstRangeLen;
                        pendingStart = -1;
                        i += 2;
                        iData = sar.packedGet(i);
                        iNeg = iData < 0;
                    } else {
                        if (actualLen == 1) {
                            currStartIdx = currEndIdx = i;
                            currStartOffset = currEndOffset = 0;
                            return;
                        }
                        currStartIdx = i;
                        currStartOffset = 0;
                        if (i + 1 == rsEndIdx || i + 2 == sar.count) {
                            currEndIdx = i + 1;
                            currEndOffset = 0;
                            return;
                        }
                        accumLen += 1;
                        pendingStart = iNextData;
                        i += 1;
                        iData = iNextData;
                        iNeg = false;
                    }
                }
            } else {
                final long firstRangeLen = 1 - currStartOffset;
                if (firstRangeLen >= actualLen) {
                    currEndIdx = currStartIdx;
                    currEndOffset = currStartOffset + actualLen - 1;
                    return;
                }
                accumLen += 1 - currStartOffset;
                i = currStartIdx + 1;
                pendingStart = -1;
                iData = sar.packedGet(i);
                iNeg = iData < 0;
            }
            while (true) {
                if (iNeg) {
                    long delta = -iData - pendingStart;
                    if (accumLen + delta >= actualLen || i == rsEndIdx) {
                        delta = Math.min(delta, actualLen - accumLen);
                        currEndIdx = i;
                        currEndOffset = iData + pendingStart + delta;
                        return;
                    }
                    accumLen += delta;
                    pendingStart = -1;
                } else {
                    ++accumLen;
                    if (i == rsEndIdx) {
                        currEndIdx = i;
                        currEndOffset = 0;
                        return;
                    }
                    if (accumLen == actualLen) {
                        if (i + 1 < sar.count) {
                            final long nextData = sar.packedGet(i + 1);
                            if (nextData < 0) {
                                currEndIdx = i + 1;
                                currEndOffset = iData + nextData;
                                return;
                            }
                        }
                        currEndIdx = i;
                        currEndOffset = 0;
                        return;
                    }
                    pendingStart = iData;
                }
                ++i;
                iData = sar.packedGet(i);
                iNeg = iData < 0;
            }
        }

        @Override
        public RowSequence getNextRowSequenceWithLength(final long desiredLen) {
            final long actualLen = Math.min(desiredLen, sizeLeft);
            if (actualLen == 0) {
                return RowSequenceFactory.EMPTY;
            }
            nextRowSequenceWithLength(actualLen);
            nextKey = -1;
            currBuf.reset(currBuf.startPos + currBuf.size + pendingAdvanceSize, currStartIdx, currStartOffset,
                    currEndIdx, currEndOffset, actualLen);
            pendingAdvanceSize = 0;
            sizeLeft -= actualLen;
            return currBuf;
        }

        @Override
        public boolean advance(final long toKey) {
            if (sizeLeft == 0 ||
                    toKey <= 0 ||
                    (currEndIdx != -1 && toKey <= currBuf.lastRowKey())) {
                return sizeLeft > 0;
            }
            final long sz = updateCurrThrough(toKey - 1);
            nextKey = -1;
            pendingAdvanceSize = sz;
            sizeLeft -= sz;
            return sizeLeft > 0;
        }

        @Override
        public long getRelativePosition() {
            return -sizeLeft;
        }
    }

    private void ifDebugValidate() {
        if (DEBUG) {
            validate();
        }
    }

    private static void validateOffset(final String m, final int idx, final long offset, final SortedRanges sr) {
        final long v = sr.unpackedGet(idx);
        final BiConsumer<String, String> fail = (final String prefix, final String suffix) -> {
            throw new IllegalStateException(m +
                    ((prefix != null && prefix.length() > 0) ? (" " + prefix) : "") +
                    ": idx=" + idx + ", v=" + v + ", offset=" + offset +
                    ((suffix != null && suffix.length() > 0) ? (", " + suffix) : ""));
        };
        if (v >= 0) {
            if (offset != 0) {
                fail.accept("v >= 0 && offset !=0", null);
            }
            if (idx < sr.count - 1) {
                final long next = sr.unpackedGet(idx + 1);
                if (next < 0) {
                    fail.accept("v >=0 ", "next=" + next);
                }
            }
        } else {
            if (idx >= 1) {
                final long prev = sr.unpackedGet(idx - 1);
                if (prev < 0 || -v + offset < prev) {
                    fail.accept("v < 0", "prev=" + prev);
                }
            } else {
                fail.accept("v < 0 && idx <= 0", null);
            }
        }
    }

    public void validate() {
        if (startIdx < 0 || endIdx > sar.count) {
            throw new IllegalStateException("startIdx=" + startIdx + ", endIdx=" + endIdx);
        }
        validateOffset("start", startIdx, startOffset, sar);
        validateOffset("end", endIdx, endOffset, sar);
    }
}
