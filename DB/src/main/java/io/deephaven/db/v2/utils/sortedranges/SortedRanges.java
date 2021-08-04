package io.deephaven.db.v2.utils.sortedranges;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.*;
import io.deephaven.db.v2.utils.metrics.IntCounterMetric;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public abstract class SortedRanges extends RefCountedCow<SortedRanges> implements TreeIndexImpl {
    private static final IntCounterMetric sortedRangesToRspConversions =
            new IntCounterMetric("sortedRangesToRspConversions");

    public abstract SortedRanges deepCopy();

    public final SortedRanges self() {
        return this;
    }

    protected static final int INITIAL_SIZE = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "initialSize", 2);

    public static final boolean DEBUG = Configuration.getInstance().getBooleanForClassWithDefault(
            SortedRanges.class, "debug", false);

    public static final int LONG_DENSE_MAX_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "longDenseMaxCapacity", 256);

    public static final int LONG_SPARSE_MAX_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "longSparseCapacity", 4096);

    public static final int INT_DENSE_MAX_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "intDenseMaxCapacity", arraySizeRoundingInt(2*LONG_DENSE_MAX_CAPACITY));

    public static final int INT_SPARSE_MAX_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "intSparseMaxCapacity", arraySizeRoundingInt(2*LONG_SPARSE_MAX_CAPACITY));

    public static final int SHORT_MAX_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "shortMaxCapacity", 4096 - 6);  // 12 bytes of array object overhead = 6 shorts

    public static final int ELEMENTS_PER_BLOCK_DENSE_THRESHOLD = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "elementsPerBlockDenseThreshold", 16);

    public static final int MAX_CAPACITY = Math.max(INT_SPARSE_MAX_CAPACITY, SHORT_MAX_CAPACITY);
    static {
        Assert.assertion(ELEMENTS_PER_BLOCK_DENSE_THRESHOLD >= 2,
                "ELEMENTS_PER_BLOCK_DENSE_THRESHOLD >= 2");
        Assert.assertion(LONG_DENSE_MAX_CAPACITY <= LONG_SPARSE_MAX_CAPACITY,
                "LONG_DENSE_MAX_CAPACITY <= LONG_SPARSE_MAX_CAPACITY");
        Assert.assertion(INT_DENSE_MAX_CAPACITY <= INT_SPARSE_MAX_CAPACITY,
                "INT_DENSE_MAX_CAPACITY <= INT_SPARSE_MAX_CAPACITY");
        Assert.assertion(LONG_SPARSE_MAX_CAPACITY <= INT_SPARSE_MAX_CAPACITY,
                "LONG_SPARSE_MAX_CAPACITY <= INT_SPARSE_MAX_CAPACITY");
        Assert.assertion(LONG_DENSE_MAX_CAPACITY <= INT_DENSE_MAX_CAPACITY,
                "LONG_DENSE_MAX_CAPACITY <= INT_DENSE_MAX_CAPACITY");
    }

    // *_EXTENT properties must be a power of two.
    protected static final int LONG_EXTENT = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "longExtent", 64);

    protected static final int INT_EXTENT = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "intExtent", 2*LONG_EXTENT);

    protected static final int SHORT_EXTENT = Configuration.getInstance().getIntegerForClassWithDefault(
            SortedRanges.class, "shortExtent", 2*INT_EXTENT);

    protected static final boolean POOL_ARRAYS = Configuration.getInstance().getBooleanForClassWithDefault(
            SortedRanges.class, "poolArrays", false);

    public static final boolean USE_RANGES_ARRAY = Configuration.getInstance().getBooleanForClassWithDefault(
            SortedRanges.class, "useRangesArray", true);

    // Example:
    // Sizing of a short array object in a 64 bit JVM (Hotspot) uses
    // 12 bytes of object overhead (including array.length), plus
    // the space for the short elements payload, plus padding to round
    // objects to 8 bytes boundaries.
    // So an array of 4 elements uses: 12 + 2*4 + 4 padding = 24 bytes = 3*8 bytes.
    // The 4 bytes of padding are wasted and can be used for another 2 short elements.
    private static int arraySizeRounding(final int sizeToRound, final int elementSizeAsPowerOfTwo) {
        final int sz = 12 + (sizeToRound << elementSizeAsPowerOfTwo);
        final int szMod8 = sz & 7;
        final int padding = szMod8 > 0 ? 8 - szMod8 : 0;
        return sizeToRound + (padding >> elementSizeAsPowerOfTwo);
    }

    public static int arraySizeRoundingInt(final int sizeToRound) {
        return arraySizeRounding(sizeToRound, 2);
    }

    public static int arraySizeRoundingShort(final int sizeToRound) {
        return arraySizeRounding(sizeToRound, 1);
    }

    protected SortedRanges() {}

    public static SortedRanges makeSingleRange(final long start, final long end) {
        return SortedRangesLong.makeSingleRange(start, end);
    }

    public static SortedRanges makeSingleElement(final long v) {
        return makeSingleRange(v, v);
    }

    public static SortedRanges makeEmpty() {
        return new SortedRangesLong();
    }

    public static SortedRanges tryMakeForKnownRangeFinalCapacityLowerBound(
            final int initialCapacity, final int finalCapacityLowerBound, final long first, final long last, final boolean isDense) {
        final long range = last - first;
        final long offset = first;
        if (range <= Short.MAX_VALUE) {
            if (finalCapacityLowerBound > SHORT_MAX_CAPACITY) {
                return null;
            }
            return new SortedRangesShort(initialCapacity, offset);
        }
        final int intBound = isDense ? INT_DENSE_MAX_CAPACITY : INT_SPARSE_MAX_CAPACITY;
        if (finalCapacityLowerBound > intBound) {
            return null;
        }
        if (range <= Integer.MAX_VALUE) {
            return new SortedRangesInt(initialCapacity, offset);
        }
        final int longBound = isDense ? LONG_DENSE_MAX_CAPACITY : LONG_SPARSE_MAX_CAPACITY;
        if (finalCapacityLowerBound > longBound) {
            return null;
        }
        return new SortedRangesLong(initialCapacity);
    }

    // Implicitly this is unknown max capacity.
    public static SortedRanges makeForKnownRange(final long first, final long last, final boolean isDense) {
        return tryMakeForKnownRangeUnknownMaxCapacity(INITIAL_SIZE, first, last, isDense);
    }

    public static SortedRanges tryMakeForKnownRangeUnknownMaxCapacity(final int initialCapacity, final long first, final long last, final boolean isDense) {
        return tryMakeForKnownRangeFinalCapacityLowerBound(initialCapacity, initialCapacity, first, last, isDense);
    }

    public static SortedRanges tryMakeForKnownRangeKnownCount(final int count, final long first, final long last) {
        final boolean isDense = isDenseLongSample(first, last, count);
        return tryMakeForKnownRangeFinalCapacityLowerBound(count, count, first, last, isDense);
    }

    public final boolean isEmpty() {
        return count == 0;
    }

    public final void clear() {
        count = 0;
    }

    public final long first() {
        return unpackedGet(0);
    }

    public final long last() {
        return Math.abs(unpackedGet(count - 1));
    }

    public final boolean hasMoreThanOneRange() {
        if (count > 2) {
            return true;
        }
        if (count <= 1) {
            return false;
        }
        return unpackedGet(1) >= 0;
    }

    public final void validate() {
        validate(-1, -1);
    }

    @Override
    public final String toString() {
        final StringBuilder sb = new StringBuilder(Integer.toString(refCount()));
        sb.append(" { ");
        if (count == 0) {
            sb.append("}");
            return sb.toString();
        }
        boolean first = true;
        int i = 0;
        long iData = unpackedGet(0);
        boolean iNeg = false;
        long pendingStart = -1;
        while (true) {
            if (iNeg) {
                if (!first) {
                    sb.append(",");
                }
                sb.append(pendingStart).append("-").append(-iData);
                pendingStart = -1;
                first = false;
            } else {
                if (pendingStart != -1) {
                    if (!first) {
                        sb.append(",");
                    }
                    sb.append(pendingStart);
                    first = false;
                }
                pendingStart = iData;
            }
            ++i;
            if (i >= count) {
                break;
            }
            iData = unpackedGet(i);
            iNeg = iData < 0;
        }
        if (pendingStart != -1) {
            if (!first) {
                sb.append(",");
            }
            sb.append(pendingStart);
        }
        sb.append(" }");
        return sb.toString();
    }

    public final boolean contains(final long v) {
        if (count == 0) {
            return false;
        }
        final int pos = unpackedBinarySearch(v, 0);
        return pos >= 0;
    }

    public final boolean containsRange(final long start, final long end) {
        if (count == 0) {
            return false;
        }
        final int pos = unpackedBinarySearch(start, 0);
        if (pos < 0) {
            return false;
        }
        if (start == end) {
            return true;
        }
        if (pos == count - 1) {
            return false;
        }
        final long data = unpackedGet(pos + 1);
        final boolean neg = data < 0;
        if (!neg) {
            return false;
        }
        final long value = -data;
        return end <= value;
    }

    public final long find(final long v) {
        if (count == 0) {
            return -1;
        }
        final long packedValue = pack(v);
        if (packedValue < 0) {
            return -1;
        }
        return findPacked(packedValue);
    }

    private long findPacked(final long packedValue) {
        int i = 0;
        int pos = 0;
        long iData = packedGet(0);
        long iValue = iData;
        while (true) {
            if (packedValue < iValue) {
                return -pos - 1;
            }
            if (packedValue == iValue) {
                return pos;
            }
            int j = i + 1;
            if (j == count) {
                return -pos - 2;
            }
            final long jData = packedGet(j);
            final boolean jNeg = jData < 0;
            if (!jNeg) {
                ++pos;
                i = j;
                iValue = jData;
                continue;
            }
            final long jValue = -jData;
            if (packedValue <= jValue) {
                return pos + packedValue - iValue;
            }
            i = j + 1;
            if (i == count) {
                return -(pos + jValue + 2 - iValue);
            }
            pos += jValue - iValue + 1;
            iData = packedGet(i);
            iValue = iData < 0 ? -iData : iData;
        }
    }

    public final long get(final long targetPos) {
        if (targetPos < 0 || cardinality <= targetPos) {
            return -1;
        }
        int i = 0;
        long pos = 0;
        long iValue = packedGet(0);
        while (true) {
            if (pos == targetPos) {
                return unpack(iValue);
            }
            int j = i + 1;
            final long jData = packedGet(j);
            final boolean jNeg = jData < 0;
            if (!jNeg) {
                ++pos;
                i = j;
                iValue = jData;
                continue;
            }
            final long jValue = -jData;
            pos += jValue - iValue;
            if (targetPos <= pos) {
                return unpack(jValue + targetPos - pos);
            }
            i = j + 1;
            if (DEBUG && i == count) {
                throw new IllegalStateException("Broken invariants: " + this.toString());
            }
            ++pos;
            iValue = packedGet(i);
        }
    }

    public abstract SortedRanges applyShift(final long shift);

    public abstract SortedRanges applyShiftOnNew(final long shift);

    public final SortedRanges add(final long v) {
        return addInternal(v, true);
    }

    public final SortedRanges addUnsafe(final long v) {
        return addInternal(v, false);
    }

    public final SortedRanges addRange(final long start, final long end) {
        return addRangeInternal(start, end, true);
    }

    public final SortedRanges addRangeUnsafe(final long start, final long end) {
        return addRangeInternal(start, end, false);
    }

    public final SortedRanges append(final long v) {
        return appendInternal(v, true);
    }

    public final SortedRanges appendUnsafe(final long v) {
        return appendInternal(v, false);
    }

    public final SortedRanges appendRange(final long start, final long end) {
        return appendRangeInternal(start, end, true);
    }

    public final SortedRanges appendRangeUnsafe(final long start, final long end) {
        return appendRangeInternal(start, end, false);
    }

    public final SortedRanges remove(final long v) {
        return removeInternal(v);
    }

    public final SortedRanges removeRange(final long start, final long end) {
        return removeRangeInternal(start, end);
    }

    public final boolean forEachLongRange(final LongRangeAbortableConsumer lrac) {
        long pendingStart = -1;
        for (int i = 0; i < count; ++i) {
            final long data = unpackedGet(i);
            if (data < 0) {
                if (!lrac.accept(pendingStart, -data)) {
                    return false;
                }
                pendingStart = -1;
            } else {
                if (pendingStart != -1 && !lrac.accept(pendingStart, pendingStart)) {
                    return false;
                }
                pendingStart = data;
            }
        }
        if (pendingStart != -1 && !lrac.accept(pendingStart, pendingStart)) {
            return false;
        }
        return true;
    }

    public final boolean forEachLong(final LongAbortableConsumer lac) {
        long prev = -1;
        for (int i = 0; i < count; ++i) {
            final long iData = unpackedGet(i);
            if (iData < 0) {
                final long iValue = -iData;
                long v = prev;
                while(true) {
                    if (!lac.accept(v)) {
                        return false;
                    }
                    if (v == iValue) {
                        break;
                    }
                    ++v;
                }
                prev = -1;
            } else {
                if (prev != -1) {
                    if (!lac.accept(prev)) {
                        return false;
                    }
                }
                prev = iData;
            }
        }
        if (prev != -1) {
            if (!lac.accept(prev)) {
                return false;
            }
        }
        return true;
    }

    private static final class Iterator implements Index.Iterator {
        private int nextRangeIdx = 0;
        private long rangeCurr = -1;
        private long rangeEnd = -1;
        private SortedRanges sar;

        private Iterator(final SortedRanges sar) {
            if (sar.isEmpty()) {
                this.sar = null;
                return;
            }
            sar.acquire();
            this.sar = sar;
        }

        @Override
        public long nextLong() {
            if (++rangeCurr > rangeEnd) {
                nextRange();
            }
            return rangeCurr;
        }

        @Override
        public boolean hasNext() {
            return (rangeCurr < rangeEnd || (sar != null && nextRangeIdx < sar.count));
        }

        private void nextRange() {
            rangeCurr = sar.unpackedGet(nextRangeIdx);
            if (++nextRangeIdx == sar.count) {
                rangeEnd = rangeCurr;
                close();
                return;
            }
            final long after = sar.unpackedGet(nextRangeIdx);
            if (after < 0) {
                rangeEnd = -after;
                ++nextRangeIdx;
                if (nextRangeIdx == sar.count) {
                    close();
                }
            } else {
                rangeEnd = rangeCurr;
            }
        }

        @Override
        public void close() {
            if (sar != null) {
                sar.release();
                sar = null;
            }
        }
    }

    public final Index.Iterator getIterator() {
        return new Iterator(this);
    }

    private static class RangeIteratorBase {
        protected int nextRangeIdx = 0;
        protected long currRangeStart = -1;
        protected long currRangeEnd = -1;
        protected SortedRanges sar;

        protected RangeIteratorBase(final SortedRanges sar) {
            if (sar.isEmpty()) {
                this.sar = null;
                return;
            }
            sar.acquire();
            this.sar = sar;
        }

        protected final void nextRange() {
            currRangeStart = sar.unpackedGet(nextRangeIdx);
            if (++nextRangeIdx == sar.count) {
                currRangeEnd = currRangeStart;
                closeImpl();
                return;
            }
            final long after = sar.unpackedGet(nextRangeIdx);
            if (after < 0) {
                currRangeEnd = -after;
                ++nextRangeIdx;
                if (nextRangeIdx == sar.count) {
                    closeImpl();
                }
                return;
            }
            currRangeEnd = currRangeStart;
        }

        protected final boolean hasNextRange() {
            return sar != null && nextRangeIdx < sar.count;
        }

        public final boolean advanceImpl(final long v) {
            if (currRangeStart == -1) {
                if (!hasNextRange()) {
                    return false;
                }
                nextRange();
            }
            if (v <= currRangeEnd) {
                if (currRangeStart < v) {
                    currRangeStart = v;
                }
                return true;
            }
            if (sar == null || nextRangeIdx == sar.count) {
                currRangeStart = currRangeEnd = -1;
                closeImpl();
                return false;
            }
            int p = sar.unpackedBinarySearch(v, nextRangeIdx);
            if (p < 0) {
                p = ~p;
                if (p == sar.count) {
                    nextRangeIdx = sar.count;
                    currRangeStart = currRangeEnd = -1;
                    closeImpl();
                    return false;
                }
            }
            final long x = sar.unpackedGet(p);
            currRangeStart = Math.max(x, v);
            ++p;
            if (p == sar.count) {
                currRangeEnd = currRangeStart;
                nextRangeIdx = sar.count;
                return true;
            }
            final long after = sar.unpackedGet(p);
            if (after < 0) {
                currRangeEnd = -after;
                nextRangeIdx = p + 1;
                return true;
            }
            currRangeEnd = currRangeStart;
            nextRangeIdx = p;
            return true;
        }

        public final void closeImpl() {
            if (sar != null) {
                sar.release();
                sar = null;
            }
        }
    }

    public static final class RangeIterator extends RangeIteratorBase implements Index.RangeIterator {
        private RangeIterator(final SortedRanges sar) {
            super(sar);
        }

        @Override
        public void postpone(final long v) {
            currRangeStart = v;
        }

        @Override
        public long currentRangeStart() {
            return currRangeStart;
        }

        @Override
        public long currentRangeEnd() {
            return currRangeEnd;
        }

        @Override
        public boolean hasNext() {
            return hasNextRange();
        }

        @Override
        public long next() {
            nextRange();
            return currRangeStart;
        }

        @Override
        public boolean advance(final long v) {
            return advanceImpl(v);
        }

        @Override
        public void close() {
            closeImpl();
        }
    }

    public Index.RangeIterator getRangeIterator() {
        return new RangeIterator(this);
    }

    private static final class SearchIterator extends RangeIteratorBase implements Index.SearchIterator {
        private boolean pendingNext = false;
        private SearchIterator(final SortedRanges sar) {
            super(sar);
        }

        @Override
        public boolean hasNext() {
            return pendingNext || currRangeStart < currRangeEnd || hasNextRange();
        }

        @Override
        public long nextLong() {
            if (pendingNext) {
                pendingNext = false;
                return currRangeStart;
            }
            if (currRangeStart < currRangeEnd) {
                ++currRangeStart;
            } else if (hasNextRange()) {
                nextRange();
            }
            if (currRangeStart == currRangeEnd && !hasNextRange()) {
                close();
            }
            return currRangeStart;
        }

        @Override
        public long currentValue() {
            return currRangeStart;
        }

        @Override
        public void close() {
            if (sar != null) {
                sar.release();
                sar = null;
            }
        }

        @Override
        public boolean advance(long v) {
            pendingNext = false;
            return advanceImpl(v);
        }

        private static int searchPos(
                final MutableLong outData,
                final SortedRanges sar,
                final IndexUtilities.Comparator comp, final int startPos) {  // startPos points to the beginning of a range.
            final long endPosUnpackedData = sar.unpackedGet(sar.count - 1);
            final long endPosUnpackedValue = Math.abs(endPosUnpackedData);
            int c = comp.directionToTargetFrom(endPosUnpackedValue);
            if (c >= 0) {
                outData.setValue(endPosUnpackedData);
                return sar.count - 1;
            }
            int maxPos = sar.count - 1;
            final long startPosUnpackedValue = sar.unpackedGet(startPos);
            c = comp.directionToTargetFrom(startPosUnpackedValue);
            if (c <= 0) {
                if (c == 0) {
                    outData.setValue(startPosUnpackedValue);
                    return startPos;
                }
                return startPos - 1;
            }
            int minPos = startPos;
            long minPosUnpackedData = startPosUnpackedValue;
            while (maxPos - minPos > 1) {
                final int midPos = (minPos + maxPos) / 2;
                final long midPosUnpackedData = sar.unpackedGet(midPos);
                final long midPosUnpackedValue = Math.abs(midPosUnpackedData);
                c = comp.directionToTargetFrom(midPosUnpackedValue);
                if (c > 0) {
                    minPos = midPos;
                    minPosUnpackedData = midPosUnpackedData;
                    continue;
                }
                if (c < 0) {
                    maxPos = midPos;
                    continue;
                }
                outData.setValue(midPosUnpackedData);
                return midPos;
            }
            outData.setValue(minPosUnpackedData);
            return minPos;
        }

        @Override
        public long binarySearchValue(final ReadOnlyIndex.TargetComparator comp, final int dir) {
            pendingNext = false;
            boolean rollbackNextIfNotFound = false;
            if (currRangeStart == -1) {
                if (!hasNext()) {
                    return -1;
                }
                rollbackNextIfNotFound = true;
                nextRange();
            }
            int c = comp.compareTargetTo(currRangeEnd, dir);
            if (c <= 0) {
                if (c == 0) {
                    currRangeStart = currRangeEnd;
                    return currRangeStart;
                }
                c = comp.compareTargetTo(currRangeStart, dir);
                if (c < 0) {
                    if (rollbackNextIfNotFound) {
                        pendingNext = true;
                    }
                    return -1;
                }
                currRangeStart = IndexUtilities.rangeSearch(currRangeStart, currRangeEnd,
                        (final long v) -> comp.compareTargetTo(v, dir));
                return currRangeStart;
            }
            if (sar == null || nextRangeIdx == sar.count) {
                currRangeStart = currRangeEnd;
                close();
                return currRangeEnd;
            }
            final IndexUtilities.Comparator ixComp = (final long v) ->
                    comp.compareTargetTo(v, dir);
            final MutableLong outValue = new MutableLong();
            final int i = searchPos(outValue, sar, ixComp, nextRangeIdx);
            if (i < nextRangeIdx) {
                currRangeStart = currRangeEnd;
                return currRangeStart;
            }
            final long data = outValue.longValue();
            final boolean neg = data < 0;
            if (neg) {
                currRangeStart = currRangeEnd = -data;
                nextRangeIdx = i + 1;
                return currRangeStart;
            }
            final int next = i + 1;
            if (next == sar.count) {
                currRangeStart = currRangeEnd = data;
                nextRangeIdx = next;
                return currRangeStart;
            }
            final long nextData = sar.unpackedGet(next);
            final boolean nextNeg = nextData < 0;
            if (nextNeg) {
                final long searchEndValue = -nextData - 1;
                currRangeStart = IndexUtilities.rangeSearch(data, searchEndValue,
                        (final long v) -> comp.compareTargetTo(v, dir));
                currRangeEnd = -nextData;
                nextRangeIdx = next + 1;
                return currRangeStart;
            }
            currRangeStart = currRangeEnd = data;
            nextRangeIdx = next;
            return currRangeStart;
        }
    }

    public final Index.SearchIterator getSearchIterator() {
        return new SearchIterator(this);
    }

    private static final class ReverseIterator implements ReadOnlyIndex.SearchIterator {
        private int nextRangeIdx = -1;
        private long rangeCurr = -1;
        private long rangeStart = -1;
        private SortedRanges sar;

        private ReverseIterator(final SortedRanges sar) {
            if (sar.isEmpty()) {
                this.sar = null;
                return;
            }
            sar.acquire();
            this.sar = sar;
            nextRangeIdx = sar.count - 1;
        }

        @Override
        public void close() {
            if (sar != null) {
                sar.release();
                sar = null;
            }
        }

        @Override
        public boolean hasNext() {
            return rangeCurr > rangeStart || nextRangeIdx >= 0;
        }

        @Override
        public long currentValue() {
            return rangeCurr;
        }

        private void nextRange() {
            final long data = sar.unpackedGet(nextRangeIdx--);
            final boolean neg = data < 0;
            if (neg) {
                rangeCurr = -data;
                rangeStart = sar.unpackedGet(nextRangeIdx--);
            } else {
                rangeCurr = rangeStart = data;
            }
            if (nextRangeIdx < 0) {
                close();
            }
        }

        @Override
        public long nextLong() {
            if (--rangeCurr < rangeStart) {
                nextRange();
            }
            return rangeCurr;
        }

        @Override
        public boolean advance(final long v) {
            if (rangeCurr == -1) {
                if (sar == null) {
                    return false;
                }
                nextRange();
            }
            if (rangeStart <= v) {
                rangeCurr = Math.min(v, rangeCurr);
                return true;
            }
            if (sar == null || nextRangeIdx < 0) {
                rangeCurr = rangeStart;
                close();
                return false;
            }
            final long packedValue = sar.pack(v);
            if (packedValue < 0) {
                rangeCurr = rangeStart = sar.unpackedGet(0);
                close();
                return false;
            }
            int i = sar.absRawBinarySearch(packedValue, 0, nextRangeIdx);
            long iData = sar.packedGet(i);
            boolean iNeg = iData < 0;
            if (iNeg) {
                final long iPrevValue = sar.packedGet(i - 1);
                nextRangeIdx = i - 2;
                rangeStart = sar.unpack(iPrevValue);
                rangeCurr = v;
                if (nextRangeIdx < 0) {
                    close();
                }
                return true;
            }
            if (iData == packedValue) {
                rangeStart = rangeCurr = v;
                nextRangeIdx = i - 1;
                if (nextRangeIdx < 0) {
                    close();
                }
                return true;
            }
            --i;
            if (i < 0) {
                nextRangeIdx = i;
                rangeStart = rangeCurr = sar.unpack(iData);
                close();
                return false;
            }
            iData = sar.packedGet(i);
            iNeg = iData < 0;
            if (iNeg) {
                nextRangeIdx = i - 2;
                rangeCurr = sar.unpack(-iData);
                rangeStart = sar.unpackedGet(i - 1);
                if (nextRangeIdx < 0) {
                    close();
                }
                return true;
            }
            nextRangeIdx = i - 1;
            rangeCurr = rangeStart = sar.unpack(iData);
            if (nextRangeIdx < 0) {
                close();
            }
            return true;
        }

        @Override
        public long binarySearchValue(ReadOnlyIndex.TargetComparator comp, int dir) {
            throw new UnsupportedOperationException("Reverse iterator does not support binary search.");
        }
    }

    public final Index.SearchIterator getReverseIterator() {
        return new ReverseIterator(this);
    }

    public final long getCardinality() {
        return cardinality;
    }

    public final void getKeysForPositions(final PrimitiveIterator.OfLong inputPositions, final LongConsumer outputKeys) {
        if (!inputPositions.hasNext()) {
            return;
        }
        int i = 0;
        long iPos = 0;
        long iData = packedGet(0);
        boolean iDataNeg = false;
        long iPrevData = iData;
        do {
            final long targetPos = inputPositions.nextLong();
            while (iPos < targetPos) {
                ++i;
                iData = packedGet(i);
                iDataNeg = iData < 0;
                if (iDataNeg) {
                    iPos += -iData - iPrevData;
                } else {
                    ++iPos;
                    iPrevData = iData;
                }
            }
            final long iValue = iDataNeg ? -iData : iData;
            final long v = unpack(iValue) - (iPos - targetPos);
            outputKeys.accept(v);
        } while (inputPositions.hasNext());
    }

    public final SortedRanges subRangesByPos(final long startPosIn, final long endPosIn) {
        if (endPosIn < 0) {
            return null;
        }
        final long startPos = Math.max(0, startPosIn);
        if (endPosIn < startPos || startPos >= cardinality) {
            // The conditions also cover the case where we are empty.
            return null;
        }
        final long endPos = Math.min(endPosIn, cardinality - 1);
        final long inputRangeSpan = endPos - startPos;
        int i = 0;
        long iData = packedGet(0);
        if (count == 1) {
            // we know startPos < cardinality, and cardinality == 1.
            final SortedRanges ans = makeMyTypeAndOffset(2);
            ans.packedSet(0, iData);
            ans.cardinality = 1;
            ans.count = 1;
            if (DEBUG) validate(startPosIn, endPosIn);
            return ans;
        }
        long pos = 0;
        long iPrevData = iData;
        i = 0;
        while (pos < startPos) {
            ++i;
            iData = packedGet(i);
            final boolean iNeg = iData < 0;
            if (iNeg) {
                final long delta = -iData - iPrevData;
                pos += delta;
            } else {
                ++pos;
                iPrevData = iData;
            }
        }
        // startPos <= pos.
        if (endPos <= pos) {
            // single range.
            if (endPos == startPos) {
                // single value.
                final SortedRanges ans = makeMyTypeAndOffset(2);
                ans.packedSet(0, Math.abs(iData) - (pos - startPos));
                ans.cardinality = 1;
                ans.count = 1;
                if (DEBUG) validate(startPosIn, endPosIn);
                return ans;
            }
            // we know endPos > startPos, so there is more than a single value,
            // iData has to be the end of a range (and thus negative).
            final long s = -iData - (pos - startPos);
            final long e = -iData - (pos - endPos);
            final SortedRanges ans = makeMyTypeAndOffset(2);
            ans.packedSet(0, s);
            ans.packedSet(1, -e);
            ans.cardinality = e - s + 1;
            ans.count = 2;
            if (DEBUG) validate(startPosIn, endPosIn);
            return ans;
        }

        // We can't tell in advance exactly how big an array we will need.
        // We don't want to do two passes, we allocated an array big enough instead.
        final boolean brokenInitialRange = startPos < pos;
        int ansLen = count - i + (brokenInitialRange ? 2 : 1);
        ansLen = (int) Math.min(ansLen, (inputRangeSpan + 1));
        final SortedRanges ans = makeMyTypeAndOffset(ansLen);
        ans.count = 0;
        ans.cardinality = 0;
        if (brokenInitialRange) {
            final long s = -iData - (pos - startPos);
            ans.packedSet(ans.count++, s);
            ans.cardinality += pos - startPos + 1;
        } else {
            iData = Math.abs(iData);
            ++ans.cardinality;
        }
        long deltaCard = 0;
        while (pos < endPos) {
            ans.cardinality += deltaCard;
            ans.packedSet(ans.count++, iData);
            ++i;
            iData = packedGet(i);
            final boolean iNeg = iData < 0;
            if (iNeg) {
                deltaCard = -iData - iPrevData;
                pos += deltaCard;
            } else {
                ++pos;
                iPrevData = iData;
                deltaCard = 1;
            }
        }
        final boolean brokenFinalRange = endPos < pos;
        if (brokenFinalRange) {
            final long e = -iData - (pos - endPos);
            ans.packedSet(ans.count++, -e);
            ans.cardinality += e - iPrevData;
        } else {
            // endPos == pos.
            ans.packedSet(ans.count++, iData);
            ans.cardinality += deltaCard;
        }
        if (DEBUG) validate(startPosIn, endPosIn);
        return ans;
    }

    public final SortedRanges subRangesByKey(final long start, final long end) {
        if (isEmpty() || end < first() || last() < start) {
            return null;
        }
        final long packedStart = Math.max(pack(start), 0);
        final long packedEnd = pack(end);
        return subRangesByKeyPacked(packedStart, packedEnd);
    }

    public final boolean overlapsRange(final long start, final long end) {
        if (end < start || isEmpty()) {
            return false;
        }
        final long last = last();
        if (last() < start) {
            return false;
        }
        final long first = first();
        if (end < first()) {
            return false;
        }
        final long packedStart = pack(Math.max(start, first));
        final long packedEnd = pack(Math.min(end, last));
        return overlapsRangeInternal(0, packedStart, packedEnd) == -1;
    }

    // startIdx is the array position index where to begin the search for packedStart.
    // returns -1 if this array overlaps the provided range, or if it doesn't, returns the array position index
    // where to begin a subsequent call for a later range that might overlap.
    private int overlapsRangeInternal(final int startIdx, final long packedStart, final long packedEnd) {
        final int iStart = absRawBinarySearch(packedStart, startIdx, count - 1);
        // is < count since we know start < end < first().
        final long iStartData = packedGet(iStart);
        if (iStartData < 0 || iStartData == packedStart) {
            return -1;
        }
        if (iStartData <= packedEnd) {
            return -1;
        }
        return iStart;
    }

    public final boolean overlaps(final Index.RangeIterator rangeIter) {
        if (isEmpty()) {
            return false;
        }
        if (!rangeIter.advance(first())) {
            return false;
        }
        int i = 0;
        final long last = last();
        while (true) {
            final long start = rangeIter.currentRangeStart();
            if (last < start) {
                return false;
            }
            final long end = rangeIter.currentRangeEnd();
            i = overlapsRangeInternal(i, pack(start), pack(end));
            if (i < 0) {
                return true;
            }
            if (!rangeIter.hasNext()) {
                return false;
            }
            rangeIter.next();
        }
    }

    // first <= start && end <= last assumed on entry.
    public final SortedRanges retainRange(long start, long end) {
        if (isEmpty()) {
            return this;
        }
        if (!canWrite()) {
            final SortedRanges ans = subRangesByKey(start, end);
            return ans;
        }
        SortedRanges ans = this;
        final long last = last();
        if (end < last) {
            ans = ans.removeRange(end + 1, last);
        }
        final long first = first();
        if (start > first) {
            ans = ans.removeRange(0, start - 1);
        }
        return ans;
    }

    // Guarantee for the caller: if this method returns null, no state change has been made on the this object.
    private SortedRanges packedAppend(final long packedData, final long unpackedData, final boolean writeCheck) {
        SortedRanges ans = ensureCanAppend(count, unpackedData, writeCheck);
        if (ans == null) {
            return null;
        }
        if (this == ans) {
            packedSet(count++, packedData);
        } else {
            ans.unpackedSet(ans.count++, unpackedData);
        }
        return ans;
    }

    // Guarantee for the caller: if this method returns null, no state change has been made on the this object.
    private SortedRanges unpackedAppend(final long unpackedData, final boolean writeCheck) {
        SortedRanges ans = ensureCanAppend(count, unpackedData, writeCheck);
        if (ans == null) {
            return null;
        }
        ans.unpackedSet(ans.count++, unpackedData);
        return ans;
    }

    // Guarantee for the caller: if this method returns null, no state change has been made on the this object.
    private SortedRanges packedAppend2(
            final long packedData1, final long packedData2, final long unpackedData1, final long unpackedData2, final boolean writeCheck) {
        SortedRanges ans = ensureCanAppend(count + 1, unpackedData2, writeCheck);
        if (ans == null) {
            return null;
        }
        if (this == ans) {
            packedSet(count++, packedData1);
            packedSet(count++, packedData2);
        } else {
            ans.unpackedSet(ans.count++, unpackedData1);
            ans.unpackedSet(ans.count++, unpackedData2);
        }
        return ans;
    }

    // required on entry: out.canWrite().
    // sar.first() <= start && end <= sar.last()
    // returns null if we exceed maxCapacity in the process of building the answer
    // (which can happen if you have, say, a big single range and retain a gazillion individual elements).
    // Writes to iStartOut an array position index into sar where to continue the intersection for ranges after
    // the one provided.
    private static SortedRanges intersectRangeImplStep(
            SortedRanges out,
            final SortedRanges sar,
            final int iStart, final long start, final long end, final MutableInt iStartOut) {
        if (!out.fits(start, end)) {
            return null;
        }
        final long packedStart = sar.pack(start);
        int srcIndex = sar.absRawBinarySearch(packedStart, iStart, sar.count - 1);
        long srcData = sar.packedGet(srcIndex);
        boolean srcNeg = srcData < 0;
        final long packedEnd = sar.pack(end);
        if (srcNeg) {
            final long srcValue = -srcData;
            if (srcValue == packedStart) {
                out = out.unpackedAppend(start, false);
                if (out == null) {
                    return null;
                }
                ++out.cardinality;
                if (packedEnd == packedStart) {
                    iStartOut.setValue(srcIndex + 1);
                    if (DEBUG) out.validate(start, end);
                    return out;
                }
            } else {
                // packedStart < srcValue
                out = out.unpackedAppend(start, false);
                if (out == null) {
                    return null;
                }
                if (packedEnd <= srcValue) {
                    if (packedStart == packedEnd) {
                        out.cardinality += 1;
                    } else {
                        out = out.unpackedAppend(-end, false);
                        if (out == null) {
                            return null;
                        }
                        out.cardinality += packedEnd - packedStart + 1;
                    }
                    iStartOut.setValue((packedEnd < srcValue) ? srcIndex : srcIndex + 1);
                    if (DEBUG) out.validate(start, end);
                    return out;
                }
                out = out.unpackedAppend(sar.unpack(srcData), false);
                if (out == null) {
                    return null;
                }
                out.cardinality += srcValue - packedStart + 1;
            }
            ++srcIndex;
            // srcIndex < count at this point, since we know
            // srcValue < packedEnd and packedEnd was clamped
            // to within our array's range.
            srcData = sar.packedGet(srcIndex);
            srcNeg = false;
        }
        long srcValue = srcData;
        long prevStart = srcData;
        boolean pastEnd = false;
        while (srcValue <= packedEnd) {
            out = out.unpackedAppend(sar.unpack(srcData), false);
            if (out == null) {
                return null;
            }
            if (srcNeg) {
                out.cardinality += srcValue - prevStart;
            } else {
                ++out.cardinality;
                prevStart = srcData;
            }
            ++srcIndex;
            if (srcIndex == sar.count) {
                pastEnd = true;
                break;
            }
            srcData = sar.packedGet(srcIndex);
            srcNeg = srcData < 0;
            srcValue = srcNeg ? -srcData : srcData;
        }
        if (!pastEnd && srcNeg && prevStart < packedEnd) {
            out = out.unpackedAppend(-end, false);
            if (out == null) {
                return null;
            }
            out.cardinality += packedEnd - prevStart;
        }
        iStartOut.setValue(srcIndex);
        if (DEBUG) out.validate(start, end);
        return out;
    }

    private static ThreadLocal<SortedRangesLong> workSortedRangesLongPerThread =
            ThreadLocal.withInitial(() -> new SortedRangesLong(new long[MAX_CAPACITY], 0, 0));

    private static boolean forEachLongRangeFromLongRangesArray(
            final long[] arr, final int count, final LongRangeAbortableConsumer lrac) {
        long pendingStart = -1;
        for (int i = 0; i < count; ++i) {
            final long data = arr[i];
            if (data < 0) {
                if (!lrac.accept(pendingStart, -data)) {
                    return false;
                }
                pendingStart = -1;
            } else {
                if (pendingStart != -1 && !lrac.accept(pendingStart, pendingStart)) {
                    return false;
                }
                pendingStart = data;
            }
        }
        if (pendingStart != -1 && !lrac.accept(pendingStart, pendingStart)) {
            return false;
        }
        return true;
    }

    private static TreeIndexImpl makeRspBitmapFromLongRangesArray(final long[] ranges, final int count) {
        final RspBitmapSequentialBuilder builder = new RspBitmapSequentialBuilder();
        forEachLongRangeFromLongRangesArray(ranges, count, (final long start, final long end) -> {
            builder.appendRange(start, end);
            return true;
        });
        return builder.getTreeIndexImpl();
    }

    public static boolean isDenseShort(final short[] data, final int count) {
        return count >= ELEMENTS_PER_BLOCK_DENSE_THRESHOLD;
    }

    // v0 >= 0, v1 > v0.
    private static long nKeys(final long v0, final long v1) {
        final long k0Shifted = v0 >> RspBitmap.BITS_PER_BLOCK;
        final long k1Shifted = v1 >> RspBitmap.BITS_PER_BLOCK;
        return k1Shifted - k0Shifted + 1;
    }

    // v0 >= 0, v1 > v0.
    protected static boolean isDenseLongSample(final long v0, final long v1, final int count) {
        return nKeys(v0, v1) * ELEMENTS_PER_BLOCK_DENSE_THRESHOLD <= count;
    }

    public static boolean isDenseInt(final int[] data, final int count) {
        if (count < ELEMENTS_PER_BLOCK_DENSE_THRESHOLD) {
            return false;
        }
        return isDenseLongSample(data[0], Math.abs(data[count - 1]), count);
    }

    public static boolean isDenseLong(final long[] data, final int count) {
        if (count < ELEMENTS_PER_BLOCK_DENSE_THRESHOLD) {
            return false;
        }
        return isDenseLongSample(data[0], Math.abs(data[count - 1]), count);
    }

    public abstract boolean isDense();
    public final boolean isSparse() {
        return !isDense();
    }

    private static TreeIndexImpl makeTreeIndexImplFromLongRangesArray(
            final long[] ranges, final int count, final long card, final SortedRanges out) {
        if (count == 0) {
            return TreeIndexImpl.EMPTY;
        }
        if (count == 1) {
            return SingleRange.make(ranges[0], ranges[0]);
        }
        if (count == 2 && ranges[1] < 0) {
            return SingleRange.make(ranges[0], -ranges[1]);
        }
        final long offset = ranges[0];
        final long domain = Math.abs(ranges[count - 1]) - offset;
        if (domain <= Short.MAX_VALUE) {
            if (count > SHORT_MAX_CAPACITY) {
                return makeRspBitmapFromLongRangesArray(ranges, count);
            }
            final SortedRangesShort sr;
            if (out instanceof SortedRangesShort && out.dataLength() >= count) {
                sr = (SortedRangesShort) out;
                sr.offset = offset;
            } else {
                sr = new SortedRangesShort(count, offset);
            }
            for (int sri = 0; sri < count; ++sri) {
                sr.unpackedSet(sri, ranges[sri]);
            }
            sr.cardinality = card;
            sr.count = count;
            return sr;
        }
        if (isDenseLong(ranges, count)) {
            return makeRspBitmapFromLongRangesArray(ranges, count);
        }
        if (domain <= Integer.MAX_VALUE) {
            if (count > INT_SPARSE_MAX_CAPACITY) {
                return makeRspBitmapFromLongRangesArray(ranges, count);
            }
            final SortedRangesInt sr;
            if (out instanceof SortedRangesInt && out.dataLength() >= count) {
                sr = (SortedRangesInt) out;
                sr.offset = offset;
            } else {
                sr = new SortedRangesInt(count, offset);
            }
            for (int sri = 0; sri < count; ++sri) {
                sr.unpackedSet(sri, ranges[sri]);
            }
            sr.cardinality = card;
            sr.count = count;
            return sr;
        }
        if (count > LONG_SPARSE_MAX_CAPACITY) {
            return makeRspBitmapFromLongRangesArray(ranges, count);
        }
        final SortedRangesLong sr;
        if (out instanceof SortedRangesLong && out.dataLength() >= count) {
            sr = (SortedRangesLong) out;
        } else {
            sr = new SortedRangesLong(count);
        }
        System.arraycopy(ranges, 0, sr.data, 0, count);
        sr.cardinality = card;
        sr.count = count;
        return sr;
    }

    // Neither argument can be empty.
    private static SortedRangesLong intersect(
            final SortedRanges sr,
            final TreeIndexImpl tix) {
        return intersect(sr, tix, false);
    }

    // Neither argument can be empty.
    private static SortedRangesLong intersect(
            final SortedRanges sr,
            final TreeIndexImpl tix,
            final boolean takeComplement) {
        final SortedRangesLong res = workSortedRangesLongPerThread.get();
        res.reset();
        try (ReadOnlyIndex.RangeIterator it1 = sr.getRangeIterator();
             ReadOnlyIndex.RangeIterator it2 = takeComplement
                     ? new ComplementRangeIterator(tix.ixRangeIterator())
                     : tix.ixRangeIterator()) {
            it1.next();
            it2.next();
            long s1 = it1.currentRangeStart();
            long e1 = it1.currentRangeEnd();
            long s2 = it2.currentRangeStart();
            long e2 = it2.currentRangeEnd();
            while (true) {
                if (s1 < s2) {
                    if (e1 < s2) {
                        final boolean valid = it1.advance(s2);
                        if (!valid) {
                            break;
                        }
                        s1 = it1.currentRangeStart();
                        e1 = it1.currentRangeEnd();
                        continue;
                    }
                    if (e1 < e2) {
                        if (!res.trySimpleAppend(s2, e1)) {
                            return null;
                        }
                        if (!it1.hasNext()) {
                            break;
                        }
                        s2 = e1 + 1;
                        it1.next();
                        s1 = it1.currentRangeStart();
                        e1 = it1.currentRangeEnd();
                        continue;
                    }
                    // e2 <= e1.
                    if (!res.trySimpleAppend(s2, e2)) {
                        return null;
                    }
                    if (e2 < e1) {
                        if (!it2.hasNext()) {
                            break;
                        }
                        s1 = e2 + 1;
                        it2.next();
                        s2 = it2.currentRangeStart();
                        e2 = it2.currentRangeEnd();
                        continue;
                    }
                    // e1 == e2.
                    if (!it1.hasNext() || !it2.hasNext()) {
                        break;
                    }
                    it1.next();
                    s1 = it1.currentRangeStart();
                    e1 = it1.currentRangeEnd();
                    it2.next();
                    s2 = it2.currentRangeStart();
                    e2 = it2.currentRangeEnd();
                    continue;
                }
                // s2 <= s1.
                if (e2 < s1) {
                    final boolean valid = it2.advance(s1);
                    if (!valid) {
                        break;
                    }
                    s2 = it2.currentRangeStart();
                    e2 = it2.currentRangeEnd();
                    continue;
                }
                if (e2 < e1) {
                    if (!res.trySimpleAppend(s1, e2)) {
                        return null;
                    }
                    if (!it2.hasNext()) {
                        break;
                    }
                    s1 = e2 + 1;
                    it2.next();
                    s2 = it2.currentRangeStart();
                    e2 = it2.currentRangeEnd();
                    continue;
                }
                // e1 <= e2.
                if (!res.trySimpleAppend(s1, e1)) {
                    return null;
                }
                if (e1 < e2) {
                    if (!it1.hasNext()) {
                        break;
                    }
                    s2 = e1 + 1;
                    it1.next();
                    s1 = it1.currentRangeStart();
                    e1 = it1.currentRangeEnd();
                    continue;
                }
                // e1 == e2.
                if (!it1.hasNext() || !it2.hasNext()) {
                    break;
                }
                it1.next();
                s1 = it1.currentRangeStart();
                e1 = it1.currentRangeEnd();
                it2.next();
                s2 = it2.currentRangeStart();
                e2 = it2.currentRangeEnd();
            }
            return res;
        }
    }

    // Neither argument can be empty.
    private static SortedRangesLong union(final SortedRanges sr1, final SortedRanges sr2) {
        final SortedRangesLong res = workSortedRangesLongPerThread.get();
        res.reset();
        try (ReadOnlyIndex.RangeIterator it1 = sr1.getRangeIterator();
             ReadOnlyIndex.RangeIterator it2 = sr2.getRangeIterator()) {
            it1.next();
            it2.next();
            long s1 = it1.currentRangeStart();
            long e1 = it1.currentRangeEnd();
            long s2 = it2.currentRangeStart();
            long e2 = it2.currentRangeEnd();
            while (true) {
                if (e1 + 1 < s2) {
                    if (!res.trySimpleAppend(s1, e1)) {
                        return null;
                    }
                    if (!it1.hasNext()) {
                        if (!res.trySimpleAppend(s2, e2)) {
                            return null;
                        }
                        break;
                    }
                    it1.next();
                    s1 = it1.currentRangeStart();
                    e1 = it1.currentRangeEnd();
                    continue;
                }
                if (e2 + 1 < s1) {
                    if (!res.trySimpleAppend(s2, e2)) {
                        return null;
                    }
                    if (!it2.hasNext()) {
                        if (!res.trySimpleAppend(s1, e1)) {
                            return null;
                        }
                        break;
                    }
                    it2.next();
                    s2 = it2.currentRangeStart();
                    e2 = it2.currentRangeEnd();
                    continue;
                }
                // The ranges are adjacent or overlap.
                final long min = Math.min(s1, s2);
                final long max = Math.max(e1, e2);
                final boolean it1Valid = it1.advance(max + 1);
                final boolean it2Valid = it2.advance(max + 1);
                if (it1Valid) {
                    s1 = it1.currentRangeStart();
                    e1 = it1.currentRangeEnd();
                    if (it2Valid) {
                        s2 = it2.currentRangeStart();
                        e2 = it2.currentRangeEnd();
                        if (max + 1 == s1) {
                            s1 = min;
                        } else if (max + 1 == s2) {
                            s2 = min;
                        } else {
                            if (!res.trySimpleAppend(min, max)) {
                                return null;
                            }
                        }
                        continue;
                    }
                    if (max + 1 == s1) {
                        s1 = min;
                    } else {
                        if (!res.trySimpleAppend(min, max)) {
                            return null;
                        }
                    }
                    if (!res.trySimpleAppend(s1, e1)) {
                        return null;
                    }
                    break;
                }
                if (it2Valid) {
                    s2 = it2.currentRangeStart();
                    e2 = it2.currentRangeEnd();
                    if (max + 1 == s2) {
                        s2 = min;
                    } else {
                        if (!res.trySimpleAppend(min, max)) {
                            return null;
                        }
                    }
                    if (!res.trySimpleAppend(s2, e2)) {
                        return null;
                    }
                    break;
                }
                if (!res.trySimpleAppend(min, max)) {
                    return null;
                }
                break;
            }
            // add any leftovers.
            while (it1.hasNext()) {
                it1.next();
                s1 = it1.currentRangeStart();
                e1 = it1.currentRangeEnd();
                if (!res.trySimpleAppend(s1, e1)) {
                    return null;
                }
            }
            while (it2.hasNext()) {
                it2.next();
                s2 = it2.currentRangeStart();
                e2 = it2.currentRangeEnd();
                if (!res.trySimpleAppend(s2, e2)) {
                    return null;
                }
            }
            return res;
        }
    }

    final TreeIndexImpl retain(final TreeIndexImpl tix) {
        if (!USE_RANGES_ARRAY) {
            final MutableObject<SortedRanges> sarOut = new MutableObject<>(this);
            final boolean valid = retainLegacy(sarOut, tix);
            if (!valid) {
                return sarOut.getValue().toRsp().ixRetain(tix);
            }
            final SortedRanges sr = sarOut.getValue();
            if (sr.isEmpty()) {
                return TreeIndexImpl.EMPTY;
            }
            return sr;
        }
        final SortedRangesLong sr = intersect(this, tix);
        if (sr == null) {
            return toRsp().ixRetain(tix);
        }
        return makeTreeIndexImplFromLongRangesArray(sr.data, sr.count, sr.cardinality, this);
    }

    private static boolean retainLegacy(final MutableObject<SortedRanges> sarOut, final TreeIndexImpl tix) {
        try (ReadOnlyIndex.RangeIterator rangeIter = tix.ixRangeIterator()) {
            SortedRanges sar = sarOut.getValue();
            final long first = sar.first();
            final boolean valid = rangeIter.advance(first);
            if (!valid) {
                throw new IllegalStateException();
            }
            final long rstartFirst = rangeIter.currentRangeStart();
            if (rstartFirst > 0) {
                final SortedRanges ans = sar.removeRange(0, rstartFirst - 1);
                if (ans == null) {
                    return false;
                }
                sar = ans;
                if (sar.isEmpty()) {
                    sarOut.setValue(sar);
                    return true;
                }
            }
            long previousRangeEnd = rangeIter.currentRangeEnd();
            final long last = sar.last();
            while (rangeIter.hasNext()) {
                rangeIter.next();
                final long rstart = rangeIter.currentRangeStart();
                if (last < rstart) {
                    break;
                }
                final SortedRanges ans = sar.removeRange(previousRangeEnd + 1, rstart - 1);
                if (ans == null) {
                    sarOut.setValue(sar);
                    return false;
                }
                sar = ans;
                previousRangeEnd = rangeIter.currentRangeEnd();
            }
            if (previousRangeEnd < last) {
                final SortedRanges ans = sar.removeRange(previousRangeEnd + 1, last);
                if (ans == null) {
                    sarOut.setValue(sar);
                    return false;
                }
                sar = ans;
            }
            sarOut.setValue(sar);
            return true;
        }
    }

    // This call assumes the basic overlapping checks that
    // would immediately imply an empty return have already been done by the caller;
    // if we are here it is not obvious that the intersection would be empty and we need
    // to actually visit the respective elements to find out.
    public final TreeIndexImpl intersectOnNewImpl(final TreeIndexImpl other) {
        final SortedRangesLong sr = intersect(this, other);
        if (sr == null) {
            return null;
        }
        return makeTreeIndexImplFromLongRangesArray(sr.data, sr.count, sr.cardinality, null);
    }

    public final int count() {
        return count;
    }

    public final boolean subsetOf(final Index.RangeIterator ritOther) {
        try (final Index.RangeIterator rit = getRangeIterator()) {
            while (rit.hasNext()) {
                rit.next();
                final long start = rit.currentRangeStart();
                final boolean valid = ritOther.advance(start);
                if (!valid) {
                    return false;
                }
                final long otherStart = ritOther.currentRangeStart();
                final long otherEnd = ritOther.currentRangeEnd();
                final long end = rit.currentRangeEnd();
                if (otherStart > start || otherEnd < end) {
                    return false;
                }
            }
            return true;
        } finally {
            ritOther.close();
        }
    }

    final TreeIndexImpl minusOnNew(final TreeIndexImpl other) {
        if (!USE_RANGES_ARRAY) {
            return minusOnNewLegacy(other.ixRangeIterator());
        }
        final SortedRangesLong sr = intersect(this, other, true);
        if (sr == null) {
            return null;
        }
        return makeTreeIndexImplFromLongRangesArray(sr.data, sr.count, sr.cardinality, null);
    }

    private SortedRanges minusOnNewLegacy(final Index.RangeIterator ritOther) {
        SortedRanges ans = makeMyTypeAndOffset(2);
        int i = 0;
        long iData = unpackedGet(0);
        boolean ritValid = ritOther.advance(iData);
        long rStart, rEnd;
        if (ritValid) {
            rStart = ritOther.currentRangeStart();
            rEnd = ritOther.currentRangeEnd();
        } else {
            rStart = rEnd = -1;
        }
        boolean iNeg = false;
        long pendingStart = -1;
        while (true) {
            if (iNeg) {
                final long iValue = -iData;
                if (rStart != -1) {
                    if (iValue < rStart) {
                        ans = appendRangeUnpacked(ans, pendingStart, iValue, false);
                        if (ans == null) {
                            return null;
                        }
                    } else {
                        if (pendingStart < rStart) {
                            ans = appendRangeUnpacked(ans, pendingStart, rStart - 1, false);
                            if (ans == null) {
                                return null;
                            }
                        }
                        if (rEnd < iValue) {
                            pendingStart = Math.max(rEnd + 1, pendingStart);
                            if (!ritOther.hasNext()) {
                                ans = appendRangeUnpacked(ans, pendingStart, iValue, false);
                                if (ans == null) {
                                    return null;
                                }
                                rStart = -1;
                            } else {
                                ritValid = ritOther.advance(pendingStart);
                                if (!ritValid) {
                                    rStart = -1;
                                } else {
                                    rStart = ritOther.currentRangeStart();
                                    rEnd = ritOther.currentRangeEnd();
                                    if (!ans.fits(rStart, rEnd)) {
                                        return null;
                                    }
                                }
                                continue;
                            }
                        }
                    }
                } else {
                    ans = appendRangeUnpacked(ans, pendingStart, iValue, false);
                    if (ans == null) {
                        return null;
                    }
                }
                pendingStart = -1;
            } else {
                if (rStart != -1) {
                    if (pendingStart != -1) {
                        if (pendingStart < rStart) {
                            ans = appendUnpacked(ans, pendingStart, false);
                            if (ans == null) {
                                return null;
                            }
                        } else if (pendingStart > rEnd) {
                            if (!ritOther.hasNext()) {
                                ans = appendUnpacked(ans, pendingStart, false);
                                if (ans == null) {
                                    return null;
                                }
                                rStart = -1;
                            } else {
                                ritOther.next();
                                rStart = ritOther.currentRangeStart();
                                rEnd = ritOther.currentRangeEnd();
                                if (!ans.fits(rStart, rEnd)) {
                                    return null;
                                }
                                continue;
                            }
                        }
                    }
                } else {
                    if (pendingStart != -1) {
                        ans = appendUnpacked(ans, pendingStart, false);
                        if (ans == null) {
                            return null;
                        }
                    }
                }
                pendingStart = iData;
            }
            ++i;
            if (i == count) {
                if (pendingStart != -1) {
                    boolean append = rStart == -1 || pendingStart < rStart;
                    if (!append && pendingStart > rEnd) {
                        ritValid = ritOther.advance(pendingStart);
                        append = !ritValid || ritOther.currentRangeStart() != pendingStart;
                    }
                    if (append) {
                        ans = appendUnpacked(ans, pendingStart, false);
                    }
                }
                return ans;
            }
            iData = unpackedGet(i);
            iNeg = iData < 0;
        }
    }

    // !sar.isEmpty() && !otherSar.isEmpty() true on entry.
    public static TreeIndexImpl unionOnNew(final SortedRanges sar, final SortedRanges otherSar) {
        if (!USE_RANGES_ARRAY) {
            return unionOnNewLegacy(sar, otherSar);
        }
        final SortedRangesLong sr = SortedRanges.union(sar, otherSar);
        if (sr == null) {
            return null;
        }
        return makeTreeIndexImplFromLongRangesArray(sr.data, sr.count, sr.cardinality, null);
    }

    public static SortedRanges unionOnNewLegacy(final SortedRanges sar, final SortedRanges otherSar) {
        final long unionFirst = Math.min(sar.first(), otherSar.first());
        final long unionLast = Math.max(sar.last(), otherSar.last());
        final int count = sar.count();
        final int otherCount = otherSar.count();
        final SortedRanges out = SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                Math.max(count, otherCount),
                count + otherCount,
                unionFirst,
                unionLast,
                sar.isDense() && otherSar.isDense());
        if (out != null) {
            try (final Index.RangeIterator sarIter = sar.getRangeIterator();
                 final Index.RangeIterator otherIter = otherSar.getRangeIterator()) {
                SortedRanges.unionOnNewHelper(out, sarIter, otherIter);
            }
        }
        return out;
    }

    // {riter1, riter2}.hasNext() true on entry.
    private static void unionOnNewHelper(SortedRanges out, final Index.RangeIterator riter1, final Index.RangeIterator riter2) {
        riter1.next();
        long start1 = riter1.currentRangeStart();
        long end1 = riter1.currentRangeEnd();
        riter2.next();
        long start2 = riter2.currentRangeStart();
        long end2 = riter2.currentRangeEnd();
        while (true) {
            if (end1 < start2) {
                out = out.appendRange(start1, end1);
                if (riter1.hasNext()) {
                    riter1.next();
                    start1 = riter1.currentRangeStart();
                    end1 = riter1.currentRangeEnd();
                    continue;
                }
                out.appendRange(start2, end2);
                break;
            }
            if (end2 < start1) {
                out = out.appendRange(start2, end2);
                if (riter2.hasNext()) {
                    riter2.next();
                    start2 = riter2.currentRangeStart();
                    end2 = riter2.currentRangeEnd();
                    continue;
                }
                out.appendRange(start1, end1);
                break;
            }
            // ranges overlap.
            if (end1 < end2) {
                out = out.appendRange(Math.min(start1, start2), end2);
                final boolean valid1 = riter1.advance(end2 + 1);
                if (!riter2.hasNext()) {
                    if (valid1) {
                        out = out.appendRange(riter1.currentRangeStart(), riter1.currentRangeEnd());
                    }
                    break;
                }
                if (!valid1) {
                    break;
                }
                riter2.next();
            } else {
                out = out.appendRange(Math.min(start1, start2), end1);
                final boolean valid2 = riter2.advance(end1 + 1);
                if (!riter1.hasNext()) {
                    if (valid2) {
                        out = out.appendRange(riter2.currentRangeStart(), riter2.currentRangeEnd());
                    }
                    break;
                }
                if (!valid2) {
                    break;
                }
                riter1.next();
            }
            start1 = riter1.currentRangeStart();
            end1 = riter1.currentRangeEnd();
            start2 = riter2.currentRangeStart();
            end2 = riter2.currentRangeEnd();
        }
        while (riter1.hasNext()) {
            riter1.next();
            final long start = riter1.currentRangeStart();
            final long end = riter1.currentRangeEnd();
            out = out.appendRange(start, end);
        }
        while (riter2.hasNext()) {
            riter2.next();
            final long start = riter2.currentRangeStart();
            final long end = riter2.currentRangeEnd();
            out = out.appendRange(start, end);
        }
    }

    public final TreeIndexImpl insertImpl(final SortedRanges other) {
        return insertImpl(other, true);
    }

    public final TreeIndexImpl insertImpl(final SortedRanges other, final boolean writeCheck) {
        if (!USE_RANGES_ARRAY) {
            final MutableObject<SortedRanges> holder = new MutableObject<>(this);
            boolean valid = insertInternal(holder, other, writeCheck);
            if (valid) {
                return holder.getValue();
            }
        } else {
            final SortedRangesLong sr = union(this, other);
            if (sr != null) {
                return makeTreeIndexImplFromLongRangesArray(sr.data, sr.count, sr.cardinality,
                        (!writeCheck || canWrite()) ? this : null);
            }
        }
        final RspBitmap rb = ixToRspOnNew();
        rb.insertTreeIndexUnsafeNoWriteCheck(other);
        rb.finishMutations();
        return rb;
    }

    // Assumption: none of the provided SortedRanges are empty.
    // We can't offer a guarantee of returning false means we didn't modify out;
    // we /can/ offer the guarantee that, under a false return, the partial result
    // left in sarHolder can be used to repeat the operation (presumably on a different TreeIndexImpl type)
    // to produce the correct result.
    private static boolean insertInternal(final MutableObject<SortedRanges> sarHolder, final SortedRanges other, final boolean writeCheckArg) {
        int iOther = 0;
        long pendingStart = -1;
        SortedRanges sar = sarHolder.getValue();
        final long otherLast = other.last();
        if (!sar.fits(other.first(), otherLast)) {
            return false;
        }
        final MutableInt iAdd = new MutableInt(0);
        boolean writeCheck = writeCheckArg;
        while (iOther < other.count) {
            final long iData = other.unpackedGet(iOther);
            final boolean iNeg = iData < 0;
            if (iNeg) {
                final long startPacked = sar.pack(pendingStart);
                final long endPacked = sar.pack(-iData);
                final long deltaCard = endPacked - startPacked + 1;
                iAdd.setValue(sar.absRawBinarySearch(startPacked, iAdd.intValue(), sar.count - 1));
                final SortedRanges ans = addRangePackedWithStart(
                        sar, iAdd.intValue(), startPacked, endPacked, pendingStart, -iData, deltaCard, iAdd, writeCheck);
                if (ans == null) {
                    sarHolder.setValue(sar);
                    return false;
                }
                if (sar != ans) {
                    if (!ans.fits(otherLast)) {
                        sarHolder.setValue(sar);
                        return false;
                    }
                    sar = ans;
                    writeCheck = false;
                }
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    final long pendingStartPacked = sar.pack(pendingStart);
                    iAdd.setValue(sar.absRawBinarySearch(pendingStartPacked, iAdd.intValue(), sar.count - 1));
                    final SortedRanges ans = addPackedWithStart(
                            sar, iAdd.intValue(), pendingStartPacked, pendingStart, iAdd, writeCheck);
                    if (ans == null) {
                        sarHolder.setValue(sar);
                        return false;
                    }
                    if (sar != ans) {
                        if (!ans.fits(otherLast)) {
                            sarHolder.setValue(sar);
                            return false;
                        }
                        sar = ans;
                        writeCheck = false;
                    }
                }
                pendingStart = iData;
            }
            ++iOther;
        }
        if (pendingStart != -1) {
            final long pendingStartPacked = sar.pack(pendingStart);
            final int iStart = sar.absRawBinarySearch(pendingStartPacked, iAdd.intValue(), sar.count - 1);
            final SortedRanges ans = addPackedWithStart(
                    sar, iStart, pendingStartPacked, pendingStart, null, writeCheck);
            if (ans == null) {
                sarHolder.setValue(sar);
                return false;
            }
            sar = ans;
        }
        sarHolder.setValue(sar);
        if (DEBUG) sar.validate();
        return true;
    }

    // We can't offer a guarantee of returning null means we didn't modify sar;
    // we /can/ offer the guarantee that, under a false return, the partial result
    // left in sarOut can be used to repeat the operation (presumably on a different TreeIndexImpl type)
    // to produce the correct result.
    // !isEmpty() && rit.hasNext() true on entry.
    static boolean removeLegacy(final MutableObject<SortedRanges> sarOut, final Index.RangeIterator rit) {
        try {
            final MutableInt iRm = new MutableInt(0);
            SortedRanges sar = sarOut.getValue();
            final long first = sar.first();
            final boolean valid = rit.advance(first);
            if (!valid) {
                return true;
            }
            final long last = sar.last();
            boolean writeCheck = true;
            while (true) {
                final long start = rit.currentRangeStart();
                if (start > last) {
                    break;
                }
                long end = Math.min(rit.currentRangeEnd(), last);
                if (end > last) {
                    end = last;
                }
                final long packedStart = sar.pack(start);
                final long packedEnd = sar.pack(end);
                int i = iRm.intValue();
                i = sar.absRawBinarySearch(packedStart, i, sar.count - 1);
                final SortedRanges ans = removeRangePackedWithStart(
                        sar, i, packedStart, packedEnd, start, end, iRm, writeCheck);
                if (ans == null) {
                    sarOut.setValue(sar);
                    return false;
                }
                if (ans != sar) {
                    sar = ans;
                    writeCheck = false;
                }
                if (!rit.hasNext()) {
                    break;
                }
                rit.next();
            }
            sarOut.setValue(sar);
            return true;
        } finally {
            rit.close();
        }
    }

    // !isEmpty() on entry.
    public final TreeIndexImpl invertRangeOnNew(final long start, final long end, final long maxPosition) {
        final long packedStart = pack(start);
        int i = 0;
        long pos = 0;
        long data = packedGet(i);
        boolean neg = false;
        long pendingStart = -1;
        while (true) {
            if (neg) {
                final long rangeEnd = -data;
                if (packedStart <= rangeEnd) {
                    final long packedEnd = pack(end);
                    if (packedEnd > rangeEnd) {
                        return null;
                    }
                    final long rangeOffsetPos = pos - 1;
                    final long resultStart = rangeOffsetPos + packedStart - pendingStart;
                    if (resultStart > maxPosition) {
                        return TreeIndexImpl.EMPTY;
                    }
                    final long resultEnd = Math.min(rangeOffsetPos + packedEnd - pendingStart, maxPosition);
                    return SingleRange.make(resultStart, resultEnd);
                }
                pos += rangeEnd - pendingStart;
            } else {
                if (packedStart <= data) {
                    if (packedStart < data) {
                        return null;
                    }
                    if (end == start || pos == maxPosition) {
                        return SingleRange.make(pos, pos);
                    }
                    if (i + 1 >= count) {
                        return null;
                    }
                    final long nextData = packedGet(i + 1);
                    if (nextData > 0) {
                        return null;
                    }
                    final long nextValue = -nextData;
                    final long packedEnd = pack(end);
                    if (packedEnd > nextValue) {
                        return null;
                    }
                    return SingleRange.make(
                            pos, Math.min(maxPosition, pos + packedEnd - data));
                }
                ++pos;
                pendingStart = data;
            }
            if (pos > maxPosition) {
                return TreeIndexImpl.EMPTY;
            }
            ++i;
            if (i >= count) {
                return null;
            }
            data = packedGet(i);
            neg = data < 0;
        }
    }

    // !isEmpty() && rit.hasNext() true on entry.
    public final boolean invertOnNew(
            final Index.RangeIterator rit,
            final TreeIndexImplSequentialBuilder builder,
            final long maxPosition) {
        rit.next();
        long start = rit.currentRangeStart();
        long end = rit.currentRangeEnd();
        long packedStart = pack(start);
        int i = 0;
        long pos = 0;
        long data = packedGet(i);
        boolean neg = false;
        long pendingStart = -1;
        while (true) {
            if (neg) {
                final long rangeEnd = -data;
                if (packedStart <= rangeEnd) {
                    final long packedEnd = pack(end);
                    if (packedEnd > rangeEnd) {
                        return false;
                    }
                    final long rangeOffsetPos = pos - 1;
                    final long resultStart = rangeOffsetPos + packedStart - pendingStart;
                    if (resultStart > maxPosition) {
                        return true;
                    }
                    final long resultEnd = Math.min(rangeOffsetPos + packedEnd - pendingStart, maxPosition);
                    builder.appendRange(resultStart, resultEnd);
                    if (resultEnd == maxPosition || !rit.hasNext()) {
                        return true;
                    }
                    rit.next();
                    start = rit.currentRangeStart();
                    end = rit.currentRangeEnd();
                    packedStart = pack(start);
                    if (packedStart <= rangeEnd) {
                        pos += packedStart - pendingStart;
                        pendingStart = packedStart;
                        continue;
                    }
                }
                pos += rangeEnd - pendingStart;
            } else {
                if (packedStart <= data) {
                    if (packedStart < data) {
                        return false;
                    }
                    if (end == start || pos == maxPosition) {
                        builder.appendKey(pos);
                        if (pos == maxPosition) {
                            return true;
                        }
                    } else {
                        if (i + 1 >= count) {
                            return false;
                        }
                        final long nextData = packedGet(i + 1);
                        if (nextData > 0) {
                            return false;
                        }
                        final long nextValue = -nextData;
                        final long packedEnd = pack(end);
                        if (packedEnd > nextValue) {
                            return false;
                        }
                        final long resultEnd = pos + packedEnd - data;
                        if (resultEnd >= maxPosition) {
                            builder.appendRange(pos, maxPosition);
                            return true;
                        }
                        builder.appendRange(pos, resultEnd);
                    }
                    if (!rit.hasNext()) {
                        return true;
                    }
                    rit.next();
                    start = rit.currentRangeStart();
                    end = rit.currentRangeEnd();
                    packedStart = pack(start);
                }
                ++pos;
                pendingStart = data;
            }
            if (pos > maxPosition) {
                return true;
            }
            ++i;
            if (i >= count) {
                return false;
            }
            data = packedGet(i);
            neg = data < 0;
        }
    }

    public final OrderedKeys getOrderedKeysByPosition(final long pos, long length) {
        final long card = getCardinality();
        if (isEmpty() || pos >= card) {
            return OrderedKeys.EMPTY;
        }
        if (pos + length >= card) {
            length = card - pos;
        }
        return getOrderedKeysByPositionWithStart(0, 0, pos, length);
    }

    public final OrderedKeys getOrderedKeysByPositionWithStart(
            final long iStartPos, final int istart, final long startPosForOK, final long lengthForOK) {
        int i = istart;
        long iPos = iStartPos;
        long iData = packedGet(i);
        boolean iNeg = false;
        long pendingStart = -1;
        int startIdx = -1;
        int endIdx = -1;
        long startOffset = 1;
        long endOffset = -1;
        while (true) {
            if (iNeg) {
                iPos += -iData - pendingStart - 1;
                if (iPos >= startPosForOK) {
                    startIdx = i;
                    startOffset = startPosForOK - iPos;
                    break;
                }
                pendingStart = -1;
            } else {
                if (iPos >= startPosForOK) {
                    if (i + 1 >= count) {
                        startIdx = i;
                        startOffset = 0;
                        endIdx = i;
                        endOffset = 0;
                        return new SortedRangesOrderedKeys(this, startPosForOK, startIdx, startOffset, endIdx, endOffset, 1L);
                    }
                    final long nextData = packedGet(i + 1);
                    if (nextData < 0) {
                        startIdx = i + 1;
                        startOffset = iData + nextData;
                        iPos += -nextData - iData;
                    } else {
                        startIdx = i;
                        startOffset = 0;
                    }
                    break;
                }
                pendingStart = iData;
            }
            ++i;
            ++iPos;
            iData = packedGet(i);
            iNeg = iData < 0;
        }
        final long endPositionInclusive = startPosForOK + lengthForOK - 1;
        if (iPos >= endPositionInclusive) {
            endIdx = startIdx;
            endOffset = startOffset + lengthForOK - 1;
            return new SortedRangesOrderedKeys(this, startPosForOK, startIdx, startOffset, endIdx, endOffset, lengthForOK);
        }
        i = startIdx + 1;
        ++iPos;
        iData = packedGet(i);
        iNeg = false;
        while (true) {
            if (iNeg) {
                iPos += -iData - pendingStart - 1;
                if (iPos >= endPositionInclusive) {
                    endIdx = i;
                    endOffset = endPositionInclusive - iPos;
                    break;
                }
                pendingStart = -1;
            } else {
                if (iPos >= endPositionInclusive) {
                    if (i + 1 >= count) {
                        endIdx = i;
                        endOffset = 0;
                        break;
                    }
                    final long nextData = packedGet(i + 1);
                    if (nextData < 0) {
                        endIdx = i + 1;
                        endOffset = iData + nextData;
                    } else {
                        endIdx = i;
                        endOffset = 0;
                    }
                    break;
                }
                pendingStart = iData;
            }
            if (i + 1 >= count) {
                endIdx = i;
                endOffset = 0;
                break;
            }
            ++i;
            ++iPos;
            iData = packedGet(i);
            iNeg = iData < 0;
        }
        return new SortedRangesOrderedKeys(this, startPosForOK, startIdx, startOffset, endIdx, endOffset, lengthForOK);
    }

    public final OrderedKeys getOrderedKeysByKeyRange(final long start, final long end) {
        if (isEmpty()) {
            return OrderedKeys.EMPTY;
        }
        final long last = last();
        if (last < start) {
            return OrderedKeys.EMPTY;
        }
        final long first = first();
        if (end < first) {
            return OrderedKeys.EMPTY;
        }
        final long packedStart = pack(Math.max(start, first));
        final long packedEnd = pack(Math.min(end, last));
        return getOrderedKeysByKeyRangePackedWithStart(0, 0, packedStart, packedEnd);
    }

    final OrderedKeys getOrderedKeysByKeyRangePackedWithStart(
            final long iStartPos, final int iStart, final long packedStart, final long packedEnd) {
        int i = iStart;
        long iPos = iStartPos;
        long iData = packedGet(i);
        boolean iNeg = false;
        long pendingStart = iData;
        int startIdx = -1;
        long startOffset = 0;
        long startPos = -1;
        while (true) {
            if (iNeg) {
                final long iValue = -iData;
                iPos += iValue - pendingStart;
                if (iValue >= packedStart) {
                    startPos = iPos + packedStart - pendingStart;
                    startIdx = i;
                    startOffset = packedStart - iValue;
                    break;
                }
                pendingStart = -1;
            } else {
                ++iPos;
                if (iData >= packedStart) {
                    if (iData > packedStart && iData > packedEnd) {
                        return OrderedKeys.EMPTY;
                    }
                    if (i + 1 >= count) {
                        return new SortedRangesOrderedKeys(this, iPos, i, 0, i, 0, 1);
                    }
                    startPos = iPos;
                    final int iNext = i + 1;
                    final long iNextData = packedGet(iNext);
                    final boolean iNextNeg = iNextData < 0;
                    if (iNextNeg) {
                        final long iNextValue = -iNextData;
                        if (iNextValue >= packedEnd) {
                            return new SortedRangesOrderedKeys(
                                    this, iPos,
                                    iNext, iData - iNextValue,
                                    iNext, packedEnd - iNextValue,
                                    packedEnd - iData + 1);
                        }
                        startIdx = iNext;
                        startOffset = iData - iNextValue;
                        pendingStart = iData;
                        i = iNext;
                        iNeg = true;
                        iData = iNextData;
                    } else {
                        if (iNext > packedEnd) {
                            return new SortedRangesOrderedKeys(this, startPos, i, 0, i, 0, 1);
                        }
                        pendingStart = -1;
                        startOffset = 0;
                        startIdx = i;
                        i = iNext;
                        iNeg = false;
                        iData = iNextData;
                    }
                    break;
                }
                pendingStart = iData;
            }
            ++i;
            if (DEBUG && i >= count) {
                throw new IllegalStateException("Broken invariant.");
            }
            iData = packedGet(i);
            iNeg = iData < 0;
        }
        while (true) {
            if (iNeg) {
                final long iValue = -iData;
                if (iValue >= packedEnd) {
                    final long endOffset = packedEnd - iValue;
                    return new SortedRangesOrderedKeys(
                            this, startPos, startIdx, startOffset, i, endOffset,
                            packedEnd - pendingStart + iPos - startPos + 1);
                }
                iPos += iValue - pendingStart;
                pendingStart = -1;
            } else {
                if (iData > packedEnd) {
                    return new SortedRangesOrderedKeys(
                            this, startPos,
                            startIdx, startOffset,
                            i - 1, 0,
                            iPos - startPos + 1);
                }
                ++iPos;
                pendingStart = iData;
            }
            if (i + 1 >= count) {
                return new SortedRangesOrderedKeys(
                        this, startPos, startIdx, startOffset, i, 0, iPos - startPos + 1);
            }
            ++i;
            iData = packedGet(i);
            iNeg = iData < 0;
        }
    }

    public final OrderedKeys.Iterator getOrderedKeysIterator() {
        if (isEmpty()) {
            return OrderedKeys.Iterator.EMPTY;
        }
        return new SortedRangesOrderedKeys.Iterator(
                new SortedRangesOrderedKeys(this));
    }

    public final long getAverageRunLengthEstimate() {
        if (isEmpty()) {
            return 0;
        }
        final int count = count();
        int n = Math.min(9, count);
        int negs = 0;
        for (int i = 1; i < n; ++i) {
            if (packedGet(i) < 0) {
                ++negs;
            }
        }
        final double initialRanges = n - negs;
        final double initialFactor = n / (initialRanges * count);
        return Math.round(initialFactor * getCardinality());
    }

    private static SortedRanges intersectLegacy(
            final SortedRanges sar, final long last, final Index.RangeIterator rangeIter) {
        try {
            // We could do better wrt offset...
            SortedRanges out = sar.makeMyTypeAndOffset(sar.count);
            final MutableInt iOut = new MutableInt(0);
            int i = 0;
            int lasti = i;
            while (true) {
                if (lasti != i) {
                    final boolean valid = rangeIter.advance(sar.unpackedGet(i));
                    if (!valid) {
                        break;
                    }
                }
                final long start = rangeIter.currentRangeStart();
                if (last < start) {
                    break;
                }
                long end = rangeIter.currentRangeEnd();
                end = Math.min(end, last);
                out = intersectRangeImplStep(out, sar, i, start, end, iOut);
                if (out == null) {
                    return null;
                }
                lasti = i;
                i = iOut.intValue();
                if (i >= sar.count) {
                    break;
                }
                if (!rangeIter.hasNext()) {
                    break;
                }
                rangeIter.next();
            }
            return out;
        } finally {
            rangeIter.close();
        }
    }

    private SortedRanges subRangesByKeyPacked(final long packedStart, final long packedEnd) {
        int iStart = absRawBinarySearch(packedStart, 0, count - 1);
        int iEnd = absRawBinarySearch(packedEnd, iStart, count - 1);
        if (iEnd >= count) {
            iEnd = count - 1;
        }

        final long iStartData = packedGet(iStart);
        final boolean iStartNeg = iStartData < 0;
        final long iStartValue = iStartNeg ? -iStartData : iStartData;

        final long iEndData = packedGet(iEnd);
        final boolean iEndNeg = iEndData < 0;
        final long iEndValue = iEndNeg ? -iEndData : iEndData;

        int requiredLen = iEnd - iStart;
        if (iStartNeg && iStartValue > packedStart) {
            ++requiredLen;
        }
        if (iEndNeg) {
            if (iEndValue <= packedEnd) {
                ++requiredLen;
            } else {
                // iEndValue < iEnd
                if (iEnd > 0) {
                    final long iEndPrevValue = packedGet(iEnd - 1);
                    if (iEndPrevValue < iEndValue - 1) {
                        ++requiredLen;
                    }
                }
            }
        } else {
            if (iEndValue <= packedEnd) {
                ++requiredLen;
            }
        }

        int iSrc = iStart;
        final SortedRanges ans = makeMyTypeAndOffset(requiredLen);
        if (iStartNeg) {
            if (packedStart < iStartValue) {
                ans.packedSet(0, packedStart);
                if (packedEnd <= iStartValue || iEnd == iStart) {
                    if (packedEnd > packedStart) {
                        final long end = Math.min(packedEnd, iEndValue);
                        ans.packedSet(1, -end);
                        ans.cardinality = end - packedStart + 1;
                        ans.count = 2;
                    } else {
                        ans.cardinality = 1;
                        ans.count = 1;
                    }
                    if (DEBUG) ans.validate(packedStart, packedEnd);
                    return ans;
                }
                ans.packedSet(1, iStartData);
                ans.count = 2;
                ans.cardinality += iStartValue - packedStart + 1;
            } else {
                ans.packedSet(0, iStartValue);
                ans.count = 1;
                ++ans.cardinality;
                if (iStart == iEnd) {
                    ans.cardinality = 1;
                    ans.count = 1;
                    if (DEBUG) ans.validate(packedStart, packedEnd);
                    return ans;
                }
            }
            ++iSrc;
        } else {
            if (packedEnd < iStartValue) {
                return null;
            }
        }

        long pendingStart = -1;
        while (iSrc < iEnd) {
            final long data = packedGet(iSrc);
            ans.packedSet(ans.count, data);
            final boolean neg = data < 0;
            if (neg) {
                ans.cardinality += -data - pendingStart + 1;
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    ++ans.cardinality;
                }
                pendingStart = data;
            }
            ++iSrc;
            ++ans.count;
        }

        if (iEndNeg) {
            final long end = Math.min(packedEnd, iEndValue);
            ans.packedSet(ans.count++, -end);
            ans.cardinality += end - pendingStart + 1;
        } else {
            if (pendingStart != -1) {
                ++ans.cardinality;
            }
            if (iEndValue <= packedEnd) {
                ++ans.cardinality;
                ans.packedSet(ans.count++, iEndValue);
            }
        }
        if (DEBUG) ans.validate(packedStart, packedEnd);
        return ans;
    }

    protected static int shortArrayCapacityForLastIndex(final int lastIndex) {
        final int c = capacityForLastIndex(lastIndex, SHORT_EXTENT, SHORT_MAX_CAPACITY);
        if (c == 0) {
            return 0;
        }
        return arraySizeRoundingShort(c);
    }

    protected static int intArrayCapacityForLastIndex(final int lastIndex, final boolean isDense) {
        final int c = capacityForLastIndex(lastIndex, INT_EXTENT, isDense ? INT_DENSE_MAX_CAPACITY : INT_SPARSE_MAX_CAPACITY);
        if (c == 0) {
            return 0;
        }
        return arraySizeRoundingInt(c);
    }

    protected static int longArrayCapacityForLastIndex(final int lastIndex, final boolean isDense) {
        return capacityForLastIndex(lastIndex, LONG_EXTENT, isDense ? LONG_DENSE_MAX_CAPACITY : LONG_SPARSE_MAX_CAPACITY);
    }

    protected long cardinality;
    protected int count;

    public abstract boolean fits(long value);
    public abstract boolean fits(long start, long end);
    public abstract boolean fitsForAppend(final long end);
    protected abstract SortedRanges makeMyTypeAndOffset(int initialCapacity);
    protected abstract SortedRanges growOnNew(int capacity);
    protected abstract int packedValuesPerCacheLine();
    protected abstract long packedGet(int i);
    protected final long absPackedGet(final int i) {
        return Math.abs(packedGet(i));
    }
    protected abstract void packedSet(int i, long packedValue);
    protected abstract long pack(long unpackedValue);
    protected abstract long unpackedGet(int i);
    protected abstract long absUnpackedGet(int i);
    protected abstract void unpackedSet(int i, long unpackedValue);
    protected abstract long unpack(long packedValue);
    protected abstract int dataLength();
    protected abstract SortedRanges ensureCanAppend(int newLastPosition, long unpackedNewLastKey, final boolean writeCheck);
    protected abstract void moveData(int srcPos, int dstPos, int len);
    protected abstract void copyData(int newCapacity);

    protected abstract SortedRanges addInternal(long v, boolean writeCheck);
    protected abstract SortedRanges addRangeInternal(long start, long end, boolean writeCheck);
    protected abstract SortedRanges appendInternal(long v, boolean writeCheck);
    protected abstract SortedRanges appendRangeInternal(long start, long end, boolean writeCheck);
    protected abstract SortedRanges removeInternal(long v);
    protected abstract SortedRanges removeRangeInternal(long start, long end);

    protected abstract SortedRanges tryPackFor(long first, long last, int maxPos, boolean isDense);
    protected final SortedRanges tryPackWithNewLast(final long newLastKey, int maxPos, final boolean isDense) {
        return tryPackFor(first(), newLastKey, maxPos, isDense);
    }

    protected abstract SortedRanges tryPack();

    public abstract int bytesAllocated();
    public abstract int bytesUsed();
    /**
     * @param k if k == 0, compact if count < capacity.
     *             k > 0, compact if (capacity - count > (capacity >> k).
     */
    public abstract SortedRanges tryCompactUnsafe(int k);

    public final SortedRanges tryCompact(final int k) {
        if (!canWrite()) {
            return this;
        }
        return tryCompactUnsafe(k);
    }

    // Return a capacity that can contain lastIndex.
    private static int capacityForLastIndex(final int lastIndex, final int extent, final int maxCapacity) {
        if (lastIndex >= maxCapacity) {
            return 0;
        }
        if (extent == maxCapacity) {
            return maxCapacity;
        }
        final int tgtMinCap = lastIndex + 1;
        if (tgtMinCap < extent) {
            final int log2 = 32 - Integer.numberOfLeadingZeros(tgtMinCap);
            int pow2 = 1 << (log2 - 1);
            if (tgtMinCap > pow2) {
                pow2 <<= 1;
            }
            // pow2 >= tgtMinCap.
            if (pow2 <= extent) {
                return pow2;
            }
        }
        // linear in extent increments.
        int e = lastIndex & ~(extent - 1);
        if (e < extent)
            e = extent;
        while (e <= lastIndex)
            e += extent;
        if (e > maxCapacity) {
            e = maxCapacity;
        }
        return e;
    }

    protected static boolean isLongAllocationSize(final int length) {
        return isAllocationSize(length, LONG_EXTENT);
    }

    protected static boolean isIntAllocationSize(final int length) {
        final int beforeRounding = length - 1;  // space for 1 int after a 12 byte object header to an 8-byte boundary.
        return isAllocationSize(beforeRounding, INT_EXTENT);

    }

    protected static boolean isShortAllocationSize(final int length) {
        final int beforeRounding = length - 2;  // space for 2 shorts after a 12 byte object header to an 8-byte boundary.
        return isAllocationSize(beforeRounding, SHORT_EXTENT);
    }

    private static boolean isAllocationSize(final int beforeRounding, final int extent) {
        if (beforeRounding > extent) {
            return (beforeRounding & (extent - 1)) == 0;
        }
        return Integer.bitCount(beforeRounding) == 1;
    }

    /**
     * Run a binary search over the ranges in [pos, count).
     * Assumes pos points to a position of a range start (eg, value at pos can't be negative).
     *
     * Assumes count > startPos on entry.
     *
     * @param unpackedTarget The (unpacked) target value to search for.
     * @param startPos A position in our array pointing to the start of a range from where to start the search.
     * @return r >= 0 if the target value is present.  r is the position of the start of a range containing the target value.
     *         r < 0 if the target value is not present. pos = -r - 1 (== ~r) is the position where the target value would
     *         be inserted; this could be the start of a range which would be expanded in the target value where added,
     *         or the position where it would have to go as a single value range pushing the ranges from there to the right.
     */
    final int unpackedBinarySearch(final long unpackedTarget, final int startPos) {
        final long packedTarget = pack(unpackedTarget);
        return packedBinarySearch(packedTarget, startPos);
    }

    private int packedBinarySearch(final long packedTarget, final int startPos) {
        int minPos = startPos;
        long absMinPosPackedValue = packedGet(minPos);
        if (packedTarget <= absMinPosPackedValue) {
            return (packedTarget < absMinPosPackedValue) ? ~minPos : minPos;
        }
        int maxPos = count - 1;
        long maxPosPackedValue = packedGet(maxPos);
        boolean maxPosNeg = maxPosPackedValue < 0;
        long absMaxPosPackedValue = maxPosNeg ? -maxPosPackedValue : maxPosPackedValue;
        if (absMaxPosPackedValue <= packedTarget) {
            if (absMaxPosPackedValue == packedTarget) {
                if (maxPosNeg) {
                    return count - 2;
                }
                return count - 1;
            }
            return ~count;
        }
        // at this point, we know absPackedGet(minPos) < t && t < absPackedGet(maxPos).
        while (maxPos - minPos > packedValuesPerCacheLine()) {
            int midPos = (minPos + maxPos) / 2;
            final long midPosPackedValue = packedGet(midPos);
            final boolean midPosNeg = midPosPackedValue < 0;
            final long absMidPosPackedValue = midPosNeg ? -midPosPackedValue : midPosPackedValue;
            if (absMidPosPackedValue > packedTarget) {
                maxPos = midPos;
                maxPosNeg = midPosNeg;
                continue;
            }
            if (absMidPosPackedValue < packedTarget) {
                minPos = midPos;
                continue;
            }
            return midPosNeg ? midPos - 1 : midPos;
        }

        for (int i = minPos + 1; i < maxPos; ++i) {
            final long packedValue = packedGet(i);
            final boolean neg = packedValue < 0;
            final long absPackedValue = neg ? -packedValue : packedValue;
            if (packedTarget <= absPackedValue) {
                if (packedTarget == absPackedValue) {
                    return neg ? (i - 1) : i;
                }
                return neg ? i - 1 : ~i;
            }
        }

        return maxPosNeg ? maxPos - 1 : ~maxPos;
    }

    /**
     * Run a binary search over the ranges in [startIdx, endIdx]
     *
     * Assumes count > startIdx on entry.
     *
     * @param packedTarget The (packed) target value to search for.
     * @param startIdx A position in our array pointing to where to start the search.
     * @param endIdx last position (inclusive) for the search.
     * @return A position where either a range containing the target already exists, or where it would be extended,
     *         or a new range inserted if not.  Note this may never be negative but it might be endIdx + 1.
     */
    final int absRawBinarySearch(final long packedTarget, final int startIdx, final int endIdx) {
        final long absEndPosPackedValue = absPackedGet(endIdx);
        if (absEndPosPackedValue <= packedTarget) {
            if (absEndPosPackedValue == packedTarget) {
                return endIdx;
            }
            return endIdx + 1;
        }
        int maxPos = endIdx;
        final long absStartPosPackedValue = absPackedGet(startIdx);
        if (packedTarget <= absStartPosPackedValue) {
            return startIdx;
        }
        int minPos = startIdx;
        // at this point, we know absPackedGet(minPos) < packedTarget && packedTarget < absPackedGet(maxPos).
        while (maxPos - minPos > packedValuesPerCacheLine()) {
            int midPos = (minPos + maxPos) / 2;
            final long absMidPosPackedValue = absPackedGet(midPos);
            if (absMidPosPackedValue > packedTarget) {
                maxPos = midPos;
                continue;
            }
            if (absMidPosPackedValue < packedTarget) {
                minPos = midPos;
                continue;
            }
            return midPos;
        }

        for (int i = minPos + 1; i < maxPos; ++i) {
            final long absPackedValue = absPackedGet(i);
            if (packedTarget <= absPackedValue) {
                return i;
            }
        }

        return maxPos;
    }

    protected abstract SortedRanges checkSizeAndMoveData(
            final int srcPos, final int dstPos, final int len, final long first, boolean writeCheck);

    // Note the returned SortedRangesTreeIndexImpl might have a different offset.
    // packedData >= 0 on entry.
    private SortedRanges open(final int pos, final long packedData, final boolean writeCheck) {
        final long first = (pos == 0) ? unpack(packedData) : first();
        final long offset = unpack(0);
        final SortedRanges ans = checkSizeAndMoveData(pos, pos + 1, count - pos, first, writeCheck);
        if (ans == null) {
            return null;
        } else if (ans == this) {
            packedSet(pos, packedData);
            ++count;
        } else {
            final long dOff = offset - ans.unpack(0);
            ans.packedSet(pos, packedData + dOff);
            ans.count = count + 1;
            ans.cardinality = cardinality;
        }
        return ans;
    }

    // Note the returned SortedRangesTreeIndexImpl might have a different offset.
    // packedData < 0 on entry.
    private SortedRanges openNeg(final int pos, final long packedData, final boolean writeCheck) {
        final long offset = unpack(0);
        final SortedRanges ans = checkSizeAndMoveData(pos, pos + 1, count - pos, first(), writeCheck);
        if (ans == null) {
            return null;
        } else if (ans == this) {
            packedSet(pos, packedData);
            ++count;
        } else {
            final long dOff = offset - ans.unpack(0);
            ans.packedSet(pos, -dOff + packedData);
            ans.count = count + 1;
            ans.cardinality = cardinality;
        }
        return ans;
    }

    // Note the returned SortedRangesTreeIndexImpl might have a different offset.
    // packedData1 > 0 && packedData2 < 0 on entry.
    private SortedRanges open(final int pos, final long packedData1, final long packedData2, final boolean writeCheck) {
        final long first = (pos == 0) ? unpack(packedData1) : first();
        final long offset = unpack(0);
        final SortedRanges ans = checkSizeAndMoveData(pos, pos + 1, count - pos, first, writeCheck);
        if (ans == null) {
            return null;
        } else if (ans == this) {
            packedSet(pos, packedData1);
            packedSet(pos + 1, packedData2);
            ++count;
        } else {
            final long dOff = offset - ans.unpack(0);
            ans.packedSet(pos, packedData1 + dOff);
            ans.packedSet(pos + 1, -dOff + packedData2);
            ans.count = count + 1;
            ans.cardinality = cardinality;
        }
        return ans;
    }

    // Note the returned SortedRangesTreeIndexImpl might have a different offset.
    // packedData1 < 0 && packedData2 > 0 on entry.
    private SortedRanges openNeg(final int pos, final long packedData1, final long packedData2, final boolean writeCheck) {
        final long offset = unpack(0);
        final SortedRanges ans = checkSizeAndMoveData(pos, pos + 1, count - pos, first(), writeCheck);
        if (ans == null) {
            return null;
        } else if (ans == this) {
            packedSet(pos, packedData1);
            packedSet(pos + 1, packedData2);
            ++count;
        } else {
            final long dOff = offset - ans.unpack(0);
            ans.packedSet(pos, -dOff + packedData1);
            ans.packedSet(pos + 1, packedData2 + dOff);
            ans.count = count + 1;
            ans.cardinality = cardinality;
        }
        return ans;
    }

    // Note the returned SortedRangesTreeIndexImpl might have a different offset.
    // packedData1 >= 0 && packedData2 < 0 on entry.
    private SortedRanges open2(final int pos, final long packedData1, final long packedData2, final boolean writeCheck) {
        final long first = (pos == 0) ? unpack(packedData1) : first();
        final long offset = unpack(0);
        final SortedRanges ans = checkSizeAndMoveData(pos, pos + 2, count - pos, first, writeCheck);
        if (ans == null) {
            return null;
        } else if (ans == this) {
            packedSet(pos, packedData1);
            packedSet(pos + 1, packedData2);
            count += 2;
        } else {
            final long dOff = offset - ans.unpack(0);
            ans.packedSet(pos, packedData1 + dOff);
            ans.packedSet(pos + 1, -dOff + packedData2);
            ans.count = count + 2;
            ans.cardinality = cardinality;
        }
        return ans;
    }

    // Note the returned SortedRangesTreeIndexImpl might have a different offset.
    // packedData1 < 0 && packedData2 > 0 on entry.
    private SortedRanges open2Neg(final int pos, final long packedData1, final long packedData2, final boolean writeCheck) {
        final long offset = unpack(0);
        final SortedRanges ans = checkSizeAndMoveData(pos, pos + 2, count - pos, first(), writeCheck);
        if (ans == null) {
            return null;
        } else if (ans == this) {
            packedSet(pos, packedData1);
            packedSet(pos + 1, packedData2);
            count += 2;
        } else {
            final long dOff = offset - ans.unpack(0);
            ans.packedSet(pos, -dOff + packedData1);
            ans.packedSet(pos + 1, packedData2 + dOff);
            ans.count = count + 2;
            ans.cardinality = cardinality;
        }
        return ans;
    }

    protected final void close(final int pos) {
        moveData(pos + 1, pos, count - pos - 1);
        --count;
    }

    protected final void close2(final int pos) {
        moveData(pos + 2, pos, count - pos - 2);
        count -= 2;
    }

    protected static SortedRanges addPacked(SortedRanges sar, final long packedValue, final long value, final boolean writeCheck) {
        if (sar.count == 0) {
            if (writeCheck) {
                sar = sar.getWriteRef();
            }
            sar.packedSet(0, packedValue);
            sar.count = 1;
            sar.cardinality = 1;
            return sar;
        }
        final int iStart = sar.absRawBinarySearch(packedValue, 0, sar.count - 1);
        return addPackedWithStart(sar, iStart, packedValue, value, null, writeCheck);
    }

    // sar.count > 0 assumed on entry.
    // if iStartOut != null, this method stores in iStartOut the position from where to continue adding later values.
    protected static SortedRanges addPackedWithStart(
            SortedRanges sar, final int iStart, final long packedValue, final long value,
            final MutableInt iStartOut, final boolean writeCheck) {
        int i = iStart;
        if (i == sar.count) {
            int j = sar.count - 1;
            long jData = sar.packedGet(j);
            boolean jNeg = jData < 0;
            long jValue = jNeg ? -jData : jData;
            if (jValue == packedValue - 1) {
                if (jNeg) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    ++sar.cardinality;
                    sar.packedSet(j, -packedValue);
                    if (iStartOut != null) {
                        iStartOut.setValue(j + 1);
                    }
                    if (DEBUG) sar.validate(packedValue, packedValue);
                    return sar;
                }
                sar = sar.packedAppend(-packedValue, -value, writeCheck);
                if (sar == null) {
                    return null;
                }
                ++sar.cardinality;
                if (iStartOut != null) {
                    iStartOut.setValue(sar.count);
                }
                if (DEBUG) sar.validate(packedValue, packedValue);
                return sar;
            }
            sar = sar.packedAppend(packedValue, value, writeCheck);
            if (sar == null) {
                return null;
            }
            ++sar.cardinality;
            if (iStartOut != null) {
                iStartOut.setValue(sar.count);
            }
            if (DEBUG) sar.validate(packedValue, packedValue);
            return sar;
        }


        final long iData = sar.packedGet(i);
        final boolean iNeg = iData < 0;
        final long iValue = iNeg ? -iData : iData;
        if (packedValue == iValue || iNeg) {
            return sar;
        }

        boolean mergeToLeftRange = false;
        boolean mergeToLeftSingle = false;
        if (i > 0) {  // check for merge to left range.
            final int j = i - 1;
            final long jData = sar.packedGet(j);
            final boolean jNeg = jData < 0;
            final long jValue = jNeg ? -jData : jData;
            if (jValue == packedValue - 1) {
                if (jNeg) {
                    mergeToLeftRange = true;
                } else {
                    mergeToLeftSingle = true;
                }
            }
        }
        boolean mergeToRightRange = false;
        boolean mergeToRightSingle = false;
        if (iValue == packedValue + 1) {
            if (i < sar.count - 1) {  // check to merge to right range.
                final int j = i + 1;
                final long jData = sar.packedGet(j);
                final boolean jNeg = jData < 0;
                if (jNeg) {
                    mergeToRightRange = true;
                } else {
                    mergeToRightSingle = true;
                }
            } else {
                mergeToRightSingle = true;
            }
        }

        if (mergeToLeftRange) {
            if (writeCheck) {
                sar = sar.getWriteRef();
            }
            ++sar.cardinality;
            if (mergeToRightRange) {
                sar.close2(i - 1);
                if (iStartOut != null) {
                    iStartOut.setValue(i - 1);
                }
            } else if (mergeToRightSingle) {
                sar.close(i);
                sar.packedSet(i - 1, -(packedValue + 1));
                if (iStartOut != null) {
                    iStartOut.setValue(i - 2);
                }
            } else {
                sar.packedSet(i - 1, -packedValue);
                if (iStartOut != null) {
                    iStartOut.setValue(i);
                }
            }
        } else if (mergeToLeftSingle) {
            if (mergeToRightRange) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                ++sar.cardinality;
                sar.close(i);
                if (iStartOut != null) {
                    iStartOut.setValue(i);
                }
            } else if (mergeToRightSingle) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                ++sar.cardinality;
                sar.packedSet(i, -(packedValue + 1));
                if (iStartOut != null) {
                    iStartOut.setValue(i - 1);
                }
            } else {
                sar = sar.openNeg(i, -packedValue, writeCheck);
                if (sar == null) {
                    return null;
                }
                ++sar.cardinality;
                if (iStartOut != null) {
                    iStartOut.setValue(i + 1);
                }
            }
        } else {
            if (mergeToRightRange) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                ++sar.cardinality;
                sar.packedSet(i, packedValue);
                if (iStartOut != null) {
                    iStartOut.setValue(i + 1);
                }
            } else if (mergeToRightSingle) {
                sar = sar.open(i, packedValue, -(packedValue + 1), writeCheck);
                if (sar == null) {
                    return null;
                }
                ++sar.cardinality;
                if (iStartOut != null) {
                    iStartOut.setValue(i);
                }
            } else {
                sar = sar.open(i, packedValue, writeCheck);
                if (sar == null) {
                    return null;
                }
                ++sar.cardinality;
                if (iStartOut != null) {
                    iStartOut.setValue(i + 1);
                }
            }
        }
        if (DEBUG) sar.validate(packedValue, packedValue);
        return sar;
    }

    // iDst < iSrc.
    final void collapse(final int iDst, final int iSrc) {
        if (iSrc <= iDst) {
            return;
        }
        if (iSrc < count) {
            moveData(iSrc, iDst,count - iSrc);
            count -= iSrc - iDst;
            return;
        }
        count = iDst;
    }

    protected static SortedRanges addRangePacked(
            SortedRanges sar, long packedStart, long packedEnd, final long start, final long end, final boolean writeCheck) {
        final long deltaCard = packedEnd - packedStart + 1;
        if (deltaCard == 1) {
            return addPacked(sar, packedStart, start, writeCheck);
        }

        if (sar.count == 0) {
            if (writeCheck) {
                sar = sar.getWriteRef();
            }
            sar.packedSet(0, packedStart);
            sar.packedSet(1, -packedEnd);
            sar.count = 2;
            sar.cardinality = deltaCard;
            if (DEBUG) sar.validate(packedStart, packedEnd);
            return sar;
        }

        final int iStart = sar.absRawBinarySearch(packedStart, 0, sar.count - 1);
        return addRangePackedWithStart(sar, iStart, packedStart, packedEnd, start, end, deltaCard, null, writeCheck);
    }

    // Assumption: sar is not empty.
    // packedStart != packedEnd assumed on entry,
    // if iStartOut != null, this method stores in iStartOut the position from where to continue adding later ranges.
    protected static SortedRanges addRangePackedWithStart(
            SortedRanges sar, int iStart,
            long packedStart, long packedEnd, final long start, final long end,
            long deltaCard, final MutableInt iStartOut, final boolean writeCheck) {
        if (iStart == sar.count) {
            int j = sar.count - 1;
            long jData = sar.packedGet(j);
            boolean jNeg = jData < 0;
            long jValue = jNeg ? -jData : jData;
            if (jValue == packedStart - 1) {
                if (jNeg) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(j, -packedEnd);
                    sar.cardinality += deltaCard;
                    if (iStartOut != null) {
                        iStartOut.setValue(j + 1);
                    }
                    if (DEBUG) sar.validate(packedStart, packedEnd);
                    return sar;
                }
                sar = sar.packedAppend(-packedEnd, -end, writeCheck);
                if (sar == null) {
                    return null;
                }
                sar.cardinality += deltaCard;
                if (iStartOut != null) {
                    iStartOut.setValue(sar.count);
                }
                if (DEBUG) sar.validate(packedStart, packedEnd);
                return sar;
            }
            sar = sar.packedAppend2(packedStart, -packedEnd, start, -end, writeCheck);
            if (sar == null) {
                return null;
            }
            sar.cardinality += deltaCard;
            if (iStartOut != null) {
                iStartOut.setValue(sar.count);
            }
            if (DEBUG) sar.validate(packedStart, packedEnd);
            return sar;
        }
        long iStartData = sar.packedGet(iStart);
        boolean iStartNeg = iStartData < 0;
        long iStartValue = iStartNeg ? -iStartData : iStartData;
        if (packedEnd <= iStartValue && iStartNeg) {
            // the whole [packedStart, packedEnd] range was contained in an existing range.
            if (iStartOut != null) {
                iStartOut.setValue(iStart);
            }
            if (DEBUG) sar.validate(packedStart, packedEnd);
            return sar;
        }

        // we will find a beginning of range (or single) with no intersection to the range to be added,
        // which might result in adjusting iStart, packedStart and deltaCard.
        boolean mergeToLeftRange = false;
        boolean mergeToLeftSingle = false;
        if (packedStart == iStartValue) {
            ++iStart;
            if (iStart == sar.count) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                if (iStartNeg) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(iStart - 1, -packedEnd);
                    if (iStartOut != null) {
                        iStartOut.setValue(sar.count);
                    }
                } else {
                    sar = sar.packedAppend(-packedEnd, -end, writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(sar.count);
                    }
                }
                sar.cardinality += deltaCard - 1;
                if (DEBUG) sar.validate(packedStart, packedEnd);
                return sar;
            }
            iStartData = sar.packedGet(iStart);
            if (iStartNeg) {
                mergeToLeftRange = true;
                ++packedStart;
                --deltaCard;
                iStartNeg = false;
                iStartValue = iStartData;
            } else {
                iStartNeg = iStartData < 0;
                iStartValue = iStartNeg ? -iStartData : iStartData;
                if (!iStartNeg) {
                    --deltaCard;
                    mergeToLeftSingle = true;
                } else {
                    if (packedEnd <= -iStartData) {
                        // the whole [packedStart, packedEnd] range was contained in an existing range.
                        if (iStartOut != null) {
                            iStartOut.setValue(iStart - 1);
                        }
                        if (DEBUG) sar.validate(packedStart, packedEnd);
                        return sar;
                    }
                    ++iStart;
                    if (iStart == sar.count) {
                        if (iStartValue >= packedEnd) {
                            if (iStartOut != null) {
                                iStartOut.setValue(sar.count - 2);
                            }
                            if (DEBUG) sar.validate(packedStart, packedEnd);
                            return sar;
                        }
                        if (writeCheck) {
                            sar = sar.getWriteRef();
                        }
                        sar.packedSet(sar.count - 1, -packedEnd);
                        if (iStartOut != null) {
                            iStartOut.setValue(sar.count - 2);
                        }
                        sar.cardinality += packedEnd - iStartValue;
                        if (DEBUG) sar.validate(packedStart, packedEnd);
                        return sar;
                    }
                    deltaCard -= iStartValue - packedStart + 1;
                    packedStart = iStartValue + 1;
                    iStartNeg = false;
                    iStartValue = sar.packedGet(iStart);
                    mergeToLeftRange = true;
                }
            }
        } else if (iStartNeg) {
            if (iStart == sar.count - 1) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.cardinality += packedEnd - iStartValue;
                sar.packedSet(iStart, -packedEnd);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart + 1);
                }
                if (DEBUG) sar.validate(packedStart, packedEnd);
                return sar;
            }
            ++iStart;
            packedStart = iStartValue + 1;
            deltaCard = packedEnd - packedStart + 1;
            iStartNeg = false;
            iStartValue = sar.packedGet(iStart);
            mergeToLeftRange = true;
        } else if (iStart > 0) {
            final int j = iStart - 1;
            final long jData = sar.packedGet(j);
            final boolean jNeg = jData < 0;
            final long jValue = jNeg ? -jData : jData;
            if (jValue == packedStart - 1) {
                if (jNeg) {
                    mergeToLeftRange = true;
                } else {
                    mergeToLeftSingle = true;
                }
            }
        }

        // iStart now points to the beginning of a range (or single).

        // We will find the last beginning of range (or single) with no intersection to the range to be added
        // and store it in iEnd; we may need to adjust packedEnd and deltaCard.
        int iEnd;
        long pendingStart = -1;
        long betweenCard = 0;  // will accumulate existing cardinality between iStart and iEnd.
        boolean mergeToRightRange = false;
        boolean mergeToRightSingle = false;
        int i = iStart;
        boolean iNeg = iStartNeg;
        long iValue = iStartValue;
        while (true) {
            if (packedEnd <= iValue) {
                if (iNeg) {
                    deltaCard -= packedEnd - pendingStart + 1;
                    packedEnd = pendingStart - 1;
                    mergeToRightRange = true;
                    iEnd = i - 1;
                } else {
                    if (iValue <= packedEnd + 1) {
                        if (iValue == packedEnd) {
                            --packedEnd;
                            --deltaCard;
                        }
                        if (i < sar.count - 1) {
                            final int j = i + 1;
                            final long jData = sar.packedGet(j);
                            if (jData < 0) {
                                mergeToRightRange = true;
                            } else {
                                mergeToRightSingle = true;
                            }
                        } else {
                            mergeToRightSingle = true;
                        }
                    }
                    if (pendingStart != -1) {
                        ++betweenCard;
                    }
                    iEnd = i;
                }
                pendingStart = -1;
                break;
            }
            if (iNeg) {
                betweenCard += iValue - pendingStart + 1;
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    ++betweenCard;
                }
                pendingStart = iValue;
            }
            ++i;
            if (i == sar.count) {
                iEnd = sar.count;
                break;
            }
            final long iData = sar.packedGet(i);
            iNeg = iData < 0;
            iValue = iNeg ? -iData : iData;
        }
        if (pendingStart != -1) {
            ++betweenCard;
        }

        final int len = iEnd - iStart;
        if (mergeToLeftRange) {
            if (writeCheck) {
                sar = sar.getWriteRef();
            }
            if (mergeToRightRange) {
                sar.collapse(iStart - 1, iEnd + 1);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart - 2);
                }
            } else if (mergeToRightSingle) {
                sar.packedSet(iStart - 1, -(packedEnd + 1));
                sar.collapse(iStart, iEnd + 1);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart - 2);
                }
            } else {
                sar.packedSet(iStart - 1, -packedEnd);
                sar.collapse(iStart, iEnd);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart);
                }
            }
        } else if (mergeToLeftSingle) {
            if (mergeToRightRange) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.collapse(iStart, iEnd + 1);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart - 1);
                }
            } else if (mergeToRightSingle) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.packedSet(iStart, -(packedEnd + 1));
                sar.collapse(iStart + 1, iEnd + 1);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart - 1);
                }
            } else {
                if (len > 0) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(iStart, -packedEnd);
                    sar.collapse(iStart + 1, iEnd);
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 1);
                    }
                } else {
                    sar = sar.openNeg(iStart, -packedEnd, writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 1);
                    }
                }
            }
        } else {
            if (mergeToRightRange) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.packedSet(iStart, packedStart);
                sar.collapse(iStart + 1, iEnd + 1);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart);
                }
            } else if (mergeToRightSingle) {
                if (len > 0) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(iStart, packedStart);
                    sar.packedSet(iStart + 1, -(packedEnd + 1));
                    sar.collapse(iStart + 2, iEnd + 1);
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart);
                    }
                } else {
                    sar = sar.open(iStart, packedStart, -(packedEnd + 1), writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart);
                    }
                }
            } else {
                if (len == 0) {
                    sar = sar.open2(iStart, packedStart, -packedEnd, writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 2);
                    }
                } else if (len == 1) {
                    sar = sar.open(iStart, packedStart, -packedEnd, writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 2);
                    }
                } else {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(iStart, packedStart);
                    sar.packedSet(iStart + 1, -packedEnd);
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 2);
                    }
                }
                sar.collapse(iStart + 2, iEnd);
            }
        }
        sar.cardinality += deltaCard - betweenCard;
        if (DEBUG) sar.validate(packedStart, packedEnd);
        return sar;
    }

    private static SortedRanges appendUnpacked(final SortedRanges sar, final long value, final boolean writeCheck) {
        if (!sar.fits(value)) {
            return null;
        }
        return appendPacked(sar, sar.pack(value), value, writeCheck);
    }

    protected static SortedRanges appendPacked(SortedRanges sar, final long packedValue, final long value, final boolean writeCheck) {
        if (sar.count == 0) {
            if (writeCheck) {
                sar = sar.getWriteRef();
            }
            sar.cardinality = 1;
            sar.count = 1;
            sar.packedSet(0, packedValue);
            if (DEBUG) sar.validate(packedValue, packedValue);
            return sar;
        }
        final long lastData = sar.packedGet(sar.count - 1);
        final boolean lastNeg = lastData < 0;
        final long lastValue = lastNeg ? -lastData : lastData;
        if (packedValue <= lastValue + 1) {
            if (packedValue <= lastValue) {
                throw new IllegalArgumentException("Trying to append v=" + packedValue + " when last=" + lastValue);
            }
            // packedValue == lastValue + 1.
            if (lastNeg) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.packedSet(sar.count - 1, -packedValue);
                ++sar.cardinality;
                if (DEBUG) sar.validate(packedValue, packedValue);
                return sar;
            }
            sar = sar.packedAppend(-packedValue, -value, writeCheck);
            if (sar == null) {
                return null;
            }
            ++sar.cardinality;
            if (DEBUG) sar.validate(packedValue, packedValue);
            return sar;
        }
        sar = sar.packedAppend(packedValue, value, writeCheck);
        if (sar == null) {
            return null;
        }
        ++sar.cardinality;
        if (DEBUG) sar.validate(packedValue, packedValue);
        return sar;
    }

    protected static SortedRanges appendRangeUnpacked(
            final SortedRanges sar, final long start, final long end, final boolean writeCheck) {
        if (!sar.fits(start, end)) {
            return null;
        }
        return appendRangePacked(sar, sar.pack(start), sar.pack(end), start, end, writeCheck);
    }

    protected static SortedRanges appendRangePacked(
            SortedRanges sar,
            final long packedStart, final long packedEnd, final long start, final long end,
            final boolean writeCheck) {
        final long deltaCard = packedEnd - packedStart + 1;
        if (deltaCard == 1) {
            return appendPacked(sar, packedStart, start, writeCheck);
        }
        if (sar.count == 0) {
            if (writeCheck) {
                sar = sar.getWriteRef();
            }
            sar.packedSet(0, packedStart);
            sar.packedSet(1, -packedEnd);
            sar.cardinality = deltaCard;
            sar.count = 2;
            if (DEBUG) sar.validate(packedStart, packedEnd);
            return sar;
        }
        final long lastData = sar.packedGet(sar.count - 1);
        final boolean lastNeg = lastData < 0;
        final long lastValue = lastNeg ? -lastData : lastData;
        if (packedStart <= lastValue + 1) {
            if (packedStart <= lastValue) {
                throw new IllegalArgumentException(
                        "Trying to append start=" + packedStart + " end=" + packedEnd + " when last=" + lastValue);
            }
            // packedValue == lastValue + 1.
            if (lastNeg) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.cardinality += deltaCard;
                sar.packedSet(sar.count - 1, -packedEnd);
                if (DEBUG) sar.validate(packedStart, packedEnd);
                return sar;
            }
            sar = sar.packedAppend(-packedEnd, -end, writeCheck);
            if (sar == null) {
                return null;
            }
            sar.cardinality += deltaCard;
            if (DEBUG) sar.validate(packedStart, packedEnd);
            return sar;
        }
        sar = sar.packedAppend2(packedStart, -packedEnd, start, -end, writeCheck);
        if (sar == null) {
            return null;
        }
        sar.cardinality += deltaCard;
        if (DEBUG) sar.validate(packedStart, packedEnd);
        return sar;
    }

    protected static SortedRanges removePacked(SortedRanges sar, final long packedValue, final long value) {
        if (sar.count == 0) {
            return sar;
        }
        int j = sar.count - 1;
        long jData = sar.packedGet(j);
        boolean jNeg = jData < 0;
        long jValue = jNeg ? -jData : jData;
        if (jValue <= packedValue) {
            if (jValue < packedValue) {
                return sar;
            }
            // vj == packedValue.
            sar = sar.getWriteRef();
            --sar.cardinality;
            if (jNeg) {
                int i = j - 1;
                long iValue = sar.packedGet(i);
                if (iValue == packedValue - 1) {
                    --sar.count;
                    if (DEBUG) sar.validate(packedValue, packedValue);
                    return sar;
                }
                sar.packedSet(j, -(packedValue - 1));
                if (DEBUG) sar.validate(packedValue, packedValue);
                return sar;
            }
            --sar.count;
            if (DEBUG) sar.validate(packedValue, packedValue);
            return sar;
        }

        int i = 0;
        long iData = sar.packedGet(i);
        boolean iNeg = iData < 0;
        long iValue = iNeg ? -iData : iData;
        if (packedValue <= iValue) {
            if (packedValue < iValue) {
                return sar;
            }
            // vi == packedValue.
            sar = sar.getWriteRef();
            --sar.cardinality;
            // sar.count > 1 since otherwise we would have bailed earlier.
            j = i + 1;
            jData = sar.packedGet(j);
            jNeg = jData < 0;
            if (!jNeg) {
                sar.close(i);
                if (DEBUG) sar.validate(packedValue, packedValue);
                return sar;
            }
            jValue = -jData;
            if (packedValue + 1 == jValue) {
                sar.close(i);
                sar.packedSet(i, jValue);
                if (DEBUG) sar.validate(packedValue, packedValue);
                return sar;
            }
            sar.packedSet(i, packedValue + 1);
            if (DEBUG) sar.validate(packedValue, packedValue);
            return sar;
        }

        if (sar.count == 2) {  // sar.count can't be 1: we would have returned earlier.
            if (!jNeg) {
                return sar;
            }
            // i, j and friends as per previously assigned values;
            // at this point we know ej == true and vi < packedValue < vj
        } else {
            j = sar.absRawBinarySearch(packedValue, 1, sar.count - 2);
            jData = sar.packedGet(j);
            jNeg = jData < 0;
            jValue = jNeg ? -jData : jData;
            if (jValue == packedValue) {
                sar = sar.getWriteRef();
                --sar.cardinality;
                if (!jNeg) {
                    i = j + 1;
                    iData = sar.packedGet(i);
                    iNeg = iData < 0;
                    if (!iNeg) {
                        sar.close(j);
                        if (DEBUG) sar.validate(packedValue, packedValue);
                        return sar;
                    }
                    iValue = -iData;
                    if (packedValue + 1 == iValue) {
                        sar.close(j);
                        sar.packedSet(j, iValue);
                        if (DEBUG) sar.validate(packedValue, packedValue);
                        return sar;
                    }
                    sar.packedSet(j, packedValue + 1);
                    return sar;
                }
                i = j - 1;
                iValue = sar.packedGet(i);
                if (iValue == packedValue - 1) {
                    sar.close(j);
                    if (DEBUG) sar.validate(packedValue, packedValue);
                    return sar;
                }
                sar.packedSet(j, -(packedValue - 1));
                if (DEBUG) sar.validate(packedValue, packedValue);
                return sar;
            }
            if (!jNeg) {
                return sar;
            }
            i = j - 1;
            iValue = sar.packedGet(i);
            // at this point we know ej = true and vi < packedValue < vj.
        }

        if (iValue == packedValue - 1) {
            if (jValue == packedValue + 1) {
                sar = sar.getWriteRef();
                --sar.cardinality;
                sar.packedSet(j, jValue);
                if (DEBUG) sar.validate(packedValue, packedValue);
                return sar;
            }
            sar = sar.open(j, packedValue + 1, true);
            if (sar == null) {
                return null;
            }
            --sar.cardinality;
            if (DEBUG) sar.validate(packedValue, packedValue);
            return sar;
        }
        if (jValue == packedValue + 1) {
            sar = sar.openNeg(j, -(packedValue - 1), jValue, true);
            if (sar == null) {
                return null;
            }
            --sar.cardinality;
            if (DEBUG) sar.validate(packedValue, packedValue);
            return sar;
        }
        sar = sar.open2Neg(j, -(packedValue - 1), packedValue + 1, true);
        if (sar == null) {
            return null;
        }
        --sar.cardinality;
        if (DEBUG) sar.validate(packedValue, packedValue);
        return sar;
    }

    protected static SortedRanges removeRangePacked(
            SortedRanges sar, final long packedStart, final long packedEnd, final long start, final long end) {
        final int iStart = sar.absRawBinarySearch(packedStart, 0, sar.count - 1);
        return removeRangePackedWithStart(sar, iStart, packedStart, packedEnd, start, end, null, true);
    }

    // if iStartOut != null, this method stores in iStartOut the position from where to continue removing later ranges.
    protected static SortedRanges removeRangePackedWithStart(
            SortedRanges sar, int iStart,
            final long packedStart, final long packedEnd,
            final long start, final long end,
            final MutableInt iStartOut, final boolean writeCheck) {
        // iStart will be adjusted to be the start index of the positions to be eliminated from the array.
        if (iStart >= sar.count) {
            if (iStartOut != null) {
                iStartOut.setValue(sar.count);
            }
            return sar;
        }
        long iStartData = sar.packedGet(iStart);
        boolean iStartNeg = iStartData < 0;
        long iStartValue = iStartNeg ? -iStartData : iStartData;
        if (iStart == 0 && packedEnd < iStartValue) {
            return sar;
        }
        boolean truncateLeftRange = false;
        long deltaCard = 0;
        long pendingStart = -1;
        if (iStartValue == packedStart) {
            if (iStartNeg) {
                final long pre = sar.packedGet(iStart - 1);
                if (pre < packedStart - 1) {
                    truncateLeftRange = true;
                }
                if (iStart + 1 >= sar.count) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    if (truncateLeftRange) {
                        sar.packedSet(iStart, -(packedStart - 1));
                        --sar.cardinality;
                        if (iStartOut != null) {
                            iStartOut.setValue(iStart + 1);
                        }
                        if (DEBUG) sar.validate(packedStart, packedEnd);
                        return sar;
                    }
                    --sar.count;
                    --sar.cardinality;
                    if (iStartOut != null) {
                        iStartOut.setValue(sar.count);
                    }
                    if (DEBUG) sar.validate(packedStart, packedEnd);
                    return sar;
                }
                pendingStart = iStartValue;
            } else {
                if (iStart + 1 >= sar.count) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    --sar.count;
                    --sar.cardinality;
                    if (iStartOut != null) {
                        iStartOut.setValue(sar.count);
                    }
                    if (DEBUG) sar.validate(packedStart, packedEnd);
                    return sar;
                }
            }
        } else if (iStartNeg) {
            deltaCard = Math.min(packedEnd, iStartValue) - packedStart + 1;
            final long pre = sar.packedGet(iStart - 1);
            if (pre < packedStart - 1) {
                truncateLeftRange = true;
            }
        }

        boolean truncateRightRange = false;
        boolean truncateRightSingle = false;
        int i = iStart;
        boolean iNeg = iStartNeg;
        long iValue = iStartValue;
        // iEndExclusive will be set to mark the end index (exclusive) of the positions to be eliminated from the array.
        int iEndExclusive = -1;
        while (true) {
            if (packedEnd <= iValue) {
                if (iNeg) {
                    if (pendingStart != -1) {
                        deltaCard += packedEnd - pendingStart + 1;
                    }
                    if (packedEnd < iValue) {
                        if (iValue > packedEnd + 1) {
                            truncateRightRange = true;
                        } else {
                            truncateRightSingle = true;
                        }
                        iEndExclusive = i;
                    } else {
                        iEndExclusive = i + 1;
                    }
                } else {
                    if (pendingStart != -1) {
                        ++deltaCard;
                    }
                    if (packedEnd < iValue) {
                        iEndExclusive = i;
                    } else {
                        ++deltaCard;
                        iEndExclusive = i + 1;
                        if (iEndExclusive < sar.count) {
                            final long nextData = sar.packedGet(iEndExclusive);
                            final boolean nextNeg = nextData < 0;
                            if (nextNeg) {
                                final long nextValue = -nextData;
                                if (nextValue > packedEnd + 1) {
                                    truncateRightRange = true;
                                } else {
                                    truncateRightSingle = true;
                                }
                            }
                        }
                    }
                }
                break;
            }
            if (iNeg) {
                if (pendingStart != -1) {
                    deltaCard += iValue - pendingStart + 1;
                    pendingStart = -1;
                }
            } else {
                if (pendingStart != -1) {
                    ++deltaCard;
                }
                pendingStart = iValue;
            }
            ++i;
            if (i >= sar.count) {
                iEndExclusive = i;
                if (pendingStart != -1) {
                    ++deltaCard;
                }
                break;
            }
            final long iData = sar.packedGet(i);
            iNeg = iData < 0;
            iValue = iNeg ? -iData : iData;
        }
        final int len = iEndExclusive - iStart;
        if (truncateLeftRange) {
            if (truncateRightRange) {
                if (len > 1) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(iStart, -(packedStart - 1));
                    sar.packedSet(iStart + 1, packedEnd + 1);
                    sar.collapse(iStart + 2, iEndExclusive);
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 1);
                    }
                } else {  // len == 0; it can't be the case that len==1 if we are truncating at both sides.
                    sar = sar.open2Neg(iStart, -(packedStart - 1), packedEnd + 1, writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 1);
                    }
                }
            } else if (truncateRightSingle) {
                if (len > 0) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(iStart, -(packedStart - 1));
                    sar.packedSet(iStart + 1, packedEnd + 1);
                    sar.collapse(iStart + 2, iEndExclusive + 1);
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 1);
                    }
                } else {  // len == 0.
                    sar = sar.openNeg(iStart, -(packedStart -1), packedEnd + 1, writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 1);
                    }
                }
            } else {
                // len > 0, since we are only truncating on the left.
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.packedSet(iStart, -(packedStart - 1));
                sar.collapse(iStart + 1, iEndExclusive);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart + 1);
                }
            }
        } else {
            if (truncateRightRange) {
                if (len > 0) {
                    if (writeCheck) {
                        sar = sar.getWriteRef();
                    }
                    sar.packedSet(iStart, packedEnd + 1);
                    sar.collapse(iStart + 1, iEndExclusive);
                    if (iStartOut != null) {
                        iStartOut.setValue(iStart + 1);
                    }
                } else {  // len == 0.
                    sar = sar.open(iEndExclusive, packedEnd + 1, writeCheck);
                    if (sar == null) {
                        return null;
                    }
                    if (iStartOut != null) {
                        iStartOut.setValue(iEndExclusive);
                    }
                }
            } else if (truncateRightSingle) {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.packedSet(iStart, packedEnd + 1);
                sar.collapse(iStart + 1, iEndExclusive + 1);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart + 1);
                }
            } else {
                if (writeCheck) {
                    sar = sar.getWriteRef();
                }
                sar.collapse(iStart, iEndExclusive);
                if (iStartOut != null) {
                    iStartOut.setValue(iStart);
                }
            }
        }
        sar.cardinality -= deltaCard;
        if (DEBUG) sar.validate(packedStart, packedEnd);
        return sar;
    }

    protected final void validate(final long iv1, final long iv2) {
        validate(null, iv1, iv2);
    }
    protected final void validate(String strArg, final long iv1, final long iv2) {
        final String str = strArg == null ? "" : strArg + " ";
        final String msg = str + "(" + iv1 + "," + iv2 + ")";
        long sz = 0;
        boolean eprev = false;
        long vprev = -1;
        for (int i = 0; i < count; ++i) {
            final String cmsg = "i=" + i + msg;
            final long di = packedGet(i);
            final boolean ei = di < 0;
            final long vi = ei ? -di : di;
            if (i == 0) {
                if (ei)
                    throw new IllegalStateException(cmsg + ": negative at i=0");
                ++sz;
            } else {
                if (ei) {
                    if (eprev) {
                        throw new IllegalStateException(cmsg + ": two consecutive negatives i=" + i);
                    }
                    long delta = vi - vprev;
                    if (delta < 1) {
                        throw new IllegalStateException(cmsg + ": range delta=" + delta + " at i=" + i);
                    }
                    sz += delta;
                }
                else {
                    if (vi - vprev < 2) {
                        throw new IllegalStateException(cmsg + ": adjacent not merged at i=" + i);
                    }
                    ++sz;
                }
                if (vprev >= vi) {
                    throw new IllegalStateException(cmsg + ": out of order at i=" + i);
                }
            }
            eprev = ei;
            vprev = vi;
        }
        if (sz != cardinality) {
            throw new IllegalStateException(msg + " wrong cardinality=" + cardinality + " should be " + sz);
        }
    }

    //
    // TreeIndexImpl methods.
    //

    static void checkEquals(final TreeIndexImpl expected, final TreeIndexImpl ans) {
        checkEquals(expected, ans, null);
    }

    static void checkEquals(final TreeIndexImpl expected, final TreeIndexImpl ans, final TreeIndexImpl orig) {
        ans.ixValidate();
        final long expCard = expected.ixCardinality();
        final long ansCard = ans.ixCardinality();
        final boolean failedCard = expCard != ansCard;
        final boolean expSubset = expected.ixSubsetOf(ans);
        final boolean ansSubset = ans.ixSubsetOf(expected);
        if (failedCard || !expSubset || !ansSubset) {
            throw new IllegalStateException(
                    (failedCard ? "cardinality" : "subset") +
                    " check failed for for " +
                            "expected(" + expCard + ")=" + expected +
                            ", ans(" + ansCard + ")=" + ans +
                            ((orig == null) ? "" : ", orig(" + orig.ixCardinality() + ")=" + orig));
        }
    }

    @Override
    public final SortedRanges ixCowRef() {
        return cowRef();
    }

    @Override
    public final void ixRelease() {
        release();
    }

    @Override
    public final int ixRefCount() {
        return refCount();
    }

    @Override
    public final TreeIndexImpl ixInsert(final long key) {
        final SortedRanges ans = add(key);
        if (ans != null) {
            return ans;
        }
        final RspBitmap rb = ixToRspOnNew();
        rb.addUnsafeNoWriteCheck(key);
        rb.finishMutations();
        return rb;
    }

    @Override
    public final TreeIndexImpl ixInsertRange(final long startKey, final long endKey) {
        final SortedRanges ans = addRange(startKey, endKey);
        if (ans != null) {
            return ans;
        }
        final RspBitmap rb = ixToRspOnNew();
        rb.addRangeUnsafeNoWriteCheck(startKey, endKey);
        rb.finishMutations();
        return rb;
    }

    @Override
    public final TreeIndexImpl ixInsertSecondHalf(final LongChunk<Attributes.OrderedKeyIndices> keys, final int offset, final int length) {
        return ixInsert(TreeIndexImpl.fromChunk(keys, offset, length, true));
    }

    @Override
    public final TreeIndexImpl ixRemoveSecondHalf(final LongChunk<Attributes.OrderedKeyIndices> keys, final int offset, final int length) {
        return ixRemove(TreeIndexImpl.fromChunk(keys, offset, length, true));
    }

    @Override
    public final TreeIndexImpl ixAppendRange(final long startKey, final long endKey) {
        final SortedRanges ans = appendRange(startKey, endKey);
        if (ans != null) {
            return ans;
        }
        final RspBitmap rb = ixToRspOnNew();
        rb.appendRangeUnsafeNoWriteCheck(startKey, endKey);
        rb.finishMutations();
        return rb;
    }

    @Override
    public final TreeIndexImpl ixRemove(final long key) {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        final SortedRanges ans = remove(key);
        if (ans != null) {
            if (ans.isEmpty()) {
                return TreeIndexImpl.EMPTY;
            }
            return ans;
        }
        final RspBitmap rb = ixToRspOnNew();
        rb.removeUnsafeNoWriteCheck(key);
        rb.finishMutations();
        return rb;
    }

    @Override
    public final long ixLastKey() {
        return isEmpty() ? Index.NULL_KEY : last();
    }

    @Override
    public final long ixFirstKey() {
        return isEmpty() ? Index.NULL_KEY : first();
    }

    @Override
    public final boolean ixForEachLong(final LongAbortableConsumer lc) {
        return forEachLong(lc);
    }

    @Override
    public final boolean ixForEachLongRange(LongRangeAbortableConsumer lrac) {
        return forEachLongRange(lrac);
    }

    @Override
    public final TreeIndexImpl ixSubindexByPosOnNew(final long startPos, final long endPosExclusive) {
        if (endPosExclusive <= startPos || endPosExclusive <= 0 || startPos >= getCardinality()) {
            return TreeIndexImpl.EMPTY;
        }
        if (startPos == 0 && endPosExclusive >= getCardinality()) {
            return this.cowRef();
        }
        final SortedRanges ans = subRangesByPos(startPos, endPosExclusive - 1);
        return ans;
    }

    @Override
    public final TreeIndexImpl ixSubindexByKeyOnNew(final long startKey, final long endKey) {
        final SortedRanges ans = subRangesByKey(startKey, endKey);
        if (ans == null) {
            return TreeIndexImpl.EMPTY;
        }
        return ans;
    }

    @Override
    public final long ixGet(final long pos) {
        return get(pos);
    }

    @Override
    public final void ixGetKeysForPositions(final PrimitiveIterator.OfLong inputPositions, final LongConsumer outputKeys) {
        getKeysForPositions(inputPositions, outputKeys);
    }

    @Override
    public final long ixFind(final long key) {
        return find(key);
    }

    @Override
    public final ReadOnlyIndex.Iterator ixIterator() {
        return getIterator();
    }

    @Override
    public final ReadOnlyIndex.SearchIterator ixSearchIterator() {
        return getSearchIterator();
    }

    @Override
    public final ReadOnlyIndex.SearchIterator ixReverseIterator() {
        return getReverseIterator();
    }

    @Override
    public final ReadOnlyIndex.RangeIterator ixRangeIterator() {
        return getRangeIterator();
    }

    @Override
    public final long ixCardinality() {
        return getCardinality();
    }

    @Override
    public final boolean ixIsEmpty() {
        return isEmpty();
    }

    @Override
    public final TreeIndexImpl ixUpdate(final TreeIndexImpl added, final TreeIndexImpl removed) {
        if (isEmpty()) {
            return added.ixCowRef();
        }
        if (!removed.ixIsEmpty()) {
            if (removed instanceof SingleRange) {
                final TreeIndexImpl removeResult = ixRemoveRange(removed.ixFirstKey(), removed.ixLastKey());
                return removeResult.ixInsert(added);
            }
            final TreeIndexImpl ans = remove(removed);
            if (ans == null) {
                return ixToRspOnNew().ixUpdateNoWriteCheck(added, removed);
            }
            return ans.ixInsert(added);
        }
        // removed.isEmpty() is true.
        if (added.ixIsEmpty()) {
            return this;
        }
        if (added instanceof SingleRange) {
            return ixInsertRange(added.ixFirstKey(), added.ixLastKey());
        }
        if (added instanceof SortedRanges) {
            final SortedRanges addedSar = (SortedRanges) added;
            return insertImpl(addedSar);
        }
        return ixToRspOnNew().ixUpdate(added, removed);
    }

    @Override
    public final TreeIndexImpl ixRemove(final TreeIndexImpl removed) {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        if (removed.ixIsEmpty()) {
            return this;
        }
        if (removed instanceof SingleRange) {
            return ixRemoveRange(removed.ixFirstKey(), removed.ixLastKey());
        }
        final TreeIndexImpl ans = remove(removed);
        if (ans != null) {
            return ans;
        }
        return toRsp().ixRemoveNoWriteCheck(removed);
    }

    public final TreeIndexImpl remove(final TreeIndexImpl removed) {
        if (!USE_RANGES_ARRAY) {
            try (final ReadOnlyIndex.RangeIterator removedIter = removed.ixRangeIterator()){
                final MutableObject<SortedRanges> holder = new MutableObject<>(this);
                final boolean valid = removeLegacy(holder, removedIter);
                if (!valid) {
                    return null;
                }
                final SortedRanges ans = holder.getValue();
                if (ans.isEmpty()) {
                    return TreeIndexImpl.EMPTY;
                }
                return ans;
            }
        }
        final SortedRangesLong sr = intersect(this, removed, true);
        if (sr == null) {
            return null;
        }
        return makeTreeIndexImplFromLongRangesArray(sr.data, sr.count, sr.cardinality, canWrite() ? this : null);
    }

    @Override
    public final TreeIndexImpl ixRemoveRange(final long startKey, final long endKey) {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        final SortedRanges ans = removeRange(startKey, endKey);
        if (ans == null) {
            final RspBitmap rb = ixToRspOnNew();
            rb.removeRangeUnsafeNoWriteCheck(startKey, endKey);
            rb.finishMutations();
            return rb;
        }
        if (ans.isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        return ans;
    }

    @Override
    public final TreeIndexImpl ixRetain(final TreeIndexImpl toIntersect) {
        if (toIntersect.ixIsEmpty() ||
                isEmpty() ||
                toIntersect.ixLastKey() < first() ||
                last() < toIntersect.ixFirstKey()) {
            return TreeIndexImpl.EMPTY;
        }
        if (!canWrite()) {
            final TreeIndexImpl ix = ixIntersectOnNew(toIntersect);
            return ix;
        }
        if (toIntersect instanceof SingleRange) {
            return ixRetainRange(toIntersect.ixFirstKey(), toIntersect.ixLastKey());
        }
        if (toIntersect instanceof SortedRanges) {
            return retain(toIntersect);
        }
        return ixToRspOnNew().ixRetainNoWriteCheck(toIntersect);
    }

    @Override
    public final TreeIndexImpl ixRetainRange(final long start, final long end) {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        final long first = first();
        if (end < first) {
            return TreeIndexImpl.EMPTY;
        }
        final long last = last();
        if (last < start) {
            return TreeIndexImpl.EMPTY;
        }
        final SortedRanges ans = retainRange(Math.max(start, first), Math.min(end, last));
        if (ans == null) {
            return ixToRspOnNew().ixRetainRangeNoWriteCheck(start, end);
        }
        if (ans.isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        return ans;
    }

    @Override
    public final TreeIndexImpl ixIntersectOnNew(final TreeIndexImpl toIntersect) {
        if (toIntersect instanceof SingleRange) {
            return ixSubindexByKeyOnNew(toIntersect.ixFirstKey(), toIntersect.ixLastKey());
        }
        return intersectOnNew(toIntersect);
    }

    public final TreeIndexImpl intersectOnNew(final TreeIndexImpl toIntersect) {
        if (isEmpty() || toIntersect.ixIsEmpty() ||
                last() < toIntersect.ixFirstKey() ||
                toIntersect.ixLastKey() < first()) {
            return TreeIndexImpl.EMPTY;
        }
        if (!USE_RANGES_ARRAY) {
            final ReadOnlyIndex.RangeIterator rangeIter = toIntersect.ixRangeIterator();
            rangeIter.advance(first());
            final long last = last();
            final SortedRanges sr = intersectLegacy(this, last, rangeIter);
            if (sr != null) {
                return sr;
            }
        } else {
            final TreeIndexImpl ans = intersectOnNewImpl(toIntersect);
            if (ans != null) {
                return ans;
            }
        }
        return ixToRspOnNew().ixRetainNoWriteCheck(toIntersect);
    }

    @Override
    public final boolean ixContainsRange(final long start, final long end) {
        return containsRange(start, end);
    }

    @Override
    public final boolean ixOverlaps(final TreeIndexImpl impl) {
        if (impl.ixIsEmpty()) {
            return false;
        }
        if (isEmpty()) {
            return false;
        }
        if (impl instanceof SingleRange) {
            return overlapsRange(impl.ixFirstKey(), impl.ixLastKey());
        }
        final Index.RangeIterator it = impl.ixRangeIterator();
        return overlaps(it);
    }

    @Override
    public final boolean ixOverlapsRange(final long start, final long end) {
        return overlapsRange(start, end);
    }

    @Override
    public final boolean ixSubsetOf(final TreeIndexImpl other) {
        if (isEmpty()) {
            return true;
        }
        if (other.ixIsEmpty()) {
            return false;
        }
        final long last = last();
        final long otherFirst = other.ixFirstKey();
        if (last < otherFirst) {
            return false;
        }
        final long first = first();
        final long otherLast = other.ixLastKey();
        if (otherLast < first) {
            return false;
        }
        if (other instanceof SingleRange) {
            return otherFirst <= first && last <= otherLast;
        }
        if (getCardinality() > other.ixCardinality()) {
            return false;
        }
        final Index.RangeIterator rit = other.ixRangeIterator();
        return subsetOf(rit);
    }

    @Override
    public final TreeIndexImpl ixMinusOnNew(final TreeIndexImpl other) {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        if (other.ixIsEmpty() ||
                last() < other.ixFirstKey() ||
                other.ixLastKey() < first()) {
            return cowRef();
        }
        if (other instanceof SingleRange) {
            SortedRanges ans = deepCopy();
            ans = ans.removeRange(other.ixFirstKey(), other.ixLastKey());
            if (ans != null) {
                return ans;
            }
        } else {
            final TreeIndexImpl ans = minusOnNew(other);
            if (ans != null) {
                return ans;
            }
        }
        return ixToRspOnNew().ixRemoveNoWriteCheck(other);
    }

    @Override
    public final TreeIndexImpl ixUnionOnNew(final TreeIndexImpl other) {
        if (other.ixIsEmpty()) {
            if (isEmpty()) {
                return TreeIndexImpl.EMPTY;
            }
            return cowRef();
        }
        if (isEmpty()) {
            return other.ixCowRef();
        }
        if (other instanceof SingleRange) {
            final long start = other.ixFirstKey();
            final long end = other.ixLastKey();
            SortedRanges ans = deepCopy();
            ans = ans.addRange(start, end);
            if (ans != null) {
                return ans;
            }
            final RspBitmap rb = ixToRspOnNew();
            rb.addRangeUnsafeNoWriteCheck(start, end);
            rb.finishMutations();
            return rb;
        }
        if (other instanceof SortedRanges) {
            final SortedRanges otherSar = (SortedRanges) other;
            final TreeIndexImpl out = SortedRanges.unionOnNew(this, otherSar);
            if (out != null) {
                return out;
            }
        }
        return ixToRspOnNew().ixInsertNoWriteCheck(other);
    }

    @Override
    public final TreeIndexImpl ixShiftOnNew(final long shiftAmount) {
        final SortedRanges ans = applyShiftOnNew(shiftAmount);
        if (ans != null) {
            return ans;
        }
        return toRsp().applyOffsetNoWriteCheck(shiftAmount);
    }

    @Override
    public final TreeIndexImpl ixShiftInPlace(final long shiftAmount) {
        final SortedRanges ans = applyShift(shiftAmount);
        if (ans != null) {
            return ans;
        }
        return toRsp().applyOffsetNoWriteCheck(shiftAmount);
    }

    @Override
    public final TreeIndexImpl ixInsertWithShift(final long shiftAmount, final TreeIndexImpl other) {
        if (other.ixIsEmpty()) {
            return this;
        }
        if (ixIsEmpty()) {
            return other.ixShiftOnNew(shiftAmount);
        }
        if (other instanceof SingleRange) {
            final long start = other.ixFirstKey() + shiftAmount;
            final long end = other.ixLastKey() + shiftAmount;
            final SortedRanges ans = addRange(start, end);
            if (ans != null) {
                return ans;
            }
            RspBitmap rspAns = toRsp();
            rspAns.addRangeUnsafeNoWriteCheck(start, end);
            rspAns.finishMutations();
            return rspAns;
        }
        if (other instanceof SortedRanges) {
            SortedRanges sr = (SortedRanges) other;
            sr = sr.applyShiftOnNew(shiftAmount);
            return ixInsertImpl(sr);
        }
        RspBitmap rsp = (RspBitmap) other;
        rsp = rsp.applyOffsetOnNew(shiftAmount);
        rsp.insertTreeIndexUnsafeNoWriteCheck(this);
        rsp.finishMutations();
        return rsp;
    }

    private TreeIndexImpl ixInsertImpl(final SortedRanges addedSar) {
        return insertImpl(addedSar);
    }

    @Override
    public final TreeIndexImpl ixInsert(final TreeIndexImpl added) {
        if (added.ixIsEmpty()) {
            if (isEmpty()) {
                return TreeIndexImpl.EMPTY;
            }
            return this;
        }
        if (isEmpty()) {
            return added.ixCowRef();
        }
        if (added instanceof SingleRange) {
            SortedRanges ans = addRange(added.ixFirstKey(), added.ixLastKey());
            if (ans != null) {
                return ans;
            }
            final RspBitmap rb = ixToRspOnNew();
            rb.addRangeUnsafeNoWriteCheck(added.ixFirstKey(), added.ixLastKey());
            rb.finishMutations();
            return rb;
        }
        if (added instanceof SortedRanges) {
            final SortedRanges addedSar = (SortedRanges) added;
            return ixInsertImpl(addedSar);
        }
        final RspBitmap rsp = ixToRspOnNew();
        rsp.orEqualsUnsafeNoWriteCheck((RspBitmap) added);
        rsp.finishMutations();
        return rsp;
    }

    @Override
    public final OrderedKeys ixGetOrderedKeysByPosition(final long startPositionInclusive, final long length) {
        return getOrderedKeysByPosition(startPositionInclusive, length);
    }

    @Override
    public final OrderedKeys ixGetOrderedKeysByKeyRange(final long startKeyInclusive, final long endKeyInclusive) {
        return getOrderedKeysByKeyRange(startKeyInclusive, endKeyInclusive);
    }

    @Override
    public final OrderedKeys.Iterator ixGetOrderedKeysIterator() {
        return getOrderedKeysIterator();
    }

    @Override
    public final long ixRangesCountUpperBound() {
        return count();
    }

    @Override
    public final long ixGetAverageRunLengthEstimate() {
        return getAverageRunLengthEstimate();
    }

    @Override
    public final RspBitmap ixToRspOnNew() {
        return toRsp();
    }

    public final RspBitmap toRsp() {
        RspBitmap rsp = new RspBitmap();
        forEachLongRange((final long start, final long end) -> {
            rsp.appendRangeUnsafeNoWriteCheck(start, end);
            return true;
        });
        rsp.finishMutations();
        sortedRangesToRspConversions.sample(1);
        return rsp;
    }

    @Override
    public final TreeIndexImpl ixInvertOnNew(final TreeIndexImpl keys, final long maxPosition) {
        if (keys.ixIsEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        if (keys instanceof SingleRange) {
            final TreeIndexImpl r = invertRangeOnNew(keys.ixFirstKey(), keys.ixLastKey(), maxPosition);
            if (r != null) {
                return r;
            }
        } else {
            final Index.RangeIterator rit = keys.ixRangeIterator();
            final TreeIndexImplSequentialBuilder builder = new TreeIndexImplSequentialBuilder();
            if (invertOnNew(rit, builder, maxPosition)) {
                return builder.getTreeIndexImpl();
            }
        }
        throw new IllegalArgumentException("keys argument has elements not in the index");
    }

    @Override
    public final TreeIndexImpl ixCompact() {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        if (!hasMoreThanOneRange()) {
            return SingleRange.make(first(), last());
        }
        return tryCompact(4);
    }

    @Override
    public final void ixValidate(final String failMsg) {
        validate(failMsg, -1, -1);
    }
}

