package io.deephaven.engine.rowset.impl.sortedranges;

import gnu.trove.map.hash.TIntObjectHashMap;

public final class SortedRangesLong extends SortedRangesTyped<long[]> {

    @Override
    public boolean fits(final long v) {
        return true;
    }

    @Override
    public boolean fits(final long start, final long end) {
        return true;
    }

    @Override
    public boolean fitsForAppend(final long end) {
        return true;
    }

    @Override
    public SortedRangesLong deepCopy() {
        return new SortedRangesLong(copyData(this), count, cardinality);
    }

    @Override
    protected int packedValuesPerCacheLine() {
        return 8;
    }

    @Override
    protected int capacityForLastIndex(final int lastIndex, final boolean isDense) {
        return longArrayCapacityForLastIndex(lastIndex, isDense);
    }

    private static ThreadLocal<TIntObjectHashMap<long[]>> ARRAY_POOL =
            ThreadLocal.withInitial(() -> new TIntObjectHashMap<>(16));

    @Override
    protected long[] makeArray(final int capacity) {
        if (POOL_ARRAYS) {
            final TIntObjectHashMap<long[]> localPool = ARRAY_POOL.get();
            final long[] arr = localPool.remove(capacity);
            if (arr != null) {
                return arr;
            }
        }
        return new long[capacity];
    }

    @Override
    protected void freeArray(final long[] arr) {
        if (!POOL_ARRAYS) {
            return;
        }
        // Only pool sizes that are allocation increments, not the result of trimming.
        if (!isLongAllocationSize(arr.length)) {
            return;
        }
        final TIntObjectHashMap<long[]> localPool = ARRAY_POOL.get();
        localPool.put(arr.length, arr);
    }

    @Override
    protected SortedRangesLong makeMyTypeAndOffset(final int initialCapacity) {
        return new SortedRangesLong(initialCapacity);
    }

    @Override
    protected int dataLength() {
        return data.length;
    }

    @Override
    protected long packedGet(final int i) {
        return data[i];
    }

    @Override
    protected void packedSet(final int i, final long value) {
        data[i] = value;
    }

    @Override
    protected long pack(final long value) {
        return value;
    }

    @Override
    protected long unpackedGet(final int i) {
        return data[i];
    }

    @Override
    protected long absUnpackedGet(final int i) {
        return Math.abs(data[i]);
    }

    @Override
    protected void unpackedSet(final int i, final long value) {
        data[i] = value;
    }

    @Override
    protected long unpack(final long rawValue) {
        return rawValue;
    }

    public SortedRangesLong() {
        this(INITIAL_SIZE);
    }

    public SortedRangesLong(final int initialCapacity) {
        super(initialCapacity);
    }

    public SortedRangesLong(final long[] data) {
        super(data);
    }

    public SortedRangesLong(final long[] data, final int count, final long cardinality) {
        super(data, count, cardinality);
    }

    public static SortedRangesLong makeSingleRange(final long start, final long end) {
        return new SortedRangesLong(start, end);
    }

    private SortedRangesLong(final long start, final long end) {
        super(2);
        if (start == end) {
            count = 1;
            cardinality = 1;
            packedSet(0, start);
            if (DEBUG)
                validate(start, end);
            return;
        }
        count = 2;
        cardinality = end - start + 1;
        packedSet(0, start);
        packedSet(1, -end);
        if (DEBUG)
            validate(start, end);
    }

    @Override
    public SortedRanges addInternal(final long v, final boolean writeCheck) {
        return addPacked(this, v, v, writeCheck);
    }

    @Override
    public SortedRanges addRangeInternal(final long start, final long end, final boolean writeCheck) {
        return addRangePacked(this, start, end, start, end, writeCheck);
    }

    @Override
    public final SortedRanges appendInternal(final long v, final boolean writeCheck) {
        return appendPacked(this, v, v, writeCheck);
    }

    @Override
    public final SortedRanges appendRangeInternal(final long start, final long end, final boolean writeCheck) {
        return appendRangePacked(this, start, end, start, end, writeCheck);
    }

    @Override
    public SortedRanges removeInternal(final long v) {
        return removePacked(this, v, v);
    }

    @Override
    public SortedRanges removeRangeInternal(final long start, final long end) {
        return removeRangePacked(this, start, end, start, end);
    }

    private static void shiftValues(
            final long[] dataOut, final long shiftOffset, final SortedRangesLong sar, long v) {
        dataOut[0] = v + shiftOffset;
        for (int i = 1; i < sar.count; ++i) {
            v = sar.data[i];
            dataOut[i] = v < 0 ? v - shiftOffset : v + shiftOffset;
        }
    }

    @Override
    public SortedRanges applyShift(final long shiftOffset) {
        if (shiftOffset == 0 || isEmpty()) {
            return this;
        }
        long v = data[0];
        if (v + shiftOffset < 0) {
            throw new IllegalArgumentException("shiftOffset=" + shiftOffset + " when first=" + v);
        }
        final boolean isNew = !canWrite();
        final long[] targetData = isNew ? new long[data.length] : data;
        shiftValues(targetData, shiftOffset, this, v);
        if (isNew) {
            return new SortedRangesLong(targetData, count, cardinality);
        }
        if (DEBUG)
            validate();
        return this;
    }

    @Override
    public SortedRanges applyShiftOnNew(final long shiftOffset) {
        if (shiftOffset == 0 || isEmpty()) {
            acquire();
            return this;
        }
        long v = data[0];
        if (v + shiftOffset < 0) {
            throw new IllegalArgumentException("shiftOffset=" + shiftOffset + " when first=" + v);
        }
        final long[] targetData = new long[data.length];
        shiftValues(targetData, shiftOffset, this, v);
        return new SortedRangesLong(targetData, count, cardinality);
    }

    @Override
    public SortedRanges tryPackFor(final long first, final long last, final int newLastPos, final boolean isDense) {
        final long range = last - first;
        if (range > Integer.MAX_VALUE) {
            return null;
        }
        final int initialIntCapacity = intArrayCapacityForLastIndex(newLastPos, isDense);
        if (initialIntCapacity == 0) {
            return null;
        }
        return new SortedRangesInt(first, this, initialIntCapacity);
    }

    @Override
    public SortedRanges tryMakePackedType(final int maxPos, final long first, final boolean isDense) {
        final long range = last() - first;
        if (range > Integer.MAX_VALUE) {
            return null;
        }
        final int initialIntCapacity = intArrayCapacityForLastIndex(maxPos, isDense);
        if (initialIntCapacity == 0) {
            return null;
        }
        return new SortedRangesInt(initialIntCapacity, first);
    }

    @Override
    protected SortedRangesLong growOnNew(final int capacity) {
        final SortedRangesLong ans = new SortedRangesLong(capacity);
        System.arraycopy(data, 0, ans.data, 0, count);
        ans.cardinality = cardinality;
        ans.count = count;
        if (POOL_ARRAYS && canWrite()) {
            freeArray(data);
        }
        return ans;
    }

    @Override
    public int bytesAllocated() {
        return data.length * Long.BYTES;
    }

    @Override
    public int bytesUsed() {
        return count * Long.BYTES;
    }


    @Override
    protected SortedRanges tryPack() {
        final long first = first();
        final long last = last();
        final long range = last - first;
        if (range > Integer.MAX_VALUE) {
            return null;
        }
        final SortedRanges sr = (range > Short.MAX_VALUE)
                ? new SortedRangesInt(count, first)
                : new SortedRangesShort(count, first);
        copyTo(sr);
        return sr;
    }

    // This method differs from appendRange in that it will not grow the data array;
    // it will only try to append and fail (return false) if there isn't enough space.
    boolean trySimpleAppend(final long start, final long end) {
        if (count >= data.length) {
            return false;
        }
        data[count++] = start;
        if (start != end) {
            if (count >= data.length) {
                return false;
            }
            data[count++] = -end;
            cardinality += end - start + 1;
        } else {
            ++cardinality;
        }
        return true;
    }

    void reset() {
        count = 0;
        cardinality = 0;
    }

    @Override
    public boolean isDense() {
        return isDenseLong(data, count);
    }
}
