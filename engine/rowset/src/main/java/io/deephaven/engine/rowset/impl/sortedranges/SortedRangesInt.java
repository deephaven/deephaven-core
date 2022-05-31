package io.deephaven.engine.rowset.impl.sortedranges;

import gnu.trove.map.hash.TIntObjectHashMap;

public final class SortedRangesInt extends SortedRangesPacked<int[]> {

    @Override
    public SortedRangesInt deepCopy() {
        return new SortedRangesInt(copyData(this), offset, count, cardinality);
    }

    @Override
    protected int packedValuesPerCacheLine() {
        return 16;
    }

    @Override
    protected SortedRangesInt makeMyTypeAndOffset(final int initialCapacity) {
        return new SortedRangesInt(initialCapacity, offset);
    }

    private static ThreadLocal<TIntObjectHashMap<int[]>> ARRAY_POOL =
            ThreadLocal.withInitial(() -> new TIntObjectHashMap<>(16));

    @Override
    protected int[] makeArray(final int capacity) {
        final int roundedCapacity = arraySizeRoundingInt(capacity);
        if (POOL_ARRAYS) {
            final TIntObjectHashMap<int[]> localPool = ARRAY_POOL.get();
            final int[] arr = localPool.remove(roundedCapacity);
            if (arr != null) {
                return arr;
            }
        }
        return new int[roundedCapacity];
    }

    @Override
    public void freeArray(final int[] arr) {
        if (!POOL_ARRAYS) {
            return;
        }
        // Only pool sizes that are allocation increments, not the result of trimming.
        if (!isIntAllocationSize(arr.length)) {
            return;
        }
        final TIntObjectHashMap<int[]> localPool = ARRAY_POOL.get();
        localPool.put(arr.length, arr);
    }

    @Override
    protected int dataLength() {
        return data.length;
    }

    @Override
    protected long maxValueForType() {
        return Integer.MAX_VALUE;
    }

    @Override
    protected int capacityForLastIndex(final int lastIndex, final boolean isDense) {
        return intArrayCapacityForLastIndex(lastIndex, isDense);
    }

    @Override
    protected long unpackedGet(final int i) {
        return unpack(data[i]);
    }

    @Override
    protected long absUnpackedGet(final int i) {
        final int iData = data[i];
        if (iData < 0) {
            return offset() - iData;
        }
        return offset() + iData;
    }

    @Override
    protected void unpackedSet(final int i, final long v) {
        data[i] = (int) pack(v);
    }

    @Override
    protected long packedGet(final int i) {
        return data[i];
    }

    @Override
    protected void packedSet(final int i, final long v) {
        data[i] = (int) v;
    }

    public SortedRangesInt(final int initialCapacity, final long offset) {
        super(initialCapacity, offset);
    }

    public SortedRangesInt(final int[] data, final long offset, final int count, final long cardinality) {
        super(data, offset, count, cardinality);
    }

    public SortedRangesInt(final long offset, final SortedRangesLong sa, final int initialCapacity) {
        super(fromLongArray(sa.data, sa.count, offset, initialCapacity), offset, sa.count, sa.cardinality);
    }

    private static int[] fromLongArray(final long[] longArray, final int count,
            final long newOffset, final int capacity) {
        final int[] intArray = new int[capacity];
        for (int i = 0; i < count; ++i) {
            final long v = longArray[i];
            final int iv = (int) (v < 0 ? newOffset + v : v - newOffset);
            intArray[i] = iv;
        }
        return intArray;
    }

    @Override
    protected SortedRanges tryPackFor(final long first, final long last, final int maxPos, final boolean isDense) {
        final long range = last - first;
        if (range > Short.MAX_VALUE) {
            return null;
        }
        final int initialShortCapacity = shortArrayCapacityForLastIndex(maxPos);
        if (initialShortCapacity == 0) {
            return null;
        }
        return new SortedRangesShort(first, this, initialShortCapacity);
    }

    @Override
    public SortedRanges tryMakePackedType(final int maxPos, final long first, final boolean isDense) {
        final long range = last() - first;
        if (range > Short.MAX_VALUE) {
            return null;
        }
        final int initialShortCapacity = shortArrayCapacityForLastIndex(maxPos);
        if (initialShortCapacity == 0) {
            return null;
        }
        return new SortedRangesShort(initialShortCapacity, first);
    }

    @Override
    protected SortedRangesInt growOnNew(final int capacity) {
        final SortedRangesInt ans = new SortedRangesInt(capacity, offset);
        System.arraycopy(data, 0, ans.data, 0, count);
        ans.cardinality = cardinality;
        ans.count = count;
        if (POOL_ARRAYS && canWrite()) {
            freeArray(data);
        }
        return ans;
    }

    @Override
    protected SortedRangesInt makeMyType(final int[] data, final long offset, final int count, final long cardinality) {
        return new SortedRangesInt(data, offset, count, cardinality);
    }

    @Override
    protected void rebaseAndShift(
            final int[] dataOut, final long newOffset, final long shiftOffset,
            final SortedRangesTyped<int[]> sar, final long first) {
        final long newUnpackedFirst = first + shiftOffset;
        dataOut[0] = (int) (newUnpackedFirst - newOffset);
        final long netOffset = offset + shiftOffset - newOffset;
        for (int i = 1; i < sar.count; ++i) {
            final long v = sar.data[i];
            dataOut[i] = (int) (v < 0 ? v - netOffset : v + netOffset);
        }
    }

    @Override
    public int bytesAllocated() {
        return data.length * Integer.BYTES;
    }

    @Override
    public int bytesUsed() {
        return count * Integer.BYTES;
    }

    @Override
    protected SortedRangesShort tryPack() {
        final long first = first();
        final long range = last() - first;
        if (range > Short.MAX_VALUE) {
            return null;
        }
        final SortedRangesShort sr = new SortedRangesShort(count, first);
        copyTo(sr);
        return sr;
    }

    @Override
    public boolean isDense() {
        return isDenseInt(data, count);
    }
}
