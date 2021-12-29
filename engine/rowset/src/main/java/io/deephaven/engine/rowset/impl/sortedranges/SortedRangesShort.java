package io.deephaven.engine.rowset.impl.sortedranges;

import gnu.trove.map.hash.TIntObjectHashMap;

public final class SortedRangesShort extends SortedRangesPacked<short[]> {

    @Override
    public SortedRangesShort deepCopy() {
        return new SortedRangesShort(copyData(this), offset, count, cardinality);
    }

    @Override
    protected int packedValuesPerCacheLine() {
        return 32;
    }

    @Override
    protected SortedRangesShort makeMyTypeAndOffset(final int initialCapacity) {
        return new SortedRangesShort(initialCapacity, offset);
    }

    private static ThreadLocal<TIntObjectHashMap<short[]>> ARRAY_POOL =
            ThreadLocal.withInitial(() -> new TIntObjectHashMap<>(16));

    @Override
    protected short[] makeArray(final int capacity) {
        final int roundedCapacity = arraySizeRoundingShort(capacity);
        if (POOL_ARRAYS) {
            final TIntObjectHashMap<short[]> localPool = ARRAY_POOL.get();
            final short[] arr = localPool.remove(roundedCapacity);
            if (arr != null) {
                return arr;
            }
        }
        return new short[roundedCapacity];
    }

    @Override
    protected void freeArray(final short[] arr) {
        if (!POOL_ARRAYS) {
            return;
        }
        // Only pool sizes that are allocation increments, not the result of trimming.
        if (!isShortAllocationSize(arr.length)) {
            return;
        }
        final TIntObjectHashMap<short[]> localPool = ARRAY_POOL.get();
        localPool.put(arr.length, arr);
    }

    @Override
    protected int dataLength() {
        return data.length;
    }

    @Override
    protected long maxValueForType() {
        return Short.MAX_VALUE;
    }

    @Override
    protected int capacityForLastIndex(final int lastIndex, final boolean isDense) {
        return shortArrayCapacityForLastIndex(lastIndex);
    }

    @Override
    protected long unpackedGet(final int i) {
        return unpack(data[i]);
    }

    @Override
    protected long absUnpackedGet(final int i) {
        final short iData = data[i];
        if (iData < 0) {
            return offset() - iData;
        }
        return offset() + iData;
    }

    @Override
    protected void unpackedSet(final int i, final long v) {
        data[i] = (short) pack(v);
    }

    @Override
    protected long packedGet(final int i) {
        return data[i];
    }

    @Override
    protected void packedSet(final int i, final long v) {
        data[i] = (short) v;
    }

    public SortedRangesShort(final int initialCapacity, final long offset) {
        super(initialCapacity, offset);
    }

    public SortedRangesShort(final short[] data, final long offset, final int count, final long cardinality) {
        super(data, offset, count, cardinality);
    }

    public SortedRangesShort(final SortedRangesLong sa) {
        super(fromLongArray(sa.data, sa.count), sa.first(), sa.count, sa.cardinality);
    }

    private static short[] fromLongArray(final long[] longArray, final int count) {
        final short[] shortArray = new short[longArray.length];
        shortArray[0] = 0;
        final long offset = longArray[0];
        for (int i = 1; i < count; ++i) {
            final long v = longArray[i];
            final short iv = (short) (v < 0 ? v + offset : v - offset);
            shortArray[i] = iv;
        }
        return shortArray;
    }

    public SortedRangesShort(final long offset, final SortedRangesInt sa, final int initialCapacity) {
        super(fromIntArray(sa.data, sa.count, sa.offset, offset, initialCapacity),
                offset, sa.count, sa.cardinality);
    }

    private static short[] fromIntArray(
            final int[] intArray, final int count, final long oldOffset, final long newOffset, final int capacity) {
        final short[] shortArray = new short[capacity];
        for (int i = 0; i < count; ++i) {
            final int v = intArray[i];
            final short sv = (short) (v < 0 ? newOffset - oldOffset + v : v + oldOffset - newOffset);
            shortArray[i] = sv;
        }
        return shortArray;
    }

    @Override
    protected SortedRanges tryPackFor(final long first, final long last, final int maxPos, final boolean isDense) {
        return null;
    }

    @Override
    protected SortedRanges tryMakePackedType(final int maxPos, final long first, final boolean isDense) {
        return null;
    }

    @Override
    protected SortedRangesShort growOnNew(final int capacity) {
        final SortedRangesShort ans = new SortedRangesShort(capacity, offset);
        System.arraycopy(data, 0, ans.data, 0, count);
        ans.cardinality = cardinality;
        ans.count = count;
        if (POOL_ARRAYS && canWrite()) {
            freeArray(data);
        }
        return ans;
    }

    @Override
    protected SortedRangesShort makeMyType(final short[] data, final long offset, final int count,
            final long cardinality) {
        return new SortedRangesShort(data, offset, count, cardinality);
    }

    @Override
    protected void rebaseAndShift(
            final short[] dataOut, final long newOffset, final long shiftOffset,
            final SortedRangesTyped<short[]> sar, final long first) {
        final long newUnpackedFirst = first + shiftOffset;
        dataOut[0] = (short) (newUnpackedFirst - newOffset);
        final long netOffset = offset + shiftOffset - newOffset;
        for (int i = 1; i < sar.count; ++i) {
            final long v = sar.data[i];
            dataOut[i] = (short) (v < 0 ? v - netOffset : v + netOffset);
        }
    }

    @Override
    public int bytesAllocated() {
        return data.length * Short.BYTES;
    }

    @Override
    public int bytesUsed() {
        return count * Short.BYTES;
    }

    @Override
    protected SortedRanges tryPack() {
        return null;
    }

    @Override
    public boolean isDense() {
        return isDenseShort(data, count);
    }
}
