package io.deephaven.db.v2.utils.sortedranges;

public abstract class SortedRangesTyped<ArrayType> extends SortedRanges {
    protected abstract ArrayType makeArray(final int capacity);

    protected abstract void freeArray(ArrayType arr);

    protected abstract int capacityForLastIndex(int i, boolean isDense);

    protected abstract SortedRanges tryMakePackedType(final int maxPos, final long first,
        boolean isDense);

    protected abstract SortedRangesTyped<ArrayType> makeMyTypeAndOffset(int initialCapacity);

    protected ArrayType data;

    protected SortedRangesTyped(final int initialCapacity) {
        data = makeArray(initialCapacity);
        this.count = 0;
        this.cardinality = 0;
    }

    protected SortedRangesTyped(final ArrayType data) {
        this(data, 0, 0);
    }

    protected SortedRangesTyped(final ArrayType data, final int count, final long cardinality) {
        this.data = data;
        this.count = count;
        this.cardinality = cardinality;
    }

    protected final void copyDataForMoveToNew(final SortedRanges srOut, final int srcPos,
        final int dstPos, final int len) {
        for (int i = 0; i < srcPos; ++i) {
            srOut.unpackedSet(i, unpackedGet(i));
        }
        copyData(srOut, srcPos, dstPos, len);
    }

    protected final void copyData(final SortedRanges srOut, final int srcPos, final int dstPos,
        final int len) {
        for (int i = 0; i < len; ++i) {
            srOut.unpackedSet(dstPos + i, unpackedGet(srcPos + i));
        }
    }

    protected final void copyTo(final SortedRanges srOut) {
        copyData(srOut, 0, 0, count);
        srOut.count = count;
        srOut.cardinality = cardinality;
    }

    @Override
    public final SortedRanges tryCompactUnsafe(final int k) {
        final SortedRanges packed = tryPack();
        if (packed != null) {
            return packed;
        }
        final int dataLen = dataLength();
        if (k == 0) {
            if (count == dataLen) {
                return this;
            }
        } else if (dataLen - count <= (dataLen >> k)) {
            return this;
        }
        final ArrayType newData = makeArray(count);
        System.arraycopy(data, 0, newData, 0, count);
        freeArray(data);
        data = newData;
        return this;
    }

    @Override
    protected final SortedRanges checkSizeAndMoveData(
        final int srcPos, final int dstPos, final int len, final long first,
        final boolean writeCheck) {
        final int maxPos = dstPos + len - 1;
        if (maxPos < dataLength()) {
            if (!writeCheck || canWrite()) {
                moveData(srcPos, dstPos, len);
                return this;
            }
            final SortedRangesTyped<ArrayType> ans = makeMyTypeAndOffset(dataLength());
            System.arraycopy(data, 0, ans.data, 0, srcPos);
            System.arraycopy(data, srcPos, ans.data, dstPos, len);
            return ans;
        }
        final boolean isDense = isDenseLongSample(first, last(), count);
        final int newCapacity = capacityForLastIndex(maxPos, isDense);
        if (newCapacity == 0) {
            final SortedRanges ans = tryMakePackedType(maxPos, first, isDense);
            if (ans == null) {
                return null;
            }
            copyDataForMoveToNew(ans, srcPos, dstPos, len);
            if (POOL_ARRAYS && canWrite()) {
                freeArray(data);
            }
            return ans;
        }
        if (!writeCheck || canWrite()) {
            final ArrayType newData = makeArray(newCapacity);
            System.arraycopy(data, 0, newData, 0, srcPos);
            System.arraycopy(data, srcPos, newData, dstPos, len);
            freeArray(data);
            data = newData;
            return this;
        }
        final SortedRangesTyped<ArrayType> ans = makeMyTypeAndOffset(newCapacity);
        System.arraycopy(data, 0, ans.data, 0, srcPos);
        System.arraycopy(data, srcPos, ans.data, dstPos, len);
        if (POOL_ARRAYS && canWrite()) {
            freeArray(data);
        }
        return ans;
    }

    @Override
    protected final SortedRanges ensureCanAppend(final int newLastPos,
        final long unpackedNewLastKey, final boolean writeCheck) {
        if (newLastPos < dataLength()) {
            return writeCheck ? getWriteRef() : this;
        }
        final long absUnpackedLastKey = Math.abs(unpackedNewLastKey);
        boolean isDense = true; // try smaller allocation for dense first.
        int newCapacity = capacityForLastIndex(newLastPos, isDense);
        if (newCapacity == 0) {
            isDense = isDenseLongSample(first(), absUnpackedLastKey, count);
            final SortedRanges ans = tryPackWithNewLast(absUnpackedLastKey, newLastPos, isDense);
            if (ans != null) {
                if (POOL_ARRAYS && canWrite()) {
                    freeArray(data);
                }
                return ans;
            }
            // we couldn't pack; we can try sparse allocation in our own type only if we are sparse.
            if (isDense) {
                return null;
            }
            newCapacity = capacityForLastIndex(newLastPos, false);
            if (newCapacity == 0) {
                return null;
            }
        }
        if (!writeCheck || canWrite()) {
            copyData(newCapacity);
            return this;
        }
        final SortedRanges ans = growOnNew(newCapacity);
        return ans;
    }

    protected static <ArrayType> ArrayType copyData(final SortedRangesTyped<ArrayType> sta) {
        int newSz = sta.dataLength();
        final ArrayType dst = sta.makeArray(newSz);
        System.arraycopy(sta.data, 0, dst, 0, sta.count);
        return dst;
    }

    @Override
    protected final void copyData(final int newCapacity) {
        final ArrayType dst = makeArray(newCapacity);
        System.arraycopy(data, 0, dst, 0, count);
        freeArray(data);
        data = dst;
    }

    @Override
    protected final void moveData(final int srcPos, final int dstPos, final int len) {
        System.arraycopy(data, srcPos, data, dstPos, len);
    }
}
