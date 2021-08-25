package io.deephaven.db.v2.utils.sortedranges;

public abstract class SortedRangesPacked<ArrayType> extends SortedRangesTyped<ArrayType> {
    protected abstract long maxValueForType();

    protected long offset;

    @Override
    public boolean fits(final long value) {
        return offset <= value && value - offset <= maxValueForType();
    }

    @Override
    public boolean fits(final long start, final long end) {
        return offset <= start && end - offset <= maxValueForType();
    }

    @Override
    public boolean fitsForAppend(final long end) {
        return end - offset <= maxValueForType();
    }

    @Override
    protected final long pack(final long unpackedValue) {
        if (unpackedValue < 0) {
            return offset + unpackedValue;
        }
        return unpackedValue - offset;
    }

    @Override
    protected final long unpack(final long packedValue) {
        if (packedValue < 0) {
            return -offset + packedValue;
        }
        return offset + packedValue;
    }

    protected SortedRangesPacked(final int initialCapacity, final long offset) {
        super(initialCapacity);
        this.offset = offset;
    }

    protected SortedRangesPacked(final ArrayType data, final long offset) {
        super(data);
        this.offset = offset;
    }

    protected SortedRangesPacked(final ArrayType data, final long offset, final int count, final long cardinality) {
        super(data, count, cardinality);
        this.offset = offset;
    }

    protected abstract SortedRanges makeMyType(ArrayType data, long offset, int count, long cardinality);

    protected final long offset() {
        return offset;
    }

    public final SortedRanges addInternal(final long v, final boolean writeCheck) {
        final long packedValue = pack(v);
        if (packedValue < 0 || packedValue > maxValueForType()) {
            final SortedRangesLong ans = tryConvertToSrLong(1);
            if (ans == null) {
                return null;
            }
            return ans.addInternal(v, false);
        }
        return addPacked(this, packedValue, v, writeCheck);
    }

    public final SortedRanges addRangeInternal(final long start, final long end, final boolean writeCheck) {
        final long packedStart = pack(start);
        if (packedStart >= 0) {
            final long packedEnd = pack(end);
            if (packedEnd <= maxValueForType()) {
                return addRangePacked(this, packedStart, packedEnd, start, end, writeCheck);
            }
        }
        final SortedRangesLong ans = tryConvertToSrLong(2);
        if (ans == null) {
            return null;
        }
        return ans.addRangeInternal(start, end, false);
    }

    public final SortedRanges appendInternal(final long v, final boolean writeCheck) {
        final long packedValue = pack(v);
        if (packedValue < 0 || packedValue > maxValueForType()) {
            final SortedRangesLong ans = tryConvertToSrLong(1);
            if (ans == null) {
                return null;
            }
            return ans.appendInternal(v, false);
        }
        return appendPacked(this, packedValue, v, writeCheck);
    }

    public final SortedRanges appendRangeInternal(final long start, final long end, final boolean writeCheck) {
        final long packedStart = pack(start);
        if (packedStart >= 0) {
            final long packedEnd = pack(end);
            if (packedEnd <= maxValueForType()) {
                return appendRangePacked(this, packedStart, packedEnd, start, end, writeCheck);
            }
        }
        final SortedRangesLong ans = tryConvertToSrLong(2);
        if (ans == null) {
            return null;
        }
        return ans.appendRangeInternal(start, end, false);
    }

    public final SortedRanges removeInternal(final long v) {
        final long packedValue = pack(v);
        if (packedValue < 0 || packedValue > maxValueForType()) {
            return this;
        }
        return removePacked(this, packedValue, v);
    }

    public final SortedRanges removeRangeInternal(final long start, final long end) {
        long packedEnd = pack(end);
        if (packedEnd < 0) {
            return this;
        }
        long packedStart = pack(start);
        if (packedStart < 0) {
            packedStart = 0;
        }
        if (packedEnd > maxValueForType()) {
            packedEnd = maxValueForType();
        }
        return removeRangePacked(this, packedStart, packedEnd, start, end);
    }

    protected abstract void rebaseAndShift(
            final ArrayType dataOut, final long newOffset, final long shiftOffset,
            final SortedRangesTyped<ArrayType> sar, final long first);

    @Override
    public SortedRanges applyShift(final long shiftOffset) {
        if (shiftOffset == 0 || isEmpty()) {
            return this;
        }
        long v = unpackedGet(0);
        if (v + shiftOffset < 0) {
            throw new IllegalArgumentException("shiftOffset=" + shiftOffset + " when first=" + v);
        }
        return applyShiftImpl(shiftOffset, v, !canWrite());
    }

    private SortedRanges applyShiftImpl(final long shiftOffset, final long first, final boolean isNew) {
        long newOffset = offset + shiftOffset;
        if (newOffset >= first) {
            if (isNew) {
                final ArrayType targetData = copyData(this);
                return makeMyType(targetData, newOffset, count, cardinality);
            }
            offset = newOffset;
            if (DEBUG)
                validate();
            return this;
        }
        newOffset = first + shiftOffset;
        if (isNew) {
            final ArrayType targetData = makeArray(dataLength());
            rebaseAndShift(targetData, newOffset, shiftOffset, this, first);
            return makeMyType(targetData, newOffset, count, cardinality);
        }
        rebaseAndShift(data, newOffset, shiftOffset, this, first);
        offset = newOffset;
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
        long v = unpackedGet(0);
        if (v + shiftOffset < 0) {
            throw new IllegalArgumentException("offsetDelta=" + shiftOffset + " when first=" + v);
        }
        return applyShiftImpl(shiftOffset, v, true);
    }

    // try to convert this to a SortedArrayLong with enough space to accomodate deltaCapacity elements
    // in its data array.
    private SortedRangesLong tryConvertToSrLong(final int deltaCapacity) {
        if (count == 0) {
            if (deltaCapacity > LONG_DENSE_MAX_CAPACITY) {
                return null;
            }
            return new SortedRangesLong(deltaCapacity);
        }
        final int desiredCap = count + deltaCapacity;
        final int capacity = longArrayCapacityForLastIndex(
                desiredCap - 1, isDenseLongSample(first(), last(), count));
        if (capacity == 0) {
            return null;
        }
        final SortedRangesLong ans = new SortedRangesLong(capacity);
        copyTo(ans);
        return ans;
    }
}
