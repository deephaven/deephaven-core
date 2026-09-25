//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;

public final class NonNullCounter {
    private final LongArraySource nonNullCount = new LongArraySource();

    void incrementNonNull(long destPos) {
        addNonNull(destPos, 1);
    }

    /**
     * Add non-null values to destPos, return the updated count.
     * 
     * @param destPos the destination to mark non-nulls for
     * @param count how many additional non-nulls
     * @return the new count
     */
    long addNonNull(long destPos, int count) {
        nonNullCount.ensureCapacity(destPos + 1);
        return addNonNullUnsafe(destPos, count);
    }

    /**
     * Add non-null values to destPos, return the updated count.
     *
     * Note, this function does not check for capacity
     *
     * @param destPos the destination to mark non-nulls for
     * @param count how many additional non-nulls
     * @return the new count
     */
    long addNonNullUnsafe(long destPos, int count) {
        long value = nonNullCount.getUnsafe(destPos);
        if (value == QueryConstants.NULL_LONG) {
            value = count;
        } else {
            if (count == 0) {
                return value;
            }
            value += count;
        }
        nonNullCount.set(destPos, value);
        return value;
    }

    long decrementNonNull(long destPos) {
        final long value = nonNullCount.getUnsafe(destPos) - 1;
        nonNullCount.set(destPos, value);
        return value;
    }

    boolean onlyNulls(long destPos) {
        final long aLong = nonNullCount.getLong(destPos);
        return aLong == 0 || aLong == QueryConstants.NULL_LONG;
    }

    long getCount(long destPos) {
        final long aLong = nonNullCount.getLong(destPos);
        return aLong == QueryConstants.NULL_LONG ? 0 : aLong;
    }

    boolean onlyNullsUnsafe(long destPos) {
        final long aLong = nonNullCount.getUnsafe(destPos);
        return aLong == 0 || aLong == QueryConstants.NULL_LONG;
    }

    long getCountUnsafe(long destPos) {
        final long aLong = nonNullCount.getUnsafe(destPos);
        return aLong == QueryConstants.NULL_LONG ? 0 : aLong;
    }

    public void ensureCapacity(long tableSize) {
        nonNullCount.ensureCapacity(tableSize);
    }

    LongArraySource getColumnSource() {
        return nonNullCount;
    }

    void startTrackingPrevValues() {
        nonNullCount.startTrackingPrevValues();
    }

    public void shift(RowSetShiftData shiftData) {
        nonNullCount.shift(shiftData);
    }

    public void releaseBlocks(long firstOutputPosition, long lastOutputPosition) {
        nonNullCount.releaseBlocks(firstOutputPosition, lastOutputPosition);
    }

    public void clear(long firstOutputPosition, long lastOutputPosition) {
        zeroRange(nonNullCount, firstOutputPosition, lastOutputPosition);
    }

    /**
     * Set a range of a count source to zero, filling it from a chunk of zeros one block at a time.
     *
     * @param counts the count source
     * @param firstKey the first row key to zero
     * @param lastKey the last row key to zero, inclusive
     */
    static void zeroRange(final LongArraySource counts, final long firstKey, final long lastKey) {
        if (lastKey < firstKey) {
            return;
        }
        final int chunkCapacity = (int) Math.min(ArrayBackedColumnSource.BLOCK_SIZE, lastKey - firstKey + 1);
        try (final ChunkSink.FillFromContext fillFromContext = counts.makeFillFromContext(chunkCapacity);
                final WritableLongChunk<Values> zeros = WritableLongChunk.makeWritableChunk(chunkCapacity);
                final RowSequence range = RowSequenceFactory.forRange(firstKey, lastKey);
                final RowSequence.Iterator rangeIterator = range.getRowSequenceIterator()) {
            zeros.fillWithValue(0, chunkCapacity, 0L);
            while (rangeIterator.hasMore()) {
                final RowSequence slice = rangeIterator.getNextRowSequenceWithLength(chunkCapacity);
                zeros.setSize(slice.intSize());
                counts.fillFromChunk(fillFromContext, zeros, slice);
            }
        }
    }
}
