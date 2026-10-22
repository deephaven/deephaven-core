//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

/**
 * A {@link RowSet.RangeIterator} over the ranges of consecutive keys in a chunk of ordered row keys.
 */
public final class RowKeysChunkRangeIterator implements RowSet.RangeIterator {
    private final LongChunk<OrderedRowKeys> keys;
    /** The offset in {@link #keys} of the first key after the current range. */
    private int nextOffset;
    private long currentStart = -1;
    private long currentEnd = -1;

    public RowKeysChunkRangeIterator(final LongChunk<OrderedRowKeys> keys) {
        this.keys = keys;
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
        return nextOffset < keys.size();
    }

    @Override
    public long next() {
        currentStart = keys.get(nextOffset++);
        currentEnd = currentStart;
        final int size = keys.size();
        while (nextOffset < size && keys.get(nextOffset) == currentEnd + 1) {
            ++currentEnd;
            ++nextOffset;
        }
        return currentStart;
    }

    @Override
    public boolean advance(final long v) {
        if (currentStart >= 0 && currentEnd >= v) {
            currentStart = Math.max(currentStart, v);
            return true;
        }
        final int size = keys.size();
        if (nextOffset >= size) {
            return false;
        }
        // Find the first key at or after v. Every key before it is less than v, so the range that contains v (if any)
        // begins there, and next() reads it from that offset.
        if (keys.get(nextOffset) < v) {
            // gallop until we pass v, then binary search the last step; keys.get(below) < v throughout
            int below = nextOffset;
            int step = 1;
            int above = below + 1;
            while (above < size && keys.get(above) < v) {
                below = above;
                step <<= 1;
                above = (int) Math.min((long) below + step, size);
            }
            int low = below + 1;
            int high = Math.min(above, size);
            while (low < high) {
                final int mid = (low + high) >>> 1;
                if (keys.get(mid) < v) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            nextOffset = low;
            if (nextOffset >= size) {
                return false;
            }
        }
        next();
        return true;
    }

    @Override
    public void postpone(final long v) {
        currentStart = v;
    }

    @Override
    public long currentRangeStart() {
        return currentStart;
    }

    @Override
    public long currentRangeEnd() {
        return currentEnd;
    }
}
