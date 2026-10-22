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
        while (hasNext()) {
            next();
            if (currentEnd >= v) {
                currentStart = Math.max(currentStart, v);
                return true;
            }
        }
        return false;
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
