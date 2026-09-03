//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.singlerange;

import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.RowSequenceAsChunkImpl;
import io.deephaven.chunk.WritableLongChunk;

public class SingleRangeRowSequence extends RowSequenceAsChunkImpl implements SingleRangeMixin {
    private long rangeStart;
    private long rangeEnd;

    @Override
    public long rangeStart() {
        return rangeStart;
    }

    @Override
    public long rangeEnd() {
        return rangeEnd;
    }

    public SingleRangeRowSequence(final long rangeStart, final long rangeEnd) {
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    protected void reset(final long rangeStart, final long rangeEnd) {
        invalidateRowSequenceAsChunkImpl();
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    public SingleRangeRowSequence copy() {
        return new SingleRangeRowSequence(rangeStart, rangeEnd);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long lastRowKey() {
        return rangeEnd;
    }

    @Override
    public long firstRowKey() {
        return rangeStart;
    }

    @Override
    public long size() {
        return rangeEnd - rangeStart + 1;
    }

    @Override
    public long rangesCountUpperBound() {
        return 1;
    }

    @Override
    public RowSet asRowSet() {
        return new WritableRowSetImpl(SingleRange.make(rangeStart(), rangeEnd()));
    }

    @Override
    public void fillRowKeyChunk(final WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        final int n = intSize();
        for (int i = 0; i < n; ++i) {
            chunkToFill.set(i, rangeStart() + i);
        }
        chunkToFill.setSize(n);
    }

    @Override
    public void fillRowKeyRangesChunk(final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        // The chunk's capacity is what has to hold the ranges; the size it arrives with says nothing about how much
        // room there is, and a caller who has already zeroed it is still owed the range.
        chunkToFill.set(0, rangeStart());
        chunkToFill.set(1, rangeEnd());
        chunkToFill.setSize(2);
    }

    static final class Iterator implements RowSequence.Iterator {
        private long currStart;
        private long currEnd;
        private long sizeLeft;
        private final SingleRangeRowSequence currBuf;

        public Iterator(final long rangeStart, final long rangeEnd) {
            currStart = rangeStart;
            currEnd = -1;
            sizeLeft = rangeEnd - rangeStart + 1;
            currBuf = new SingleRangeRowSequence(0, 0);
        }

        @Override
        public boolean hasMore() {
            return sizeLeft > 0;
        }

        @Override
        public long peekNextKey() {
            if (sizeLeft <= 0) {
                return RowSequence.NULL_ROW_KEY;
            }
            if (currEnd == -1) {
                return currStart;
            }
            return currEnd + 1;
        }

        @Override
        public RowSequence getNextRowSequenceThrough(final long maxKeyInclusive) {
            if (maxKeyInclusive < 0 || sizeLeft <= 0) {
                return RowSequenceFactory.EMPTY;
            }
            if (currEnd != -1) {
                if (maxKeyInclusive <= currEnd) {
                    return RowSequenceFactory.EMPTY;
                }
                currStart = currEnd + 1;
            } else if (maxKeyInclusive < currStart) {
                return RowSequenceFactory.EMPTY;
            }
            currEnd = Math.min(currStart + sizeLeft - 1, maxKeyInclusive);
            sizeLeft -= currEnd - currStart + 1;
            currBuf.reset(currStart, currEnd);
            return currBuf;
        }

        @Override
        public RowSequence getNextRowSequenceWithLength(final long numberOfKeys) {
            if (numberOfKeys <= 0 || sizeLeft <= 0) {
                return RowSequenceFactory.EMPTY;
            }
            if (currEnd != -1) {
                currStart = currEnd + 1;
            }
            currEnd = currStart + Math.min(sizeLeft, numberOfKeys) - 1;
            sizeLeft -= currEnd - currStart + 1;
            currBuf.reset(currStart, currEnd);
            return currBuf;
        }

        @Override
        public boolean advance(final long toKey) {
            if (sizeLeft <= 0) {
                return false;
            }
            // The first key not yet consumed; anything at or before it makes the advance a no-op.
            final long next = (currEnd == -1) ? currStart : currEnd + 1;
            final long last = next + sizeLeft - 1;
            if (last < toKey) {
                sizeLeft = 0;
                return false;
            }
            if (toKey > next) {
                currStart = toKey;
                currEnd = -1;
                sizeLeft = last - currStart + 1;
            }
            return true;
        }

        @Override
        public long getRelativePosition() {
            return -sizeLeft;
        }

        @Override
        public void close() {
            currBuf.close();
        }
    }
}
