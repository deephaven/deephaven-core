package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;

public abstract class RowSequenceAsChunkImpl implements RowSequence {

    private WritableLongChunk<OrderedRowKeys> keyIndicesChunk;
    private boolean keyIndicesChunkInvalidated;
    private WritableLongChunk<OrderedRowKeyRanges> keyRangesChunk;
    private boolean keyRangesChunkInvalidated;

    private void makeKeyIndicesChunk() {
        final int isize = intSize();
        keyIndicesChunk = WritableLongChunk.makeWritableChunk(isize);
    }

    protected long runsUpperBound() {
        final long size = size();
        final long range = lastRowKey() - firstRowKey() + 1;
        final long holesUpperBound = range - size;
        final long runsUpperBound = 1 + holesUpperBound;
        return runsUpperBound;
    }

    private int sizeForRangesChunk() {
        final long runsUpperBound = runsUpperBound();
        if (runsUpperBound <= 1024) {
            return 2 * (int) runsUpperBound;
        }
        final long rangesCount = rangesCountUpperBound();
        return 2 * (int) rangesCount;
    }

    private void makeKeyRangesChunk(final int size) {
        final WritableLongChunk<OrderedRowKeyRanges> chunk =
                WritableLongChunk.makeWritableChunk(size);
        keyRangesChunk = chunk;
    }

    @Override
    public final LongChunk<OrderedRowKeys> asRowKeyChunk() {
        if (size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        if (keyIndicesChunk == null || keyIndicesChunkInvalidated) {
            if (keyIndicesChunk != null) {
                if (keyIndicesChunk.capacity() >= size()) {
                    keyIndicesChunk.setSize(keyIndicesChunk.capacity());
                    fillRowKeyChunk(keyIndicesChunk);
                } else {
                    keyIndicesChunk.close();
                    keyIndicesChunk = null;
                }
            }
            if (keyIndicesChunk == null) {
                makeKeyIndicesChunk();
                fillRowKeyChunk(keyIndicesChunk);
            }
            keyIndicesChunkInvalidated = false;
        }

        return keyIndicesChunk;
    }

    @Override
    public final LongChunk<OrderedRowKeyRanges> asRowKeyRangesChunk() {
        if (size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        if (keyRangesChunk == null || keyRangesChunkInvalidated) {
            final int size = sizeForRangesChunk();
            if (keyRangesChunk != null) {
                if (keyRangesChunk.capacity() >= size) {
                    fillRowKeyRangesChunk(keyRangesChunk);
                } else {
                    keyRangesChunk.close();
                    keyRangesChunk = null;
                }
            }
            if (keyRangesChunk == null) {
                makeKeyRangesChunk(size);
                fillRowKeyRangesChunk(keyRangesChunk);
            }
            keyRangesChunkInvalidated = false;
        }

        return keyRangesChunk;
    }

    abstract public long lastRowKey();

    abstract public long rangesCountUpperBound();

    @Override
    public void close() {
        closeRowSequenceAsChunkImpl();
    }

    protected final void closeRowSequenceAsChunkImpl() {
        if (keyIndicesChunk != null) {
            keyIndicesChunk.close();
            keyIndicesChunk = null;
        }
        if (keyRangesChunk != null) {
            keyRangesChunk.close();
            keyRangesChunk = null;
        }
    }

    protected final void invalidateRowSequenceAsChunkImpl() {
        keyIndicesChunkInvalidated = true;
        keyRangesChunkInvalidated = true;
    }
}
