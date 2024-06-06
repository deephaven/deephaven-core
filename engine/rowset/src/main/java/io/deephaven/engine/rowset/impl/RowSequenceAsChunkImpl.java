//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;

import javax.annotation.OverridingMethodsMustInvokeSuper;

public abstract class RowSequenceAsChunkImpl implements RowSequence {

    private WritableLongChunk<OrderedRowKeys> keyIndicesChunk;
    private boolean keyIndicesChunkInvalidated;
    private WritableLongChunk<OrderedRowKeyRanges> keyRangesChunk;
    private boolean keyRangesChunkInvalidated;

    private void makeKeyIndicesChunk() {
        final int isize = intSize();
        keyIndicesChunk = ChunkPoolReleaseTracking.untracked(() -> WritableLongChunk.makeWritableChunk(isize));
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
                ChunkPoolReleaseTracking.untracked(() -> WritableLongChunk.makeWritableChunk(size));
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
                    ChunkPoolReleaseTracking.untracked(keyIndicesChunk::close);
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
                    ChunkPoolReleaseTracking.untracked(keyRangesChunk::close);
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
    @OverridingMethodsMustInvokeSuper
    public void close() {
        closeRowSequenceAsChunkImpl();
    }

    /**
     * Close any resources associated with this RowSequenceAsChunkImpl. This is the implementation for {@link #close()
     * close}, made available for subclasses that have a need to release parent class resources independently of their
     * own {@link #close() close} implementation. Most uses should prefer to {@link #invalidateRowSequenceAsChunkImpl()
     * invalidate}, instead.
     */
    protected final void closeRowSequenceAsChunkImpl() {
        if (keyIndicesChunk != null) {
            ChunkPoolReleaseTracking.untracked(keyIndicesChunk::close);
            keyIndicesChunk = null;
        }
        if (keyRangesChunk != null) {
            ChunkPoolReleaseTracking.untracked(keyRangesChunk::close);
            keyRangesChunk = null;
        }
    }

    protected final void invalidateRowSequenceAsChunkImpl() {
        keyIndicesChunkInvalidated = true;
        keyRangesChunkInvalidated = true;
    }
}
