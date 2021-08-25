package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;

public abstract class OrderedKeysAsChunkImpl implements OrderedKeys {
    private WritableLongChunk<OrderedKeyIndices> keyIndicesChunk;
    private boolean keyIndicesChunkInvalidated;
    private WritableLongChunk<OrderedKeyRanges> keyRangesChunk;
    private boolean keyRangesChunkInvalidated;

    private void makeKeyIndicesChunk() {
        final int isize = intSize();
        keyIndicesChunk = WritableLongChunk.makeWritableChunk(isize);
    }

    protected long runsUpperBound() {
        final long size = size();
        final long range = lastKey() - firstKey() + 1;
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
        final WritableLongChunk<OrderedKeyRanges> chunk =
                WritableLongChunk.makeWritableChunk(size);
        keyRangesChunk = chunk;
    }

    @Override
    public final LongChunk<OrderedKeyIndices> asKeyIndicesChunk() {
        if (size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        if (keyIndicesChunk == null || keyIndicesChunkInvalidated) {
            if (keyIndicesChunk != null) {
                if (keyIndicesChunk.capacity() >= size()) {
                    keyIndicesChunk.setSize(keyIndicesChunk.capacity());
                    fillKeyIndicesChunk(keyIndicesChunk);
                } else {
                    keyIndicesChunk.close();
                    keyIndicesChunk = null;
                }
            }
            if (keyIndicesChunk == null) {
                makeKeyIndicesChunk();
                fillKeyIndicesChunk(keyIndicesChunk);
            }
            keyIndicesChunkInvalidated = false;
        }

        return keyIndicesChunk;
    }

    @Override
    public final LongChunk<OrderedKeyRanges> asKeyRangesChunk() {
        if (size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        if (keyRangesChunk == null || keyRangesChunkInvalidated) {
            final int size = sizeForRangesChunk();
            if (keyRangesChunk != null) {
                if (keyRangesChunk.capacity() >= size) {
                    fillKeyRangesChunk(keyRangesChunk);
                } else {
                    keyRangesChunk.close();
                    keyRangesChunk = null;
                }
            }
            if (keyRangesChunk == null) {
                makeKeyRangesChunk(size);
                fillKeyRangesChunk(keyRangesChunk);
            }
            keyRangesChunkInvalidated = false;
        }

        return keyRangesChunk;
    }

    abstract public long lastKey();

    abstract public long rangesCountUpperBound();

    @Override
    public void close() {
        closeOrderedKeysAsChunkImpl();
    }

    protected final void closeOrderedKeysAsChunkImpl() {
        if (keyIndicesChunk != null) {
            keyIndicesChunk.close();
            keyIndicesChunk = null;
        }
        if (keyRangesChunk != null) {
            keyRangesChunk.close();
            keyRangesChunk = null;
        }
    }

    protected final void invalidateOrderedKeysAsChunkImpl() {
        keyIndicesChunkInvalidated = true;
        keyRangesChunkInvalidated = true;
    }
}
