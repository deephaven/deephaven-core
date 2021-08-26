/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.OrderedChunkUtils;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;

public class OrderedKeysKeyIndicesChunkImpl implements OrderedKeys {

    private final LongChunk<OrderedKeyIndices> backingChunk;
    private final WritableLongChunk<OrderedKeyIndices> toReleaseChunk;
    private WritableLongChunk<OrderedKeyRanges> asRangesChunk = null;

    private OrderedKeysKeyIndicesChunkImpl(final LongChunk<OrderedKeyIndices> backingChunk) {
        this.backingChunk = backingChunk;
        this.toReleaseChunk = null;
    }

    static OrderedKeysKeyIndicesChunkImpl makeByWrapping(
        final LongChunk<OrderedKeyIndices> backingChunk) {
        return new OrderedKeysKeyIndicesChunkImpl(backingChunk);
    }

    private OrderedKeysKeyIndicesChunkImpl(
        final WritableLongChunk<OrderedKeyIndices> backingChunk) {
        this.backingChunk = this.toReleaseChunk = backingChunk;
    }

    static OrderedKeysKeyIndicesChunkImpl makeByTaking(
        final WritableLongChunk<OrderedKeyIndices> backingChunkToOwn) {
        return new OrderedKeysKeyIndicesChunkImpl(backingChunkToOwn);
    }

    private class Iterator implements OrderedKeys.Iterator {

        private int iteratorOffset = 0;
        private OrderedKeysKeyIndicesChunkImpl pendingClose;

        private void tryClosePendingClose() {
            if (pendingClose != null) {
                pendingClose.close();
                pendingClose = null;
            }
        }

        @Override
        public final void close() {
            tryClosePendingClose();
        }

        @Override
        public final boolean hasMore() {
            return iteratorOffset < backingChunk.size();
        }

        @Override
        public long peekNextKey() {
            return hasMore() ? backingChunk.get(iteratorOffset) : Index.NULL_KEY;
        }

        @Override
        public final OrderedKeys getNextOrderedKeysThrough(final long maxKey) {
            tryClosePendingClose();
            final int newEndOffset = findFirstIndexAfterKey(maxKey, iteratorOffset);
            int newLen = newEndOffset - iteratorOffset;
            if (newLen == 0) {
                return OrderedKeys.EMPTY;
            }
            pendingClose =
                new OrderedKeysKeyIndicesChunkImpl(backingChunk.slice(iteratorOffset, newLen));
            iteratorOffset = newEndOffset;
            return pendingClose;
        }

        @Override
        public final OrderedKeys getNextOrderedKeysWithLength(final long numberOfKeys) {
            tryClosePendingClose();
            final int newLen =
                Math.toIntExact(Math.min(numberOfKeys, backingChunk.size() - iteratorOffset));
            if (newLen == 0) {
                return OrderedKeys.EMPTY;
            }
            pendingClose =
                new OrderedKeysKeyIndicesChunkImpl(backingChunk.slice(iteratorOffset, newLen));
            iteratorOffset += newLen;
            return pendingClose;
        }

        @Override
        public final boolean advance(long nextKey) {
            iteratorOffset = findLowerBoundOfKey(nextKey, iteratorOffset);
            return hasMore();
        }

        @Override
        public long getRelativePosition() {
            return iteratorOffset;
        }
    }

    @Override
    public final OrderedKeys.Iterator getOrderedKeysIterator() {
        return new Iterator();
    }

    @Override
    public final OrderedKeys getOrderedKeysByPosition(final long startPositionInclusive,
        final long length) {
        final int newStartOffset =
            Math.toIntExact(Math.min(backingChunk.size(), startPositionInclusive));
        final int newLen = Math.toIntExact(Math.min(backingChunk.size() - newStartOffset, length));
        if (newLen == 0) {
            return OrderedKeys.EMPTY;
        }
        return new OrderedKeysKeyIndicesChunkImpl(backingChunk.slice(newStartOffset, newLen));
    }

    @Override
    public final OrderedKeys getOrderedKeysByKeyRange(final long startKeyInclusive,
        final long endKeyInclusive) {
        final int newStartOffset = findLowerBoundOfKey(startKeyInclusive, 0);
        final int newLen = findFirstIndexAfterKey(endKeyInclusive, newStartOffset) - newStartOffset;
        if (newLen == 0) {
            return OrderedKeys.EMPTY;
        }
        return new OrderedKeysKeyIndicesChunkImpl(backingChunk.slice(newStartOffset, newLen));
    }

    @Override
    public final Index asIndex() {
        final int size = backingChunk.size();
        if (size == 0) {
            return Index.FACTORY.getEmptyIndex();
        }
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        builder.setDomain(backingChunk.get(0), backingChunk.get(size - 1));
        builder.appendOrderedKeyIndicesChunk(backingChunk);
        return builder.getIndex();
    }

    @Override
    public final LongChunk<OrderedKeyIndices> asKeyIndicesChunk() {
        return backingChunk;
    }

    @Override
    public final LongChunk<OrderedKeyRanges> asKeyRangesChunk() {
        if (backingChunk.size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        if (asRangesChunk != null) {
            return asRangesChunk;
        }
        return asRangesChunk = ChunkUtils.convertToOrderedKeyRanges(backingChunk);
    }

    @Override
    public final void fillKeyIndicesChunk(
        final WritableLongChunk<? extends KeyIndices> chunkToFill) {
        final int newSize = Math.toIntExact(size());
        // noinspection unchecked
        backingChunk.copyToChunk(0, (WritableLongChunk) chunkToFill, 0, newSize);
        chunkToFill.setSize(newSize);
    }

    @Override
    public final void fillKeyRangesChunk(final WritableLongChunk<OrderedKeyRanges> chunkToFill) {
        ChunkUtils.convertToOrderedKeyRanges(backingChunk, chunkToFill);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public long firstKey() {
        return backingChunk.size() > 0 ? backingChunk.get(0) : Index.NULL_KEY;
    }

    @Override
    public long lastKey() {
        final int sz = backingChunk.size();
        return sz > 0 ? backingChunk.get(sz - 1) : Index.NULL_KEY;
    }

    @Override
    public final long size() {
        return backingChunk.size();
    }

    @Override
    public long getAverageRunLengthEstimate() {
        final long first = firstKey();
        final long last = lastKey();
        final long range = last - first + 1;
        Assert.leq(first, "first", last, "last");
        final long numMinHoles = range - size();
        return size() == 0 ? 1 : Math.max(1, size() / (numMinHoles + 1));
    }

    @Override
    public boolean forEachLong(final LongAbortableConsumer lc) {
        for (int i = 0; i < backingChunk.size(); ++i) {
            if (!lc.accept(backingChunk.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachLongRange(final LongRangeAbortableConsumer lc) {
        long pendingStart = -2;
        long pendingEnd = -2;
        for (int i = 0; i < backingChunk.size(); ++i) {
            final long v = backingChunk.get(i);
            if (pendingStart == -2) {
                pendingStart = pendingEnd = v;
                continue;
            }
            if (pendingEnd + 1 == v) {
                pendingEnd = v;
                continue;
            }
            if (!lc.accept(pendingStart, pendingEnd)) {
                return false;
            }
            pendingStart = pendingEnd = v;
        }
        if (pendingStart != -2) {
            return lc.accept(pendingStart, pendingEnd);
        }
        return true;
    }

    private int findLowerBoundOfKey(final long key, final int offset) {
        int off = OrderedChunkUtils.findInChunk(backingChunk, key, offset, backingChunk.size());
        while (off > 0 && backingChunk.get(off - 1) == key) {
            --off;
        }
        return off;
    }

    private int findFirstIndexAfterKey(final long key, final int offset) {
        int off = OrderedChunkUtils.findInChunk(backingChunk, key, offset, backingChunk.size());
        while (off < backingChunk.size() && backingChunk.get(off) == key) {
            ++off;
        }
        return off;
    }

    @Override
    public void close() {
        if (asRangesChunk != null) {
            asRangesChunk.close();
            asRangesChunk = null;
        }
        if (toReleaseChunk != null) {
            toReleaseChunk.close();
        }
    }
}
