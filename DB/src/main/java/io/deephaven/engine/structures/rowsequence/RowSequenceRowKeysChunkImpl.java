/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.structures.rowsequence;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.OrderedChunkUtils;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.v2.utils.*;

public class RowSequenceRowKeysChunkImpl implements RowSequence {

    private final LongChunk<OrderedRowKeys> backingChunk;
    private final WritableLongChunk<OrderedRowKeys> toReleaseChunk;
    private WritableLongChunk<Attributes.OrderedRowKeyRanges> asRangesChunk = null;

    private RowSequenceRowKeysChunkImpl(final LongChunk<OrderedRowKeys> backingChunk) {
        this.backingChunk = backingChunk;
        this.toReleaseChunk = null;
    }

    static RowSequenceRowKeysChunkImpl makeByWrapping(final LongChunk<OrderedRowKeys> backingChunk) {
        return new RowSequenceRowKeysChunkImpl(backingChunk);
    }

    private RowSequenceRowKeysChunkImpl(final WritableLongChunk<OrderedRowKeys> backingChunk) {
        this.backingChunk = this.toReleaseChunk = backingChunk;
    }

    static RowSequenceRowKeysChunkImpl makeByTaking(final WritableLongChunk<OrderedRowKeys> backingChunkToOwn) {
        return new RowSequenceRowKeysChunkImpl(backingChunkToOwn);
    }

    private class Iterator implements RowSequence.Iterator {

        private int iteratorOffset = 0;
        private RowSequenceRowKeysChunkImpl pendingClose;

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
            return hasMore() ? backingChunk.get(iteratorOffset) : RowSet.NULL_ROW_KEY;
        }

        @Override
        public final RowSequence getNextRowSequenceThrough(final long maxKey) {
            tryClosePendingClose();
            final int newEndOffset = findFirstIndexAfterKey(maxKey, iteratorOffset);
            int newLen = newEndOffset - iteratorOffset;
            if (newLen == 0) {
                return RowSequence.EMPTY;
            }
            pendingClose =
                    new RowSequenceRowKeysChunkImpl(backingChunk.slice(iteratorOffset, newLen));
            iteratorOffset = newEndOffset;
            return pendingClose;
        }

        @Override
        public final RowSequence getNextRowSequenceWithLength(final long numberOfKeys) {
            tryClosePendingClose();
            final int newLen = Math.toIntExact(Math.min(numberOfKeys, backingChunk.size() - iteratorOffset));
            if (newLen == 0) {
                return RowSequence.EMPTY;
            }
            pendingClose =
                    new RowSequenceRowKeysChunkImpl(backingChunk.slice(iteratorOffset, newLen));
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
    public final RowSequence.Iterator getRowSequenceIterator() {
        return new Iterator();
    }

    @Override
    public final RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        final int newStartOffset = Math.toIntExact(Math.min(backingChunk.size(), startPositionInclusive));
        final int newLen = Math.toIntExact(Math.min(backingChunk.size() - newStartOffset, length));
        if (newLen == 0) {
            return RowSequence.EMPTY;
        }
        return new RowSequenceRowKeysChunkImpl(backingChunk.slice(newStartOffset, newLen));
    }

    @Override
    public final RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        final int newStartOffset = findLowerBoundOfKey(startRowKeyInclusive, 0);
        final int newLen = findFirstIndexAfterKey(endRowKeyInclusive, newStartOffset) - newStartOffset;
        if (newLen == 0) {
            return RowSequence.EMPTY;
        }
        return new RowSequenceRowKeysChunkImpl(backingChunk.slice(newStartOffset, newLen));
    }

    @Override
    public final RowSet asRowSet() {
        final int size = backingChunk.size();
        if (size == 0) {
            return RowSetFactory.empty();
        }
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.setDomain(backingChunk.get(0), backingChunk.get(size - 1));
        builder.appendOrderedRowKeysChunk(backingChunk);
        return builder.build();
    }

    @Override
    public final LongChunk<Attributes.OrderedRowKeys> asRowKeyChunk() {
        return backingChunk;
    }

    @Override
    public final LongChunk<Attributes.OrderedRowKeyRanges> asRowKeyRangesChunk() {
        if (backingChunk.size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        if (asRangesChunk != null) {
            return asRangesChunk;
        }
        return asRangesChunk = ChunkUtils.convertToOrderedKeyRanges(backingChunk);
    }

    @Override
    public final void fillRowKeyChunk(final WritableLongChunk<? extends RowKeys> chunkToFill) {
        final int newSize = Math.toIntExact(size());
        // noinspection unchecked
        backingChunk.copyToChunk(0, (WritableLongChunk) chunkToFill, 0, newSize);
        chunkToFill.setSize(newSize);
    }

    @Override
    public final void fillRowKeyRangesChunk(final WritableLongChunk<Attributes.OrderedRowKeyRanges> chunkToFill) {
        ChunkUtils.convertToOrderedKeyRanges(backingChunk, chunkToFill);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public long firstRowKey() {
        return backingChunk.size() > 0 ? backingChunk.get(0) : RowSet.NULL_ROW_KEY;
    }

    @Override
    public long lastRowKey() {
        final int sz = backingChunk.size();
        return sz > 0 ? backingChunk.get(sz - 1) : RowSet.NULL_ROW_KEY;
    }

    @Override
    public final long size() {
        return backingChunk.size();
    }

    @Override
    public long getAverageRunLengthEstimate() {
        final long first = firstRowKey();
        final long last = lastRowKey();
        final long range = last - first + 1;
        Assert.leq(first, "first", last, "last");
        final long numMinHoles = range - size();
        return size() == 0 ? 1 : Math.max(1, size() / (numMinHoles + 1));
    }

    @Override
    public boolean forEachRowKey(final LongAbortableConsumer lc) {
        for (int i = 0; i < backingChunk.size(); ++i) {
            if (!lc.accept(backingChunk.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachRowKeyRange(final LongRangeAbortableConsumer lc) {
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
