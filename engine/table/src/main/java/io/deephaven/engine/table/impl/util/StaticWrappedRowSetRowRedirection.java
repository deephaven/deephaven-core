//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.LongChunkAppender;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.NotNull;

/**
 * RowRedirection that maps from outer key (position in the wrapped RowSet) to inner key. Reads only the current value
 * of the RowSet for both current and previous lookups, so it is safe to use with non-tracking RowSets. Subclasses that
 * have a {@link io.deephaven.engine.rowset.TrackingRowSet} should override the previous-value methods to provide proper
 * prev semantics.
 */
public class StaticWrappedRowSetRowRedirection implements RowRedirection {

    protected final RowSet wrappedRowSet;

    public StaticWrappedRowSetRowRedirection(final RowSet wrappedRowSet) {
        this.wrappedRowSet = wrappedRowSet;
    }

    @Override
    public synchronized long get(long outerRowKey) {
        return wrappedRowSet.get(outerRowKey);
    }

    @Override
    public synchronized long getPrev(long outerRowKey) {
        return wrappedRowSet.get(outerRowKey);
    }

    protected static final class FillContext implements ChunkSource.FillContext {

        final WritableLongChunk<OrderedRowKeys> rowPositions;

        FillContext(final int chunkCapacity) {
            rowPositions = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            rowPositions.close();
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        // NB: No need to implement sharing at this level. RedirectedColumnSource uses a SharedContext to share
        // WritableRowRedirection lookup results.
        return new FillContext(chunkCapacity);
    }

    @Override
    public void fillChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<OrderedRowKeys> rowPositions = ((FillContext) fillContext).rowPositions;
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        outerRowKeys.fillRowKeyChunk(rowPositions);
        wrappedRowSet.getKeysForPositions(
                new LongChunkIterator(rowPositions), new LongChunkAppender(innerRowKeysTyped));
        innerRowKeysTyped.setSize(outerRowKeys.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        fillChunk(fillContext, innerRowKeys, outerRowKeys);
    }

    @Override
    public boolean ascendingMapping() {
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{");

        long positionStart = 0;

        for (final RowSet.RangeIterator rangeIterator = wrappedRowSet.rangeIterator(); rangeIterator.hasNext();) {
            rangeIterator.next();

            if (positionStart > 0) {
                builder.append(", ");
            }
            final long rangeStart = rangeIterator.currentRangeStart();
            final long length = rangeIterator.currentRangeEnd() - rangeStart + 1;
            if (length > 1) {
                builder.append(positionStart).append("-").append(positionStart + length - 1)
                        .append(" -> ").append(rangeStart).append("-").append(rangeIterator.currentRangeEnd());
            } else {
                builder.append(positionStart).append(" -> ").append(rangeStart);
            }
            positionStart += length;
        }

        builder.append("}");

        return builder.toString();
    }
}
