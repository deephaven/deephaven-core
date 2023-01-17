/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public final class IntColumnSourceWritableRowRedirection implements WritableRowRedirection {

    private final WritableColumnSource<Integer> columnSource;

    public IntColumnSourceWritableRowRedirection(WritableColumnSource<Integer> columnSource) {
        this.columnSource = columnSource;
    }

    @Override
    public long put(long outerRowKey, long innerRowKey) {
        final int previous = columnSource.getInt(outerRowKey);

        columnSource.set(outerRowKey, (int) innerRowKey);

        return previous == QueryConstants.NULL_INT ? RowSequence.NULL_ROW_KEY : previous;
    }

    @Override
    public void putVoid(long outerRowKey, long innerRowKey) {
        columnSource.set(outerRowKey, (int) innerRowKey);
    }

    @Override
    public long get(long outerRowKey) {
        final int innerIndex = columnSource.getInt(outerRowKey);
        if (innerIndex == QueryConstants.NULL_INT) {
            return RowSequence.NULL_ROW_KEY;
        }
        return innerIndex;
    }

    @Override
    public long getPrev(long outerRowKey) {
        final int innerIndex = columnSource.getPrevInt(outerRowKey);
        if (innerIndex == QueryConstants.NULL_INT) {
            return RowSequence.NULL_ROW_KEY;
        }
        return innerIndex;
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final ColumnSource.FillContext colSrcCtx;
        private final WritableIntChunk<Values> intChunk;

        private FillContext(final IntColumnSourceWritableRowRedirection csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
            intChunk = WritableIntChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public void close() {
            colSrcCtx.close();
            intChunk.close();
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkSize, final SharedContext sharedContext) {
        return new FillContext(this, chunkSize);
    }

    @Override
    public void fillChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final FillContext effectiveContext = (FillContext) fillContext;
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        columnSource.fillChunk(effectiveContext.colSrcCtx, effectiveContext.intChunk, outerRowKeys);
        final int sz = outerRowKeys.intSize();
        for (int ii = 0; ii < sz; ++ii) {
            final int innerIndex = effectiveContext.intChunk.get(ii);
            innerRowKeysTyped.set(ii, innerIndex == QueryConstants.NULL_INT ? RowSequence.NULL_ROW_KEY : innerIndex);
        }
        innerRowKeysTyped.setSize(sz);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final FillContext effectiveContext = (FillContext) fillContext;
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        columnSource.fillPrevChunk(effectiveContext.colSrcCtx, effectiveContext.intChunk, outerRowKeys);
        final int sz = outerRowKeys.intSize();
        for (int ii = 0; ii < sz; ++ii) {
            final int innerIndex = effectiveContext.intChunk.get(ii);
            innerRowKeysTyped.set(ii, innerIndex == QueryConstants.NULL_INT ? RowSequence.NULL_ROW_KEY : innerIndex);
        }
        innerRowKeysTyped.setSize(sz);
    }

    @Override
    public long remove(long outerRowKey) {
        final int previous = columnSource.getInt(outerRowKey);
        if (previous == QueryConstants.NULL_INT) {
            return RowSequence.NULL_ROW_KEY;
        }
        columnSource.set(outerRowKey, QueryConstants.NULL_INT);
        return previous;
    }

    @Override
    public void removeVoid(long outerRowKey) {
        columnSource.set(outerRowKey, QueryConstants.NULL_INT);
    }

    @Override
    public void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }
}
