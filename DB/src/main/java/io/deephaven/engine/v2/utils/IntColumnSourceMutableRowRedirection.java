package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.chunk.Attributes.RowKeys;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.rftable.ChunkSource;
import io.deephaven.engine.rftable.SharedContext;
import io.deephaven.engine.chunk.WritableIntChunk;
import io.deephaven.engine.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public final class IntColumnSourceMutableRowRedirection implements MutableRowRedirection {

    private final WritableSource<Integer> columnSource;

    public IntColumnSourceMutableRowRedirection(WritableSource<Integer> columnSource) {
        this.columnSource = columnSource;
    }

    @Override
    public final long put(long outerRowKey, long innerRowKey) {
        final int previous = columnSource.getInt(outerRowKey);

        columnSource.set(outerRowKey, (int) innerRowKey);

        return previous == QueryConstants.NULL_INT ? RowSet.NULL_ROW_KEY : previous;
    }

    @Override
    public final void putVoid(long outerRowKey, long innerRowKey) {
        columnSource.set(outerRowKey, (int) innerRowKey);
    }

    @Override
    public final long get(long outerRowKey) {
        final int innerIndex = columnSource.getInt(outerRowKey);
        if (innerIndex == QueryConstants.NULL_INT) {
            return RowSet.NULL_ROW_KEY;
        }
        return innerIndex;
    }

    @Override
    public final long getPrev(long outerRowKey) {
        final int innerIndex = columnSource.getPrevInt(outerRowKey);
        if (innerIndex == QueryConstants.NULL_INT) {
            return RowSet.NULL_ROW_KEY;
        }
        return innerIndex;
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final ColumnSource.FillContext colSrcCtx;
        private final WritableIntChunk<Values> intChunk;

        private FillContext(final IntColumnSourceMutableRowRedirection csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
            intChunk = WritableIntChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public final void close() {
            colSrcCtx.close();
            intChunk.close();
        }
    }

    @Override
    public final ChunkSource.FillContext makeFillContext(final int chunkSize, final SharedContext sharedContext) {
        return new FillContext(this, chunkSize);
    }

    @Override
    public final void fillChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillChunk(effectiveContext.colSrcCtx, effectiveContext.intChunk, outerRowKeys);
        final int sz = outerRowKeys.intSize();
        for (int ii = 0; ii < sz; ++ii) {
            final int innerIndex = effectiveContext.intChunk.get(ii);
            innerRowKeys.set(ii, innerIndex == QueryConstants.NULL_INT ? RowSet.NULL_ROW_KEY : innerIndex);
        }
        innerRowKeys.setSize(sz);
    }

    @Override
    public final void fillPrevChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillPrevChunk(effectiveContext.colSrcCtx, effectiveContext.intChunk, outerRowKeys);
        final int sz = outerRowKeys.intSize();
        for (int ii = 0; ii < sz; ++ii) {
            final int innerIndex = effectiveContext.intChunk.get(ii);
            innerRowKeys.set(ii, innerIndex == QueryConstants.NULL_INT ? RowSet.NULL_ROW_KEY : innerIndex);
        }
        innerRowKeys.setSize(sz);
    }

    @Override
    public final long remove(long outerRowKey) {
        final int previous = columnSource.getInt(outerRowKey);
        if (previous == QueryConstants.NULL_INT) {
            return RowSet.NULL_ROW_KEY;
        }
        columnSource.set(outerRowKey, QueryConstants.NULL_INT);
        return previous;
    }

    @Override
    public final void removeVoid(long outerRowKey) {
        columnSource.set(outerRowKey, QueryConstants.NULL_INT);
    }

    @Override
    public final void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }
}
