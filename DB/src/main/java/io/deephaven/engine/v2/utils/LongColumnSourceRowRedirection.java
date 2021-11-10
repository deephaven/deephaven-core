package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Attributes.RowKeys;
import io.deephaven.engine.rftable.ChunkSource;
import io.deephaven.engine.rftable.SharedContext;
import io.deephaven.engine.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * {@link RowRedirection} implementation that wraps a {@link ColumnSource} of {@code longs}.
 */
public class LongColumnSourceRowRedirection<CST extends ColumnSource<Long>> implements RowRedirection {

    protected final CST columnSource;

    public LongColumnSourceRowRedirection(CST columnSource) {
        this.columnSource = columnSource;
    }

    @Override
    public final long get(final long outerRowKey) {
        final long innerIndex = columnSource.getLong(outerRowKey);
        if (innerIndex == QueryConstants.NULL_LONG) {
            return RowSet.NULL_ROW_KEY;
        }
        return innerIndex;
    }

    @Override
    public final long getPrev(final long outerRowKey) {
        final long innerIndex = columnSource.getPrevLong(outerRowKey);
        if (innerIndex == QueryConstants.NULL_LONG) {
            return RowSet.NULL_ROW_KEY;
        }
        return innerIndex;
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
        final WritableLongChunk<Attributes.Values> asValuesChunk = WritableLongChunk.upcast(innerRowKeys);
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillChunk(effectiveContext.colSrcCtx, asValuesChunk, outerRowKeys);
        for (int ii = 0; ii < innerRowKeys.size(); ++ii) {
            if (innerRowKeys.get(ii) == QueryConstants.NULL_LONG) {
                innerRowKeys.set(ii, RowSet.NULL_ROW_KEY);
            }
        }
    }

    @Override
    public final void fillPrevChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<Attributes.Values> asValuesChunk =
                WritableLongChunk.downcast(WritableLongChunk.upcast(innerRowKeys));
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillPrevChunk(effectiveContext.colSrcCtx, asValuesChunk, outerRowKeys);
        for (int ii = 0; ii < innerRowKeys.size(); ++ii) {
            if (innerRowKeys.get(ii) == QueryConstants.NULL_LONG) {
                innerRowKeys.set(ii, RowSet.NULL_ROW_KEY);
            }
        }
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final ColumnSource.FillContext colSrcCtx;

        private FillContext(@NotNull final LongColumnSourceRowRedirection csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
        }

        @Override
        public final void close() {
            colSrcCtx.close();
        }
    }
}
