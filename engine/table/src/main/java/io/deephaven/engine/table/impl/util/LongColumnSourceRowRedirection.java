//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link RowRedirection} implementation that wraps a {@link ColumnSource} of {@code longs}.
 */
public class LongColumnSourceRowRedirection<CST extends ColumnSource<Long>> implements RowRedirection {

    protected final CST columnSource;

    public LongColumnSourceRowRedirection(@NotNull final CST columnSource) {
        this.columnSource = columnSource;
    }

    @Override
    public final long get(final long outerRowKey) {
        final long innerRowKey = columnSource.getLong(outerRowKey);
        if (innerRowKey == NULL_LONG) {
            return NULL_ROW_KEY;
        }
        return innerRowKey;
    }

    @Override
    public final long getPrev(final long outerRowKey) {
        final long innerRowKey = columnSource.getPrevLong(outerRowKey);
        if (innerRowKey == NULL_LONG) {
            return NULL_ROW_KEY;
        }
        return innerRowKey;
    }

    @Override
    public final ChunkSource.FillContext makeFillContext(final int chunkSize, final SharedContext sharedContext) {
        return new FillContext(this, chunkSize);
    }

    @Override
    public final void fillChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        final WritableLongChunk<? super Values> asValuesChunk = WritableLongChunk.upcast(innerRowKeysTyped);
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillChunk(effectiveContext.colSrcCtx, asValuesChunk, outerRowKeys);
        final int size = innerRowKeysTyped.size();
        for (int ii = 0; ii < size; ++ii) {
            if (innerRowKeysTyped.get(ii) == NULL_LONG) {
                innerRowKeysTyped.set(ii, NULL_ROW_KEY);
            }
        }
    }

    @Override
    public final void fillPrevChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        final WritableLongChunk<? super Values> asValuesChunk = WritableLongChunk.upcast(innerRowKeysTyped);
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillPrevChunk(effectiveContext.colSrcCtx, asValuesChunk, outerRowKeys);
        final int size = innerRowKeysTyped.size();
        for (int ii = 0; ii < size; ++ii) {
            if (innerRowKeysTyped.get(ii) == NULL_LONG) {
                innerRowKeysTyped.set(ii, NULL_ROW_KEY);
            }
        }
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final ColumnSource.FillContext colSrcCtx;

        private FillContext(@NotNull final LongColumnSourceRowRedirection<?> csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
        }

        @Override
        public void close() {
            colSrcCtx.close();
        }

        @Override
        public boolean supportsUnboundedFill() {
            return colSrcCtx.supportsUnboundedFill();
        }
    }
}
