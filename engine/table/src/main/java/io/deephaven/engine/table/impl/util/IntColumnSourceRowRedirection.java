//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link RowRedirection} implementation that wraps a {@link ColumnSource} of {@code ints}.
 */
public class IntColumnSourceRowRedirection<CST extends ColumnSource<Integer>> implements RowRedirection {

    protected final CST columnSource;

    public IntColumnSourceRowRedirection(@NotNull final CST columnSource) {
        this.columnSource = columnSource;
    }

    @Override
    public long get(final long outerRowKey) {
        final int innerRowKey = columnSource.getInt(outerRowKey);
        if (innerRowKey == NULL_INT) {
            return NULL_ROW_KEY;
        }
        return innerRowKey;
    }

    @Override
    public long getPrev(final long outerRowKey) {
        final int innerRowKey = columnSource.getPrevInt(outerRowKey);
        if (innerRowKey == NULL_INT) {
            return NULL_ROW_KEY;
        }
        return innerRowKey;
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
            final int innerRowKey = effectiveContext.intChunk.get(ii);
            innerRowKeysTyped.set(ii, innerRowKey == NULL_INT ? NULL_ROW_KEY : innerRowKey);
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
            final int innerRowKey = effectiveContext.intChunk.get(ii);
            innerRowKeysTyped.set(ii, innerRowKey == NULL_INT ? NULL_ROW_KEY : innerRowKey);
        }
        innerRowKeysTyped.setSize(sz);
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final ColumnSource.FillContext colSrcCtx;
        private final WritableIntChunk<Values> intChunk;

        private FillContext(final IntColumnSourceRowRedirection<?> csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
            intChunk = WritableIntChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public void close() {
            colSrcCtx.close();
            intChunk.close();
        }
    }
}
