/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnSource} that provides a redirected view into another {@link ColumnSource} by mapping keys using a
 * {@link WritableRowRedirection}.
 */
public class WritableRedirectedColumnSource<T> extends RedirectedColumnSource<T> implements WritableColumnSource<T> {
    private long maxInnerIndex;

    /**
     * Create a type-appropriate WritableRedirectedColumnSource for the supplied {@link WritableRowRedirection} and
     * inner {@link ColumnSource}.
     *
     * @param rowRedirection The row redirection to use
     * @param innerSource The column source to redirect
     * @param maxInnerIndex The maximum row key available in innerSource
     */
    public WritableRedirectedColumnSource(@NotNull final WritableRowRedirection rowRedirection,
            @NotNull final ColumnSource<T> innerSource,
            final long maxInnerIndex) {
        super(rowRedirection, innerSource);
        this.maxInnerIndex = maxInnerIndex;
    }

    @Override
    public void set(long key, T value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, byte value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, char value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, double value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, float value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, int value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, long value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, short value) {
        ((WritableColumnSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ((WritableColumnSource<T>) innerSource).ensureCapacity(capacity, nullFill);
    }

    private class RedirectionFillFrom implements FillFromContext {
        final ChunkSource.FillContext redirectionFillContext;
        final FillFromContext innerFillFromContext;
        final WritableLongChunk<RowKeys> redirections;

        private RedirectionFillFrom(int chunkCapacity) {
            this.redirectionFillContext = rowRedirection.makeFillContext(chunkCapacity, null);
            this.innerFillFromContext = ((WritableColumnSource<T>) innerSource).makeFillFromContext(chunkCapacity);
            this.redirections = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            redirectionFillContext.close();
            innerFillFromContext.close();
            redirections.close();
        }
    }

    @Override
    public FillFromContext makeFillFromContext(int chunkCapacity) {
        return new RedirectionFillFrom(chunkCapacity);
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        final RedirectionFillFrom redirectionFillFrom = (RedirectionFillFrom) context;
        rowRedirection.fillChunk(redirectionFillFrom.redirectionFillContext, redirectionFillFrom.redirections,
                rowSequence);
        ((WritableColumnSource<T>) innerSource).fillFromChunkUnordered(redirectionFillFrom.innerFillFromContext, src,
                redirectionFillFrom.redirections);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context,
            @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        // noinspection unchecked
        final RedirectionFillFrom redirectionFillFrom = (RedirectionFillFrom) context;
        rowRedirection.fillChunkUnordered(redirectionFillFrom.redirectionFillContext,
                redirectionFillFrom.redirections, keys);
        ((WritableColumnSource<T>) innerSource).fillFromChunkUnordered(redirectionFillFrom.innerFillFromContext, src,
                redirectionFillFrom.redirections);
    }
}
