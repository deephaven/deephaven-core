/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.utils.MutableRowRedirection;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnSource} that provides a redirected view into another {@link ColumnSource} by mapping keys using a
 * {@link MutableRowRedirection}.
 */
public class WritableRedirectedColumnSource<T> extends RedirectedColumnSource<T> implements WritableSource<T> {
    private long maxInnerIndex;

    /**
     * Create a type-appropriate WritableRedirectedColumnSource for the supplied {@link MutableRowRedirection} and inner
     * {@link ColumnSource}.
     *
     * @param rowRedirection The redirection rowSet to use
     * @param innerSource The column source to redirect
     * @param maxInnerIndex The maximum rowSet key available in innerSource
     */
    public WritableRedirectedColumnSource(@NotNull final MutableRowRedirection rowRedirection,
                                          @NotNull final ColumnSource<T> innerSource,
                                          final long maxInnerIndex) {
        super(rowRedirection, innerSource);
        this.maxInnerIndex = maxInnerIndex;
    }

    @Override
    public void set(long key, T value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, byte value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, char value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, double value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, float value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, int value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, long value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void set(long key, short value) {
        ((WritableSource<T>) innerSource).set(rowRedirection.get(key), value);
    }

    @Override
    public void copy(ColumnSource<? extends T> sourceColumn, long sourceKey, long destKey) {
        long innerDest = rowRedirection.get(destKey);
        if (innerDest == -1) {
            innerDest = ++maxInnerIndex;
            ensureCapacity(maxInnerIndex + 1);
            rowRedirection.mutableCast().put(destKey, innerDest);
        } else if (innerDest > maxInnerIndex) {
            ensureCapacity(innerDest + 1);
            maxInnerIndex = innerDest;
        }
        ((WritableSource<T>) innerSource).copy(sourceColumn, sourceKey, innerDest);
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ((WritableSource<T>) innerSource).ensureCapacity(capacity, nullFill);
    }

    private class RedirectionFillFrom implements FillFromContext {
        final ChunkSource.FillContext redirectionFillContext;
        final FillFromContext innerFillFromContext;
        final WritableLongChunk<RowKeys> redirections;

        private RedirectionFillFrom(int chunkCapacity) {
            this.redirectionFillContext = rowRedirection.makeFillContext(chunkCapacity, null);
            this.innerFillFromContext = ((WritableSource<T>) innerSource).makeFillFromContext(chunkCapacity);
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
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Attributes.Values> src,
            @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        final RedirectionFillFrom redirectionFillFrom = (RedirectionFillFrom) context;
        rowRedirection.fillChunk(redirectionFillFrom.redirectionFillContext, redirectionFillFrom.redirections,
                rowSequence);
        ((WritableSource<T>) innerSource).fillFromChunkUnordered(redirectionFillFrom.innerFillFromContext, src,
                redirectionFillFrom.redirections);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context,
            @NotNull Chunk<? extends Attributes.Values> src, @NotNull LongChunk<RowKeys> keys) {
        // noinspection unchecked
        final RedirectionFillFrom redirectionFillFrom = (RedirectionFillFrom) context;
        rowRedirection.fillChunkUnordered(redirectionFillFrom.redirectionFillContext,
                redirectionFillFrom.redirections, keys);
        ((WritableSource<T>) innerSource).fillFromChunkUnordered(redirectionFillFrom.innerFillFromContext, src,
                redirectionFillFrom.redirections);
    }
}
