/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.RedirectionIndex;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnSource} that provides a redirected view into another {@link ColumnSource} by mapping keys using a
 * {@link RedirectionIndex}.
 */
public class RedirectedColumnSource<T> extends ReadOnlyRedirectedColumnSource<T> implements WritableSource<T> {
    private long maxInnerIndex;

    /**
     * Create a type-appropriate RedirectedColumnSource for the supplied {@link RedirectionIndex} and inner
     * {@link ColumnSource}.
     *
     * @param redirectionIndex The redirection index to use
     * @param innerSource      The column source to redirect
     * @param maxInnerIndex    The maximum index key available in innerSource
     */
    public RedirectedColumnSource(@NotNull final RedirectionIndex redirectionIndex,
                                   @NotNull final ColumnSource<T> innerSource,
                                   final long maxInnerIndex) {
        super(redirectionIndex, innerSource);
        this.maxInnerIndex = maxInnerIndex;
    }

    @Override
    public void set(long key, T value) {
        //noinspection unchecked
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void set(long key, byte value) {
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void set(long key, char value) {
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void set(long key, double value) {
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void set(long key, float value) {
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void set(long key, int value) {
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void set(long key, long value) {
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void set(long key, short value) {
        ((WritableSource)innerSource).set(redirectionIndex.get(key),value);
    }

    @Override
    public void copy(ColumnSource<T> sourceColumn, long sourceKey, long destKey) {
        long innerDest = redirectionIndex.get(destKey);
        if (innerDest == -1) {
            innerDest = ++maxInnerIndex;
            ensureCapacity(maxInnerIndex + 1);
            redirectionIndex.put(destKey,innerDest);
        }
        else if (innerDest > maxInnerIndex) {
            ensureCapacity(innerDest + 1);
            maxInnerIndex = innerDest;
        }
        //noinspection unchecked
        ((WritableSource)innerSource).copy(sourceColumn, sourceKey, innerDest);
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ((WritableSource)innerSource).ensureCapacity(capacity, nullFill);
    }

    private class RedirectionFillFrom implements FillFromContext {
        final RedirectionIndex.FillContext redirectionFillContext;
        final FillFromContext innerFillFromContext;
        final WritableLongChunk<KeyIndices> redirections;

        private RedirectionFillFrom(int chunkCapacity) {
            this.redirectionFillContext = redirectionIndex.makeFillContext(chunkCapacity, null);
            this.innerFillFromContext = ((WritableSource)innerSource).makeFillFromContext(chunkCapacity);
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
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Attributes.Values> src, @NotNull OrderedKeys orderedKeys) {
        //noinspection unchecked
        final RedirectionFillFrom redirectionFillFrom = (RedirectionFillFrom)context;
        redirectionIndex.fillChunk(redirectionFillFrom.redirectionFillContext, redirectionFillFrom.redirections, orderedKeys);
        ((WritableSource)innerSource).fillFromChunkUnordered(redirectionFillFrom.innerFillFromContext, src, redirectionFillFrom.redirections);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Attributes.Values> src, @NotNull LongChunk<KeyIndices> keys) {
        //noinspection unchecked
        final RedirectionFillFrom redirectionFillFrom = (RedirectionFillFrom)context;
        redirectionIndex.fillChunkUnordered(redirectionFillFrom.redirectionFillContext, redirectionFillFrom.redirections, keys);
        ((WritableSource)innerSource).fillFromChunkUnordered(redirectionFillFrom.innerFillFromContext, src, redirectionFillFrom.redirections);
    }
}