/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper {@link ColumnSource} that maps current data accessors to previous data accessors (and disables previous data
 * accessors).
 */
public final class PrevColumnSource<T> extends AbstractColumnSource<T> {

    private final ColumnSource<T> originalSource;

    public PrevColumnSource(ColumnSource<T> originalSource) {
        super(originalSource.getType());
        this.originalSource = originalSource;
    }

    @Override
    public final T get(long index) {
        return originalSource.getPrev(index);
    }

    @Override
    public final Boolean getBoolean(long index) {
        return originalSource.getPrevBoolean(index);
    }

    @Override
    public final byte getByte(long index) {
        return originalSource.getPrevByte(index);
    }

    @Override
    public final char getChar(long index) {
        return originalSource.getPrevChar(index);
    }

    @Override
    public final double getDouble(long index) {
        return originalSource.getPrevDouble(index);
    }

    @Override
    public final float getFloat(long index) {
        return originalSource.getPrevFloat(index);
    }

    @Override
    public final int getInt(long index) {
        return originalSource.getPrevInt(index);
    }

    @Override
    public final long getLong(long index) {
        return originalSource.getPrevLong(index);
    }

    @Override
    public final short getShort(long index) {
        return originalSource.getPrevShort(index);
    }

    @Override
    public final T getPrev(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Boolean getPrevBoolean(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final byte getPrevByte(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final char getPrevChar(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final double getPrevDouble(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final float getPrevFloat(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getPrevInt(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getPrevLong(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final short getPrevShort(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Class<T> getType() {
        return originalSource.getType();
    }

    @Override
    public final Class<?> getComponentType() {
        return originalSource.getComponentType();
    }

    @Override
    public final boolean isImmutable() {
        return originalSource.isImmutable();
    }

    @Override
    public final ChunkType getChunkType() {
        return originalSource.getChunkType();
    }

    @Override
    public final GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return originalSource.makeGetContext(chunkCapacity, sharedContext);
    }

    @Override
    public final Chunk<? extends Values> getChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        return originalSource.getPrevChunk(context, rowSequence);
    }

    @Override
    public final Chunk<? extends Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return originalSource.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        originalSource.fillPrevChunk(context, destination, rowSequence);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean preventsParallelism() {
        return originalSource.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return originalSource.isStateless();
    }
}
