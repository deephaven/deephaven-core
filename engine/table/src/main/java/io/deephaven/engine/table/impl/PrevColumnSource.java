/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
    public final T get(long rowKey) {
        return originalSource.getPrev(rowKey);
    }

    @Override
    public final Boolean getBoolean(long rowKey) {
        return originalSource.getPrevBoolean(rowKey);
    }

    @Override
    public final byte getByte(long rowKey) {
        return originalSource.getPrevByte(rowKey);
    }

    @Override
    public final char getChar(long rowKey) {
        return originalSource.getPrevChar(rowKey);
    }

    @Override
    public final double getDouble(long rowKey) {
        return originalSource.getPrevDouble(rowKey);
    }

    @Override
    public final float getFloat(long rowKey) {
        return originalSource.getPrevFloat(rowKey);
    }

    @Override
    public final int getInt(long rowKey) {
        return originalSource.getPrevInt(rowKey);
    }

    @Override
    public final long getLong(long rowKey) {
        return originalSource.getPrevLong(rowKey);
    }

    @Override
    public final short getShort(long rowKey) {
        return originalSource.getPrevShort(rowKey);
    }

    @Override
    public final T getPrev(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Boolean getPrevBoolean(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final byte getPrevByte(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final char getPrevChar(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final double getPrevDouble(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final float getPrevFloat(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getPrevInt(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getPrevLong(long rowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final short getPrevShort(long rowKey) {
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
    public boolean isStateless() {
        return originalSource.isStateless();
    }
}
