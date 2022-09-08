/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

public class DelegatingColumnSource<T, R> extends AbstractColumnSource<T> {
    final ColumnSource<R> delegate;

    public DelegatingColumnSource(Class<T> type, Class<?> componentType, ColumnSource<R> delegate) {
        super(type, componentType);
        this.delegate = delegate;
    }

    @Override
    public WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, RowSet mapper,
            Object... keys) {
        return delegate.match(invertMatch, usePrev, caseInsensitive, mapper, keys);
    }

    @Override
    public void startTrackingPrevValues() {
        delegate.startTrackingPrevValues();
    }

    @Override
    public boolean isImmutable() {
        return delegate.isImmutable();
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        delegate.releaseCachedResources();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return delegate.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return delegate.reinterpret(alternateDataType);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return delegate.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return delegate.getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        delegate.fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return delegate.getPrevChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return delegate.getPrevChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        delegate.fillPrevChunk(context, destination, rowSequence);
    }

    @Override
    public T get(long rowKey) {
        return (T) delegate.get(rowKey);
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        return delegate.getBoolean(rowKey);
    }

    @Override
    public byte getByte(long rowKey) {
        return delegate.getByte(rowKey);
    }

    @Override
    public char getChar(long rowKey) {
        return delegate.getChar(rowKey);
    }

    @Override
    public double getDouble(long rowKey) {
        return delegate.getDouble(rowKey);
    }

    @Override
    public float getFloat(long rowKey) {
        return delegate.getFloat(rowKey);
    }

    @Override
    public int getInt(long rowKey) {
        return delegate.getInt(rowKey);
    }

    @Override
    public long getLong(long rowKey) {
        return delegate.getLong(rowKey);
    }

    @Override
    public short getShort(long rowKey) {
        return delegate.getShort(rowKey);
    }

    @Override
    public T getPrev(long rowKey) {
        return (T) delegate.getPrev(rowKey);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return delegate.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return delegate.makeGetContext(chunkCapacity, sharedContext);
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        return delegate.getPrevBoolean(rowKey);
    }

    @Override
    public byte getPrevByte(long rowKey) {
        return delegate.getPrevByte(rowKey);
    }

    @Override
    public char getPrevChar(long rowKey) {
        return delegate.getPrevChar(rowKey);
    }

    @Override
    public double getPrevDouble(long rowKey) {
        return delegate.getPrevDouble(rowKey);
    }

    @Override
    public float getPrevFloat(long rowKey) {
        return delegate.getPrevFloat(rowKey);
    }

    @Override
    public int getPrevInt(long rowKey) {
        return delegate.getPrevInt(rowKey);
    }

    @Override
    public long getPrevLong(long rowKey) {
        return delegate.getPrevLong(rowKey);
    }

    @Override
    public short getPrevShort(long rowKey) {
        return delegate.getPrevShort(rowKey);
    }

    @Override
    public boolean isStateless() {
        return delegate.isStateless();
    }
}
