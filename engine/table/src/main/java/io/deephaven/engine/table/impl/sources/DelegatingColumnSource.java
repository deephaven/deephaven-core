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
    public T get(long index) {
        return (T) delegate.get(index);
    }

    @Override
    public Boolean getBoolean(long index) {
        return delegate.getBoolean(index);
    }

    @Override
    public byte getByte(long index) {
        return delegate.getByte(index);
    }

    @Override
    public char getChar(long index) {
        return delegate.getChar(index);
    }

    @Override
    public double getDouble(long index) {
        return delegate.getDouble(index);
    }

    @Override
    public float getFloat(long index) {
        return delegate.getFloat(index);
    }

    @Override
    public int getInt(long index) {
        return delegate.getInt(index);
    }

    @Override
    public long getLong(long index) {
        return delegate.getLong(index);
    }

    @Override
    public short getShort(long index) {
        return delegate.getShort(index);
    }

    @Override
    public T getPrev(long index) {
        return (T) delegate.getPrev(index);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return delegate.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    @FinalDefault
    public FillContext makeFillContext(int chunkCapacity) {
        return delegate.makeFillContext(chunkCapacity);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return delegate.makeGetContext(chunkCapacity, sharedContext);
    }

    @Override
    @FinalDefault
    public GetContext makeGetContext(int chunkCapacity) {
        return delegate.makeGetContext(chunkCapacity);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        return delegate.getPrevBoolean(index);
    }

    @Override
    public byte getPrevByte(long index) {
        return delegate.getPrevByte(index);
    }

    @Override
    public char getPrevChar(long index) {
        return delegate.getPrevChar(index);
    }

    @Override
    public double getPrevDouble(long index) {
        return delegate.getPrevDouble(index);
    }

    @Override
    public float getPrevFloat(long index) {
        return delegate.getPrevFloat(index);
    }

    @Override
    public int getPrevInt(long index) {
        return delegate.getPrevInt(index);
    }

    @Override
    public long getPrevLong(long index) {
        return delegate.getPrevLong(index);
    }

    @Override
    public short getPrevShort(long index) {
        return delegate.getPrevShort(index);
    }

    @Override
    public boolean preventsParallelism() {
        return delegate.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return delegate.isStateless();
    }
}
