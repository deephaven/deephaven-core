//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * A column source that reads every value from a delegate column source.
 * <p>
 * Pushdown filtering is deliberately not forwarded to the delegate. A subclass may override the reads to change the
 * values or the type it presents, as {@code SnapshotUtils}' direct-vector source does, and a forwarded pushdown would
 * still answer from the delegate's values. A filter on a delegating column source therefore scans the values through
 * {@link #fillChunk}, and a subclass that can answer a filter without a scan, such as
 * {@link io.deephaven.engine.table.impl.by.AggregationKeyColumnSource}, implements the pushdown methods itself.
 */
public class DelegatingColumnSource<T, R> extends AbstractColumnSource<T> implements FillUnordered<Values> {
    final ColumnSource<R> delegate;

    public DelegatingColumnSource(Class<T> type, Class<?> componentType, ColumnSource<R> delegate) {
        super(type, componentType);
        this.delegate = delegate;
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

    /**
     * Whether the delegate fills unordered row keys. A subclass that changes what {@link #makeFillContext} or
     * {@link #fillChunk} produce returns {@code false}, unless it also overrides the unordered fills.
     */
    @Override
    public boolean providesFillUnordered() {
        return FillUnordered.providesFillUnordered(delegate);
    }

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        // noinspection unchecked
        ((FillUnordered<Values>) delegate).fillChunkUnordered(context, dest, keys);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        // noinspection unchecked
        ((FillUnordered<Values>) delegate).fillPrevChunkUnordered(context, dest, keys);
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
