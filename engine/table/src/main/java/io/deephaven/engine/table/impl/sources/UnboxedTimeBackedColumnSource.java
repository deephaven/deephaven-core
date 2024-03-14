//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates various Time sources to {@code long} values.
 */
public abstract class UnboxedTimeBackedColumnSource<TIME_TYPE> extends AbstractColumnSource<Long>
        implements MutableColumnSourceGetDefaults.ForLong {
    private final ColumnSource<TIME_TYPE> alternateColumnSource;

    private class UnboxingFillContext implements FillContext {
        final FillContext alternateFillContext;
        final WritableObjectChunk<TIME_TYPE, Values> innerChunk;

        private UnboxingFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateFillContext = alternateColumnSource.makeFillContext(chunkCapacity, sharedContext);
            innerChunk = WritableObjectChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            alternateFillContext.close();
            innerChunk.close();
        }
    }

    public UnboxedTimeBackedColumnSource(ColumnSource<TIME_TYPE> alternateColumnSource) {
        super(long.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    protected abstract long toEpochNano(TIME_TYPE val);

    @Override
    public long getLong(long rowKey) {
        return toEpochNano(alternateColumnSource.get(rowKey));
    }

    @Override
    public long getPrevLong(long rowKey) {
        return toEpochNano(alternateColumnSource.getPrev(rowKey));
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == alternateColumnSource.getType();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new UnboxingFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        final UnboxingFillContext unboxingFillContext = (UnboxingFillContext) context;
        final WritableObjectChunk<TIME_TYPE, Values> innerChunk = unboxingFillContext.innerChunk;
        alternateColumnSource.fillChunk(unboxingFillContext.alternateFillContext, innerChunk, rowSequence);
        convertToLong(destination, innerChunk);
        innerChunk.fillWithNullValue(0, innerChunk.size());
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        final UnboxingFillContext unboxingFillContext = (UnboxingFillContext) context;
        final WritableObjectChunk<TIME_TYPE, Values> innerChunk = unboxingFillContext.innerChunk;
        alternateColumnSource.fillPrevChunk(unboxingFillContext.alternateFillContext, innerChunk, rowSequence);
        convertToLong(destination, innerChunk);
        innerChunk.fillWithNullValue(0, innerChunk.size());
    }

    private void convertToLong(@NotNull WritableChunk<? super Values> destination,
            ObjectChunk<TIME_TYPE, Values> innerChunk) {
        final WritableLongChunk<? super Values> longDestination = destination.asWritableLongChunk();
        for (int ii = 0; ii < innerChunk.size(); ++ii) {
            longDestination.set(ii, toEpochNano(innerChunk.get(ii)));
        }
        longDestination.setSize(innerChunk.size());
    }
}
