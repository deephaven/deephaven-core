/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.util.BooleanUtils;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link byte} to {@code Boolean} values.
 */
@AbstractColumnSource.IsSerializable(value = true)
public class ByteAsBooleanColumnSource extends AbstractColumnSource<Boolean> implements MutableColumnSourceGetDefaults.ForBoolean {

    private final ColumnSource<Byte> alternateColumnSource;

    public ByteAsBooleanColumnSource(@NotNull final ColumnSource<Byte> alternateColumnSource) {
        super(Boolean.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public Boolean get(final long index) {
        return BooleanUtils.byteAsBoolean(alternateColumnSource.getByte(index));
    }

    @Override
    public Boolean getPrev(final long index) {
        return BooleanUtils.byteAsBoolean(alternateColumnSource.getPrevByte(index));
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == byte.class || alternateDataType == Byte.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        //noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }

    private class ToBooleanFillContext implements FillContext {
        final GetContext alternateGetContext;

        private ToBooleanFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateGetContext = alternateColumnSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            alternateGetContext.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ToBooleanFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        final ByteChunk<? extends Values> byteChunk = alternateColumnSource.getChunk(toBooleanFillContext.alternateGetContext, rowSequence).asByteChunk();
        convertToBoolean(destination, byteChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        final ByteChunk<? extends Values> byteChunk = alternateColumnSource.getPrevChunk(toBooleanFillContext.alternateGetContext, rowSequence).asByteChunk();
        convertToBoolean(destination, byteChunk);
    }

    private static void convertToBoolean(@NotNull final WritableChunk<? super Values> destination, @NotNull final ByteChunk<? extends Values> byteChunk) {
        final WritableObjectChunk<Boolean, ? super Values> booleanObjectDestination = destination.asWritableObjectChunk();
        for (int ii = 0; ii < byteChunk.size(); ++ii) {
            booleanObjectDestination.set(ii, BooleanUtils.byteAsBoolean(byteChunk.get(ii)));
        }
        booleanObjectDestination.setSize(byteChunk.size());
    }
}
