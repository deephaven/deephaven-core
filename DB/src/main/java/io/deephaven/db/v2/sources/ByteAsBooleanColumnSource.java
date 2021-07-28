/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.util.BooleanUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
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
        final byte byteValue = alternateColumnSource.getByte(index);
        // the underlying column source may store null values (which we define differently than null boolean), which we should convert to null values
        if (byteValue == QueryConstants.NULL_BYTE) {
            return null;
        }
        return BooleanUtils.byteAsBoolean(byteValue);
    }

    @Override
    public Boolean getPrev(final long index) {
        final byte byteValue = alternateColumnSource.getPrevByte(index);
        if (byteValue == QueryConstants.NULL_BYTE) {
            return null;
        }
        return BooleanUtils.byteAsBoolean(byteValue);
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
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        final ByteChunk<? extends Values> byteChunk = alternateColumnSource.getChunk(toBooleanFillContext.alternateGetContext, orderedKeys).asByteChunk();
        convertToBoolean(destination, byteChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        final ByteChunk<? extends Values> byteChunk = alternateColumnSource.getPrevChunk(toBooleanFillContext.alternateGetContext, orderedKeys).asByteChunk();
        convertToBoolean(destination, byteChunk);
    }

    private static void convertToBoolean(@NotNull final WritableChunk<? super Values> destination, @NotNull final ByteChunk<? extends Values> byteChunk) {
        final WritableObjectChunk<Boolean, ? super Values> booleanObjectDestination = destination.asWritableObjectChunk();
        for (int ii = 0; ii < byteChunk.size(); ++ii) {
            final byte byteValue = byteChunk.get(ii);
            if (byteValue == QueryConstants.NULL_BYTE) {
                booleanObjectDestination.set(ii, null);
            } else {
                booleanObjectDestination.set(ii, BooleanUtils.byteAsBoolean(byteValue));
            }
        }
        booleanObjectDestination.setSize(byteChunk.size());
    }
}
