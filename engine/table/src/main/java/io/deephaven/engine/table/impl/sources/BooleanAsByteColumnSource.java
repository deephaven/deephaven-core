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
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link Boolean} to {@code byte} values.
 */
public class BooleanAsByteColumnSource extends AbstractColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte {

    private final ColumnSource<Boolean> alternateColumnSource;

    public BooleanAsByteColumnSource(@NotNull final ColumnSource<Boolean> alternateColumnSource) {
        super(byte.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public byte getByte(final long index) {
        return BooleanUtils.booleanAsByte(alternateColumnSource.get(index));
    }

    @Override
    public byte getPrevByte(final long index) {
        return BooleanUtils.booleanAsByte(alternateColumnSource.getPrev(index));
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == Boolean.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        //noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }

    private class UnboxedFillContext implements FillContext {
        final GetContext alternateGetContext;

        private UnboxedFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateGetContext = alternateColumnSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            alternateGetContext.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new UnboxedFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final UnboxedFillContext unboxedFillContext = (UnboxedFillContext) context;
        final ObjectChunk<Boolean, ? extends Values> booleanObjectChunk = alternateColumnSource.getChunk(unboxedFillContext.alternateGetContext, rowSequence).asObjectChunk();
        convertToByte(destination, booleanObjectChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final UnboxedFillContext unboxedFillContext = (UnboxedFillContext) context;
        final ObjectChunk<Boolean, ? extends Values> booleanObjectChunk = alternateColumnSource.getPrevChunk(unboxedFillContext.alternateGetContext, rowSequence).asObjectChunk();
        convertToByte(destination, booleanObjectChunk);
    }

    private static void convertToByte(@NotNull final WritableChunk<? super Values> destination, @NotNull final ObjectChunk<Boolean, ? extends Values> booleanObjectChunk) {
        final WritableByteChunk<? super Values> byteDestination = destination.asWritableByteChunk();
        for (int ii = 0; ii < booleanObjectChunk.size(); ++ii) {
            byteDestination.set(ii, BooleanUtils.booleanAsByte(booleanObjectChunk.get(ii)));
        }
        byteDestination.setSize(booleanObjectChunk.size());
    }

    @Override
    public boolean preventsParallelism() {
        return alternateColumnSource.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return alternateColumnSource.isStateless();
    }
}
