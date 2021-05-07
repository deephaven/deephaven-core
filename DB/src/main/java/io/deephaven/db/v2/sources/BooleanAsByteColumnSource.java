/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link Boolean} to {@code byte} values.
 */
@AbstractColumnSource.IsSerializable(value = true)
public class BooleanAsByteColumnSource extends AbstractColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte {

    private final ColumnSource<Boolean> alternateColumnSource;

    public BooleanAsByteColumnSource(ColumnSource<Boolean> alternateColumnSource) {
        super(byte.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public byte getByte(long index) {
        return BooleanUtils.booleanAsByte(alternateColumnSource.get(index));
    }

    @Override
    public byte getPrevByte(long index) {
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
        final FillContext alternateFillContext;
        final WritableObjectChunk<Boolean, Values> booleanObjectChunk;

        private UnboxedFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateFillContext = alternateColumnSource.makeFillContext(chunkCapacity, sharedContext);
            booleanObjectChunk = WritableObjectChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            alternateFillContext.close();
            booleanObjectChunk.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new UnboxedFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        final UnboxedFillContext unboxedFillContext = (UnboxedFillContext) context;
        final WritableObjectChunk<Boolean, Values> booleanObjectChunk = unboxedFillContext.booleanObjectChunk;
        alternateColumnSource.fillChunk(unboxedFillContext.alternateFillContext, booleanObjectChunk, orderedKeys);
        convertToByte(destination, booleanObjectChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        final UnboxedFillContext unboxedFillContext = (UnboxedFillContext) context;
        final WritableObjectChunk<Boolean, Values> booleanObjectChunk = unboxedFillContext.booleanObjectChunk;
        alternateColumnSource.fillPrevChunk(unboxedFillContext.alternateFillContext, booleanObjectChunk, orderedKeys);
        convertToByte(destination, booleanObjectChunk);
    }

    private static void convertToByte(@NotNull WritableChunk<? super Values> destination, ObjectChunk<Boolean, Values> booleanObjectChunk) {
        final WritableByteChunk<? super Values> byteDestination = destination.asWritableByteChunk();
        for (int ii = 0; ii < booleanObjectChunk.size(); ++ii) {
            byteDestination.set(ii, BooleanUtils.booleanAsByte(booleanObjectChunk.get(ii)));
        }
        byteDestination.setSize(booleanObjectChunk.size());
    }
}
