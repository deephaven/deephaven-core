package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

/**
 * Regioned column source implementation for columns of Booleans.
 */
final class RegionedColumnSourceBoolean
        extends RegionedColumnSourceReferencing<Boolean, Attributes.Values, Byte, ColumnRegionByte<Attributes.Values>>
        implements ColumnSourceGetDefaults.ForBoolean {

    public RegionedColumnSourceBoolean() {
        super(NullColumnRegionBooleanAsByte.INSTANCE, Boolean.class, RegionedColumnSourceByte.NativeType.AsValues::new);
    }

    @Override
    public void convertRegion(WritableChunk<? super Attributes.Values> destination,
                              Chunk<? extends Attributes.Values> source, OrderedKeys orderedKeys) {
        WritableObjectChunk<Boolean, ? super Attributes.Values> objectChunk = destination.asWritableObjectChunk();
        ByteChunk<? extends Attributes.Values> byteChunk = source.asByteChunk();

        final int size = destination.size();
        final int length = byteChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, BooleanUtils.byteAsBoolean(byteChunk.get(i)));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public Boolean get(long elementIndex) {
        return elementIndex == NULL_KEY ? null :
                BooleanUtils.byteAsBoolean(lookupRegion(elementIndex).getReferencedRegion().getByte(elementIndex));
    }

    static final class NullColumnRegionBooleanAsByte extends ColumnRegion.Null<Attributes.Values> implements ColumnRegionByte<Attributes.Values> {
        private static final NullColumnRegionBooleanAsByte INSTANCE = new NullColumnRegionBooleanAsByte();

        private NullColumnRegionBooleanAsByte() {
            super(PARAMETERS.regionMask);
        }

        @Override
        public byte getByte(long elementIndex) {
            return BooleanUtils.NULL_BOOLEAN_AS_BYTE;
        }

        @Override
        public byte[] getBytes(long firstElementIndex, @NotNull byte[] destination, int destinationOffset, int length) {
            Arrays.fill(destination, destinationOffset, destinationOffset + length, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
            return destination;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, int size) {
           int offset = destination.size();

           destination.asWritableByteChunk().fillWithValue(offset, size, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
           destination.setSize(offset + size);
        }
    }
}
