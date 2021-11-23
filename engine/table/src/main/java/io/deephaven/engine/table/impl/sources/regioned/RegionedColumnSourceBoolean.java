package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.rowset.RowSequence;

/**
 * Regioned column source implementation for columns of Booleans.
 */
final class RegionedColumnSourceBoolean
        extends RegionedColumnSourceReferencing<Boolean, Attributes.Values, Byte, ColumnRegionByte<Attributes.Values>>
        implements ColumnSourceGetDefaults.ForBoolean {

    public RegionedColumnSourceBoolean() {
        super(ColumnRegionByte.createNull(PARAMETERS.regionMask), Boolean.class,
                RegionedColumnSourceByte.NativeType.AsValues::new);
    }

    @Override
    public void convertRegion(WritableChunk<? super Attributes.Values> destination,
                              Chunk<? extends Attributes.Values> source, RowSequence rowSequence) {
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
        return elementIndex == RowSequence.NULL_ROW_KEY ? null :
                BooleanUtils.byteAsBoolean(lookupRegion(elementIndex).getReferencedRegion().getByte(elementIndex));
    }
}
