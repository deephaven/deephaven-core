/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

/**
 * Regioned column source implementation for columns of Booleans.
 */
final class RegionedColumnSourceBoolean
        extends RegionedColumnSourceReferencing<Boolean, Values, Byte, ColumnRegionByte<Values>>
        implements ColumnSourceGetDefaults.ForBoolean {

    public RegionedColumnSourceBoolean() {
        this(new RegionedColumnSourceByte.AsValues());
    }

    public RegionedColumnSourceBoolean(@NotNull final RegionedColumnSourceByte<Values> inner) {
        super(ColumnRegionByte.createNull(PARAMETERS.regionMask), Boolean.class, inner);
    }

    @Override
    public void convertRegion(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final Chunk<? extends Values> source,
            @NotNull final RowSequence rowSequence) {
        WritableObjectChunk<Boolean, ? super Values> objectChunk = destination.asWritableObjectChunk();
        ByteChunk<? extends Values> byteChunk = source.asByteChunk();

        final int size = destination.size();
        final int length = byteChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, BooleanUtils.byteAsBoolean(byteChunk.get(i)));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public Boolean get(long rowKey) {
        return rowKey == RowSequence.NULL_ROW_KEY ? null :
                BooleanUtils.byteAsBoolean(getNativeSource().lookupRegion(rowKey).getByte(rowKey));
    }
}
