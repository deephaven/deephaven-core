/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPage and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.page;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

public class ShortChunkPage<ATTR extends Any> extends ShortChunk<ATTR> implements ChunkPage<ATTR> {

    private final long mask;
    private final long firstRow;

    public static <ATTR extends Any> ShortChunkPage<ATTR> pageWrap(
            final long firstRow,
            @NotNull final short[] data,
            final int offset,
            final int capacity,
            final long mask) {
        return new ShortChunkPage<>(firstRow, data, offset, capacity, mask);
    }

    public static <ATTR extends Any> ShortChunkPage<ATTR> pageWrap(
            final long firstRow,
            @NotNull final short[] data,
            final long mask) {
        return new ShortChunkPage<>(firstRow, data, 0, data.length, mask);
    }

    private ShortChunkPage(
            final long firstRow,
            @NotNull final short[] data,
            final int offset,
            final int capacity,
            final long mask) {
        super(data, offset, capacity);
        this.mask = mask;
        this.firstRow = Require.inRange(firstRow, "firstRow", mask, "mask");
    }

    @Override
    public final void fillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence) {
        WritableShortChunk<? super ATTR> to = destination.asWritableShortChunk();

        if (rowSequence.getAverageRunLengthEstimate() >= Chunk.SYSTEM_ARRAYCOPY_THRESHOLD) {
            rowSequence.forAllRowKeyRanges((final long rangeStartKey, final long rangeEndKey) -> to.appendTypedChunk(
                    this, getChunkOffset(rangeStartKey), (int) (rangeEndKey - rangeStartKey + 1)));
        } else {
            rowSequence.forAllRowKeys((final long key) -> to.add(get(getChunkOffset(key))));
        }
    }

    @Override
    public final long firstRowOffset() {
        return firstRow;
    }

    @Override
    public final long mask() {
        return mask;
    }
}
