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

public class ObjectChunkPage<T, ATTR extends Any> extends WritableObjectChunk<T, ATTR> implements ChunkPage<ATTR> {

    private final long mask;
    private final long firstRow;

    public static <T, ATTR extends Any> ObjectChunkPage<T, ATTR> pageWrap(long beginRow, T[] data, int offset, int capacity, long mask) {
        return new ObjectChunkPage<>(beginRow, data, offset, capacity, mask);
    }

    public static <T, ATTR extends Any> ObjectChunkPage<T, ATTR> pageWrap(long beginRow, T[] data, long mask) {
        return new ObjectChunkPage<>(beginRow, data, 0, data.length, mask);
    }

    private ObjectChunkPage(long firstRow, T[] data, int offset, int capacity, long mask) {
        super(data, offset, Require.lt(capacity, "capacity", Integer.MAX_VALUE, "INT_MAX"));
        this.mask = mask;
        this.firstRow = Require.inRange(firstRow, "firstRow", mask, "mask");
    }

    @Override
    public final void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull RowSequence rowSequence) {
        WritableObjectChunk<T, ? super ATTR> to = destination.asWritableObjectChunk();

        if (rowSequence.getAverageRunLengthEstimate() >= Chunk.SYSTEM_ARRAYCOPY_THRESHOLD) {
            rowSequence.forAllRowKeyRanges((final long rangeStartKey, final long rangeEndKey) ->
                    to.appendTypedChunk(this, getChunkOffset(rangeStartKey), (int) (rangeEndKey - rangeStartKey + 1)));
        } else {
            rowSequence.forEachRowKey((final long key) -> {
                to.add(get(getChunkOffset(key)));
                return true;
            });
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
