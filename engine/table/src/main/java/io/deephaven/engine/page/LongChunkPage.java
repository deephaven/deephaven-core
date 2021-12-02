/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPage and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.page;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

public class LongChunkPage<ATTR extends Any> extends LongChunk<ATTR> implements ChunkPage<ATTR> {

    private final long mask;
    private final long firstRow;

    public static <ATTR extends Any> LongChunkPage<ATTR> pageWrap(long beginRow, long[] data, int offset, int capacity, long mask) {
        return new LongChunkPage<>(beginRow, data, offset, capacity, mask);
    }

    public static <ATTR extends Any> LongChunkPage<ATTR> pageWrap(long beginRow, long[] data, long mask) {
        return new LongChunkPage<>(beginRow, data, 0, data.length, mask);
    }

    private LongChunkPage(long firstRow, long[] data, int offset, int capacity, long mask) {
        super(data, offset, Require.lt(capacity, "capacity", Integer.MAX_VALUE, "INT_MAX"));
        this.mask = mask;
        this.firstRow = Require.inRange(firstRow, "firstRow", mask, "mask");
    }

    @Override
    public final void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull RowSequence rowSequence) {
        WritableLongChunk<? super ATTR> to = destination.asWritableLongChunk();

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
