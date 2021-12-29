/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPage and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.page;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

public class DoubleChunkPage<ATTR extends Any> extends DoubleChunk<ATTR> implements ChunkPage<ATTR> {

    private final long mask;
    private final long firstRow;

    public static <ATTR extends Any> DoubleChunkPage<ATTR> pageWrap(long beginRow, double[] data, int offset, int capacity, long mask) {
        return new DoubleChunkPage<>(beginRow, data, offset, capacity, mask);
    }

    public static <ATTR extends Any> DoubleChunkPage<ATTR> pageWrap(long beginRow, double[] data, long mask) {
        return new DoubleChunkPage<>(beginRow, data, 0, data.length, mask);
    }

    private DoubleChunkPage(long firstRow, double[] data, int offset, int capacity, long mask) {
        super(data, offset, Require.lt(capacity, "capacity", Integer.MAX_VALUE, "INT_MAX"));
        this.mask = mask;
        this.firstRow = Require.inRange(firstRow, "firstRow", mask, "mask");
    }

    @Override
    public final void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull RowSequence rowSequence) {
        WritableDoubleChunk<? super ATTR> to = destination.asWritableDoubleChunk();

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
