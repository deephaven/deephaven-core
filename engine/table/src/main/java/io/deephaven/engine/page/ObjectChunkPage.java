package io.deephaven.engine.page;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

public class ObjectChunkPage<T, ATTR extends Any> extends ObjectChunk<T, ATTR> implements ChunkPage<ATTR> {

    private final long mask;
    private final long firstRow;

    public static <T, ATTR extends Any> ObjectChunkPage<T, ATTR> pageWrap(long begRow, T[] data, int offset, int capacity, long mask) {
        return new ObjectChunkPage<>(begRow, data, offset, capacity, mask);
    }

    public static <T, ATTR extends Any> ObjectChunkPage<T, ATTR> pageWrap(long begRow, T[] data, long mask) {
        return new ObjectChunkPage<>(begRow, data, 0, data.length, mask);
    }

    private ObjectChunkPage(long firstRow, T[] data, int offset, int capacity, long mask) {
        super(data, offset, Require.lt(capacity, "capacity", Integer.MAX_VALUE, "INT_MAX"));
        this.mask = mask;
        this.firstRow = Require.inRange(firstRow, "firstRow", mask, "mask");
    }

    @Override
    public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull RowSequence rowSequence) {
        WritableObjectChunk<T, ? super ATTR>  to = destination.asWritableObjectChunk();

        if (rowSequence.getAverageRunLengthEstimate() >= Chunk.SYSTEM_ARRAYCOPY_THRESHOLD) {
            rowSequence.forAllRowKeyRanges((final long rangeStartKey, final long rangeLastKey) ->
                    to.appendTypedChunk(this, (int) getRowOffset(rangeStartKey), (int) (rangeLastKey - rangeStartKey + 1)));
        } else {
            rowSequence.forEachRowKey((final long key) -> {
                to.add(get((int) getRowOffset(key)));
                return true;
            });
        }
    }

    @Override
    public long firstRowOffset() {
        return firstRow;
    }

    @Override
    public long mask() {
        return mask;
    }
}
