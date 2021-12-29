package io.deephaven.engine.table.impl.ssa;

import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;

public interface SsaChecker {
    default void checkSsa(SegmentedSortedArray ssa, ColumnSource<?> columnSource, RowSequence rowSequence) {
        final int size = rowSequence.intSize();
        try (final ColumnSource.FillContext fillContext = columnSource.makeFillContext(size);
                final WritableChunk<Values> valuesChunk = columnSource.getChunkType().makeWritableChunk(size);
                final WritableLongChunk<RowKeys> keyChunk = WritableLongChunk.makeWritableChunk(size);
                final LongSortKernel sortKernel = LongSortKernel.makeContext(columnSource.getChunkType(),
                        ssa.isReversed() ? SortingOrder.Descending : SortingOrder.Ascending, size, true)) {
            columnSource.fillChunk(fillContext, valuesChunk, rowSequence);
            rowSequence.fillRowKeyChunk(WritableLongChunk.downcast(keyChunk));
            sortKernel.sort(keyChunk, valuesChunk);
            checkSsa(ssa, valuesChunk, keyChunk);
        }
    }

    void checkSsa(SegmentedSortedArray ssa, Chunk<? extends Values> valueChunk,
            LongChunk<? extends RowKeys> tableIndexChunk);

    class SsaCheckException extends RuntimeException {
        SsaCheckException(String message) {
            super(message);
        }
    }
}
