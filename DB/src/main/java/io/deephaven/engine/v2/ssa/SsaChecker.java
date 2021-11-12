package io.deephaven.engine.v2.ssa;

import io.deephaven.engine.tables.SortingOrder;
import io.deephaven.engine.v2.sort.LongSortKernel;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.chunk.Attributes.RowKeys;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableLongChunk;
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
