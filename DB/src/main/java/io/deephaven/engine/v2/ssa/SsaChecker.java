package io.deephaven.engine.v2.ssa;

import io.deephaven.engine.tables.SortingOrder;
import io.deephaven.engine.v2.sort.LongSortKernel;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.chunk.Attributes.KeyIndices;
import io.deephaven.engine.structures.chunk.Attributes.Values;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableLongChunk;
import io.deephaven.engine.v2.utils.OrderedKeys;

public interface SsaChecker {
    default void checkSsa(SegmentedSortedArray ssa, ColumnSource<?> columnSource, OrderedKeys orderedKeys) {
        final int size = orderedKeys.intSize();
        try (final ColumnSource.FillContext fillContext = columnSource.makeFillContext(size);
                final WritableChunk<Values> valuesChunk = columnSource.getChunkType().makeWritableChunk(size);
                final WritableLongChunk<KeyIndices> keyChunk = WritableLongChunk.makeWritableChunk(size);
                final LongSortKernel sortKernel = LongSortKernel.makeContext(columnSource.getChunkType(),
                        ssa.isReversed() ? SortingOrder.Descending : SortingOrder.Ascending, size, true)) {
            columnSource.fillChunk(fillContext, valuesChunk, orderedKeys);
            orderedKeys.fillKeyIndicesChunk(WritableLongChunk.downcast(keyChunk));
            sortKernel.sort(keyChunk, valuesChunk);
            checkSsa(ssa, valuesChunk, keyChunk);
        }
    }

    void checkSsa(SegmentedSortedArray ssa, Chunk<? extends Values> valueChunk,
            LongChunk<? extends KeyIndices> tableIndexChunk);

    class SsaCheckException extends RuntimeException {
        SsaCheckException(String message) {
            super(message);
        }
    }
}
