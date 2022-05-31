package io.deephaven.engine.table.impl.join;

import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

class ChunkedAjUtils {
    static void bothIncrementalLeftSsaShift(RowSetShiftData shiftData, SegmentedSortedArray leftSsa,
            RowSet restampRemovals, QueryTable table,
            int nodeSize, ColumnSource<?> stampSource) {
        final ChunkType stampChunkType = stampSource.getChunkType();
        final SortingOrder sortOrder = leftSsa.isReversed() ? SortingOrder.Descending : SortingOrder.Ascending;

        try (final RowSet fullPrevRowSet = table.getRowSet().copyPrev();
                final RowSet previousToShift = fullPrevRowSet.minus(restampRemovals);
                final SizedSafeCloseable<ColumnSource.FillContext> shiftFillContext =
                        new SizedSafeCloseable<>(stampSource::makeFillContext);
                final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortContext =
                        new SizedSafeCloseable<>(
                                size -> LongSortKernel.makeContext(stampChunkType, sortOrder, size, true));
                final SizedLongChunk<RowKeys> stampKeys = new SizedLongChunk<>();
                final SizedChunk<Values> stampValues = new SizedChunk<>(stampChunkType)) {
            final RowSetShiftData.Iterator sit = shiftData.applyIterator();
            while (sit.hasNext()) {
                sit.next();
                final RowSet rowSetToShift = previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange());
                if (rowSetToShift.isEmpty()) {
                    rowSetToShift.close();
                    continue;
                }

                applyOneShift(leftSsa, nodeSize, stampSource, shiftFillContext, shiftSortContext, stampKeys,
                        stampValues, sit, rowSetToShift);
                rowSetToShift.close();
            }
        }
    }

    static void applyOneShift(SegmentedSortedArray leftSsa, int nodeSize, ColumnSource<?> stampSource,
            SizedSafeCloseable<ChunkSource.FillContext> shiftFillContext,
            SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortContext,
            SizedLongChunk<RowKeys> stampKeys, SizedChunk<Values> stampValues, RowSetShiftData.Iterator sit,
            RowSet rowSetToShift) {
        if (sit.polarityReversed()) {
            final int shiftSize = rowSetToShift.intSize();

            stampSource.fillPrevChunk(shiftFillContext.ensureCapacity(shiftSize), stampValues.ensureCapacity(shiftSize),
                    rowSetToShift);
            rowSetToShift.fillRowKeyChunk(stampKeys.ensureCapacity(shiftSize));

            shiftSortContext.ensureCapacity(shiftSize).sort(stampKeys.get(), stampValues.get());

            leftSsa.applyShiftReverse(stampValues.get(), stampKeys.get(), sit.shiftDelta());
        } else {
            try (final RowSequence.Iterator shiftIt = rowSetToShift.getRowSequenceIterator()) {
                shiftFillContext.ensureCapacity(nodeSize);
                shiftSortContext.ensureCapacity(nodeSize);
                stampValues.ensureCapacity(nodeSize);
                stampKeys.ensureCapacity(nodeSize);
                while (shiftIt.hasMore()) {
                    final RowSequence chunkOk = shiftIt.getNextRowSequenceWithLength(nodeSize);
                    stampSource.fillPrevChunk(shiftFillContext.get(), stampValues.get(), chunkOk);
                    chunkOk.fillRowKeyChunk(stampKeys.get());
                    shiftSortContext.get().sort(stampKeys.get(), stampValues.get());
                    leftSsa.applyShift(stampValues.get(), stampKeys.get(), sit.shiftDelta());
                }
            }
        }
    }
}
