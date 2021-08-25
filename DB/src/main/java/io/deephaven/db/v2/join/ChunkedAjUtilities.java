package io.deephaven.db.v2.join;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.sized.SizedChunk;
import io.deephaven.db.v2.sources.chunk.sized.SizedLongChunk;
import io.deephaven.db.v2.ssa.SegmentedSortedArray;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.SizedSafeCloseable;

import static io.deephaven.db.v2.sources.chunk.Attributes.*;

class ChunkedAjUtilities {
    static void bothIncrementalLeftSsaShift(IndexShiftData shiftData, SegmentedSortedArray leftSsa,
        Index restampRemovals, QueryTable table,
        int nodeSize, ColumnSource<?> stampSource) {
        final ChunkType stampChunkType = stampSource.getChunkType();
        final SortingOrder sortOrder =
            leftSsa.isReversed() ? SortingOrder.Descending : SortingOrder.Ascending;

        try (final Index fullPrevIndex = table.getIndex().getPrevIndex();
            final Index previousToShift = fullPrevIndex.minus(restampRemovals);
            final SizedSafeCloseable<ColumnSource.FillContext> shiftFillContext =
                new SizedSafeCloseable<>(stampSource::makeFillContext);
            final SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> shiftSortContext =
                new SizedSafeCloseable<>(
                    size -> LongSortKernel.makeContext(stampChunkType, sortOrder, size, true));
            final SizedLongChunk<KeyIndices> stampKeys = new SizedLongChunk<>();
            final SizedChunk<Values> stampValues = new SizedChunk<>(stampChunkType)) {
            final IndexShiftData.Iterator sit = shiftData.applyIterator();
            while (sit.hasNext()) {
                sit.next();
                final Index indexToShift =
                    previousToShift.subindexByKey(sit.beginRange(), sit.endRange());
                if (indexToShift.empty()) {
                    indexToShift.close();
                    continue;
                }

                applyOneShift(leftSsa, nodeSize, stampSource, shiftFillContext, shiftSortContext,
                    stampKeys, stampValues, sit, indexToShift);
                indexToShift.close();
            }
        }
    }

    static void applyOneShift(SegmentedSortedArray leftSsa, int nodeSize,
        ColumnSource<?> stampSource, SizedSafeCloseable<ChunkSource.FillContext> shiftFillContext,
        SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> shiftSortContext,
        SizedLongChunk<KeyIndices> stampKeys, SizedChunk<Values> stampValues,
        IndexShiftData.Iterator sit, Index indexToShift) {
        if (sit.polarityReversed()) {
            final int shiftSize = indexToShift.intSize();

            stampSource.fillPrevChunk(shiftFillContext.ensureCapacity(shiftSize),
                stampValues.ensureCapacity(shiftSize), indexToShift);
            indexToShift.fillKeyIndicesChunk(stampKeys.ensureCapacity(shiftSize));

            shiftSortContext.ensureCapacity(shiftSize).sort(stampKeys.get(), stampValues.get());

            leftSsa.applyShiftReverse(stampValues.get(), stampKeys.get(), sit.shiftDelta());
        } else {
            try (final OrderedKeys.Iterator shiftIt = indexToShift.getOrderedKeysIterator()) {
                shiftFillContext.ensureCapacity(nodeSize);
                shiftSortContext.ensureCapacity(nodeSize);
                stampValues.ensureCapacity(nodeSize);
                stampKeys.ensureCapacity(nodeSize);
                while (shiftIt.hasMore()) {
                    final OrderedKeys chunkOk = shiftIt.getNextOrderedKeysWithLength(nodeSize);
                    stampSource.fillPrevChunk(shiftFillContext.get(), stampValues.get(), chunkOk);
                    chunkOk.fillKeyIndicesChunk(stampKeys.get());
                    shiftSortContext.get().sort(stampKeys.get(), stampValues.get());
                    leftSsa.applyShift(stampValues.get(), stampKeys.get(), sit.shiftDelta());
                }
            }
        }
    }
}
