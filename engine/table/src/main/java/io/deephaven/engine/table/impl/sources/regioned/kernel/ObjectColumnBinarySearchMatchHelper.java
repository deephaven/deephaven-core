//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortKernel;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.insertionPoint;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectColumnBinarySearchKernel.lowerBoundAscending;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectColumnBinarySearchKernel.lowerBoundDescending;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectColumnBinarySearchKernel.upperBoundAscending;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectColumnBinarySearchKernel.upperBoundDescending;

/**
 * The match behind {@link ObjectColumnBinarySearchKernel#binarySearchMatchWithGeneralEquality}, which is correct for
 * any {@link Comparable} type.
 */
final class ObjectColumnBinarySearchMatchHelper {
    /**
     * Rows per slice when scanning a run of rows that compare equal to a search value. Matches the other chunked scans
     * in {@code engine/table}.
     */
    private static final int CHUNK_SIZE = 1 << 12;

    private ObjectColumnBinarySearchMatchHelper() {}

    /**
     * Performs a binary search on a given sorted {@link ColumnSource} to find the row keys from a provided
     * {@link RowSet} that hold a value equal to one of {@code searchValues}. The method returns the {@link RowSet}
     * containing the matched row keys.
     *
     * <p>
     * The ordering only locates the rows to test: the bounds find the run of positions whose values compare equal to a
     * search value, and each row of that run is returned exactly when {@link ObjectComparisons#eq(Object, Object)}
     * holds for it and one of the search values that compare equal to the run. The result is therefore correct even
     * where values that compare equal are not all equal.
     *
     * <p>
     * The binary search is performed over the positions defined by {@code selection}. {@link RowSet#get(long)} is used
     * to map positions to row keys, ensuring O(log n) performance even when the row key space is sparse. Each run is
     * read in chunks, gathered through a {@link RowSequence} iterator advanced across the runs rather than addressed as
     * a contiguous row key range: mapping a position to a row key is far more expensive than advancing the iterator, so
     * the iterator is advanced once and reused instead of resolving each run's start independently.
     *
     * @param source The column source in which the search will be performed.
     * @param selection The {@link RowSet} defining which rows are populated and the order in which they are searched.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues An array of keys to find within the source.
     * @param usePrev If true, the search will use the previous values instead of current values.
     *
     * @return A {@link RowSet} containing the row keys that are equal to one of the search values.
     */
    static RowSet binarySearchMatchWithGeneralEquality(
            @NotNull final ColumnSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues,
            final boolean usePrev) {
        final Object[] copiedValues = Arrays.copyOf(searchValues, searchValues.length);
        if (sortColumn.isAscending()) {
            try (final ObjectTimsortKernel.ObjectSortKernelContext<Any> context =
                    ObjectTimsortKernel.createContext(copiedValues.length)) {
                context.sort(WritableObjectChunk.writableChunkWrap(copiedValues));
            }
        } else {
            try (final ObjectTimsortDescendingKernel.ObjectSortKernelContext<Any> context =
                    ObjectTimsortDescendingKernel.createContext(copiedValues.length)) {
                context.sort(WritableObjectChunk.writableChunkWrap(copiedValues));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final long lastPos = selection.size() - 1;
        final boolean ascending = sortColumn.isAscending();
        long firstPos = 0;

        // Everything a run scan needs is allocated once here and reused across every run: the context and chunks are
        // sized together, and runs are located in increasing position order, so one iterator can advance across all
        // of them.
        final int contextSize = (int) Math.min(CHUNK_SIZE, selection.size());
        try (final ColumnSource.GetContext getContext = source.makeGetContext(contextSize);
                final WritableLongChunk<OrderedRowKeys> keys = WritableLongChunk.makeWritableChunk(contextSize);
                final WritableLongChunk<OrderedRowKeys> matches = WritableLongChunk.makeWritableChunk(contextSize);
                final RowSequence.Iterator rsIt = selection.getRowSequenceIterator()) {
            for (int idx = 0; idx < copiedValues.length && firstPos <= lastPos;) {
                // First, identify the group of search values that compare equal to each other.
                int groupEnd = idx + 1;
                while (groupEnd < copiedValues.length
                        && ObjectComparisons.compare(copiedValues[groupEnd], copiedValues[idx]) == 0) {
                    ++groupEnd;
                }
                // Second, find the bounds of the run in the column that compares equal to the group. These bounds are
                // positions within selection, not row keys, so their difference is a row count even when selection is
                // sparse.
                final Object toFind = copiedValues[idx];
                final long lowerResult = ascending
                        ? lowerBoundAscending(source, selection, firstPos, lastPos, toFind, true, usePrev)
                        : lowerBoundDescending(source, selection, firstPos, lastPos, toFind, true, usePrev);
                final long runStartPos = lowerResult >= 0 ? lowerResult : insertionPoint(lowerResult);
                final long upperResult = ascending
                        ? upperBoundAscending(source, selection, runStartPos, lastPos, toFind, true, usePrev)
                        : upperBoundDescending(source, selection, runStartPos, lastPos, toFind, true, usePrev);
                final long runEndPos = upperResult >= 0 ? upperResult + 1 : insertionPoint(upperResult);
                if (runEndPos > runStartPos) {
                    // Third, keep each row of the run that is equal to a member of the group. Resolving runStartPos is
                    // the only place a position becomes a row key; runs advance forward, so the one iterator serves
                    // them all.
                    rsIt.advance(selection.get(runStartPos));
                    long remaining = runEndPos - runStartPos;
                    while (remaining > 0 && rsIt.hasMore()) {
                        final RowSequence rows = rsIt.getNextRowSequenceWithLength(Math.min(contextSize, remaining));
                        final ObjectChunk<?, ? extends Values> valueChunk = (usePrev
                                ? source.getPrevChunk(getContext, rows)
                                : source.getChunk(getContext, rows)).asObjectChunk();
                        rows.fillRowKeyChunk(keys);
                        matches.setSize(0);
                        for (int ii = 0; ii < valueChunk.size(); ++ii) {
                            final Object value = valueChunk.get(ii);
                            for (int valueIdx = idx; valueIdx < groupEnd; ++valueIdx) {
                                if (ObjectComparisons.eq(value, copiedValues[valueIdx])) {
                                    matches.add(keys.get(ii));
                                    break;
                                }
                            }
                        }
                        builder.appendOrderedRowKeysChunk(matches);
                        remaining -= valueChunk.size();
                    }
                }
                firstPos = runEndPos;
                idx = groupEnd;
            }
        }

        return builder.build();
    }
}
