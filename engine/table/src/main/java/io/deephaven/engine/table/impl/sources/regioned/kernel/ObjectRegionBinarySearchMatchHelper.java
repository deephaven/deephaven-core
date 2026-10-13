//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortKernel;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionObject;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.insertionPoint;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectRegionBinarySearchKernel.lowerBoundAscending;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectRegionBinarySearchKernel.lowerBoundDescending;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectRegionBinarySearchKernel.upperBoundAscending;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectRegionBinarySearchKernel.upperBoundDescending;

/**
 * The match behind {@link ObjectRegionBinarySearchKernel#binarySearchMatchWithGeneralEquality}, which is correct for
 * any {@link Comparable} type.
 */
final class ObjectRegionBinarySearchMatchHelper {
    /**
     * Rows per slice when scanning a run of rows that compare equal to a search value. Matches the other chunked scans
     * in {@code engine/table}.
     */
    private static final int CHUNK_SIZE = 1 << 12;

    private ObjectRegionBinarySearchMatchHelper() {}

    /**
     * Performs a binary search on a given column region to find the row keys holding a value equal to one of
     * {@code searchValues}. The method returns the {@link RowSet} containing the matched row keys.
     *
     * <p>
     * The ordering only locates the rows to test: the bounds find the run of rows that compare equal to a search value,
     * and each row of that run is returned exactly when {@link ObjectComparisons#eq(Object, Object)} holds for it and
     * one of the search values that compare equal to the run. The result is therefore correct even where values that
     * compare equal are not all equal.
     *
     * <p>
     * A run is read in chunks rather than a row at a time. A run has no bounded length, since every row in the region
     * can compare equal to the search value, and a per-row fetch pays the region's page lookup again on each one. The
     * bounds that locate the run stay on single-row reads: a binary search probes O(log n) scattered rows, and reading
     * a chunk around each probe would fetch far more than it saves.
     *
     * @param region The column region in which the search will be performed.
     * @param firstKey The first key in the column region to consider for the search.
     * @param lastKey The last key in the column region to consider for the search.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues An array of keys to find within the column region.
     *
     * @return A {@link RowSet} containing the row keys that are equal to one of the search values.
     */
    static RowSet binarySearchMatchWithGeneralEquality(
            @NotNull final ColumnRegionObject<?, ?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        if (firstKey > lastKey || searchValues.length == 0) {
            return RowSetFactory.empty();
        }
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
        final boolean ascending = sortColumn.isAscending();

        // Allocated once and reused by every run, sized to the search span so a small region does not pay for a full
        // chunk. Runs are read in slices of at most this many rows.
        final int contextSize = (int) Math.min(CHUNK_SIZE, lastKey - firstKey + 1);
        try (final ChunkSource.GetContext getContext = region.makeGetContext(contextSize)) {
            for (int idx = 0; idx < copiedValues.length && firstKey <= lastKey;) {
                // First, identify the group of search values that compare equal to each other.
                int groupEnd = idx + 1;
                while (groupEnd < copiedValues.length
                        && ObjectComparisons.compare(copiedValues[groupEnd], copiedValues[idx]) == 0) {
                    ++groupEnd;
                }
                // Second, find the bounds of the run in the region that compares equal to the group.
                final Object toFind = copiedValues[idx];
                final long lowerResult = ascending
                        ? lowerBoundAscending(region, firstKey, lastKey, toFind, true)
                        : lowerBoundDescending(region, firstKey, lastKey, toFind, true);
                final long runStart = lowerResult >= 0 ? lowerResult : insertionPoint(lowerResult);
                final long upperResult = ascending
                        ? upperBoundAscending(region, runStart, lastKey, toFind, true)
                        : upperBoundDescending(region, runStart, lastKey, toFind, true);
                final long runEnd = upperResult >= 0 ? upperResult + 1 : insertionPoint(upperResult);
                // Third, keep each row of the run that is equal to a member of the group, reading the run a slice at a
                // time.
                for (long sliceStart = runStart; sliceStart < runEnd; sliceStart += contextSize) {
                    final long sliceEnd = Math.min(sliceStart + contextSize, runEnd);
                    final ObjectChunk<?, ?> valueChunk =
                            region.getChunk(getContext, sliceStart, sliceEnd - 1).asObjectChunk();
                    for (int ii = 0; ii < valueChunk.size(); ++ii) {
                        final Object value = valueChunk.get(ii);
                        for (int valueIdx = idx; valueIdx < groupEnd; ++valueIdx) {
                            if (ObjectComparisons.eq(value, copiedValues[valueIdx])) {
                                builder.appendKey(sliceStart + ii);
                                break;
                            }
                        }
                    }
                }
                firstKey = runEnd;
                idx = groupEnd;
            }
        }

        return builder.build();
    }
}
