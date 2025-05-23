//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRegionBinarySearchKernel and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sort.timsort.ShortTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ShortTimsortKernel;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionShort;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

public class ShortRegionBinarySearchKernel {
    /**
     * Performs a binary search on a given column region to find the positions (row keys) of specified sorted keys. The
     * method returns the RowSet containing the matched row keys.
     *
     * @param region The column region in which the search will be performed.
     * @param firstKey The first key in the column region to consider for the search.
     * @param lastKey The last key in the column region to consider for the search.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues An array of keys to find within the column region.
     *
     * @return A {@link RowSet} containing the row keys where the sorted keys were found.
     */
    public static RowSet binarySearchMatch(
            ColumnRegionShort<?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        final SortColumn.Order order = sortColumn.order();
        final short[] unboxed = ArrayTypeUtils.getUnboxedShortArray(searchValues);
        if (order == SortColumn.Order.DESCENDING) {
            try (final ShortTimsortDescendingKernel.ShortSortKernelContext<Any> context =
                    ShortTimsortDescendingKernel.createContext(unboxed.length)) {
                context.sort(WritableShortChunk.writableChunkWrap(unboxed));
            }
        } else {
            try (final ShortTimsortKernel.ShortSortKernelContext<Any> context =
                    ShortTimsortKernel.createContext(unboxed.length)) {
                context.sort(WritableShortChunk.writableChunkWrap(unboxed));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (final short toFind : unboxed) {
            final long lastFound = binarySearchSingle(region, builder, firstKey, lastKey, order, toFind);

            if (lastFound >= 0) {
                firstKey = lastFound + 1;
            }
        }

        return builder.build();
    }

    /**
     * Find the extents of the range containing the key to find, returning the last index found.
     *
     * @param builder the builder to accumulate into
     * @param firstKey the key to start searching
     * @param lastKey the key to end searching
     * @param sortDirection the sort direction of the column
     * @param toFind the element to find
     * @return the last key in the found range.
     */
    private static long binarySearchSingle(
            @NotNull final ColumnRegionShort<?> region,
            @NotNull final RowSetBuilderSequential builder,
            final long firstKey,
            final long lastKey,
            SortColumn.Order sortDirection,
            final short toFind) {
        // Find the beginning of the range
        long matchStart = binarySearchRange(region, toFind, firstKey, lastKey, sortDirection, -1);
        if (matchStart < 0) {
            return -1;
        }

        // Now we have to locate the actual start and end of the range.
        long matchEnd = matchStart;
        if (matchStart < lastKey && ShortComparisons.eq(region.getShort(matchStart + 1), toFind)) {
            matchEnd = binarySearchRange(region, toFind, matchStart + 1, lastKey, sortDirection, 1);
        }

        builder.appendRange(matchStart, matchEnd);
        return matchEnd;
    }

    /**
     * Performs a binary search on a specified column region to find a short within a given range. The method returns the
     * row key where the short was found. If the short is not found, it returns -1.
     *
     * @param region The column region in which the search will be performed.
     * @param toFind The short to find within the column region.
     * @param start The first row key in the column region to consider for the search.
     * @param end The last row key in the column region to consider for the search.
     * @param sortDirection An enum specifying the sorting direction of the column.
     * @param rangeDirection An integer indicating the direction of the range search. Positive for forward search,
     *        negative for backward search.
     *
     * @return The row key where the specified short was found. If not found, returns -1.
     */
    private static long binarySearchRange(
            @NotNull final ColumnRegionShort<?> region,
            final short toFind,
            long start,
            long end,
            final SortColumn.Order sortDirection,
            final int rangeDirection) {
        final int sortDirectionInt = sortDirection == SortColumn.Order.ASCENDING ? 1 : -1;
        long matchStart = -1;
        while (start <= end) {
            long pivot = (start + end) >>> 1;
            final short curVal = region.getShort(pivot);
            final int comparison = ShortComparisons.compare(curVal, toFind) * sortDirectionInt;
            if (comparison < 0) {
                start = pivot + 1;
            } else if (comparison == 0) {
                matchStart = pivot;
                if (rangeDirection > 0) {
                    start = pivot + 1;
                } else {
                    end = pivot - 1;
                }
            } else {
                end = pivot - 1;
            }
        }

        return matchStart;
    }
}
