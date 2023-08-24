/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRegionBinarySearchKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sort.timsort.LongTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.LongTimsortKernel;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionLong;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

public class LongRegionBinarySearchKernel {
    /**
     * Performs a binary search on a given column region to find the positions (row keys) of specified sorted keys.
     * The method returns the RowSet containing the matched row keys.
     *
     * @param region         The column region in which the search will be performed.
     * @param firstKey       The first key in the column region to consider for the search.
     * @param lastKey        The last key in the column region to consider for the search.
     * @param sortColumn     A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues   An array of keys to find within the column region.
     *
     * @return               A {@link RowSet} containing the row keys where the sorted keys were found.
     */
    public static RowSet binarySearchMatch(
            ColumnRegionLong<?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        final SortColumn.Order order = sortColumn.order();
        final long[] unboxed = ArrayTypeUtils.getUnboxedLongArray(searchValues);
        if (order == SortColumn.Order.DESCENDING) {
            try (final LongTimsortDescendingKernel.LongSortKernelContext<Any> context =
                         LongTimsortDescendingKernel.createContext(unboxed.length)) {
                context.sort(WritableLongChunk.writableChunkWrap(unboxed));
            }
        } else {
            try (final LongTimsortKernel.LongSortKernelContext<Any> context =
                         LongTimsortKernel.createContext(unboxed.length)) {
                context.sort(WritableLongChunk.writableChunkWrap(unboxed));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (final long toFind : unboxed) {
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
     * @param builder       the builder to accumulate into
     * @param firstKey      the key to start searching
     * @param lastKey       the key to end searching
     * @param sortDirection the sort direction of the column
     * @param toFind        the element to find
     * @return the last key in the found range.
     */
    private static long binarySearchSingle(
            @NotNull final ColumnRegionLong<?> region,
            @NotNull final RowSetBuilderSequential builder,
            final long firstKey,
            final long lastKey,
            SortColumn.Order sortDirection,
            final long toFind) {
        // Find the beginning of the range
        long matchStart = binarySearchRange(region, toFind, firstKey, lastKey, sortDirection, -1);
        if (matchStart < 0) {
            return -1;
        }

        // Now we have to locate the actual start and end of the range.
        long matchEnd = matchStart;
        if (matchStart < lastKey && LongComparisons.eq(region.getLong(matchStart + 1),toFind)) {
            matchEnd = binarySearchRange(region, toFind, matchStart + 1, lastKey, sortDirection, 1);
        }

        builder.appendRange(matchStart, matchEnd);
        return matchEnd;
    }

    /**
     * Performs a binary search on a specified column region to find a long within a given range.
     * The method returns the row key where the long was found. If the long is not found, it returns -1.
     *
     * @param region          The column region in which the search will be performed.
     * @param toFind          The long to find within the column region.
     * @param start           The first row key in the column region to consider for the search.
     * @param end             The last row key in the column region to consider for the search.
     * @param sortDirection   An enum specifying the sorting direction of the column.
     * @param rangeDirection  An integer indicating the direction of the range search. Positive for forward search,
     *                        negative for backward search.
     *
     * @return                The row key where the specified long was found. If not found, returns -1.
     */
    private static long binarySearchRange(
            @NotNull final ColumnRegionLong<?> region,
            final long toFind,
            long start,
            long end,
            final SortColumn.Order sortDirection,
            final int rangeDirection) {
        final int sortDirectionInt = sortDirection == SortColumn.Order.ASCENDING ? 1 : -1;
        long matchStart = -1;
        while (start <= end) {
            long pivot = (start + end) >>> 1;
            final long curVal = region.getLong(pivot);
            final int comparison = LongComparisons.compare(curVal, toFind) * sortDirectionInt;
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
