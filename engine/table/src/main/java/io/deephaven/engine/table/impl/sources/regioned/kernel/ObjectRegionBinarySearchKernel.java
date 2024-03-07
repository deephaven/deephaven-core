//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRegionBinarySearchKernel and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortKernel;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionObject;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

public class ObjectRegionBinarySearchKernel {
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
            ColumnRegionObject<?, ?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        final SortColumn.Order order = sortColumn.order();
        
        if (order == SortColumn.Order.DESCENDING) {
            try (final ObjectTimsortDescendingKernel.ObjectSortKernelContext<Any> context =
                    ObjectTimsortDescendingKernel.createContext(searchValues.length)) {
                context.sort(WritableObjectChunk.writableChunkWrap(searchValues));
            }
        } else {
            try (final ObjectTimsortKernel.ObjectSortKernelContext<Any> context =
                    ObjectTimsortKernel.createContext(searchValues.length)) {
                context.sort(WritableObjectChunk.writableChunkWrap(searchValues));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (final Object toFind : searchValues) {
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
            @NotNull final ColumnRegionObject<?, ?> region,
            @NotNull final RowSetBuilderSequential builder,
            final long firstKey,
            final long lastKey,
            SortColumn.Order sortDirection,
            final Object toFind) {
        // Find the beginning of the range
        long matchStart = binarySearchRange(region, toFind, firstKey, lastKey, sortDirection, -1);
        if (matchStart < 0) {
            return -1;
        }

        // Now we have to locate the actual start and end of the range.
        long matchEnd = matchStart;
        if (matchStart < lastKey && ObjectComparisons.eq(region.getObject(matchStart + 1), toFind)) {
            matchEnd = binarySearchRange(region, toFind, matchStart + 1, lastKey, sortDirection, 1);
        }

        builder.appendRange(matchStart, matchEnd);
        return matchEnd;
    }

    /**
     * Performs a binary search on a specified column region to find a Object within a given range. The method returns the
     * row key where the Object was found. If the Object is not found, it returns -1.
     *
     * @param region The column region in which the search will be performed.
     * @param toFind The Object to find within the column region.
     * @param start The first row key in the column region to consider for the search.
     * @param end The last row key in the column region to consider for the search.
     * @param sortDirection An enum specifying the sorting direction of the column.
     * @param rangeDirection An integer indicating the direction of the range search. Positive for forward search,
     *        negative for backward search.
     *
     * @return The row key where the specified Object was found. If not found, returns -1.
     */
    private static long binarySearchRange(
            @NotNull final ColumnRegionObject<?, ?> region,
            final Object toFind,
            long start,
            long end,
            final SortColumn.Order sortDirection,
            final int rangeDirection) {
        final int sortDirectionInt = sortDirection == SortColumn.Order.ASCENDING ? 1 : -1;
        long matchStart = -1;
        while (start <= end) {
            long pivot = (start + end) >>> 1;
            final Object curVal = region.getObject(pivot);
            final int comparison = ObjectComparisons.compare(curVal, toFind) * sortDirectionInt;
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
