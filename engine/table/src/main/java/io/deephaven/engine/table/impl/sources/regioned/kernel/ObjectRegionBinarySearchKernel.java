//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRegionBinarySearchKernel and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import java.util.Arrays;

import io.deephaven.api.SortSpec;
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

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.insertionPoint;

public class ObjectRegionBinarySearchKernel {
    /**
     * Performs a binary search on a given column region to find the positions (row keys) of specified keys. The method
     * returns the RowSet containing the matched row keys.
     *
     * @param region The column region in which the search will be performed.
     * @param firstKey The first key in the column region to consider for the search.
     * @param lastKey The last key in the column region to consider for the search.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues An array of keys to find within the column region.
     *
     * @return A {@link RowSet} containing the row keys where the sorted keys were found.
     */
    public static <T extends Comparable<? super T>> RowSet binarySearchMatch(
            ColumnRegionObject<?, ?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        final SortSpec.Order order = sortColumn.order();
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

        if (order.isAscending()) {
            for (int idx = 0; idx < copiedValues.length && firstKey <= lastKey; ++idx) {
                final Object toFind = copiedValues[idx];
                final int startResult = findStartIndexAscending(region, firstKey, lastKey, toFind, true);
                if (startResult < 0) {
                    // Advance firstKey since we didn't find the value but eliminated some rows.
                    firstKey = insertionPoint(startResult);
                    continue;
                }
                final int endResult = findEndIndexAscending(region, startResult, lastKey, toFind, true);
                if (endResult >= 0) {
                    builder.appendRange(startResult, endResult);
                    firstKey = endResult + 1;
                }
            }
        } else {
            for (int searchIndex = 0; searchIndex < copiedValues.length && firstKey <= lastKey; ++searchIndex) {
                final Object toFind = copiedValues[searchIndex];
                final int startResult = findStartIndexDescending(region, firstKey, lastKey, toFind, true);
                if (startResult < 0) {
                    // Advance firstKey since we didn't find the value but eliminated some rows.
                    firstKey = insertionPoint(startResult);
                    continue;
                }
                final int endResult = findEndIndexDescending(region, startResult, lastKey, toFind, true);
                if (endResult >= 0) {
                    builder.appendRange(startResult, endResult);
                    firstKey = endResult + 1;
                }
            }
        }

        return builder.build();
    }

    /**
     * Performs a binary search on a given column region to find the positions (row keys) of values within a specified
     * range.
     *
     * @param region The column region in which the search will be performed.
     * @param firstKey The first key in the column region to consider for the search.
     * @param lastKey The last key in the column region to consider for the search.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param min The minimum value of the range.
     * @param max The maximum value of the range.
     * @param minInc {@code true} if the minimum value is inclusive, {@code false} otherwise.
     * @param maxInc {@code true} if the maximum value is inclusive, {@code false} otherwise.
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static <T extends Comparable<? super T>> RowSet binarySearchMinMax(
            @NotNull final ColumnRegionObject<?, ?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final Object min,
            final Object max,
            final boolean minInc,
            final boolean maxInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first row that is > or >= min (depends on minInc)
            final int startResult = findStartIndexAscending(region, firstKey, lastKey, min, minInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            if (start > lastKey) {
                return RowSetFactory.empty();
            }
            final long offset = Math.max(start, firstKey);
            // The end of the range is the last row that is < or <= max (depends on maxInc)
            final int endResult = findEndIndexAscending(region, offset, lastKey, max, maxInc);
            end = endResult >= 0 ? endResult : insertionPoint(endResult) - 1;
        } else {
            // The beginning of the range is the first row that is < or <= max (depends on maxInc)
            final int startResult = findStartIndexDescending(region, firstKey, lastKey, max, maxInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            if (start > lastKey) {
                return RowSetFactory.empty();
            }
            final long offset = Math.max(start, firstKey);
            // The end of the range is the last row that is > or >= min (depends on minInc)
            final int endResult = findEndIndexDescending(region, offset, lastKey, min, minInc);
            end = endResult >= 0 ? endResult : insertionPoint(endResult) - 1;
        }

        // Validate that a logical range was found and the bounds didn't cross
        if (start <= end) {
            return RowSetFactory.fromRange(start, end);
        }

        return RowSetFactory.empty();
    }

    /**
     * Performs a binary search on a given column region to find the positions (row keys) of values greater than a
     * specified minimum.
     *
     * @param region The column region in which the search will be performed.
     * @param firstKey The first key in the column region to consider for the search.
     * @param lastKey The last key in the column region to consider for the search.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param min The minimum value of the range.
     * @param minInc {@code true} if the minimum value is inclusive, {@code false} otherwise.
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static <T extends Comparable<? super T>> RowSet binarySearchMin(
            @NotNull final ColumnRegionObject<?, ?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final Object min,
            final boolean minInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first row that is > or >= min (depends on minInc)
            final int startResult = findStartIndexAscending(region, firstKey, lastKey, min, minInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            end = Math.toIntExact(lastKey);
        } else {
            start = Math.toIntExact(firstKey);
            // The end of the range is the last row that is > or >= min (depends on minInc)
            final int endResult = findEndIndexDescending(region, firstKey, lastKey, min, minInc);
            end = endResult >= 0 ? endResult : insertionPoint(endResult) - 1;
        }

        if (start <= end) {
            return RowSetFactory.fromRange(start, end);
        }

        return RowSetFactory.empty();
    }

    /**
     * Performs a binary search on a given column region to find the positions (row keys) of values less than a
     * specified maximum.
     *
     * @param region The column region in which the search will be performed.
     * @param firstKey The first key in the column region to consider for the search.
     * @param lastKey The last key in the column region to consider for the search.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param max The maximum value of the range.
     * @param maxInc {@code true} if the maximum value is inclusive, {@code false} otherwise.
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static <T extends Comparable<? super T>> RowSet binarySearchMax(
            @NotNull final ColumnRegionObject<?, ?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final Object max,
            final boolean maxInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            start = Math.toIntExact(firstKey);
            // The end of the range is the last row that is < or <= max (depends on maxInc)
            final int endResult = findEndIndexAscending(region, firstKey, lastKey, max, maxInc);
            end = endResult >= 0 ? endResult : insertionPoint(endResult) - 1;
        } else {
            // The beginning of the range is the first row that is < or <= max (depends on maxInc)
            final int startResult = findStartIndexDescending(region, firstKey, lastKey, max, maxInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            end = Math.toIntExact(lastKey);
        }

        if (start <= end) {
            return RowSetFactory.fromRange(start, end);
        }

        return RowSetFactory.empty();
    }

    /**
     * Finds the starting index for a given value in an ascending (non-descending) sorted region.
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param min The value to find.
     * @param minInc If true, the search is inclusive of the value.
     * @return The starting index, or {@code -(insertionPoint) - 1} if not found.
     */
    private static int findStartIndexAscending(
            @NotNull final ColumnRegionObject<?, ?> region,
            final long firstKey,
            final long lastKey,
            final Object min,
            final boolean minInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final Object midValue = region.getObject(mid);
            final boolean satisfiesMin = minInc
                    ? ObjectComparisons.geq(midValue, min)
                    : ObjectComparisons.gt(midValue, min);

            if (satisfiesMin) {
                ans = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return ans >= 0 ? ans : insertionPoint(low);
    }

    /**
     * Finds the ending index for a given value in an ascending (non-descending) sorted region.
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param max The value to find.
     * @param maxInc If true, the search is inclusive of the value.
     * @return The ending index, or {@code -(insertionPoint) - 1} if not found.
     */
    private static int findEndIndexAscending(
            @NotNull final ColumnRegionObject<?, ?> region,
            final long firstKey,
            final long lastKey,
            final Object max,
            final boolean maxInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final Object midValue = region.getObject(mid);
            final boolean satisfiesMax = maxInc
                    ? ObjectComparisons.leq(midValue, max)
                    : ObjectComparisons.lt(midValue, max);

            if (satisfiesMax) {
                ans = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return ans >= 0 ? ans : insertionPoint(low);
    }

    /**
     * Finds the starting index for a given value in a descending (non-ascending) sorted region.
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param max The value to find.
     * @param maxInc If true, the search is inclusive of the value.
     * @return The starting index, or {@code -(insertionPoint) - 1} if not found.
     */
    private static int findStartIndexDescending(
            @NotNull final ColumnRegionObject<?, ?> region,
            final long firstKey,
            final long lastKey,
            final Object max,
            final boolean maxInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final Object midValue = region.getObject(mid);
            final boolean satisfiesMax = maxInc
                    ? ObjectComparisons.leq(midValue, max)
                    : ObjectComparisons.lt(midValue, max);

            if (satisfiesMax) {
                ans = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return ans >= 0 ? ans : insertionPoint(low);
    }

    /**
     * Finds the ending index for a given value in a descending (non-ascending) sorted region.
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param min The value to find.
     * @param minInc If true, the search is inclusive of the value.
     * @return The ending index, or {@code -(insertionPoint) - 1} if not found.
     */
    private static int findEndIndexDescending(
            @NotNull final ColumnRegionObject<?, ?> region,
            final long firstKey,
            final long lastKey,
            final Object min,
            final boolean minInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final Object midValue = region.getObject(mid);
            final boolean satisfiesMin = minInc
                    ? ObjectComparisons.geq(midValue, min)
                    : ObjectComparisons.gt(midValue, min);

            if (satisfiesMin) {
                ans = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return ans >= 0 ? ans : insertionPoint(low);
    }
}
