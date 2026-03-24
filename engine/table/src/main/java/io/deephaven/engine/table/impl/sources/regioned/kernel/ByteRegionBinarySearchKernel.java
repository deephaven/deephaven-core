//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRegionBinarySearchKernel and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortSpec;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sort.timsort.ByteTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ByteTimsortKernel;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte;
import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

public class ByteRegionBinarySearchKernel {
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
    public static RowSet binarySearchMatch(
            ColumnRegionByte<?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        final SortSpec.Order order = sortColumn.order();
        final byte[] unboxed = ArrayTypeUtils.getUnboxedByteArray(searchValues);
        if (sortColumn.isAscending()) {
            try (final ByteTimsortKernel.ByteSortKernelContext<Any> context =
                    ByteTimsortKernel.createContext(unboxed.length)) {
                context.sort(WritableByteChunk.writableChunkWrap(unboxed));
            }
        } else {
            try (final ByteTimsortDescendingKernel.ByteSortKernelContext<Any> context =
                    ByteTimsortDescendingKernel.createContext(unboxed.length)) {
                context.sort(WritableByteChunk.writableChunkWrap(unboxed));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        if (order.isAscending()) {
            for (final byte toFind : unboxed) {
                final int start = findStartIndexAscending(region, firstKey, lastKey, toFind, true);
                if (start == -1) {
                    // No match for this key, move to the next key.
                    continue;
                }
                final int end = findEndIndexAscending(region, start, lastKey, toFind, true);
                if (end != -1) {
                    builder.appendRange(start, end);
                    firstKey = end + 1;
                }
            }
        } else {
            for (final byte toFind : unboxed) {
                final int start = findStartIndexDescending(region, firstKey, lastKey, toFind, true);
                if (start == -1) {
                    continue;
                }
                final int end = findEndIndexDescending(region, start, lastKey, toFind, true);
                if (end != -1) {
                    builder.appendRange(start, end);
                    firstKey = end + 1;
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
    public static RowSet binarySearchMinMax(
            @NotNull final ColumnRegionByte<?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final byte min,
            final byte max,
            final boolean minInc,
            final boolean maxInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            start = findStartIndexAscending(region, firstKey, lastKey, min, minInc);
            final long offset = Math.max(start, firstKey);
            end = findEndIndexAscending(region, offset, lastKey, max, maxInc);
        } else {
            start = findStartIndexDescending(region, firstKey, lastKey, max, maxInc);
            final long offset = Math.max(start, firstKey);
            end = findEndIndexDescending(region, offset, lastKey, min, minInc);
        }

        // Validate that a logical range was found and the bounds didn't cross
        if (start != -1 && end != -1) {
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
    public static RowSet binarySearchMin(
            @NotNull final ColumnRegionByte<?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final byte min,
            final boolean minInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            start = findStartIndexAscending(region, firstKey, lastKey, min, minInc);
            end = (int) lastKey;
        } else {
            start = (int) firstKey;
            end = findEndIndexDescending(region, firstKey, lastKey, min, minInc);
        }

        if (start != -1 && end != -1 && start <= end) {
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
    public static RowSet binarySearchMax(
            @NotNull final ColumnRegionByte<?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final byte max,
            final boolean maxInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            start = (int) firstKey;
            end = findEndIndexAscending(region, firstKey, lastKey, max, maxInc);
        } else {
            start = findStartIndexDescending(region, firstKey, lastKey, max, maxInc);
            end = (int) lastKey;
        }

        if (start != -1 && end != -1 && start <= end) {
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
     * @return The starting index, or -1 if not found.
     */
    private static int findStartIndexAscending(
            @NotNull final ColumnRegionByte<?> region,
            final long firstKey,
            final long lastKey,
            final byte min,
            final boolean minInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final byte midValue = region.getByte(mid);
            final boolean satisfiesMin = minInc
                    ? ByteComparisons.geq(midValue, min)
                    : ByteComparisons.gt(midValue, min);

            if (satisfiesMin) {
                ans = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return ans;
    }

    /**
     * Finds the ending index for a given value in an ascending (non-descending) sorted region.
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param max The value to find.
     * @param maxInc If true, the search is inclusive of the value.
     * @return The ending index, or -1 if not found.
     */
    private static int findEndIndexAscending(
            @NotNull final ColumnRegionByte<?> region,
            final long firstKey,
            final long lastKey,
            final byte max,
            final boolean maxInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final byte midValue = region.getByte(mid);
            final boolean satisfiesMax = maxInc
                    ? ByteComparisons.leq(midValue, max)
                    : ByteComparisons.lt(midValue, max);

            if (satisfiesMax) {
                ans = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return ans;
    }

    /**
     * Finds the starting index for a given value in a descending (non-ascending) sorted region.
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param max The value to find.
     * @param maxInc If true, the search is inclusive of the value.
     * @return The starting index, or -1 if not found.
     */
    private static int findStartIndexDescending(
            @NotNull final ColumnRegionByte<?> region,
            final long firstKey,
            final long lastKey,
            final byte max,
            final boolean maxInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final byte midValue = region.getByte(mid);
            final boolean satisfiesMax = maxInc
                    ? ByteComparisons.leq(midValue, max)
                    : ByteComparisons.lt(midValue, max);

            if (satisfiesMax) {
                ans = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return ans;
    }

    /**
     * Finds the ending index for a given value in a descending (non-ascending) sorted region.
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param min The value to find.
     * @param minInc If true, the search is inclusive of the value.
     * @return The ending index, or -1 if not found.
     */
    private static int findEndIndexDescending(
            @NotNull final ColumnRegionByte<?> region,
            final long firstKey,
            final long lastKey,
            final byte min,
            final boolean minInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;
        int ans = -1;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final byte midValue = region.getByte(mid);
            final boolean satisfiesMin = minInc
                    ? ByteComparisons.geq(midValue, min)
                    : ByteComparisons.gt(midValue, min);

            if (satisfiesMin) {
                ans = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return ans;
    }
}
