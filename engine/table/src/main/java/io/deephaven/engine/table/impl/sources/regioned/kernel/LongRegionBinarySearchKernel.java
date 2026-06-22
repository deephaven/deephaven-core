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

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.insertionPoint;

public class LongRegionBinarySearchKernel {
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
            ColumnRegionLong<?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        final SortSpec.Order order = sortColumn.order();
        final long[] unboxed = ArrayTypeUtils.getUnboxedLongArray(searchValues);
        if (sortColumn.isAscending()) {
            try (final LongTimsortKernel.LongSortKernelContext<Any> context =
                    LongTimsortKernel.createContext(unboxed.length)) {
                context.sort(WritableLongChunk.writableChunkWrap(unboxed));
            }
        } else {
            try (final LongTimsortDescendingKernel.LongSortKernelContext<Any> context =
                    LongTimsortDescendingKernel.createContext(unboxed.length)) {
                context.sort(WritableLongChunk.writableChunkWrap(unboxed));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        if (order.isAscending()) {
            for (int idx = 0; idx < unboxed.length && firstKey <= lastKey; ++idx) {
                final long toFind = unboxed[idx];
                final int startResult = lowerBoundAscending(region, firstKey, lastKey, toFind, true);
                if (startResult < 0) {
                    // Advance firstKey since we didn't find the value but eliminated some rows.
                    firstKey = insertionPoint(startResult);
                    continue;
                }
                final int endResult = upperBoundAscending(region, startResult, lastKey, toFind, true);
                if (endResult >= 0) {
                    builder.appendRange(startResult, endResult);
                    firstKey = endResult + 1;
                }
            }
        } else {
            for (int searchIndex = 0; searchIndex < unboxed.length && firstKey <= lastKey; ++searchIndex) {
                final long toFind = unboxed[searchIndex];
                final int startResult = lowerBoundDescending(region, firstKey, lastKey, toFind, true);
                if (startResult < 0) {
                    // Advance firstKey since we didn't find the value but eliminated some rows.
                    firstKey = insertionPoint(startResult);
                    continue;
                }
                final int endResult = upperBoundDescending(region, startResult, lastKey, toFind, true);
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
    public static RowSet binarySearchMinMax(
            @NotNull final ColumnRegionLong<?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final long min,
            final long max,
            final boolean minInc,
            final boolean maxInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first row that is > or >= min (depends on minInc)
            final int startResult = lowerBoundAscending(region, firstKey, lastKey, min, minInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            if (start > lastKey) {
                return RowSetFactory.empty();
            }
            final long offset = Math.max(start, firstKey);
            // The end of the range is the last row that is < or <= max (depends on maxInc)
            final int endResult = upperBoundAscending(region, offset, lastKey, max, maxInc);
            end = endResult >= 0 ? endResult : insertionPoint(endResult) - 1;
        } else {
            // The beginning of the range is the first row that is < or <= max (depends on maxInc)
            final int startResult = lowerBoundDescending(region, firstKey, lastKey, max, maxInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            if (start > lastKey) {
                return RowSetFactory.empty();
            }
            final long offset = Math.max(start, firstKey);
            // The end of the range is the last row that is > or >= min (depends on minInc)
            final int endResult = upperBoundDescending(region, offset, lastKey, min, minInc);
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
    public static RowSet binarySearchMin(
            @NotNull final ColumnRegionLong<?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final long min,
            final boolean minInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first row that is > or >= min (depends on minInc)
            final int startResult = lowerBoundAscending(region, firstKey, lastKey, min, minInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            end = Math.toIntExact(lastKey);
        } else {
            start = Math.toIntExact(firstKey);
            // The end of the range is the last row that is > or >= min (depends on minInc)
            final int endResult = upperBoundDescending(region, firstKey, lastKey, min, minInc);
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
    public static RowSet binarySearchMax(
            @NotNull final ColumnRegionLong<?> region,
            final long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            final long max,
            final boolean maxInc) {

        final int start;
        final int end;

        if (sortColumn.isAscending()) {
            start = Math.toIntExact(firstKey);
            // The end of the range is the last row that is < or <= max (depends on maxInc)
            final int endResult = upperBoundAscending(region, firstKey, lastKey, max, maxInc);
            end = endResult >= 0 ? endResult : insertionPoint(endResult) - 1;
        } else {
            // The beginning of the range is the first row that is < or <= max (depends on maxInc)
            final int startResult = lowerBoundDescending(region, firstKey, lastKey, max, maxInc);
            start = startResult >= 0 ? startResult : insertionPoint(startResult);
            end = Math.toIntExact(lastKey);
        }

        if (start <= end) {
            return RowSetFactory.fromRange(start, end);
        }

        return RowSetFactory.empty();
    }

    /**
     * Performs a binary search on an ascending (non-descending) sorted region to find the leftmost position satisfying
     * the lower bound.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code minInc=true} and the value at the found position exactly
     * equals {@code min}. The returned value is the leftmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases. In this case {@code -(p + 1)} is the insertion
     * point â the leftmost position whose value exceeds {@code min} â or {@code lastKey + 1} if all values are &lt;=
     * {@code min}.</li>
     * </ul>
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param min The value to find.
     * @param minInc If {@code true}, an exact match at the leftmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @return A non-negative position if {@code minInc=true} and {@code min} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the insertion point.
     */
    private static int lowerBoundAscending(
            @NotNull final ColumnRegionLong<?> region,
            final long firstKey,
            final long lastKey,
            final long min,
            final boolean minInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final long midValue = region.getLong(mid);
            if (minInc ? LongComparisons.geq(midValue, min) : LongComparisons.gt(midValue, min)) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        // low is now the insertion point. For inclusive searches, check for an exact match there.
        if (minInc && low <= lastKey) {
            if (LongComparisons.eq(region.getLong(low), min)) {
                return low;
            }
        }
        return insertionPoint(low);
    }

    /**
     * Performs a binary search on an ascending (non-descending) sorted region to find the rightmost position satisfying
     * the upper bound.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code maxInc=true} and the value at the found position exactly
     * equals {@code max}. The returned value is the rightmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases. In this case {@code -(p + 1)} is the first
     * position whose value exceeds {@code max} â or {@code firstKey} if all values are &gt; {@code max}.</li>
     * </ul>
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param max The value to find.
     * @param maxInc If {@code true}, an exact match at the rightmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @return A non-negative position if {@code maxInc=true} and {@code max} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the first position whose value exceeds {@code max}.
     */
    private static int upperBoundAscending(
            @NotNull final ColumnRegionLong<?> region,
            final long firstKey,
            final long lastKey,
            final long max,
            final boolean maxInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final long midValue = region.getLong(mid);
            if (maxInc ? LongComparisons.leq(midValue, max) : LongComparisons.lt(midValue, max)) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        // high is now the last satisfying position; low = high + 1 is the first non-satisfying position.
        // For inclusive searches, check for an exact match at high.
        if (maxInc && high >= firstKey) {
            if (LongComparisons.eq(region.getLong(high), max)) {
                return high;
            }
        }
        return insertionPoint(low);
    }

    /**
     * Performs a binary search on a descending (non-ascending) sorted region to find the leftmost position satisfying
     * the upper bound.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code maxInc=true} and the value at the found position exactly
     * equals {@code max}. The returned value is the leftmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases. In this case {@code -(p + 1)} is the insertion
     * point â the leftmost position whose value falls below {@code max} â or {@code lastKey + 1} if all values are
     * &gt;= {@code max}.</li>
     * </ul>
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param max The value to find.
     * @param maxInc If {@code true}, an exact match at the leftmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @return A non-negative position if {@code maxInc=true} and {@code max} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the insertion point.
     */
    private static int lowerBoundDescending(
            @NotNull final ColumnRegionLong<?> region,
            final long firstKey,
            final long lastKey,
            final long max,
            final boolean maxInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final long midValue = region.getLong(mid);
            if (maxInc ? LongComparisons.leq(midValue, max) : LongComparisons.lt(midValue, max)) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        // low is now the insertion point. For inclusive searches, check for an exact match there.
        if (maxInc && low <= lastKey) {
            if (LongComparisons.eq(region.getLong(low), max)) {
                return low;
            }
        }
        return insertionPoint(low);
    }

    /**
     * Performs a binary search on a descending (non-ascending) sorted region to find the rightmost position satisfying
     * the lower bound.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code minInc=true} and the value at the found position exactly
     * equals {@code min}. The returned value is the rightmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases. In this case {@code -(p + 1)} is the first
     * position whose value falls below {@code min} â or {@code firstKey} if all values are &gt;= {@code min}.</li>
     * </ul>
     *
     * @param region The column region to search.
     * @param firstKey The starting key of the search range.
     * @param lastKey The ending key of the search range.
     * @param min The value to find.
     * @param minInc If {@code true}, an exact match at the rightmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @return A non-negative position if {@code minInc=true} and {@code min} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the first position whose value falls below {@code min}.
     */
    private static int upperBoundDescending(
            @NotNull final ColumnRegionLong<?> region,
            final long firstKey,
            final long lastKey,
            final long min,
            final boolean minInc) {
        int low = (int) firstKey;
        int high = (int) lastKey;

        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final long midValue = region.getLong(mid);
            if (minInc ? LongComparisons.geq(midValue, min) : LongComparisons.gt(midValue, min)) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        // high is now the last satisfying position; low = high + 1 is the first non-satisfying position.
        // For inclusive searches, check for an exact match at high.
        if (minInc && high >= firstKey) {
            if (LongComparisons.eq(region.getLong(high), min)) {
                return high;
            }
        }
        return insertionPoint(low);
    }
}
