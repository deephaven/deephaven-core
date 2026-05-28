//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharColumnBinarySearchKernel and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
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
import io.deephaven.engine.table.ElementSource;
import io.deephaven.engine.table.impl.sort.timsort.LongTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.LongTimsortKernel;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

public class LongColumnBinarySearchKernel {
    /**
     * Performs a binary search on a given sorted {@link ElementSource} to find the row keys from a provided
     * {@link RowSet} that pass a range or match filter. The method returns the {@link RowSet} containing the matched
     * row keys.
     *
     * <p>
     * The binary search is performed over the positions defined by {@code selection}. {@link RowSet#get(long)} is used
     * to map positions to row keys, ensuring O(log n) performance even when the row key space is sparse.
     *
     * @param source The element source in which the search will be performed.
     * @param selection The {@link RowSet} defining which rows are populated and the order in which they are searched.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues An array of keys to find within the source.
     * @param usePrev If true, the search will use the previous values (getPrevLong) instead of current values
     *        (getLong).
     *
     * @return A {@link RowSet} containing the row keys where the sorted keys were found.
     */
    public static RowSet binarySearchMatch(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues,
            final boolean usePrev) {
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
        final long lastPos = selection.size() - 1;

        if (order.isAscending()) {
            long firstPos = 0;
            for (int idx = 0; idx < unboxed.length && firstPos <= lastPos; ++idx) {
                final long toFind = unboxed[idx];
                final long startResult =
                        findStartPosAscending(source, selection, firstPos, lastPos, toFind, true, usePrev);
                if (startResult < 0) {
                    // Advance firstPos since we didn't find the value but eliminated some positions.
                    firstPos = -(startResult + 1);
                    continue;
                }
                final long endResult =
                        findEndPosAscending(source, selection, startResult, lastPos, toFind, true, usePrev);
                if (endResult >= 0) {
                    try (final RowSet subset = selection.subSetByPositionRange(startResult, endResult + 1)) {
                        builder.appendRowSequence(subset);
                    }
                    firstPos = endResult + 1;
                }
            }
        } else {
            long firstPos = 0;
            for (int searchIndex = 0; searchIndex < unboxed.length && firstPos <= lastPos; ++searchIndex) {
                final long toFind = unboxed[searchIndex];
                final long startResult =
                        findStartPosDescending(source, selection, firstPos, lastPos, toFind, true, usePrev);
                if (startResult < 0) {
                    // Advance firstPos since we didn't find the value but eliminated some positions.
                    firstPos = -(startResult + 1);
                    continue;
                }
                final long endResult =
                        findEndPosDescending(source, selection, startResult, lastPos, toFind, true, usePrev);
                if (endResult >= 0) {
                    try (final RowSet subset = selection.subSetByPositionRange(startResult, endResult + 1)) {
                        builder.appendRowSequence(subset);
                    }
                    firstPos = endResult + 1;
                }
            }
        }

        return builder.build();
    }

    /**
     * Performs a binary search on a given sorted {@link ElementSource} to find the positions (row keys) of values
     * within a specified range.
     *
     * <p>
     * The binary search is performed over the positions defined by {@code selection}. {@link RowSet#get(long)} is used
     * to map positions to row keys, ensuring O(log n) performance even when the row key space is sparse.
     *
     * @param source The element source in which the search will be performed.
     * @param selection The {@link RowSet} defining which rows are populated and the order in which they are searched.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param min The minimum value of the range.
     * @param max The maximum value of the range.
     * @param minInc {@code true} if the minimum value is inclusive, {@code false} otherwise.
     * @param maxInc {@code true} if the maximum value is inclusive, {@code false} otherwise.
     * @param usePrev If true, the search will use the previous values (getPrevLong) instead of current values
     *        (getLong).
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static RowSet binarySearchMinMax(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            final long min,
            final long max,
            final boolean minInc,
            final boolean maxInc,
            final boolean usePrev) {

        final long lastPos = selection.size() - 1;
        final long startPos;
        final long endPos;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first position whose value is > or >= min (depends on minInc)
            final long startResult = findStartPosAscending(source, selection, 0, lastPos, min, minInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            if (startPos > lastPos) {
                return RowSetFactory.empty();
            }
            // The end of the range is the last position whose value is < or <= max (depends on maxInc)
            final long endResult = findEndPosAscending(source, selection, startPos, lastPos, max, maxInc, usePrev);
            endPos = endResult >= 0 ? endResult : -(endResult + 1) - 1;
        } else {
            // The beginning of the range is the first position whose value is < or <= max (depends on maxInc)
            final long startResult = findStartPosDescending(source, selection, 0, lastPos, max, maxInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            if (startPos > lastPos) {
                return RowSetFactory.empty();
            }
            // The end of the range is the last position whose value is > or >= min (depends on minInc)
            final long endResult = findEndPosDescending(source, selection, startPos, lastPos, min, minInc, usePrev);
            endPos = endResult >= 0 ? endResult : -(endResult + 1) - 1;
        }

        // If the bounds didn't cross, return the subset
        if (startPos <= endPos) {
            return selection.subSetByPositionRange(startPos, endPos + 1);
        }

        return RowSetFactory.empty();
    }

    /**
     * Performs a binary search on a given sorted {@link ElementSource} to find the positions (row keys) of values
     * greater than a specified minimum.
     *
     * <p>
     * The binary search is performed over the positions defined by {@code selection}. {@link RowSet#get(long)} is used
     * to map positions to row keys, ensuring O(log n) performance even when the row key space is sparse.
     *
     * @param source The element source in which the search will be performed.
     * @param selection The {@link RowSet} defining which rows are populated and the order in which they are searched.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param min The minimum value of the range.
     * @param minInc {@code true} if the minimum value is inclusive, {@code false} otherwise.
     * @param usePrev If true, the search will use the previous values (getPrevLong) instead of current values
     *        (getLong).
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static RowSet binarySearchMin(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            final long min,
            final boolean minInc,
            final boolean usePrev) {

        final long lastPos = selection.size() - 1;
        final long startPos;
        final long endPos;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first position whose value is > or >= min (depends on minInc)
            final long startResult = findStartPosAscending(source, selection, 0, lastPos, min, minInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            endPos = lastPos;
        } else {
            startPos = 0;
            // The end of the range is the last position whose value is > or >= min (depends on minInc)
            final long endResult = findEndPosDescending(source, selection, 0, lastPos, min, minInc, usePrev);
            endPos = endResult >= 0 ? endResult : -(endResult + 1) - 1;
        }

        if (startPos <= endPos) {
            return selection.subSetByPositionRange(startPos, endPos + 1);
        }

        return RowSetFactory.empty();
    }

    /**
     * Performs a binary search on a given sorted {@link ElementSource} to find the positions (row keys) of values less
     * than a specified maximum.
     *
     * <p>
     * The binary search is performed over the positions defined by {@code selection}. {@link RowSet#get(long)} is used
     * to map positions to row keys, ensuring O(log n) performance even when the row key space is sparse.
     *
     * @param source The element source in which the search will be performed.
     * @param selection The {@link RowSet} defining which rows are populated and the order in which they are searched.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param max The maximum value of the range.
     * @param maxInc {@code true} if the maximum value is inclusive, {@code false} otherwise.
     * @param usePrev If true, the search will use the previous values (getPrevLong) instead of current values
     *        (getLong).
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static RowSet binarySearchMax(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            final long max,
            final boolean maxInc,
            final boolean usePrev) {

        final long lastPos = selection.size() - 1;
        final long startPos;
        final long endPos;

        if (sortColumn.isAscending()) {
            startPos = 0;
            // The end of the range is the last position whose value is < or <= max (depends on maxInc)
            final long endResult = findEndPosAscending(source, selection, 0, lastPos, max, maxInc, usePrev);
            endPos = endResult >= 0 ? endResult : -(endResult + 1) - 1;
        } else {
            // The beginning of the range is the first position whose value is < or <= max (depends on maxInc)
            final long startResult = findStartPosDescending(source, selection, 0, lastPos, max, maxInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            endPos = lastPos;
        }

        if (startPos <= endPos) {
            return selection.subSetByPositionRange(startPos, endPos + 1);
        }

        return RowSetFactory.empty();
    }

    /**
     * Finds the starting position for a given value in an ascending (non-descending) sorted source.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param min The value to find.
     * @param minInc If true, the search is inclusive of the value.
     * @param usePrev If true, uses getPrevLong instead of getLong.
     * @return The found position (&gt;= 0), or {@code -(insertionPos + 1)} if not found.
     */
    private static long findStartPosAscending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final long min,
            final boolean minInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;
        long ans = -1;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final long midValue = usePrev ? source.getPrevLong(selection.get(mid)) : source.getLong(selection.get(mid));
            final boolean satisfiesMin = minInc
                    ? LongComparisons.geq(midValue, min)
                    : LongComparisons.gt(midValue, min);

            if (satisfiesMin) {
                ans = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return ans >= 0 ? ans : -(low + 1);
    }

    /**
     * Finds the ending position for a given value in an ascending (non-descending) sorted source.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param max The value to find.
     * @param maxInc If true, the search is inclusive of the value.
     * @param usePrev If true, uses getPrevLong instead of getLong.
     * @return The found position (&gt;= 0), or {@code -(insertionPos + 1)} if not found.
     */
    private static long findEndPosAscending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final long max,
            final boolean maxInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;
        long ans = -1;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final long midValue = usePrev ? source.getPrevLong(selection.get(mid)) : source.getLong(selection.get(mid));
            final boolean satisfiesMax = maxInc
                    ? LongComparisons.leq(midValue, max)
                    : LongComparisons.lt(midValue, max);

            if (satisfiesMax) {
                ans = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return ans >= 0 ? ans : -(low + 1);
    }

    /**
     * Finds the starting position for a given value in a descending (non-ascending) sorted source.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param max The value to find.
     * @param maxInc If true, the search is inclusive of the value.
     * @param usePrev If true, uses getPrevLong instead of getLong.
     * @return The found position (&gt;= 0), or {@code -(insertionPos + 1)} if not found.
     */
    private static long findStartPosDescending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final long max,
            final boolean maxInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;
        long ans = -1;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final long midValue = usePrev ? source.getPrevLong(selection.get(mid)) : source.getLong(selection.get(mid));
            final boolean satisfiesMax = maxInc
                    ? LongComparisons.leq(midValue, max)
                    : LongComparisons.lt(midValue, max);

            if (satisfiesMax) {
                ans = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return ans >= 0 ? ans : -(low + 1);
    }

    /**
     * Finds the ending position for a given value in a descending (non-ascending) sorted source.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param min The value to find.
     * @param minInc If true, the search is inclusive of the value.
     * @param usePrev If true, uses getPrevLong instead of getLong.
     * @return The found position (&gt;= 0), or {@code -(insertionPos + 1)} if not found.
     */
    private static long findEndPosDescending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final long min,
            final boolean minInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;
        long ans = -1;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final long midValue = usePrev ? source.getPrevLong(selection.get(mid)) : source.getLong(selection.get(mid));
            final boolean satisfiesMin = minInc
                    ? LongComparisons.geq(midValue, min)
                    : LongComparisons.gt(midValue, min);

            if (satisfiesMin) {
                ans = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return ans >= 0 ? ans : -(low + 1);
    }
}

