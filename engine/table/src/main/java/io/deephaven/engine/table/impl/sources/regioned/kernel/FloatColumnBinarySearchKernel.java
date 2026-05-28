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
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ElementSource;
import io.deephaven.engine.table.impl.sort.timsort.FloatTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.FloatTimsortKernel;
import io.deephaven.util.compare.FloatComparisons;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.insertionPoint;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

public class FloatColumnBinarySearchKernel {
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
     * @param usePrev If true, the search will use the previous values (getPrevFloat) instead of current values
     *        (getFloat).
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
        final float[] unboxed = ArrayTypeUtils.getUnboxedFloatArray(searchValues);
        if (sortColumn.isAscending()) {
            try (final FloatTimsortKernel.FloatSortKernelContext<Any> context =
                    FloatTimsortKernel.createContext(unboxed.length)) {
                context.sort(WritableFloatChunk.writableChunkWrap(unboxed));
            }
        } else {
            try (final FloatTimsortDescendingKernel.FloatSortKernelContext<Any> context =
                    FloatTimsortDescendingKernel.createContext(unboxed.length)) {
                context.sort(WritableFloatChunk.writableChunkWrap(unboxed));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final long lastPos = selection.size() - 1;

        if (order.isAscending()) {
            long firstPos = 0;
            for (int idx = 0; idx < unboxed.length && firstPos <= lastPos; ++idx) {
                final float toFind = unboxed[idx];
                final long startResult =
                        lowerBoundAscending(source, selection, firstPos, lastPos, toFind, true, usePrev);
                if (startResult < 0) {
                    // Advance firstPos since we didn't find the value but eliminated some positions.
                    firstPos = -(startResult + 1);
                    continue;
                }
                // startResult is positive only when value at startResult == toFind; no extra check needed.
                final long endResult =
                        upperBoundAscending(source, selection, startResult, lastPos, toFind, true, usePrev);
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
                final float toFind = unboxed[searchIndex];
                final long startResult =
                        lowerBoundDescending(source, selection, firstPos, lastPos, toFind, true, usePrev);
                if (startResult < 0) {
                    // Advance firstPos since we didn't find the value but eliminated some positions.
                    firstPos = -(startResult + 1);
                    continue;
                }
                // startResult is positive only when value at startResult == toFind; no extra check needed.
                final long endResult =
                        upperBoundDescending(source, selection, startResult, lastPos, toFind, true, usePrev);
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
     * @param usePrev If true, the search will use the previous values (getPrevFloat) instead of current values
     *        (getFloat).
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static RowSet binarySearchMinMax(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            final float min,
            final float max,
            final boolean minInc,
            final boolean maxInc,
            final boolean usePrev) {

        final long lastPos = selection.size() - 1;
        final long startPos;
        final long endPos;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first position whose value is > or >= min (depends on minInc)
            final long startResult = lowerBoundAscending(source, selection, 0, lastPos, min, minInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            if (startPos > lastPos) {
                return RowSetFactory.empty();
            }
            // The end of the range is the last position whose value is < or <= max (depends on maxInc)
            final long endResult = upperBoundAscending(source, selection, startPos, lastPos, max, maxInc, usePrev);
            // -(endResult+1) is first non-satisfying pos; subtract 1 for last satisfying
            endPos = endResult >= 0 ? endResult : -(endResult + 1) - 1;
        } else {
            // The beginning of the range is the first position whose value is < or <= max (depends on maxInc)
            final long startResult = lowerBoundDescending(source, selection, 0, lastPos, max, maxInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            if (startPos > lastPos) {
                return RowSetFactory.empty();
            }
            // The end of the range is the last position whose value is > or >= min (depends on minInc)
            final long endResult = upperBoundDescending(source, selection, startPos, lastPos, min, minInc, usePrev);
            // -(endResult+1) is first non-satisfying pos; subtract 1 for last satisfying
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
     * @param usePrev If true, the search will use the previous values (getPrevFloat) instead of current values
     *        (getFloat).
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static RowSet binarySearchMin(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            final float min,
            final boolean minInc,
            final boolean usePrev) {

        final long lastPos = selection.size() - 1;
        final long startPos;
        final long endPos;

        if (sortColumn.isAscending()) {
            // The beginning of the range is the first position whose value is > or >= min (depends on minInc)
            final long startResult = lowerBoundAscending(source, selection, 0, lastPos, min, minInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            endPos = lastPos;
        } else {
            startPos = 0;
            // The end of the range is the last position whose value is > or >= min (depends on minInc)
            final long endResult = upperBoundDescending(source, selection, 0, lastPos, min, minInc, usePrev);
            // -(endResult+1) is first non-satisfying pos; subtract 1 for last satisfying
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
     * @param usePrev If true, the search will use the previous values (getPrevFloat) instead of current values
     *        (getFloat).
     * @return A {@link RowSet} containing the row keys where the values were found.
     */
    public static RowSet binarySearchMax(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            final float max,
            final boolean maxInc,
            final boolean usePrev) {

        final long lastPos = selection.size() - 1;
        final long startPos;
        final long endPos;

        if (sortColumn.isAscending()) {
            startPos = 0;
            // The end of the range is the last position whose value is < or <= max (depends on maxInc)
            final long endResult = upperBoundAscending(source, selection, 0, lastPos, max, maxInc, usePrev);
            // -(endResult+1) is first non-satisfying pos; subtract 1 for last satisfying
            endPos = endResult >= 0 ? endResult : -(endResult + 1) - 1;
        } else {
            // The beginning of the range is the first position whose value is < or <= max (depends on maxInc)
            final long startResult = lowerBoundDescending(source, selection, 0, lastPos, max, maxInc, usePrev);
            startPos = startResult >= 0 ? startResult : -(startResult + 1);
            endPos = lastPos;
        }

        if (startPos <= endPos) {
            return selection.subSetByPositionRange(startPos, endPos + 1);
        }

        return RowSetFactory.empty();
    }

    /**
     * Performs a binary search on an ascending (non-descending) sorted {@link ElementSource} to find the position of
     * {@code min} within the search range.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code minInc=true} and the value at the found position exactly
     * equals {@code min}. The returned value is the leftmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases: when {@code min} is absent from the range, when
     * {@code minInc=false} (exclusive bound), or when no position satisfies the bound. In this case {@code -(p + 1)} is
     * the insertion point â the leftmost position whose value exceeds {@code min} â or {@code lastPos + 1} if all
     * values in the range are &lt;= {@code min}.</li>
     * </ul>
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param min The value to find.
     * @param minInc If {@code true}, an exact match at the leftmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @param usePrev If true, uses getPrevFloat instead of getFloat.
     * @return A non-negative position if {@code minInc=true} and {@code min} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the insertion point.
     */
    static long lowerBoundAscending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final float min,
            final boolean minInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final float midValue = usePrev ? source.getPrevFloat(selection.get(mid)) : source.getFloat(selection.get(mid));
            if (minInc ? FloatComparisons.geq(midValue, min) : FloatComparisons.gt(midValue, min)) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        // low is now the insertion point. For inclusive searches, check for an exact match there.
        if (minInc && low <= lastPos) {
            final float lowValue = usePrev ? source.getPrevFloat(selection.get(low)) : source.getFloat(selection.get(low));
            if (FloatComparisons.eq(lowValue, min)) {
                return low;
            }
        }
        return insertionPoint(low);
    }

    /**
     * Performs a binary search on an ascending (non-descending) sorted {@link ElementSource} to find the position of
     * {@code max} within the search range.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code maxInc=true} and the value at the found position exactly
     * equals {@code max}. The returned value is the rightmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases: when {@code max} is absent from the range, when
     * {@code maxInc=false} (exclusive bound), or when no position satisfies the bound. In this case {@code -(p + 1)} is
     * the first position whose value exceeds {@code max} â or {@code firstPos} if all values in the range are &gt;
     * {@code max}.</li>
     * </ul>
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param max The value to find.
     * @param maxInc If {@code true}, an exact match at the rightmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @param usePrev If true, uses getPrevFloat instead of getFloat.
     * @return A non-negative position if {@code maxInc=true} and {@code max} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the first position whose value exceeds {@code max}.
     */
    static long upperBoundAscending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final float max,
            final boolean maxInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final float midValue = usePrev ? source.getPrevFloat(selection.get(mid)) : source.getFloat(selection.get(mid));
            if (maxInc ? FloatComparisons.leq(midValue, max) : FloatComparisons.lt(midValue, max)) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        // high is now the last satisfying position; low = high + 1 is the first non-satisfying position.
        // For inclusive searches, check for an exact match at high.
        if (maxInc && high >= firstPos) {
            final float highValue =
                    usePrev ? source.getPrevFloat(selection.get(high)) : source.getFloat(selection.get(high));
            if (FloatComparisons.eq(highValue, max)) {
                return high;
            }
        }
        return insertionPoint(low);
    }

    /**
     * Performs a binary search on a descending (non-ascending) sorted {@link ElementSource} to find the position of
     * {@code max} within the search range.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code maxInc=true} and the value at the found position exactly
     * equals {@code max}. The returned value is the leftmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases: when {@code max} is absent from the range, when
     * {@code maxInc=false} (exclusive bound), or when no position satisfies the bound. In this case {@code -(p + 1)} is
     * the insertion point â the leftmost position whose value falls below {@code max} â or {@code lastPos + 1} if all
     * values in the range are &gt;= {@code max}.</li>
     * </ul>
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param max The value to find.
     * @param maxInc If {@code true}, an exact match at the leftmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @param usePrev If true, uses getPrevFloat instead of getFloat.
     * @return A non-negative position if {@code maxInc=true} and {@code max} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the insertion point.
     */
    static long lowerBoundDescending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final float max,
            final boolean maxInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final float midValue = usePrev ? source.getPrevFloat(selection.get(mid)) : source.getFloat(selection.get(mid));
            if (maxInc ? FloatComparisons.leq(midValue, max) : FloatComparisons.lt(midValue, max)) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        // low is now the insertion point. For inclusive searches, check for an exact match there.
        if (maxInc && low <= lastPos) {
            final float lowValue = usePrev ? source.getPrevFloat(selection.get(low)) : source.getFloat(selection.get(low));
            if (FloatComparisons.eq(lowValue, max)) {
                return low;
            }
        }
        return insertionPoint(low);
    }

    /**
     * Performs a binary search on a descending (non-ascending) sorted {@link ElementSource} to find the position of
     * {@code min} within the search range.
     *
     * <p>
     * Positions are indices into {@code selection}; the row key at position {@code p} is {@code selection.get(p)}.
     *
     * <p>
     * Return value convention (mirrors {@link java.util.Arrays#binarySearch}):
     * <ul>
     * <li>A non-negative value is returned only when {@code minInc=true} and the value at the found position exactly
     * equals {@code min}. The returned value is the rightmost such position.</li>
     * <li>A negative value {@code p} is returned in all other cases: when {@code min} is absent from the range, when
     * {@code minInc=false} (exclusive bound), or when no position satisfies the bound. In this case {@code -(p + 1)} is
     * the first position whose value falls below {@code min} â or {@code firstPos} if all values in the range are &lt;=
     * {@code min}.</li>
     * </ul>
     *
     * @param source The element source to search.
     * @param selection The {@link RowSet} mapping positions to row keys.
     * @param firstPos The starting position of the search range (inclusive).
     * @param lastPos The ending position of the search range (inclusive).
     * @param min The value to find.
     * @param minInc If {@code true}, an exact match at the rightmost occurrence returns a non-negative position; if
     *        {@code false}, the result is always negative (insertion-point encoded).
     * @param usePrev If true, uses getPrevFloat instead of getFloat.
     * @return A non-negative position if {@code minInc=true} and {@code min} is found; otherwise a negative value
     *         {@code p} where {@code -(p + 1)} is the first position whose value falls below {@code min}.
     */
    static long upperBoundDescending(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            final long firstPos,
            final long lastPos,
            final float min,
            final boolean minInc,
            final boolean usePrev) {
        long low = firstPos;
        long high = lastPos;

        while (low <= high) {
            final long mid = low + (high - low) / 2;
            final float midValue = usePrev ? source.getPrevFloat(selection.get(mid)) : source.getFloat(selection.get(mid));
            if (minInc ? FloatComparisons.geq(midValue, min) : FloatComparisons.gt(midValue, min)) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        // high is now the last satisfying position; low = high + 1 is the first non-satisfying position.
        // For inclusive searches, check for an exact match at high.
        if (minInc && high >= firstPos) {
            final float highValue =
                    usePrev ? source.getPrevFloat(selection.get(high)) : source.getFloat(selection.get(high));
            if (FloatComparisons.eq(highValue, min)) {
                return high;
            }
        }
        return insertionPoint(low);
    }
}

