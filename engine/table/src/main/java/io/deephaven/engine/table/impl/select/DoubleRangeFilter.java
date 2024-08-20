//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatRangeFilter and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.engine.table.impl.chunkfilter.DoubleRangeComparator;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public class DoubleRangeFilter extends AbstractRangeFilter {

    public static DoubleRangeFilter lt(String columnName, double x) {
        return new DoubleRangeFilter(columnName, x, QueryConstants.NULL_DOUBLE, true, false);
    }

    public static DoubleRangeFilter leq(String columnName, double x) {
        return new DoubleRangeFilter(columnName, x, QueryConstants.NULL_DOUBLE, true, true);
    }

    public static DoubleRangeFilter gt(String columnName, double x) {
        return new DoubleRangeFilter(columnName, x, Double.NaN, false, true);
    }

    public static DoubleRangeFilter geq(String columnName, double x) {
        return new DoubleRangeFilter(columnName, x, Double.NaN, true, true);
    }

    private final double upper;
    private final double lower;

    public DoubleRangeFilter(String columnName, double val1, double val2) {
        this(columnName, val1, val2, true, true);
    }

    public DoubleRangeFilter(String columnName, double val1, double val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);
        if (DoubleComparisons.gt(val1, val2)) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    public static WhereFilter makeRange(String columnName, String val) {
        final int precision = findPrecision(val);
        final double parsed = Double.parseDouble(val);
        final double offset = Math.pow(10, -precision);
        final boolean positiveOrZero = parsed >= 0;
        return new DoubleRangeFilter(columnName, (double) parsed,
                (double) (positiveOrZero ? parsed + offset : parsed - offset), positiveOrZero, !positiveOrZero);
    }

    static WhereFilter makeDoubleRangeFilter(String columnName, Condition condition, double value) {
        switch (condition) {
            case LESS_THAN:
                return lt(columnName, value);
            case LESS_THAN_OR_EQUAL:
                return leq(columnName, value);
            case GREATER_THAN:
                return gt(columnName, value);
            case GREATER_THAN_OR_EQUAL:
                return geq(columnName, value);
            default:
                throw new IllegalArgumentException("RangeFilter does not support condition " + condition);
        }
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }
        chunkFilter = DoubleRangeComparator.makeDoubleFilter(lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public DoubleRangeFilter copy() {
        final DoubleRangeFilter copy = new DoubleRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "DoubleRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    @NotNull
    @Override
    WritableRowSet binarySearch(
            @NotNull final RowSet selection,
            @NotNull final ColumnSource<?> columnSource,
            final boolean usePrev,
            final boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        // noinspection unchecked
        final ColumnSource<Double> doubleColumnSource = (ColumnSource<Double>) columnSource;

        final double startValue = reverse ? upper : lower;
        final double endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? -1 : 1;

        long lowerBoundMin = bound(selection, usePrev, doubleColumnSource, 0, selection.size(), startValue,
                startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, doubleColumnSource, lowerBoundMin, selection.size(), endValue,
                endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }

    private static long bound(RowSet selection, boolean usePrev, ColumnSource<Double> doubleColumnSource,
            long minPosition, long maxPosition, double targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final double compareValue =
                    usePrev ? doubleColumnSource.getPrevDouble(midIdx) : doubleColumnSource.getDouble(midIdx);
            final int compareResult = compareSign * DoubleComparisons.compare(compareValue, targetValue);

            if (compareResult < 0) {
                minPosition = midPos + 1;
            } else if (compareResult > 0) {
                maxPosition = midPos;
            } else {
                if (end) {
                    if (inclusive) {
                        minPosition = midPos + 1;
                    } else {
                        maxPosition = midPos;
                    }
                } else {
                    if (inclusive) {
                        maxPosition = midPos;
                    } else {
                        minPosition = midPos + 1;
                    }
                }
            }
        }
        return minPosition;
    }
}
