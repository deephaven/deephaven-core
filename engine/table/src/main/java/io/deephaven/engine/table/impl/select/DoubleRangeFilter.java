/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatRangeFilter and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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

public class DoubleRangeFilter extends AbstractRangeFilter {

    private final double upper;
    private final double lower;

    public DoubleRangeFilter(String columnName, double val1, double val2) {
        this(columnName, val1, val2, true, true);
    }

    public DoubleRangeFilter(String columnName, double val1, double val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);

        if(val1 > val2) {
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

        return new DoubleRangeFilter(columnName, (double)parsed, (double)(positiveOrZero ? parsed + offset : parsed - offset), positiveOrZero, !positiveOrZero);
    }

    static WhereFilter makeDoubleRangeFilter(String columnName, Condition condition, String value) {
        switch (condition) {
            case LESS_THAN:
                return new DoubleRangeFilter(columnName, Double.parseDouble(value), QueryConstants.NULL_DOUBLE, true, false);
            case LESS_THAN_OR_EQUAL:
                return new DoubleRangeFilter(columnName, Double.parseDouble(value), QueryConstants.NULL_DOUBLE, true, true);
            case GREATER_THAN:
                return new DoubleRangeFilter(columnName, Double.parseDouble(value), Double.NaN, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new DoubleRangeFilter(columnName, Double.parseDouble(value), Double.NaN, true, true);
            default:
                throw new IllegalArgumentException("RangeConditionFilter does not support condition " + condition);
        }
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: " + tableDefinition.getColumnNames());
        }
        chunkFilter = DoubleRangeComparator.makeDoubleFilter(lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public DoubleRangeFilter copy() {
        return new DoubleRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public String toString() {
        return "DoubleRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    WritableRowSet binarySearch(RowSet selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        //noinspection unchecked
        final ColumnSource<Double> doubleColumnSource = (ColumnSource<Double>)columnSource;

        final double startValue = reverse ? upper : lower;
        final double endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? - 1 : 1;

        long lowerBoundMin = bound(selection, usePrev, doubleColumnSource, 0, selection.size(), startValue, startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, doubleColumnSource, lowerBoundMin, selection.size(), endValue, endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }

    private static long bound(RowSet selection, boolean usePrev, ColumnSource<Double> doubleColumnSource, long minPosition, long maxPosition, double targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final double compareValue = usePrev ? doubleColumnSource.getPrevDouble(midIdx) : doubleColumnSource.getDouble(midIdx);
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
