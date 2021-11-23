package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.engine.table.impl.chunkfilter.FloatRangeComparator;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.QueryConstants;

public class FloatRangeFilter extends AbstractRangeFilter {

    private final float upper;
    private final float lower;

    public FloatRangeFilter(String columnName, float val1, float val2) {
        this(columnName, val1, val2, true, true);
    }

    public FloatRangeFilter(String columnName, float val1, float val2, boolean lowerInclusive, boolean upperInclusive) {
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

        return new FloatRangeFilter(columnName, (float)parsed, (float)(positiveOrZero ? parsed + offset : parsed - offset), positiveOrZero, !positiveOrZero);
    }

    static WhereFilter makeFloatRangeFilter(String columnName, Condition condition, String value) {
        switch (condition) {
            case LESS_THAN:
                return new FloatRangeFilter(columnName, Float.parseFloat(value), QueryConstants.NULL_FLOAT, true, false);
            case LESS_THAN_OR_EQUAL:
                return new FloatRangeFilter(columnName, Float.parseFloat(value), QueryConstants.NULL_FLOAT, true, true);
            case GREATER_THAN:
                return new FloatRangeFilter(columnName, Float.parseFloat(value), Float.NaN, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new FloatRangeFilter(columnName, Float.parseFloat(value), Float.NaN, true, true);
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
        chunkFilter = FloatRangeComparator.makeFloatFilter(lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public FloatRangeFilter copy() {
        return new FloatRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public String toString() {
        return "FloatRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    WritableRowSet binarySearch(RowSet selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        //noinspection unchecked
        final ColumnSource<Float> floatColumnSource = (ColumnSource<Float>)columnSource;

        final float startValue = reverse ? upper : lower;
        final float endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? - 1 : 1;

        long lowerBoundMin = bound(selection, usePrev, floatColumnSource, 0, selection.size(), startValue, startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, floatColumnSource, lowerBoundMin, selection.size(), endValue, endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }

    private static long bound(RowSet selection, boolean usePrev, ColumnSource<Float> floatColumnSource, long minPosition, long maxPosition, float targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final float compareValue = usePrev ? floatColumnSource.getPrevFloat(midIdx) : floatColumnSource.getFloat(midIdx);
            final int compareResult = compareSign * FloatComparisons.compare(compareValue, targetValue);

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
