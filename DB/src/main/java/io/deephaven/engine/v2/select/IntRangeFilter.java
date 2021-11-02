/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRangeFilter and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.select;

import io.deephaven.engine.tables.ColumnDefinition;
import io.deephaven.engine.tables.TableDefinition;
import io.deephaven.engine.util.DhIntComparisons;
import io.deephaven.engine.v2.select.chunkfilters.IntRangeComparator;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.MutableRowSet;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;

public class IntRangeFilter extends AbstractRangeFilter {
    final int upper;
    final int lower;

    public IntRangeFilter(String columnName, int val1, int val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);

        if(DhIntComparisons.gt(val1, val2)) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    static SelectFilter makeIntRangeFilter(String columnName, Condition condition, String value) {
        switch (condition) {
            case LESS_THAN:
                return new IntRangeFilter(columnName, RangeConditionFilter.parseIntFilter(value), QueryConstants.NULL_INT, true, false);
            case LESS_THAN_OR_EQUAL:
                return new IntRangeFilter(columnName, RangeConditionFilter.parseIntFilter(value), QueryConstants.NULL_INT, true, true);
            case GREATER_THAN:
                return new IntRangeFilter(columnName, RangeConditionFilter.parseIntFilter(value), Integer.MAX_VALUE, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new IntRangeFilter(columnName, RangeConditionFilter.parseIntFilter(value), Integer.MAX_VALUE, true, true);
            default:
                throw new IllegalArgumentException("RangeConditionFilter does not support condition " + condition);
        }
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: " + tableDefinition.getColumnNames());
        }

        final Class<?> colClass = TypeUtils.getUnboxedTypeIfBoxed(def.getDataType());
        if (colClass != int.class) {
            throw new RuntimeException("Column \"" + columnName + "\" expected to be int: " + colClass);
        }

        initChunkFilter();
    }

    ChunkFilter initChunkFilter() {
        return (chunkFilter = IntRangeComparator.makeIntFilter(lower, upper, lowerInclusive, upperInclusive));
    }

    @Override
    public IntRangeFilter copy() {
        return new IntRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public String toString() {
        return "IntRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    MutableRowSet binarySearch(RowSet selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection.clone();
        }

        //noinspection unchecked
        final ColumnSource<Integer> intColumnSource = (ColumnSource<Integer>)columnSource;

        final int startValue = reverse ? upper : lower;
        final int endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? - 1 : 1;

        long lowerBoundMin = bound(selection, usePrev, intColumnSource, 0, selection.size(), startValue, startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, intColumnSource, lowerBoundMin, selection.size(), endValue, endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }

    private long bound(RowSet selection, boolean usePrev, ColumnSource<Integer> longColumnSource, long minPosition, long maxPosition, int targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final int compareValue = usePrev ? longColumnSource.getPrevInt(midIdx) : longColumnSource.getInt(midIdx);
            final int compareResult = compareSign * DhIntComparisons.compare(compareValue, targetValue);

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
