package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.util.DhCharComparisons;
import io.deephaven.db.util.DhLongComparisons;
import io.deephaven.db.v2.select.chunkfilters.CharRangeComparator;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;

public class CharRangeFilter extends AbstractRangeFilter {
    final char upper;
    final char lower;

    public CharRangeFilter(String columnName, char val1, char val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);

        if(DhCharComparisons.gt(val1, val2)) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    static SelectFilter makeCharRangeFilter(String columnName, Condition condition, String value) {
        switch (condition) {
            case LESS_THAN:
                return new CharRangeFilter(columnName, RangeConditionFilter.parseCharFilter(value), QueryConstants.NULL_CHAR, true, false);
            case LESS_THAN_OR_EQUAL:
                return new CharRangeFilter(columnName, RangeConditionFilter.parseCharFilter(value), QueryConstants.NULL_CHAR, true, true);
            case GREATER_THAN:
                return new CharRangeFilter(columnName, RangeConditionFilter.parseCharFilter(value), QueryConstants.MAX_CHAR, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new CharRangeFilter(columnName, RangeConditionFilter.parseCharFilter(value), QueryConstants.MAX_CHAR, true, true);
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
        if (colClass != char.class) {
            throw new RuntimeException("Column \"" + columnName + "\" expected to be char: " + colClass);
        }

        initChunkFilter();
    }

    ChunkFilter initChunkFilter() {
        return (chunkFilter = CharRangeComparator.makeCharFilter(lower, upper, lowerInclusive, upperInclusive));
    }

    @Override
    public CharRangeFilter copy() {
        return new CharRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public String toString() {
        return "CharRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    Index binarySearch(Index selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection;
        }

        //noinspection unchecked
        final ColumnSource<Character> charColumnSource = (ColumnSource<Character>)columnSource;

        final char startValue = reverse ? upper : lower;
        final char endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? - 1 : 1;

        long lowerBoundMin = bound(selection, usePrev, charColumnSource, 0, selection.size(), startValue, startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, charColumnSource, lowerBoundMin, selection.size(), endValue, endInclusive, compareSign, true);

        return selection.subindexByPos(lowerBoundMin, upperBoundMin);
    }

    private long bound(Index selection, boolean usePrev, ColumnSource<Character> longColumnSource, long minPosition, long maxPosition, char targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final char compareValue = usePrev ? longColumnSource.getPrevChar(midIdx) : longColumnSource.getChar(midIdx);
            final int compareResult = compareSign * DhCharComparisons.compare(compareValue, targetValue);

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
