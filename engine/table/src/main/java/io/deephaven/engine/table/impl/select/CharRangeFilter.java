//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.CharRangeComparator;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

public class CharRangeFilter extends AbstractRangeFilter {
    public static CharRangeFilter lt(String columnName, char x) {
        return new CharRangeFilter(columnName, QueryConstants.NULL_CHAR, x, true, false);
    }

    public static CharRangeFilter leq(String columnName, char x) {
        return new CharRangeFilter(columnName, QueryConstants.NULL_CHAR, x, true, true);
    }

    public static CharRangeFilter gt(String columnName, char x) {
        return new CharRangeFilter(columnName, x, QueryConstants.MAX_CHAR, false, true);
    }

    public static CharRangeFilter geq(String columnName, char x) {
        return new CharRangeFilter(columnName, x, QueryConstants.MAX_CHAR, true, true);
    }

    final char upper;
    final char lower;

    public CharRangeFilter(String columnName, char val1, char val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);
        if (CharComparisons.gt(val1, val2)) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    static WhereFilter makeCharRangeFilter(String columnName, Condition condition, char value) {
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

        final ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
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
        final CharRangeFilter copy = new CharRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "CharRangeFilter(" + columnName + " in " +
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
        final ColumnSource<Character> charColumnSource = (ColumnSource<Character>) columnSource;

        final char startValue = reverse ? upper : lower;
        final char endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? -1 : 1;

        long lowerBoundMin = bound(selection, usePrev, charColumnSource, 0, selection.size(), startValue,
                startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, charColumnSource, lowerBoundMin, selection.size(), endValue,
                endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }

    private long bound(RowSet selection, boolean usePrev, ColumnSource<Character> longColumnSource, long minPosition,
            long maxPosition, char targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final char compareValue = usePrev ? longColumnSource.getPrevChar(midIdx) : longColumnSource.getChar(midIdx);
            final int compareResult = compareSign * CharComparisons.compare(compareValue, targetValue);

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
