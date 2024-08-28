//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeFilter and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ShortRangeComparator;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

public class ShortRangeFilter extends AbstractRangeFilter {
    public static ShortRangeFilter lt(String columnName, short x) {
        return new ShortRangeFilter(columnName, QueryConstants.NULL_SHORT, x, true, false);
    }

    public static ShortRangeFilter leq(String columnName, short x) {
        return new ShortRangeFilter(columnName, QueryConstants.NULL_SHORT, x, true, true);
    }

    public static ShortRangeFilter gt(String columnName, short x) {
        return new ShortRangeFilter(columnName, x, QueryConstants.MAX_SHORT, false, true);
    }

    public static ShortRangeFilter geq(String columnName, short x) {
        return new ShortRangeFilter(columnName, x, QueryConstants.MAX_SHORT, true, true);
    }

    final short upper;
    final short lower;

    public ShortRangeFilter(String columnName, short val1, short val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);
        if (ShortComparisons.gt(val1, val2)) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    static WhereFilter makeShortRangeFilter(String columnName, Condition condition, short value) {
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
        if (colClass != short.class) {
            throw new RuntimeException("Column \"" + columnName + "\" expected to be short: " + colClass);
        }

        initChunkFilter();
    }

    ChunkFilter initChunkFilter() {
        return (chunkFilter = ShortRangeComparator.makeShortFilter(lower, upper, lowerInclusive, upperInclusive));
    }

    @Override
    public ShortRangeFilter copy() {
        final ShortRangeFilter copy = new ShortRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "ShortRangeFilter(" + columnName + " in " +
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
        final ColumnSource<Short> shortColumnSource = (ColumnSource<Short>) columnSource;

        final short startValue = reverse ? upper : lower;
        final short endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? -1 : 1;

        long lowerBoundMin = bound(selection, usePrev, shortColumnSource, 0, selection.size(), startValue,
                startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, shortColumnSource, lowerBoundMin, selection.size(), endValue,
                endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }

    private long bound(RowSet selection, boolean usePrev, ColumnSource<Short> longColumnSource, long minPosition,
            long maxPosition, short targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final short compareValue = usePrev ? longColumnSource.getPrevShort(midIdx) : longColumnSource.getShort(midIdx);
            final int compareResult = compareSign * ShortComparisons.compare(compareValue, targetValue);

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
