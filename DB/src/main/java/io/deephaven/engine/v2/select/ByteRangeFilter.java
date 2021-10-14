/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRangeFilter and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.select;

import io.deephaven.engine.tables.ColumnDefinition;
import io.deephaven.engine.tables.TableDefinition;
import io.deephaven.engine.util.DhByteComparisons;
import io.deephaven.engine.v2.select.chunkfilters.ByteRangeComparator;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.Index;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;

public class ByteRangeFilter extends AbstractRangeFilter {
    final byte upper;
    final byte lower;

    public ByteRangeFilter(String columnName, byte val1, byte val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);

        if(DhByteComparisons.gt(val1, val2)) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    static SelectFilter makeByteRangeFilter(String columnName, Condition condition, String value) {
        switch (condition) {
            case LESS_THAN:
                return new ByteRangeFilter(columnName, RangeConditionFilter.parseByteFilter(value), QueryConstants.NULL_BYTE, true, false);
            case LESS_THAN_OR_EQUAL:
                return new ByteRangeFilter(columnName, RangeConditionFilter.parseByteFilter(value), QueryConstants.NULL_BYTE, true, true);
            case GREATER_THAN:
                return new ByteRangeFilter(columnName, RangeConditionFilter.parseByteFilter(value), Byte.MAX_VALUE, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new ByteRangeFilter(columnName, RangeConditionFilter.parseByteFilter(value), Byte.MAX_VALUE, true, true);
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
        if (colClass != byte.class) {
            throw new RuntimeException("Column \"" + columnName + "\" expected to be byte: " + colClass);
        }

        initChunkFilter();
    }

    ChunkFilter initChunkFilter() {
        return (chunkFilter = ByteRangeComparator.makeByteFilter(lower, upper, lowerInclusive, upperInclusive));
    }

    @Override
    public ByteRangeFilter copy() {
        return new ByteRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public String toString() {
        return "ByteRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    Index binarySearch(Index selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection;
        }

        //noinspection unchecked
        final ColumnSource<Byte> byteColumnSource = (ColumnSource<Byte>)columnSource;

        final byte startValue = reverse ? upper : lower;
        final byte endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? - 1 : 1;

        long lowerBoundMin = bound(selection, usePrev, byteColumnSource, 0, selection.size(), startValue, startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, byteColumnSource, lowerBoundMin, selection.size(), endValue, endInclusive, compareSign, true);

        return selection.subindexByPos(lowerBoundMin, upperBoundMin);
    }

    private long bound(Index selection, boolean usePrev, ColumnSource<Byte> longColumnSource, long minPosition, long maxPosition, byte targetValue, boolean inclusive, int compareSign, boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final byte compareValue = usePrev ? longColumnSource.getPrevByte(midIdx) : longColumnSource.getByte(midIdx);
            final int compareResult = compareSign * DhByteComparisons.compare(compareValue, targetValue);

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
