//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.clientsupport.gotorow;

import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.*;

public class SeekRow {
    private final long startingRow;
    private final String columnName;
    private final Object seekValue;
    private final boolean insensitive;
    private final boolean contains;
    private final boolean isBackward;

    private ColumnSource columnSource;
    private boolean usePrev;

    private static final Logger log = LoggerFactory.getLogger(SeekRow.class);

    public SeekRow(long startingRow, String columnName, Object seekValue, boolean insensitive, boolean contains,
            boolean isBackward) {
        this.startingRow = startingRow < 0 ? 0 : startingRow;
        this.columnName = columnName;
        this.seekValue = seekValue;
        this.insensitive = insensitive;
        this.contains = contains;
        this.isBackward = isBackward;
    }

    @ConcurrentMethod
    public long seek(Table table) {
        final Mutable<Long> result = new MutableObject<>(null);

        ConstructSnapshot.callDataSnapshotFunction("SeekRow",
                ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (NotificationStepSource) table),
                ((nestedUsePrev, beforeClockValue) -> {
                    final Optional<SortingOrder> order = SortedColumnsAttribute.getOrderForColumn(table, columnName);
                    final RowSet rowSet = table.getRowSet();
                    columnSource = table.getColumnSource(columnName);
                    usePrev = nestedUsePrev;

                    if (order.isPresent()) {
                        final Comparable currValue = (Comparable) columnSourceGet(rowSet.get(startingRow));
                        int compareResult = nullSafeCompare(currValue, (Comparable) seekValue);

                        if (isBackward) {
                            // current row is seek value, check prev row
                            if (compareResult == 0 && startingRow > 0) {
                                final Comparable prevValue = (Comparable) columnSourceGet(rowSet.get(startingRow - 1));
                                if (nullSafeCompare(prevValue, (Comparable) seekValue) == 0) {
                                    result.setValue(startingRow - 1);
                                    return true;
                                }
                                // prev row is not the seek value, loop to back and find the last value
                                // algorithm is the same as if seek value is below the current row
                            } else if ((compareResult > 0 && order.get() == SortingOrder.Ascending)
                                    || (compareResult < 0 && order.get() == SortingOrder.Descending)) {
                                // current row is greater than seek value and ascending
                                // current row is less than seek value and descending
                                // which means seek value is above the current row, find the last occurrence
                                result.setValue(findEdgeOccurrence(rowSet, 0, startingRow, false,
                                        order.get() == SortingOrder.Ascending));
                                return true;
                            }
                            // seek value is below the current row
                            // loop to back and find the last value
                            result.setValue(findEdgeOccurrence(rowSet, startingRow, rowSet.size() - 1, false,
                                    order.get() == SortingOrder.Ascending));
                            return true;

                        } else {
                            // current row is seek value, check next row
                            if (compareResult == 0 && startingRow < rowSet.size() - 1) {
                                final Comparable nextValue = (Comparable) columnSourceGet(rowSet.get(startingRow + 1));
                                if (nullSafeCompare(nextValue, (Comparable) seekValue) == 0) {
                                    result.setValue(startingRow + 1);
                                    return true;
                                }
                                // next row is not the seek value, loop to start and find the first value
                                // algorithm is the same as if seek value is above the current row
                            } else if ((compareResult < 0 && order.get() == SortingOrder.Ascending)
                                    || (compareResult > 0 && order.get() == SortingOrder.Descending)) {
                                // current row is less than seek value and ascending
                                // current row is greater than seek value and descending
                                // which means seek value is below the current row, find the first occurrence
                                result.setValue(
                                        findEdgeOccurrence(rowSet, startingRow, rowSet.size() - 1, true,
                                                order.get() == SortingOrder.Ascending));
                                return true;
                            }
                            // seek value is above the current row
                            // loop to start and find the first value
                            result.setValue(findEdgeOccurrence(rowSet, 0, startingRow, true,
                                    order.get() == SortingOrder.Ascending));
                            return true;
                        }
                    }

                    long row;
                    if (isBackward) {
                        row = findRow(rowSet, 0, (int) startingRow);
                        if (row >= 0) {
                            result.setValue(row);
                            return true;
                        }
                        row = findRow(rowSet, (int) startingRow, (int) rowSet.size());
                        if (row >= 0) {
                            result.setValue(row);
                            return true;
                        }
                    } else {
                        row = findRow(rowSet, (int) startingRow + 1, (int) rowSet.size());
                        if (row >= 0) {
                            result.setValue(row);
                            return true;
                        }
                        row = findRow(rowSet, 0, (int) startingRow + 1);
                        if (row >= 0) {
                            result.setValue(row);
                            return true;
                        }
                    }
                    result.setValue(-1L);
                    return true;
                }));

        Assert.neqNull(result.getValue(), "result.getValue()");
        return result.getValue();
    }

    /**
     * Finds the first/last occurrence of the target value by using binary search
     *
     * @param start the starting index to search
     * @param end the ending index to search
     * @param findFirst whether to find the first or last occurrence (false for last)
     * @param isAscending whether the table is sorted in ascending order (false for descending)
     * @return the index of the first/last occurrence of the target value, -1 if not found
     */
    private long findEdgeOccurrence(RowSet index, long start, long end, boolean findFirst,
            boolean isAscending) {
        long result = -1;

        while (start <= end) {
            long mid = start + (end - start) / 2;
            Comparable midValue = (Comparable) columnSourceGet((int) index.get((int) mid));
            int compareResult = nullSafeCompare(midValue, (Comparable) seekValue);

            if (compareResult == 0) {
                result = mid;
                if (findFirst) {
                    end = mid - 1;
                } else {
                    start = mid + 1;
                }
            } else if ((compareResult < 0 && isAscending) || (compareResult > 0 && !isAscending)) {
                // mid less than target and list is ascending
                // mid more than target and list is descending
                // search right half
                start = mid + 1;
            } else {
                // other way around, search left half
                end = mid - 1;
            }
        }
        return result;
    }

    int nullSafeCompare(Comparable c1, Comparable c2) {
        if (c1 == c2) {
            return 0;
        }
        if (c1 == null) {
            return -1;
        }
        if (c2 == null) {
            return 1;
        }
        if (insensitive) {
            return ((String) c1).toLowerCase().compareTo(((String) c2).toLowerCase());
        }
        // noinspection unchecked
        return c1.compareTo(c2);
    }

    private Object columnSourceGet(long rowKey) {
        return usePrev ? columnSource.getPrev(rowKey) : columnSource.get(rowKey);
    }

    private long findRow(RowSet index, int start, int end) {
        final RowSet subIndex = index.subSetByPositionRange(start, end);

        final RowSet.Iterator it;
        if (isBackward) {
            it = subIndex.reverseIterator();
        } else {
            it = subIndex.iterator();
        }

        final Object useSeek =
                (seekValue instanceof String && insensitive) ? ((String) seekValue).toLowerCase() : seekValue;

        for (; it.hasNext();) {
            long key = it.nextLong();
            Object value = columnSourceGet(key);
            if (useSeek instanceof String) {
                value = value == null ? null : value.toString();
                if (insensitive) {
                    value = value == null ? null : ((String) value).toLowerCase();
                }
            }
            // noinspection ConstantConditions
            if (contains && value != null && ((String) value).contains((String) useSeek)) {
                return Require.geqZero(index.find(key), "index.find(key)");
            }
            if (value == useSeek || (useSeek != null && useSeek.equals(value))) {
                return Require.geqZero(index.find(key), "index.find(key)");
            }
        }

        return -1L;
    }
}
