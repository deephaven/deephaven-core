//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.clientsupport.gotorow;

import java.time.Instant;
import java.util.function.Function;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;

import java.util.*;

public class SeekRow implements Function<Table, Long> {
    private final long startingRow;
    private final String columnName;
    private final Object seekValue;
    private final boolean insensitive;
    private final boolean contains;
    private final boolean isBackward;

    private Comparable closestUpperValueYet;
    private Comparable closestLowerValueYet;
    private long closestUpperRowYet = -1;
    private long closestLowerRowYet = -1;

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

    @Override
    public Long apply(Table table) {
        final Optional<SortingOrder> order = SortedColumnsAttribute.getOrderForColumn(table, columnName);
        final RowSet rowSet = table.getRowSet();
        log.info().append("starting row: ").append(startingRow).endl();

        if (order.isPresent()) {
            final Comparable currValue =
                    (Comparable) table.getColumnSource(columnName).get(rowSet.get(startingRow));
            int compareResult = nullSafeCompare(currValue, (Comparable) seekValue);

            if (isBackward) {
                // current row is seek value, check prev row
                if (compareResult == 0 && startingRow > 0) {
                    final Comparable prevValue =
                            (Comparable) table.getColumnSource(columnName).get(rowSet.get(startingRow - 1));
                    if (nullSafeCompare(prevValue, (Comparable) seekValue) == 0) {
                        return startingRow - 1;
                    }
                    // prev row is not the seek value, loop to back and find the last value
                    // algorithm is the same as if seek value is below the current row
                } else if ((compareResult > 0 && order.get() == SortingOrder.Ascending)
                        || (compareResult < 0 && order.get() == SortingOrder.Descending)) {
                    // current row is greater than seek value and ascending
                    // current row is less than seek value and descending
                    // which means seek value is above the current row, find the last occurrence
                    return findEdgeOccurrence(table, rowSet, 0, startingRow - 1, false,
                            order.get() == SortingOrder.Ascending);
                }
                // seek value is below the current row
                // loop to back and find the last value
                return findEdgeOccurrence(table, rowSet, startingRow, rowSet.size() - 1, false,
                        order.get() == SortingOrder.Ascending);

            } else {
                // current row is seek value, check next row
                if (compareResult == 0 && startingRow < rowSet.size() - 1) {
                    final Comparable nextValue =
                            (Comparable) table.getColumnSource(columnName).get(rowSet.get(startingRow + 1));
                    if (nullSafeCompare(nextValue, (Comparable) seekValue) == 0) {
                        return startingRow + 1;
                    }
                    // next row is not the seek value, loop to start and find the first value
                    // algorithm is the same as if seek value is above the current row
                } else if ((compareResult < 0 && order.get() == SortingOrder.Ascending)
                        || (compareResult > 0 && order.get() == SortingOrder.Descending)) {
                    // current row is less than seek value and ascending
                    // current row is greater than seek value and descending
                    // which means seek value is below the current row, find the first occurrence
                    return findEdgeOccurrence(table, rowSet, startingRow + 1, rowSet.size() - 1, true,
                            order.get() == SortingOrder.Ascending);
                }
                // seek value is above the current row
                // loop to start and find the first value
                return findEdgeOccurrence(table, rowSet, 0, startingRow - 1, true,
                        order.get() == SortingOrder.Ascending);
            }
        }

        long row;
        if (isBackward) {
            row = findRow(table, rowSet, 0, (int) startingRow);
            if (row >= 0) {
                return row;
            }
            row = findRow(table, rowSet, (int) startingRow, (int) rowSet.size());
            if (row >= 0) {
                return row;
            }
        } else {
            row = findRow(table, rowSet, (int) startingRow + 1, (int) rowSet.size());
            if (row >= 0) {
                return row;
            }
            row = findRow(table, rowSet, 0, (int) startingRow + 1);
            if (row >= 0) {
                return row;
            }
        }

        // just go to the closest value
        if (closestLowerValueYet == null && closestUpperValueYet == null) {
            return -1L;
        } else if (closestLowerValueYet == null) {
            return rowSet.find(closestUpperRowYet);
        } else if (closestUpperValueYet == null) {
            return rowSet.find(closestLowerRowYet);
        } else {
            // we need to decide between the two
            Class columnType = table.getColumnSource(columnName).getType();
            if (Number.class.isAssignableFrom(columnType)) {
                double nu = ((Number) closestUpperValueYet).doubleValue();
                double nl = ((Number) closestLowerRowYet).doubleValue();
                double ns = ((Number) seekValue).doubleValue();
                double du = Math.abs(nu - ns);
                double dl = Math.abs(nl - ns);
                log.info().append("Using numerical distance (").appendDouble(dl).append(", ").appendDouble(du)
                        .append(")").endl();
                return rowSet.find(du < dl ? closestUpperRowYet : closestLowerRowYet);
            } else if (Instant.class.isAssignableFrom(columnType)) {
                long nu = DateTimeUtils.epochNanos(((Instant) closestUpperValueYet));
                long nl = DateTimeUtils.epochNanos(((Instant) closestLowerValueYet));
                long ns = DateTimeUtils.epochNanos(((Instant) seekValue));
                long du = Math.abs(nu - ns);
                long dl = Math.abs(nl - ns);
                log.info().append("Using nano distance (").append(dl).append(", ").append(du).append(")").endl();
                return rowSet.find(du < dl ? closestUpperRowYet : closestLowerRowYet);
            } else {
                long nu = rowSet.find(closestUpperRowYet);
                long nl = rowSet.find(closestLowerRowYet);
                long ns = startingRow;
                long du = Math.abs(nu - ns);
                long dl = Math.abs(nl - ns);
                log.info().append("Using index distance (").append(dl).append(", ").append(du).append(")").endl();
                return du < dl ? nu : nl;
            }
        }
    }

    /**
     * Finds the first/last occurrence of the target value by using binary search
     *
     * @param table the table to check for sorted-ness
     * @param start the starting index to search
     * @param end the ending index to search
     * @param findFirst whether to find the first or last occurrence (false for last)
     * @param isAscending whether the table is sorted in ascending order (false for descending)
     * @return the index of the first/last occurrence of the target value, -1 if not found
     */
    private long findEdgeOccurrence(Table table, RowSet index, long start, long end, boolean findFirst,
            boolean isAscending) {
        long result = -1;

        while (start <= end) {
            long mid = start + (end - start) / 2;
            Comparable midValue =
                    (Comparable) table.getColumnSource(columnName).get((int) index.get((int) mid));
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

    String nullSafeToString(Object o) {
        return o == null ? "(null)" : o.toString();
    }

    private long findRow(Table table, RowSet index, int start, int end) {
        final RowSet subIndex = index.subSetByPositionRange(start, end);

        final RowSet.Iterator it;
        if (isBackward) {
            it = subIndex.reverseIterator();
        } else {
            it = subIndex.iterator();
        }

        final ColumnSource columnSource = table.getColumnSource(columnName);

        final boolean isComparable = !contains
                && (Comparable.class.isAssignableFrom(columnSource.getType()) || columnSource.getType().isPrimitive());

        final Object useSeek =
                (seekValue instanceof String && insensitive) ? ((String) seekValue).toLowerCase() : seekValue;

        for (; it.hasNext();) {
            long key = it.nextLong();
            Object value = columnSource.get(key);
            if (useSeek instanceof String) {
                value = value == null ? null : value.toString();
                if (insensitive) {
                    value = value == null ? null : ((String) value).toLowerCase();
                }
            }
            // noinspection ConstantConditions
            if (contains && value != null && ((String) value).contains((String) useSeek)) {
                return (long) Require.geqZero(index.find(key), "index.find(key)");
            }
            if (value == useSeek || (useSeek != null && useSeek.equals(value))) {
                return (long) Require.geqZero(index.find(key), "index.find(key)");
            }

            if (isComparable && useSeek != null && value != null) {
                // noinspection unchecked
                long compareResult = ((Comparable) useSeek).compareTo(value);
                if (compareResult < 0) {
                    // seekValue is less than value
                    if (closestUpperRowYet == -1) {
                        closestUpperRowYet = key;
                        closestUpperValueYet = (Comparable) value;
                    } else {
                        // noinspection unchecked
                        if (closestUpperValueYet.compareTo(value) > 0) {
                            closestUpperValueYet = (Comparable) value;
                            closestUpperRowYet = key;
                        }
                    }
                } else {
                    // seekValue is greater than value
                    // seekValue is less than value
                    if (closestLowerRowYet == -1) {
                        closestLowerRowYet = key;
                        closestLowerValueYet = (Comparable) value;
                    } else {
                        // noinspection unchecked
                        if (closestLowerValueYet.compareTo(value) < 0) {
                            closestLowerValueYet = (Comparable) value;
                            closestLowerRowYet = key;
                        }
                    }
                }
            }
        }

        return -1L;
    }
}
