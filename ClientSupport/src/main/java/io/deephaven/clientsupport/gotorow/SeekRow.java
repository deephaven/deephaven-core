//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.clientsupport.gotorow;

import java.time.Instant;
import java.util.function.Function;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;

import java.util.Random;

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
    @ConcurrentMethod
    public Long apply(Table table) {
        final int sortDirection = guessSorted(table);
        final boolean isSorted = !contains && sortDirection != 0;

        final RowSet index = table.getRowSet();
        long row;
        if (isSorted) {
            final Comparable value =
                    (Comparable) table.getColumnSource(columnName).get((int) index.get((int) startingRow));
            final int compareTo =
                    sortDirection * nullSafeCompare(value, (Comparable) seekValue) * (isBackward ? -1 : 1);
            final int start = isBackward ? (int) startingRow + 1 : (int) startingRow;
            if (compareTo == 0) {
                return startingRow;
            } else if (compareTo < 0) {
                // value is less than seek value
                log.info().append("Value is before: ").append(nullSafeToString(value)).append(" < ")
                        .append(nullSafeToString(seekValue)).endl();
                row = maybeBinarySearch(table, index, sortDirection, start, (int) index.size() - 1);
            } else {
                log.info().append("Value is after: ").append(nullSafeToString(value)).append(" > ")
                        .append(nullSafeToString(seekValue)).endl();
                row = maybeBinarySearch(table, index, sortDirection, 0, start);
            }
            if (row >= 0) {
                return row;
            }
            // we aren't really sorted
        }

        if (isBackward) {
            row = findRow(table, index, 0, (int) startingRow);
            if (row >= 0) {
                return row;
            }
            row = findRow(table, index, (int) startingRow, (int) index.size());
            if (row >= 0) {
                return row;
            }
        } else {
            row = findRow(table, index, (int) startingRow + 1, (int) index.size());
            if (row >= 0) {
                return row;
            }
            row = findRow(table, index, 0, (int) startingRow + 1);
            if (row >= 0) {
                return row;
            }
        }

        // just go to the closest value
        if (closestLowerValueYet == null && closestUpperValueYet == null) {
            return -1L;
        } else if (closestLowerValueYet == null) {
            return index.find(closestUpperRowYet);
        } else if (closestUpperValueYet == null) {
            return index.find(closestLowerRowYet);
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
                return index.find(du < dl ? closestUpperRowYet : closestLowerRowYet);
            } else if (Instant.class.isAssignableFrom(columnType)) {
                long nu = DateTimeUtils.epochNanos(((Instant) closestUpperValueYet));
                long nl = DateTimeUtils.epochNanos(((Instant) closestLowerValueYet));
                long ns = DateTimeUtils.epochNanos(((Instant) seekValue));
                long du = Math.abs(nu - ns);
                long dl = Math.abs(nl - ns);
                log.info().append("Using nano distance (").append(dl).append(", ").append(du).append(")").endl();
                return index.find(du < dl ? closestUpperRowYet : closestLowerRowYet);
            } else {
                long nu = index.find(closestUpperRowYet);
                long nl = index.find(closestLowerRowYet);
                long ns = startingRow;
                long du = Math.abs(nu - ns);
                long dl = Math.abs(nl - ns);
                log.info().append("Using index distance (").append(dl).append(", ").append(du).append(")").endl();
                return du < dl ? nu : nl;
            }
        }
    }

    private long maybeBinarySearch(Table table, RowSet index, int sortDirection, int start, int end) {
        log.info().append("Doing binary search ").append(start).append(", ").append(end).endl();

        final ColumnSource columnSource = table.getColumnSource(columnName);

        int minBound = start;
        int maxBound = end;

        Comparable minValue = (Comparable) columnSource.get(index.get(minBound));
        Comparable maxValue = (Comparable) columnSource.get(index.get(maxBound));

        final Comparable comparableSeek = (Comparable) this.seekValue;

        log.info().append("Seek Value ").append(nullSafeToString(comparableSeek)).endl();

        if (nullSafeCompare(minValue, comparableSeek) * sortDirection >= 0) {
            log.info().append("Less than min ").append(nullSafeToString(comparableSeek)).append(" < ")
                    .append(nullSafeToString(minValue)).endl();
            return minBound;
        } else if (nullSafeCompare(maxValue, comparableSeek) * sortDirection <= 0) {
            log.info().append("Greater than max: ").append(nullSafeToString(comparableSeek)).append(" < ")
                    .append(nullSafeToString(maxValue)).endl();
            return maxBound;
        }


        do {
            log.info().append("Bounds (").append(minBound).append(", ").append(maxBound).append(")").endl();
            if (minBound == maxBound || minBound == maxBound - 1) {
                return minBound;
            }

            if (nullSafeCompare(minValue, maxValue) * sortDirection > 0) {
                log.info().append("Not Sorted (").append(minValue.toString()).append(", ").append(maxValue.toString())
                        .append(")").endl();
                // not really sorted
                return -1;
            }

            final int check = (minBound + maxBound) / 2;
            final Comparable checkValue = (Comparable) columnSource.get(index.get(check));
            // Search up by default, reverse the result to search down
            final int compareResult =
                    nullSafeCompare(checkValue, comparableSeek) * sortDirection * (isBackward ? -1 : 1);

            log.info().append("Check[").append(check).append("] ").append(checkValue.toString()).append(" -> ")
                    .append(compareResult).endl();

            if (compareResult == 0) {
                return check;
            } else if (compareResult < 0) {
                minBound = check;
                minValue = checkValue;
            } else {
                maxBound = check;
                maxValue = checkValue;
            }
        } while (true);
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

    /**
     * Take a guess as to whether the table is sorted, such that we should do a binary search instead
     *
     * @param table the table to check for sorted-ness
     * @return 0 if the table is not sorted; 1 if might be ascending sorted, -1 if it might be descending sorted.
     */
    int guessSorted(Table table) {
        final ColumnSource columnSource = table.getColumnSource(columnName);
        if (!Comparable.class.isAssignableFrom(columnSource.getType())) {
            return 0;
        }

        RowSet index = table.getRowSet();
        if (index.size() > 10000) {
            Random random = new Random();
            TLongSet set = new TLongHashSet();
            long sampleSize = Math.min(index.size() / 4, 10000L);
            while (sampleSize > 0) {
                final long row = (long) (random.nextDouble() * index.size() - 1);
                if (set.add(row)) {
                    sampleSize--;
                }
            }
            RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            set.forEach(row -> {
                builder.addKey(table.getRowSet().get(row));
                return true;
            });
            index = builder.build();
        }

        boolean isAscending = true;
        boolean isDescending = true;
        boolean first = true;

        Object previous = null;
        for (RowSet.Iterator it = index.iterator(); it.hasNext();) {
            long key = it.nextLong();
            Object current = columnSource.get(key);
            if (current == previous) {
                continue;
            }

            int compareTo = first ? 0 : nullSafeCompare((Comparable) previous, (Comparable) current);
            first = false;

            if (compareTo > 0) {
                isAscending = false;
            } else if (compareTo < 0) {
                isDescending = false;
            }

            if (!isAscending && !isDescending) {
                break;
            }

            previous = current;
        }

        if (isAscending)
            return 1;
        else if (isDescending)
            return -1;
        else
            return 0;
    }
}
