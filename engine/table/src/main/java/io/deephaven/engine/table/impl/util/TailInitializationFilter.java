//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Collections;
import java.util.function.LongUnaryOperator;

/**
 * When a query processing a persistent append-only table is restarted mid-day, we often know that all data of interest
 * must take place within a fixed period of time. Rather than processing all the data, we can binary search in each
 * partition to find the relevant rows based on a Timestamp.
 * <p>
 * This is only designed to operate against a source table, which must be add-only. If the Timestamp source is a
 * {@link RegionedColumnSource}, then each region is assumed to be a partition. Otherwise, each contiguous range of row
 * keys is assumed to be a partition.
 * <p>
 * If you alter the source table before calling TailInitializationFilter, these assumptions may be violated and the
 * resulting table will not be filtered as desired.
 * <p>
 * Once initialized, the filter returns all new rows, rows that have already been passed are not removed or modified.
 * <p>
 * <b>Each partition in the input must be sorted by Timestamp, or the resulting table is undefined</b>. Null timestamps
 * are not permitted.
 * <p>
 * For consistency, the last value of each partition is used to determine the threshold for that partition. As the
 * partitions must be sorted, the last row in each partition must be the most recent timestamp within that partition.
 * The provided period is subtracted from the timestamp; and a binary search is performed to identify rows that are
 * within the window defined by period.
 */
public class TailInitializationFilter {

    /**
     * Get the most recent rows from each partition in source table.
     *
     * @param table the source table to filter
     * @param timestampName the name of the timestamp column
     * @param period interval between the last row in a partition (as converted by
     *        {@link DateTimeUtils#parseDurationNanos(String)}) and rows that match the filter
     * @return a table with only the most recent values in each partition
     */
    public static Table mostRecent(final Table table, final String timestampName, final String period) {
        return mostRecent(table, timestampName, DateTimeUtils.parseDurationNanos(period));
    }

    /**
     * Get the most recent rows from each partition in source table.
     *
     * @param table the source table to filter
     * @param timestampName the name of the timestamp column
     * @param nanos interval between the last row in a partition, in nanoseconds and rows that match the filter
     * @return a table with only the most recent values in each partition
     */
    public static Table mostRecent(final Table table, final String timestampName, final long nanos) {
        return QueryPerformanceRecorder.withNugget("TailInitializationFilter(nanos=" + nanos + ")", () -> {
            final QueryTable source = validateInputTable(table);

            final ColumnSource<Instant> timestampSource = source.getColumnSource(timestampName, Instant.class);
            final boolean isRegioned = timestampSource instanceof RegionedColumnSource;
            if (timestampSource.allowsReinterpret(long.class)) {
                return mostRecentLong(source, timestampSource.reinterpret(long.class), nanos, isRegioned);
            } else {
                return mostRecentInstant(source, timestampSource, nanos, isRegioned);
            }
        });
    }

    private static Table mostRecentLong(final QueryTable table, final ColumnSource<Long> reinterpret,
            final long nanos, final boolean isRegioned) {
        return mostRecentLong(table, reinterpret::getLong, nanos, isRegioned);
    }

    private static Table mostRecentInstant(final QueryTable table, final ColumnSource<Instant> cs, final long nanos,
            boolean isRegioned) {
        return mostRecentLong(table, (idx) -> {
            Instant instant = cs.get(idx);
            return DateTimeUtils.epochNanos(instant);
        }, nanos, isRegioned);
    }

    private static Table mostRecentLong(final QueryTable table, final LongUnaryOperator getValue, final long nanos,
            boolean isRegioned) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        if (isRegioned) {
            doByRegions(table, getValue, nanos, builder);
        } else {
            doByIndexRanges(table, getValue, nanos, builder);
        }

        return makeResult(table, builder.build().toTracking());
    }

    private static void doByRegions(QueryTable table, LongUnaryOperator getValue, long nanos,
            RowSetBuilderSequential builder) {
        try (final RowSequence.Iterator it = table.getRowSet().getRowSequenceIterator()) {
            while (it.hasMore()) {
                final long nextRowKey = it.peekNextKey();
                final long maxKey = nextRowKey | RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK;
                try (final RowSet forPartition = it.getNextRowSequenceThrough(maxKey).asRowSet()) {
                    final long firstValue = getValue.applyAsLong(forPartition.firstRowKey());
                    if (firstValue == QueryConstants.NULL_LONG) {
                        throw new IllegalArgumentException(
                                "Found null timestamp at row key " + forPartition.firstRowKey());
                    }

                    final long lastValue = getValue.applyAsLong(forPartition.lastRowKey());
                    if (lastValue == QueryConstants.NULL_LONG) {
                        throw new IllegalArgumentException(
                                "Found null timestamp at row key " + forPartition.lastRowKey());
                    }

                    long minValue = firstValue;
                    long minPosition = 0;
                    long maxValue = lastValue;
                    long maxPosition = forPartition.size() - 1;
                    if (minValue > maxValue) {
                        throw new IllegalArgumentException("Found inconsistently sorted rows, " + minValue
                                + " at row key " + forPartition.firstRowKey() + " is greater than " + maxValue
                                + " at row key " + forPartition.lastRowKey());
                    }

                    final long threshold = lastValue - nanos;
                    long firstPosition = 0;
                    long lastPosition = forPartition.size();
                    while (firstPosition < lastPosition) {
                        final long mid = (firstPosition + lastPosition) / 2;
                        final long midKey = forPartition.get(mid);
                        final long midValue = getValue.applyAsLong(midKey);
                        if (midValue == QueryConstants.NULL_LONG) {
                            throw new IllegalArgumentException("Found null timestamp at row key " + midKey);
                        }
                        if (midValue > maxValue) {
                            throw new IllegalArgumentException("Found inconsistently sorted rows, " + midValue
                                    + " at row key " + midKey + " is greater than " + maxValue + " at row key "
                                    + forPartition.get(maxPosition));
                        }
                        if (midValue < minValue) {
                            throw new IllegalArgumentException("Found inconsistently sorted rows, " + midValue
                                    + " at row key " + midKey + " is less than " + minValue + " at row key "
                                    + forPartition.get(minPosition));
                        }
                        if (midValue < threshold) {
                            firstPosition = mid + 1;
                            minValue = midValue;
                            minPosition = mid;
                        } else {
                            lastPosition = mid;
                            maxValue = midValue;
                            maxPosition = mid;
                        }
                    }
                    if (firstPosition <= forPartition.size() - 1) {
                        try (WritableRowSet forPartitionTail =
                                forPartition.subSetByPositionRange(firstPosition, forPartition.size())) {
                            builder.appendRowSequence(forPartitionTail);
                        }
                    }
                }
            }
        }
    }

    private static void doByIndexRanges(QueryTable table, LongUnaryOperator getValue, long nanos,
            RowSetBuilderSequential builder) {
        // we are going to binary search each partition of this table, because the different partitions have
        // non-contiguous indices, but values within a partition are contiguous indices.
        table.getRowSet().forEachRowKeyRange((s, e) -> {
            final long firstValue = getValue.applyAsLong(s);
            if (firstValue == QueryConstants.NULL_LONG) {
                throw new IllegalArgumentException("Found null timestamp at row key " + s);
            }
            final long lastValue = getValue.applyAsLong(e);
            if (lastValue == QueryConstants.NULL_LONG) {
                throw new IllegalArgumentException("Found null timestamp at row key " + e);
            }

            long minValue = firstValue;
            long minRowKey = s;
            long maxRowKey = e;
            long maxValue = lastValue;
            if (minValue > maxValue) {
                throw new IllegalArgumentException("Found inconsistently sorted rows, " + firstValue + " at row key "
                        + s + " is greater than " + lastValue + " at row key " + e);
            }
            final long threshold = lastValue - nanos;
            long firstIndex = s;
            long lastIndex = e + 1;
            while (firstIndex < lastIndex) {
                final long mid = (firstIndex + lastIndex) / 2;
                final long midValue = getValue.applyAsLong(mid);
                if (midValue == QueryConstants.NULL_LONG) {
                    throw new IllegalArgumentException("Found null timestamp at row key " + mid);
                }
                if (midValue > maxValue) {
                    throw new IllegalArgumentException("Found inconsistently sorted rows, " + midValue + " at row key "
                            + mid + " is greater than " + maxValue + " at row key " + maxRowKey);
                }
                if (midValue < minValue) {
                    throw new IllegalArgumentException("Found inconsistently sorted rows, " + midValue + " at row key "
                            + mid + " is less than " + minValue + " at row key " + minRowKey);
                }
                if (midValue < threshold) {
                    firstIndex = mid + 1;
                    minRowKey = mid;
                    minValue = midValue;
                } else {
                    lastIndex = mid;
                    maxValue = midValue;
                    maxRowKey = mid;
                }
            }
            if (firstIndex <= e) {
                builder.appendRange(firstIndex, e);
            }
            return true;
        });
    }

    /**
     * Get the most recent rows from each partition in source table.
     *
     * @param table the source table to filter
     * @param rowCount the number of rows to include per partition
     * @return a table with only the most recent values in each partition
     */
    public static Table mostRecentRows(final Table table, final long rowCount) {
        return QueryPerformanceRecorder.withNugget("TailInitializationFilter(rows=" + rowCount + ")", () -> {
            final QueryTable source = validateInputTable(table);

            final boolean isRegioned =
                    source.getColumnSourceMap().values().stream().anyMatch(RegionedColumnSource.class::isInstance);

            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            if (isRegioned) {
                try (final RowSequence.Iterator it = source.getRowSet().getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        final long nextRowKey = it.peekNextKey();
                        final long maxKey = nextRowKey | RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK;
                        RowSequence forPartition = it.getNextRowSequenceThrough(maxKey);
                        final long firstRow = Math.max(0, forPartition.size() - rowCount);
                        final long effectiveRowCount = Math.min(rowCount, forPartition.size());
                        try (RowSequence tailForPartition =
                                forPartition.getRowSequenceByPosition(firstRow, effectiveRowCount)) {
                            builder.appendRowSequence(tailForPartition);
                        }
                    }
                }
            } else {
                source.getRowSet().forEachRowKeyRange((startRow, lastRow) -> {
                    final long firstRow = Math.max(startRow, lastRow - rowCount + 1);
                    builder.appendRange(firstRow, lastRow);
                    return true;
                });
            }
            return makeResult(source, builder.build().toTracking());
        });
    }

    private static @NotNull QueryTable validateInputTable(Table table) {
        final QueryTable source = (QueryTable) table.coalesce();
        if (!source.isAddOnly()) {
            throw new IllegalArgumentException("TailInitializationFilter requires an add-only table as input.");
        }
        source.getUpdateGraph().checkInitiateSerialTableOperation();
        return source;
    }

    private static QueryTable makeResult(QueryTable source, TrackingWritableRowSet resultRowSet) {
        final QueryTable result = source.getSubTable(resultRowSet, null,
                Collections.singletonMap(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE));
        if (source.isRefreshing()) {
            final TableUpdateListener listener =
                    new BaseTable.ListenerImpl("TailInitializationFilter", source, result) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {
                            // we've previously asserted we are add-only, these checks are just belt and suspenders to
                            // ensure
                            // we are not dropping stuff on the floor
                            Assert.assertion(upstream.removed().isEmpty(), "upstream.removed().isEmpty()");
                            Assert.assertion(upstream.shifted().empty(), "upstream.shifted().empty()");
                            resultRowSet.insert(upstream.added());
                            super.onUpdate(upstream);
                        }
                    };
            source.addUpdateListener(listener);
        }
        return result;
    }
}
