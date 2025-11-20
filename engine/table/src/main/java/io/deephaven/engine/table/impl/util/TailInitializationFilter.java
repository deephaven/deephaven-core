//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.*;
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
 * This is only designed to operate against a source table, which must be add-only. Each contiguous range of row keys is
 * assumed to be a partition. If you filter or otherwise alter the source table before calling TailInitializationFilter,
 * this assumption will be violated and the resulting table will not be filtered as desired.
 * <p>
 * Once initialized, the filter returns all new rows, rows that have already been passed are not removed or modified.
 * <p>
 * The input must be sorted by Timestamp, or the resulting table is undefined. Null timestamps are not permitted.
 * <p>
 * For consistency, the last value of each partition is used to determine the threshold for that partition.
 */
public class TailInitializationFilter {

    /**
     * Get the most recent rows from each partition in source table.
     *
     * @param table the source table to filter
     * @param timestampName the name of the timestamp column
     * @param period interval between the last row in a partition (as converted by
     *        {@link DateTimeUtils#parseDurationNanos(String)})
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
     * @param nanos interval between the last row in a partition, in nanoseconds
     * @return a table with only the most recent values in each partition
     */
    public static Table mostRecent(final Table table, final String timestampName, final long nanos) {
        return QueryPerformanceRecorder.withNugget("TailInitializationFilter(nanos=" + nanos + ")", () -> {
            final QueryTable source = validateInputTable(table);

            final ColumnSource<Instant> timestampSource = source.getColumnSource(timestampName, Instant.class);
            if (timestampSource.allowsReinterpret(long.class)) {
                return mostRecentLong(source, timestampSource.reinterpret(long.class), nanos);
            } else {
                return mostRecentInstant(source, timestampSource, nanos);
            }
        });
    }

    private static Table mostRecentLong(final QueryTable table, final ColumnSource<Long> reinterpret,
            final long nanos) {
        return mostRecentLong(table, reinterpret::getLong, nanos);
    }

    private static Table mostRecentInstant(final QueryTable table, final ColumnSource<Instant> cs, final long nanos) {
        return mostRecentLong(table, (idx) -> {
            Instant instant = cs.get(idx);
            return DateTimeUtils.epochNanos(instant);
        }, nanos);
    }

    private static Table mostRecentLong(final QueryTable table, final LongUnaryOperator getValue, final long nanos) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        // we are going to binary search each partition of this table, because the different partitions have
        // non-contiguous indices, but values within a partition are contiguous indices.
        table.getRowSet().forEachRowKeyRange((s, e) -> {
            final long lastValue = getValue.applyAsLong(e);
            if (lastValue == QueryConstants.NULL_LONG) {
                throw new IllegalArgumentException("Found null timestamp at row key " + e);
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
                if (midValue < threshold) {
                    firstIndex = mid + 1;
                } else {
                    lastIndex = mid;
                }
            }
            if (firstIndex <= e) {
                builder.appendRange(firstIndex, e);
            }
            return true;
        });

        return makeResult(table, builder.build().toTracking());
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
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            source.getRowSet().forEachRowKeyRange((startRow, lastRow) -> {
                final long firstRow = Math.max(startRow, lastRow - rowCount + 1);
                builder.appendRange(firstRow, lastRow);
                return true;
            });
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
