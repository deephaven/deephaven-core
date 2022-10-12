/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;

import java.util.function.LongUnaryOperator;

/**
 * For an Intraday restart, we often know that all data of interest must take place within a fixed period of time.
 * Rather than processing all of the data, we can binary search in each partition to find the relevant rows based on a
 * Timestamp.
 *
 * This is only designed to operate against a source table, if any rows are modified or removed from the table, then the
 * ShiftObliviousListener throws an IllegalStateException. Each contiguous range of indices is assumed to be a
 * partition. If you filter or otherwise alter the source table before calling TailInitializationFilter, this assumption
 * will be violated and the resulting table will not be filtered as desired.
 *
 * Once initialized, the filter returns all new rows, rows that have already been passed are not removed or modified.
 *
 * The input must be sorted by Timestamp, or the resulting table is undefined. Null timestamps are not permitted.
 *
 * For consistency, the last value of each partition is used to determine the threshold for that partition.
 */
public class TailInitializationFilter {
    /**
     * Get the most recent rows from each partition in source table.
     *
     * @param table the source table to filter
     * @param timestampName the name of the timestamp column
     * @param period interval between the last row in a partition (as converted by DateTimeUtils.expressionToNanos)
     * @return a table with only the most recent values in each partition
     */
    public static Table mostRecent(final Table table, final String timestampName, final String period) {
        return mostRecent(table, timestampName, DateTimeUtils.expressionToNanos(period));
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
        return QueryPerformanceRecorder.withNugget("TailInitializationFilter(" + nanos + ")", () -> {
            final ColumnSource timestampSource = table.getColumnSource(timestampName, DateTime.class);
            if (timestampSource.allowsReinterpret(long.class)) {
                // noinspection unchecked
                return mostRecentLong(table, timestampSource.reinterpret(long.class), nanos);
            } else {
                // noinspection unchecked
                return mostRecentDateTime(table, timestampSource, nanos);
            }
        });
    }

    private static Table mostRecentLong(final Table table, final ColumnSource<Long> reinterpret, final long nanos) {
        return mostRecentLong(table, reinterpret::getLong, nanos);
    }

    private static Table mostRecentDateTime(final Table table, final ColumnSource<DateTime> cs, final long nanos) {
        return mostRecentLong(table, (idx) -> cs.get(idx).getNanos(), nanos);
    }

    private static Table mostRecentLong(final Table table, final LongUnaryOperator getValue, final long nanos) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        // we are going to binary search each partition of this table, because the different partitions have
        // non-contiguous indices, but values within a partition are contiguous indices.
        table.getRowSet().forEachRowKeyRange((s, e) -> {
            final long lastValue = getValue.applyAsLong(e);
            if (lastValue == QueryConstants.NULL_LONG) {
                throw new IllegalArgumentException("Found null timestamp at rowSet " + e);
            }
            final long threshold = lastValue - nanos;
            long firstIndex = s;
            long lastIndex = e + 1;
            while (firstIndex < lastIndex) {
                final long mid = (firstIndex + lastIndex) / 2;
                final long midValue = getValue.applyAsLong(mid);
                if (midValue == QueryConstants.NULL_LONG) {
                    throw new IllegalArgumentException("Found null timestamp at rowSet " + mid);
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
        final TrackingWritableRowSet resultRowSet = builder.build().toTracking();
        final QueryTable result = new QueryTable(table.getDefinition(), resultRowSet, table.getColumnSourceMap());
        if (table.isRefreshing()) {
            // TODO: Assert AddOnly in T+, propagate AddOnly in Treasure
            final ShiftObliviousInstrumentedListener listener =
                    new BaseTable.ShiftObliviousListenerImpl("TailInitializationFilter", table, result) {
                        @Override
                        public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                            Assert.assertion(removed.isEmpty(), "removed.empty()");
                            Assert.assertion(modified.isEmpty(), "modified.empty()");
                            resultRowSet.insert(added);
                            result.notifyListeners(added.copy(), removed.copy(), modified.copy());
                        }
                    };
            table.addUpdateListener(listener, false);
        }
        return result;
    }
}
