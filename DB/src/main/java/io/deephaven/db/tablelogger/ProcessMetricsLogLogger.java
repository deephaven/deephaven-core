/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tablelogger;

import io.deephaven.tablelogger.*;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ColumnsSpecHelper;

import java.io.IOException;

public class ProcessMetricsLogLogger
        extends TableLoggerImpl2<ProcessMetricsLogLogger.ISetter> {

    private static final String TABLE_NAME = "ProcessMetricsLog";

    public ProcessMetricsLogLogger() {
        super(TABLE_NAME);
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    interface ISetter extends WritableRowContainer {
        void log(Row.Flags flags, long timestamp, String processUniqueId, String name, String interval, String type,
                long n, long sum, long last, long min, long max, long avg, long sum2, long stdev) throws IOException;
    }

    class DirectSetter extends BaseSetter implements ISetter {
        RowSetter<Long> Timestamp;
        RowSetter<String> ProcessUniqueId;
        RowSetter<String> Name;
        RowSetter<String> Interval;
        RowSetter<String> Type;
        RowSetter<Long> N;
        RowSetter<Long> Sum;
        RowSetter<Long> Last;
        RowSetter<Long> Min;
        RowSetter<Long> Max;
        RowSetter<Long> Avg;
        RowSetter<Long> Sum2;
        RowSetter<Long> Stdev;

        DirectSetter() {
            ProcessUniqueId = row.getSetter("ProcessUniqueId", String.class);
            Timestamp = row.getSetter("Timestamp", Long.class);
            Name = row.getSetter("Name", String.class);
            Interval = row.getSetter("Interval", String.class);
            Type = row.getSetter("Type", String.class);
            N = row.getSetter("N", long.class);
            Sum = row.getSetter("Sum", long.class);
            Last = row.getSetter("Last", long.class);
            Min = row.getSetter("Min", long.class);
            Max = row.getSetter("Max", long.class);
            Avg = row.getSetter("Avg", long.class);
            Sum2 = row.getSetter("Sum2", long.class);
            Stdev = row.getSetter("Stdev", long.class);
        }

        @Override
        public void log(final Row.Flags flags, final long timestamp, final String processUniqueId, final String name,
                final String interval, final String type,
                final long n, final long sum, final long last, final long min, final long max, final long avg,
                final long sum2, final long stdev) throws IOException {
            setRowFlags(flags);
            this.ProcessUniqueId.set(processUniqueId);
            this.Timestamp.set(timestamp);
            this.Name.set(name);
            this.Interval.set(interval);
            this.Type.set(type);
            this.N.setLong(n);
            this.Sum.setLong(sum);
            this.Last.setLong(last);
            this.Min.setLong(min);
            this.Max.setLong(max);
            this.Avg.setLong(avg);
            this.Sum2.setLong(sum2);
            this.Stdev.setLong(stdev);
        }
    }

    @Override
    protected String threadName() {
        return TABLE_NAME;
    }

    private static final String[] columnNames;
    private static final Class<?>[] columnDbTypes;

    static {
        final ColumnsSpecHelper cols = new ColumnsSpecHelper()
                .add("ProcessUniqueId", String.class)
                .add("Timestamp", long.class)
                .add("Name", String.class)
                .add("Interval", String.class)
                .add("Type", String.class)
                .add("N", long.class)
                .add("Sum", long.class)
                .add("Last", long.class)
                .add("Min", long.class)
                .add("Max", long.class)
                .add("Avg", long.class)
                .add("Sum2", long.class)
                .add("Stdev", long.class);
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getDbTypes();
    }

    @Override
    protected ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new DirectSetter();
    }

    public void log(final long timestamp, final String processId, final String name, final String interval,
            final String type,
            final long n, final long sum, final long last, final long min, final long max, final long avg,
            final long sum2, final long stdev) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, timestamp, processId, name, interval, type, n, sum, last, min, max, avg,
                sum2, stdev);
    }

    public void log(final Row.Flags flags, final long timestamp, final String processId, final String name,
            final String interval, final String type,
            final long n, final long sum, final long last, final long min, final long max, final long avg,
            final long sum2, final long stdev) throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, timestamp, processId, name, interval, type, n, sum, last, min, max, avg, sum2, stdev);
        } catch (Exception e) {
            setterPool.give(setter);
            throw e;
        }
        flush(setter);
    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.tableDefinition(columnDbTypes, columnNames);

    public static TableDefinition getTableDefinition() {
        return TABLE_DEFINITION;
    }

    public static String[] getColumnNames() {
        return columnNames;
    }
}
