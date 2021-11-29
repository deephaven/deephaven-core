/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.ColumnsSpecHelper;
import io.deephaven.time.DateTime;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableLoggerImpl2;
import io.deephaven.tablelogger.WritableRowContainer;

import java.io.IOException;

public class ProcessMemoryLogLogger extends TableLoggerImpl2<ProcessMemoryLogLogger.ISetter> {
    private static final String TABLE_NAME = "ProcessMemoryLog";

    public ProcessMemoryLogLogger() {
        super(TABLE_NAME);
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    interface ISetter extends WritableRowContainer {
        void log(
                final Row.Flags flags,
                final long intervalStartTime, final long intervalDurationMs,
                final long totalMemory, final long freeMemory,
                final long intervalCollection, final long intervalCollectionTimeMs) throws IOException;
    }

    class DirectSetter extends TableLoggerImpl2.BaseSetter implements ISetter {
        RowSetter<DateTime> IntervalStartTime;
        RowSetter<Long> IntervalDurationNanos;
        RowSetter<Long> TotalMemory;
        RowSetter<Long> FreeMemory;
        RowSetter<Long> IntervalCollections;
        RowSetter<Long> IntervalCollectionTimeNanos;
        RowSetter<Float> IntervalCollectionTimePercent;

        DirectSetter() {
            IntervalStartTime = row.getSetter("IntervalStartTime", DateTime.class);
            IntervalDurationNanos = row.getSetter("IntervalDurationNanos", long.class);
            TotalMemory = row.getSetter("TotalMemory", long.class);
            FreeMemory = row.getSetter("FreeMemory", long.class);
            IntervalCollections = row.getSetter("IntervalCollections", long.class);
            IntervalCollectionTimeNanos = row.getSetter("IntervalCollectionTimeNanos", long.class);
        }

        @Override
        public void log(
                final Row.Flags flags,
                final long intervalStartTime, final long intervalDurationNanos,
                final long totalMemory, final long freeMemory,
                final long intervalCollections, final long intervalCollectionTimeNanos) throws IOException {
            setRowFlags(flags);
            this.IntervalStartTime.set(DateTimeUtils.millisToTime(intervalStartTime));
            this.IntervalDurationNanos.set(intervalDurationNanos);
            this.TotalMemory.set(totalMemory);
            this.FreeMemory.set(freeMemory);
            this.IntervalCollections.set(intervalCollections);
            this.IntervalCollectionTimeNanos.set(intervalCollectionTimeNanos);
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
                .add("IntervalStartTime", DateTime.class)
                .add("IntervalDurationNanos", long.class)
                .add("TotalMemory", long.class)
                .add("FreeMemory", long.class)
                .add("IntervalCollections", long.class)
                .add("IntervalCollectionTimeNanos", long.class);
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getTypes();
    }

    @Override
    protected ProcessMemoryLogLogger.ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new ProcessMemoryLogLogger.DirectSetter();
    }

    public void log(
            final long intervalStartTime,
            final long intervalDurationNanos,
            final long totalMemory,
            final long freeMemory,
            final long intervalCollections,
            final long intervalCollectionTimeNanos) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS,
                intervalStartTime, intervalDurationNanos,
                totalMemory, freeMemory,
                intervalCollections, intervalCollectionTimeNanos);
    }

    public void log(
            final Row.Flags flags,
            final long intervalStartTime, final long intervalDurationNanos,
            final long totalMemory, final long freeMemory,
            final long intervalCollections, final long intervalCollectionTimeNanos) throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ProcessMemoryLogLogger.ISetter setter = setterPool.take();
        try {
            setter.log(flags,
                    intervalStartTime, intervalDurationNanos,
                    totalMemory, freeMemory,
                    intervalCollections, intervalCollectionTimeNanos);
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
