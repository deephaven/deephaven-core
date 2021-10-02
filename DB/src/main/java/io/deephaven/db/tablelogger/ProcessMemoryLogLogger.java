/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tablelogger;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ColumnsSpecHelper;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
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
        RowSetter<DBDateTime> IntervalStartTime;
        RowSetter<Long> IntervalDurationMs;
        RowSetter<Long> TotalMemory;
        RowSetter<Long> FreeMemory;
        RowSetter<Long> IntervalCollections;
        RowSetter<Long> IntervalCollectionTimeMs;

        DirectSetter() {
            IntervalStartTime = row.getSetter("IntervalStartTime", DBDateTime.class);
            IntervalDurationMs = row.getSetter("IntervalDurationMs", long.class);
            TotalMemory = row.getSetter("TotalMemory", long.class);
            FreeMemory = row.getSetter("FreeMemory", long.class);
            IntervalCollections = row.getSetter("IntervalCollections", long.class);
            IntervalCollectionTimeMs = row.getSetter("IntervalCollectionTimeMs", long.class);
        }

        @Override
        public void log(
                final Row.Flags flags,
                final long intervalStartTime, final long intervalDurationMs,
                final long totalMemory, final long freeMemory,
                final long intervalCollections, final long intervalCollectionTimeMs) throws IOException {
            setRowFlags(flags);
            this.IntervalStartTime.set(DBTimeUtils.millisToTime(intervalStartTime));
            this.IntervalDurationMs.set(intervalDurationMs);
            this.TotalMemory.set(totalMemory);
            this.FreeMemory.set(freeMemory);
            this.IntervalCollections.set(intervalCollections);
            this.IntervalCollectionTimeMs.set(intervalCollectionTimeMs);
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
                .add("IntervalStartTime", DBDateTime.class)
                .add("IntervalDurationMs", long.class)
                .add("TotalMemory", long.class)
                .add("FreeMemory", long.class)
                .add("IntervalCollections", long.class)
                .add("IntervalCollectionTimeMs", long.class);
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getDbTypes();
    }

    @Override
    protected ProcessMemoryLogLogger.ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new ProcessMemoryLogLogger.DirectSetter();
    }

    public void log(
            final long intervalStartTime,
            final long intervalDurationMs,
            final long totalMemory,
            final long freeMemory,
            final long intervalCollections,
            final long intervalCollectionTimeMs) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS,
                intervalStartTime, intervalDurationMs,
                totalMemory, freeMemory,
                intervalCollections, intervalCollectionTimeMs);
    }

    public void log(
            final Row.Flags flags,
            final long intervalStartTime, final long intervalDurationMs,
            final long totalMemory, final long freeMemory,
            final long intervalCollections, final long intervalCollectionTimeMs) throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ProcessMemoryLogLogger.ISetter setter = setterPool.take();
        try {
            setter.log(flags,
                    intervalStartTime, intervalDurationMs,
                    totalMemory, freeMemory,
                    intervalCollections, intervalCollectionTimeMs);
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
