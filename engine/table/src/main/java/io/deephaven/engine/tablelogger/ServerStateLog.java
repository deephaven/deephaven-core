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

public class ServerStateLog extends TableLoggerImpl2<ServerStateLog.ISetter> {
    private static final String TABLE_NAME = "ServerStateLog";

    public ServerStateLog() {
        super(TABLE_NAME);
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    interface ISetter extends WritableRowContainer {
        void log(
                Row.Flags flags,
                long intervalStartTime,
                long intervalDurationNanos,
                long totalMemory,
                long freeMemory,
                long intervalCollection,
                long intervalCollectionTimeNanos,
                long intervalUGPCyclesFinished,
                long intervalUGPCyclesFinishedTimeNanos,
                long intervalUGPCyclesFinishedSafePointTimeNanos
        ) throws IOException;
    }

    class DirectSetter extends TableLoggerImpl2.BaseSetter implements ISetter {
        RowSetter<DateTime> IntervalStartTime;
        RowSetter<Long> IntervalDurationNanos;
        RowSetter<Long> TotalMemory;
        RowSetter<Long> FreeMemory;
        RowSetter<Long> IntervalCollections;
        RowSetter<Long> IntervalCollectionTimeNanos;
        RowSetter<Long> IntervalUGPCyclesFinished;
        RowSetter<Long> IntervalUGPCyclesFinishedTimeNanos;
        RowSetter<Long> IntervalUGPCyclesFinishedSafePointTimeNanos;

        DirectSetter() {
            IntervalStartTime = row.getSetter("IntervalStartTime", DateTime.class);
            IntervalDurationNanos = row.getSetter("IntervalDurationNanos", long.class);
            TotalMemory = row.getSetter("TotalMemory", long.class);
            FreeMemory = row.getSetter("FreeMemory", long.class);
            IntervalCollections = row.getSetter("IntervalCollections", long.class);
            IntervalCollectionTimeNanos = row.getSetter("IntervalCollectionTimeNanos", long.class);
            IntervalUGPCyclesFinished = row.getSetter("IntervalUGPCyclesFinished", long.class);
            IntervalUGPCyclesFinishedTimeNanos = row.getSetter("IntervalUGPCyclesFinishedTimeNanos", long.class);
            IntervalUGPCyclesFinishedSafePointTimeNanos = row.getSetter("IntervalUGPCyclesFinishedSafePointTimeNanos", long.class);
        }

        @Override
        public void log(
                final Row.Flags flags,
                final long intervalStartTime,
                final long intervalDurationNanos,
                final long totalMemory,
                final long freeMemory,
                final long intervalCollections,
                final long intervalCollectionTimeNanos,
                final long intervalUGPCyclesFinished,
                final long intervalUGPCyclesFinishedTimeNanos,
                final long intervalUGPCyclesFinishedSafePointTimeNanos) throws IOException {
            setRowFlags(flags);
            this.IntervalStartTime.set(DateTimeUtils.millisToTime(intervalStartTime));
            this.IntervalDurationNanos.set(intervalDurationNanos);
            this.TotalMemory.set(totalMemory);
            this.FreeMemory.set(freeMemory);
            this.IntervalCollections.set(intervalCollections);
            this.IntervalCollectionTimeNanos.set(intervalCollectionTimeNanos);
            this.IntervalUGPCyclesFinished.set(intervalUGPCyclesFinished);
            this.IntervalUGPCyclesFinishedTimeNanos.set(intervalUGPCyclesFinishedTimeNanos);
            this.IntervalUGPCyclesFinishedSafePointTimeNanos.set(intervalUGPCyclesFinishedSafePointTimeNanos);
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
                .add("IntervalCollectionTimeNanos", long.class)
                .add("IntervalUGPCyclesFinished", long.class)
                .add("IntervalUGPCyclesFinishedTimeNanos", long.class)
                .add("IntervalUGPCyclesFinishedSafePointTimeNanos", long.class)
                ;
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getTypes();
    }

    @Override
    protected ServerStateLog.ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new ServerStateLog.DirectSetter();
    }

    public void log(
            final long intervalStartTime,
            final long intervalDurationNanos,
            final long totalMemory,
            final long freeMemory,
            final long intervalCollections,
            final long intervalCollectionTimeNanos,
            final long intervalUGPCycles,
            final long intervalUGPCyclesFinishedTimeNanos,
            final long intervalUGPCyclesFinishedSafePointTimeNanos
            ) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS,
                intervalStartTime,
                intervalDurationNanos,
                totalMemory,
                freeMemory,
                intervalCollections,
                intervalCollectionTimeNanos,
                intervalUGPCycles,
                intervalUGPCyclesFinishedTimeNanos,
                intervalUGPCyclesFinishedSafePointTimeNanos);
    }

    public void log(
            final Row.Flags flags,
            final long intervalStartTime,
            final long intervalDurationNanos,
            final long totalMemory,
            final long freeMemory,
            final long intervalCollections,
            final long intervalCollectionTimeNanos,
            final long intervalUGPCycles,
            final long intervalUGPCyclesFinishedTimeNanos,
            final long intervalUGPCyclesFinishedSafePointTimeNanos
    ) throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ServerStateLog.ISetter setter = setterPool.take();
        try {
            setter.log(flags,
                    intervalStartTime,
                    intervalDurationNanos,
                    totalMemory,
                    freeMemory,
                    intervalCollections,
                    intervalCollectionTimeNanos,
                    intervalUGPCycles,
                    intervalUGPCyclesFinishedTimeNanos,
                    intervalUGPCyclesFinishedSafePointTimeNanos);
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
