/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
                int intervalDurationMicros,
                int totalMemoryMiB,
                int freeMemoryMiB,
                short intervalCollections,
                int intervalCollectionTimeMicros,
                short intervalUGPCyclesOnBudget,
                int[] intervalUGPCyclesTimeMicros,
                short intervalUGPCyclesSafePoints,
                int intervalUGPCyclesSafePointTimeMicros) throws IOException;
    }

    class DirectSetter extends TableLoggerImpl2.BaseSetter implements ISetter {
        RowSetter<DateTime> IntervalStartTime;
        RowSetter<Integer> IntervalDurationMicros;
        RowSetter<Integer> TotalMemoryMiB;
        RowSetter<Integer> FreeMemoryMiB;
        RowSetter<Short> IntervalCollections;
        RowSetter<Integer> IntervalCollectionTimeMicros;
        RowSetter<int[]> IntervalUGPCyclesTimeMicros;
        RowSetter<Short> IntervalUGPCyclesOnBudget;
        RowSetter<Short> IntervalUGPCyclesSafePoints;
        RowSetter<Integer> IntervalUGPCyclesSafePointTimeMicros;

        DirectSetter() {
            IntervalStartTime = row.getSetter("IntervalStartTime", DateTime.class);
            IntervalDurationMicros = row.getSetter("IntervalDurationMicros", int.class);
            TotalMemoryMiB = row.getSetter("TotalMemoryMiB", int.class);
            FreeMemoryMiB = row.getSetter("FreeMemoryMiB", int.class);
            IntervalCollections = row.getSetter("IntervalCollections", short.class);
            IntervalCollectionTimeMicros = row.getSetter("IntervalCollectionTimeMicros", int.class);
            IntervalUGPCyclesOnBudget = row.getSetter("IntervalUGPCyclesOnBudget", short.class);
            IntervalUGPCyclesTimeMicros = row.getSetter("IntervalUGPCyclesTimeMicros", int[].class);
            IntervalUGPCyclesSafePoints = row.getSetter("IntervalUGPCyclesSafePoints", short.class);
            IntervalUGPCyclesSafePointTimeMicros =
                    row.getSetter("IntervalUGPCyclesSafePointTimeMicros", int.class);
        }

        @Override
        public void log(
                final Row.Flags flags,
                final long intervalStartTime,
                final int intervalDurationMicros,
                final int totalMemoryMiB,
                final int freeMemoryMiB,
                final short intervalCollections,
                final int intervalCollectionTimeMicros,
                final short intervalUGPCyclesOnBudget,
                final int[] intervalUGPCyclesTimeMicros,
                final short intervalUGPCyclesSafePoints,
                final int intervalUGPCyclesSafePointTimeMicros) throws IOException {
            setRowFlags(flags);
            this.IntervalStartTime.set(DateTimeUtils.millisToTime(intervalStartTime));
            this.IntervalDurationMicros.set(intervalDurationMicros);
            this.TotalMemoryMiB.set(totalMemoryMiB);
            this.FreeMemoryMiB.set(freeMemoryMiB);
            this.IntervalCollections.set(intervalCollections);
            this.IntervalCollectionTimeMicros.set(intervalCollectionTimeMicros);
            this.IntervalUGPCyclesOnBudget.set(intervalUGPCyclesOnBudget);
            this.IntervalUGPCyclesTimeMicros.set(intervalUGPCyclesTimeMicros);
            this.IntervalUGPCyclesSafePoints.set(intervalUGPCyclesSafePoints);
            this.IntervalUGPCyclesSafePointTimeMicros.set(intervalUGPCyclesSafePointTimeMicros);
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
                .add("IntervalDurationMicros", int.class)
                .add("TotalMemoryMiB", int.class)
                .add("FreeMemoryMiB", int.class)
                .add("IntervalCollections", short.class)
                .add("IntervalCollectionTimeMicros", int.class)
                .add("IntervalUGPCyclesOnBudget", short.class)
                .add("IntervalUGPCyclesTimeMicros", int[].class)
                .add("IntervalUGPCyclesSafePoints", short.class)
                .add("IntervalUGPCyclesSafePointTimeMicros", int.class);
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
            final int intervalDurationMicros,
            final int totalMemoryMiB,
            final int freeMemoryMiB,
            final short intervalCollections,
            final int intervalCollectionTimeMicros,
            final short intervalUGPCyclesOnBudget,
            final int[] intervalUGPCyclesTimeMicros,
            final short intervalUGPCyclesSafePoints,
            final int intervalUGPCyclesSafePointTimeMicros) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS,
                intervalStartTime,
                intervalDurationMicros,
                totalMemoryMiB,
                freeMemoryMiB,
                intervalCollections,
                intervalCollectionTimeMicros,
                intervalUGPCyclesOnBudget,
                intervalUGPCyclesTimeMicros,
                intervalUGPCyclesSafePoints,
                intervalUGPCyclesSafePointTimeMicros);
    }

    public void log(
            final Row.Flags flags,
            final long intervalStartTime,
            final int intervalDurationMicros,
            final int totalMemoryMiB,
            final int freeMemoryMiB,
            final short intervalCollections,
            final int intervalCollectionTimeMicros,
            final short intervalUGPCyclesOnBudget,
            final int[] intervalUGPCyclesTimeMicros,
            final short intervalUGPCyclesSafePoints,
            final int intervalUGPCyclesSafePointTimeMicros) throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ServerStateLog.ISetter setter = setterPool.take();
        try {
            setter.log(flags,
                    intervalStartTime,
                    intervalDurationMicros,
                    totalMemoryMiB,
                    freeMemoryMiB,
                    intervalCollections,
                    intervalCollectionTimeMicros,
                    intervalUGPCyclesOnBudget,
                    intervalUGPCyclesTimeMicros,
                    intervalUGPCyclesSafePoints,
                    intervalUGPCyclesSafePointTimeMicros);
        } catch (Exception e) {
            setterPool.give(setter);
            throw e;
        }
        flush(setter);
    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.from(columnNames, columnDbTypes);

    public static TableDefinition getTableDefinition() {
        return TABLE_DEFINITION;
    }

    public static String[] getColumnNames() {
        return columnNames;
    }
}
