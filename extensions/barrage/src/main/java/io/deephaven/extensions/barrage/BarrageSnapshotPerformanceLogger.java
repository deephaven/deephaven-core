/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.ColumnsSpecHelper;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableLoggerImpl2;
import io.deephaven.tablelogger.WritableRowContainer;
import io.deephaven.time.DateTime;

import java.io.IOException;

public class BarrageSnapshotPerformanceLogger
        extends TableLoggerImpl2<BarrageSnapshotPerformanceLogger.ISetter> {

    private static final String TABLE_NAME = "BarrageSnapshotPerformanceLog";

    public BarrageSnapshotPerformanceLogger() {
        super(TABLE_NAME);
    }

    @SuppressWarnings("rawtypes")
    interface ISetter extends WritableRowContainer {
        void log(Row.Flags flags, String tableId, String tableKey, DateTime time,
                long queueTm, long snapshotTm, long writeTm, long bytesWritten)
                throws IOException;
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    @SuppressWarnings("rawtypes")
    class DirectSetter extends BaseSetter implements ISetter {
        RowSetter<String> TableId;
        RowSetter<String> TableKey;
        RowSetter<DateTime> RequestTime;
        RowSetter<Double> QueueMillis;
        RowSetter<Double> SnapshotMillis;
        RowSetter<Double> WriteMillis;
        RowSetter<Double> WriteMegabits;

        DirectSetter() {
            TableId = row.getSetter("TableId", String.class);
            TableKey = row.getSetter("TableKey", String.class);
            RequestTime = row.getSetter("RequestTime", DateTime.class);
            QueueMillis = row.getSetter("QueueMillis", double.class);
            SnapshotMillis = row.getSetter("SnapshotMillis", double.class);
            WriteMillis = row.getSetter("WriteMillis", double.class);
            WriteMegabits = row.getSetter("WriteMegabits", double.class);
        }

        @Override
        public void log(Row.Flags flags, String tableId, String tableKey, DateTime requestTime,
                long queueNanos, long snapshotNanos, long writeNanons, long bytesWritten)
                throws IOException {
            setRowFlags(flags);
            this.TableId.set(tableId);
            this.TableKey.set(tableKey);
            this.RequestTime.set(requestTime);

            this.QueueMillis.setDouble(queueNanos / 1e6);
            this.SnapshotMillis.setDouble(snapshotNanos / 1e6);
            this.WriteMillis.setDouble(writeNanons / 1e6);
            this.WriteMegabits.setDouble((8 * bytesWritten) / 1e6);
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
                .add("TableId", String.class)
                .add("TableKey", String.class)
                .add("RequestTime", DateTime.class)

                .add("QueueMillis", double.class)
                .add("SnapshotMillis", double.class)
                .add("WriteMillis", double.class)
                .add("WriteMegabits", double.class)

        ;

        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getTypes();
    }

    @Override
    protected ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new DirectSetter();
    }

    public void log(
            String tableId, String tableKey, DateTime requestTime,
            long queueNanos, long snapshotNanos, long writeNanos, long bytesWritten)
            throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, tableId, tableKey, requestTime,
                queueNanos, snapshotNanos, writeNanos, bytesWritten);
    }

    public void log(
            Row.Flags flags, String tableId, String tableKey, DateTime requestTime,
            long queueNanos, long snapshotNanos, long writeNanos, long bytesWritten)
            throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, tableId, tableKey, requestTime, queueNanos, snapshotNanos, writeNanos, bytesWritten);
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

    public static Class<?>[] getColumnDbTypes() {
        return columnDbTypes;
    }

    public static String[] getColumnNames() {
        return columnNames;
    }
}
