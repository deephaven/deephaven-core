/*
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

public class BarrageStaticPerformanceLogLogger
        extends TableLoggerImpl2<BarrageStaticPerformanceLogLogger.ISetter> {

    private static final String TABLE_NAME = "BarrageStaticPerformanceLog";

    public BarrageStaticPerformanceLogLogger() {
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
        RowSetter<DateTime> Time;
        RowSetter<Double> QueueTmMs;
        RowSetter<Double> SnapshotTmMS;
        RowSetter<Double> WriteTmMs;
        RowSetter<Double> WriteMb;

        DirectSetter() {
            TableId = row.getSetter("TableId", String.class);
            TableKey = row.getSetter("TableKey", String.class);
            Time = row.getSetter("Time", DateTime.class);
            QueueTmMs = row.getSetter("QueueTmMs", double.class);
            SnapshotTmMS = row.getSetter("SnapshotTmMs", double.class);
            WriteTmMs = row.getSetter("WriteTmMs", double.class);
            WriteMb = row.getSetter("WriteMb", double.class);
        }

        @Override
        public void log(Row.Flags flags, String tableId, String tableKey, DateTime time,
                long queueTm, long snapshotTm, long writeTm, long bytesWritten)
                throws IOException {
            setRowFlags(flags);
            this.TableId.set(tableId);
            this.TableKey.set(tableKey);
            this.Time.set(time);

            this.QueueTmMs.setDouble(queueTm / 1e6);
            this.SnapshotTmMS.setDouble(snapshotTm / 1e6);
            this.WriteTmMs.setDouble(writeTm / 1e6);
            this.WriteMb.setDouble((8 * bytesWritten) / 1e6);
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
                .add("Time", DateTime.class)

                .add("QueueTmMs", double.class)
                .add("SnapshotTmMs", double.class)
                .add("WriteTmMs", double.class)
                .add("WriteMb", double.class)

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
            String tableId, String tableKey, DateTime time,
            long queueTm, long snapshotTm, long writeTm, long bytesWritten)
            throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, tableId, tableKey, time,
                queueTm, snapshotTm, writeTm, bytesWritten);
    }

    public void log(
            Row.Flags flags, String tableId, String tableKey, DateTime time,
            long queueTm, long snapshotTm, long writeTm, long bytesWritten)
            throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, tableId, tableKey, time, queueTm, snapshotTm, writeTm, bytesWritten);
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

    public static Class<?>[] getColumnDbTypes() {
        return columnDbTypes;
    }

    public static String[] getColumnNames() {
        return columnNames;
    }
}
