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

public class BarrageSubscriptionPerformanceLogger
        extends TableLoggerImpl2<BarrageSubscriptionPerformanceLogger.ISetter> {

    private static final String TABLE_NAME = "BarrageSubscriptionPerformanceLog";

    public BarrageSubscriptionPerformanceLogger() {
        super(TABLE_NAME);
    }

    @SuppressWarnings("rawtypes")
    interface ISetter extends WritableRowContainer {
        void log(Row.Flags flags, String tableId, String tableKey, String statType, DateTime time,
                long count, double pct50, double pct75, double pct90, double pct95, double pct99, double max)
                throws IOException;
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    @SuppressWarnings("rawtypes")
    class DirectSetter extends BaseSetter implements ISetter {
        RowSetter<String> TableId;
        RowSetter<String> TableKey;
        RowSetter<String> StatType;
        RowSetter<DateTime> Time;
        RowSetter<Long> Count;
        RowSetter<Double> Pct50;
        RowSetter<Double> Pct75;
        RowSetter<Double> Pct90;
        RowSetter<Double> Pct95;
        RowSetter<Double> Pct99;
        RowSetter<Double> Max;

        DirectSetter() {
            TableId = row.getSetter("TableId", String.class);
            TableKey = row.getSetter("TableKey", String.class);
            StatType = row.getSetter("StatType", String.class);
            Time = row.getSetter("Time", DateTime.class);
            Count = row.getSetter("Count", long.class);
            Pct50 = row.getSetter("Pct50", double.class);
            Pct75 = row.getSetter("Pct75", double.class);
            Pct90 = row.getSetter("Pct90", double.class);
            Pct95 = row.getSetter("Pct95", double.class);
            Pct99 = row.getSetter("Pct99", double.class);
            Max = row.getSetter("Max", double.class);
        }

        @Override
        public void log(Row.Flags flags, String tableId, String tableKey, String statType, DateTime time,
                long count, double pct50, double pct75, double pct90, double pct95, double pct99, double max)
                throws IOException {
            setRowFlags(flags);
            this.TableId.set(tableId);
            this.TableKey.set(tableKey);
            this.StatType.set(statType);
            this.Time.set(time);

            this.Count.setLong(count);
            this.Pct50.setDouble(pct50);
            this.Pct75.setDouble(pct75);
            this.Pct90.setDouble(pct90);
            this.Pct95.setDouble(pct95);
            this.Pct99.setDouble(pct99);
            this.Max.setDouble(max);
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
                .add("StatType", String.class)
                .add("Time", DateTime.class)

                .add("Count", long.class)
                .add("Pct50", double.class)
                .add("Pct75", double.class)
                .add("Pct90", double.class)
                .add("Pct95", double.class)
                .add("Pct99", double.class)
                .add("Max", double.class)

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
            String tableId, String tableKey, String statType, DateTime time,
            long count, double pct50, double pct75, double pct90, double pct95, double pct99, double max)
            throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, tableId, tableKey, statType, time,
                count, pct50, pct75, pct90, pct95, pct99, max);
    }

    public void log(
            Row.Flags flags, String tableId, String tableKey, String statType, DateTime time,
            long count, double pct50, double pct75, double pct90, double pct95, double pct99, double max)
            throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, tableId, tableKey, statType, time, count, pct50, pct75, pct90, pct95, pct99, max);
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
