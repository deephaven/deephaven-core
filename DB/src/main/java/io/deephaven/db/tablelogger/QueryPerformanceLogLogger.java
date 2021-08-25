/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tablelogger;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.tablelogger.*;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.remotequery.QueryProcessingResults;
import io.deephaven.db.tables.utils.ColumnsSpecHelper;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.QueryPerformanceNugget;
import io.deephaven.util.QueryConstants;
import java.io.IOException;

public class QueryPerformanceLogLogger
        extends TableLoggerImpl2<QueryPerformanceLogLogger.ISetter> {

    private static final String TABLE_NAME = "QueryPerformanceLog";

    private final String processUniqueId;

    public QueryPerformanceLogLogger(final String processUniqueId) {
        super(TABLE_NAME);
        this.processUniqueId = processUniqueId;
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    interface ISetter extends WritableRowContainer {
        void log(Row.Flags flags, final long evaluationNumber,
                QueryProcessingResults queryProcessingResults, QueryPerformanceNugget nugget) throws IOException;
    }

    class DirectSetter extends BaseSetter implements ISetter {
        RowSetter<String> ProcessUniqueId;
        RowSetter<Long> EvaluationNumber;
        RowSetter<DBDateTime> StartTime;
        RowSetter<DBDateTime> EndTime;
        RowSetter<Long> DurationNanos;
        RowSetter<Long> CpuNanos;
        RowSetter<Long> UserCpuNanos;
        RowSetter<Long> TotalMemoryFree;
        RowSetter<Long> TotalMemoryUsed;
        RowSetter<Long> FreeMemoryChange;
        RowSetter<Long> TotalMemoryChange;
        RowSetter<Long> AllocatedBytes;
        RowSetter<Long> PoolAllocatedBytes;
        RowSetter<Boolean> WasInterrupted;
        RowSetter<Boolean> IsReplayer;
        RowSetter<String> Exception;

        DirectSetter() {
            ProcessUniqueId = row.getSetter("ProcessUniqueId", String.class);
            EvaluationNumber = row.getSetter("EvaluationNumber", long.class);
            StartTime = row.getSetter("StartTime", DBDateTime.class);
            EndTime = row.getSetter("EndTime", DBDateTime.class);
            DurationNanos = row.getSetter("DurationNanos", long.class);
            CpuNanos = row.getSetter("CpuNanos", long.class);
            UserCpuNanos = row.getSetter("UserCpuNanos", long.class);
            TotalMemoryFree = row.getSetter("TotalMemoryFree", long.class);
            TotalMemoryUsed = row.getSetter("TotalMemoryUsed", long.class);
            FreeMemoryChange = row.getSetter("FreeMemoryChange", long.class);
            TotalMemoryChange = row.getSetter("TotalMemoryChange", long.class);
            AllocatedBytes = row.getSetter("AllocatedBytes", long.class);
            PoolAllocatedBytes = row.getSetter("PoolAllocatedBytes", long.class);
            WasInterrupted = row.getSetter("WasInterrupted", Boolean.class);
            IsReplayer = row.getSetter("IsReplayer", Boolean.class);
            Exception = row.getSetter("Exception", String.class);
        }

        @Override
        public void log(
                final Row.Flags flags, final long evaluationNumber,
                final QueryProcessingResults queryProcessingResults, final QueryPerformanceNugget nugget)
                throws IOException {
            setRowFlags(flags);
            this.ProcessUniqueId.set(processUniqueId);
            this.EvaluationNumber.setLong(evaluationNumber);
            this.StartTime.set(DBTimeUtils.millisToTime(nugget.getStartClockTime()));
            this.EndTime.set(nugget.getTotalTimeNanos() == null
                    ? null
                    : DBTimeUtils.millisToTime(
                            nugget.getStartClockTime() + DBTimeUtils.nanosToMillis(nugget.getTotalTimeNanos())));
            this.DurationNanos.setLong(
                    nugget.getTotalTimeNanos() == null ? QueryConstants.NULL_LONG : nugget.getTotalTimeNanos());
            this.CpuNanos.setLong(nugget.getCpuNanos());
            this.UserCpuNanos.setLong(nugget.getUserCpuNanos());
            this.TotalMemoryFree.setLong(nugget.getEndFreeMemory());
            this.TotalMemoryUsed.setLong(nugget.getEndTotalMemory());
            this.FreeMemoryChange.setLong(nugget.getDiffFreeMemory());
            this.TotalMemoryChange.setLong(nugget.getDiffTotalMemory());
            this.AllocatedBytes.setLong(nugget.getAllocatedBytes());
            this.PoolAllocatedBytes.setLong(nugget.getPoolAllocatedBytes());
            this.WasInterrupted.setBoolean(nugget.wasInterrupted());
            this.IsReplayer.setBoolean(queryProcessingResults.isReplayer());
            this.Exception.set(queryProcessingResults.getException());
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
                .add("EvaluationNumber", long.class)
                .add("StartTime", DBDateTime.class)
                .add("EndTime", DBDateTime.class)
                .add("DurationNanos", long.class)
                .add("CpuNanos", long.class)
                .add("UserCpuNanos", long.class)
                .add("TotalMemoryFree", long.class)
                .add("TotalMemoryUsed", long.class)
                .add("FreeMemoryChange", long.class)
                .add("TotalMemoryChange", long.class)
                .add("AllocatedBytes", long.class)
                .add("PoolAllocatedBytes", long.class)
                .add("WasInterrupted", Boolean.class)
                .add("IsReplayer", Boolean.class)
                .add("Exception", String.class);
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getDbTypes();
    }

    @Override
    protected ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new DirectSetter();
    }

    public void log(final long evaluationNumber,
            final QueryProcessingResults queryProcessingResults,
            final QueryPerformanceNugget nugget) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, evaluationNumber, queryProcessingResults, nugget);
    }

    public void log(
            final Row.Flags flags, final long evaluationNumber,
            final QueryProcessingResults queryProcessingResults, final QueryPerformanceNugget nugget)
            throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, evaluationNumber, queryProcessingResults, nugget);
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
