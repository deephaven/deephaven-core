/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tablelogger;

import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tablelogger.*;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.perf.QueryProcessingResults;
import io.deephaven.engine.util.ColumnsSpecHelper;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
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
        RowSetter<DateTime> StartTime;
        RowSetter<DateTime> EndTime;
        RowSetter<Long> DurationNanos;
        RowSetter<Long> CpuNanos;
        RowSetter<Long> UserCpuNanos;
        RowSetter<Long> FreeMemory;
        RowSetter<Long> TotalMemory;
        RowSetter<Long> FreeMemoryChange;
        RowSetter<Long> TotalMemoryChange;
        RowSetter<Long> Collections;
        RowSetter<Long> CollectionTimeNanos;
        RowSetter<Long> AllocatedBytes;
        RowSetter<Long> PoolAllocatedBytes;
        RowSetter<Boolean> WasInterrupted;
        RowSetter<Boolean> IsReplayer;
        RowSetter<String> Exception;

        DirectSetter() {
            ProcessUniqueId = row.getSetter("ProcessUniqueId", String.class);
            EvaluationNumber = row.getSetter("EvaluationNumber", long.class);
            StartTime = row.getSetter("StartTime", DateTime.class);
            EndTime = row.getSetter("EndTime", DateTime.class);
            DurationNanos = row.getSetter("DurationNanos", long.class);
            CpuNanos = row.getSetter("CpuNanos", long.class);
            UserCpuNanos = row.getSetter("UserCpuNanos", long.class);
            FreeMemory = row.getSetter("FreeMemory", long.class);
            TotalMemory = row.getSetter("TotalMemory", long.class);
            FreeMemoryChange = row.getSetter("FreeMemoryChange", long.class);
            TotalMemoryChange = row.getSetter("TotalMemoryChange", long.class);
            Collections = row.getSetter("Collections", long.class);
            CollectionTimeNanos = row.getSetter("CollectionTimeNanos", long.class);
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
            this.StartTime.set(DateTimeUtils.millisToTime(nugget.getStartClockTime()));
            this.EndTime.set(nugget.getTotalTimeNanos() == null
                    ? null
                    : DateTimeUtils.millisToTime(
                            nugget.getStartClockTime() + DateTimeUtils.nanosToMillis(nugget.getTotalTimeNanos())));
            this.DurationNanos.setLong(
                    nugget.getTotalTimeNanos() == null ? QueryConstants.NULL_LONG : nugget.getTotalTimeNanos());
            this.CpuNanos.setLong(nugget.getCpuNanos());
            this.UserCpuNanos.setLong(nugget.getUserCpuNanos());
            this.FreeMemory.setLong(nugget.getEndFreeMemory());
            this.TotalMemory.setLong(nugget.getEndTotalMemory());
            this.FreeMemoryChange.setLong(nugget.getDiffFreeMemory());
            this.TotalMemoryChange.setLong(nugget.getDiffTotalMemory());
            this.Collections.setLong(nugget.getDiffCollections());
            this.CollectionTimeNanos.setLong(nugget.getDiffCollectionTimeNanos());
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
                .add("StartTime", DateTime.class)
                .add("EndTime", DateTime.class)
                .add("DurationNanos", long.class)
                .add("CpuNanos", long.class)
                .add("UserCpuNanos", long.class)
                .add("FreeMemory", long.class)
                .add("TotalMemory", long.class)
                .add("FreeMemoryChange", long.class)
                .add("TotalMemoryChange", long.class)
                .add("Collections", long.class)
                .add("CollectionTimeNanos", long.class)
                .add("AllocatedBytes", long.class)
                .add("PoolAllocatedBytes", long.class)
                .add("WasInterrupted", Boolean.class)
                .add("IsReplayer", Boolean.class)
                .add("Exception", String.class);
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getTypes();
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
