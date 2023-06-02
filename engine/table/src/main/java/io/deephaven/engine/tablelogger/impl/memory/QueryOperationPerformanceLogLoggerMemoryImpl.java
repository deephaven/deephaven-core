/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.tablelogger.impl.memory;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.util.EngineMetrics;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.ColumnsSpecHelper;
import io.deephaven.tablelogger.*;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.QueryConstants;
import java.io.IOException;
import java.time.Instant;

class QueryOperationPerformanceLogLoggerMemoryImpl
        extends MemoryTableLogger<QueryOperationPerformanceLogLoggerMemoryImpl.ISetter>
        implements QueryOperationPerformanceLogLogger {

    private static final String TABLE_NAME = "QueryOperationPerformanceLog";

    private final String processUniqueId;

    public QueryOperationPerformanceLogLoggerMemoryImpl() {
        super(TABLE_NAME, TABLE_DEFINITION);
        this.processUniqueId = EngineMetrics.getProcessInfo().getId().value();
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    interface ISetter extends WritableRowContainer {
        void log(Row.Flags flags, int operationNumber, QueryPerformanceNugget nugget) throws IOException;
    }

    class DirectSetter extends BaseSetter implements ISetter {
        RowSetter<String> ProcessUniqueId;
        RowSetter<Integer> EvaluationNumber;
        RowSetter<Integer> OperationNumber;
        RowSetter<Integer> Depth;
        RowSetter<String> Description;
        RowSetter<String> CallerLine;
        RowSetter<Boolean> IsTopLevel;
        RowSetter<Boolean> IsCompilation;
        RowSetter<Instant> StartTime;
        RowSetter<Instant> EndTime;
        RowSetter<Long> DurationNanos;
        RowSetter<Long> CpuNanos;
        RowSetter<Long> UserCpuNanos;
        RowSetter<Long> FreeMemoryChange;
        RowSetter<Long> TotalMemoryChange;
        RowSetter<Long> Collections;
        RowSetter<Long> CollectionTimeNanos;
        RowSetter<Long> AllocatedBytes;
        RowSetter<Long> PoolAllocatedBytes;
        RowSetter<Long> InputSizeLong;
        RowSetter<Boolean> WasInterrupted;

        DirectSetter() {
            ProcessUniqueId = row.getSetter("ProcessUniqueId", String.class);
            EvaluationNumber = row.getSetter("EvaluationNumber", int.class);
            OperationNumber = row.getSetter("OperationNumber", int.class);
            Depth = row.getSetter("Depth", int.class);
            Description = row.getSetter("Description", String.class);
            CallerLine = row.getSetter("CallerLine", String.class);
            IsTopLevel = row.getSetter("IsTopLevel", Boolean.class);
            IsCompilation = row.getSetter("IsCompilation", Boolean.class);
            StartTime = row.getSetter("StartTime", Instant.class);
            EndTime = row.getSetter("EndTime", Instant.class);
            DurationNanos = row.getSetter("DurationNanos", long.class);
            CpuNanos = row.getSetter("CpuNanos", long.class);
            UserCpuNanos = row.getSetter("UserCpuNanos", long.class);
            FreeMemoryChange = row.getSetter("FreeMemoryChange", long.class);
            TotalMemoryChange = row.getSetter("TotalMemoryChange", long.class);
            Collections = row.getSetter("Collections", long.class);
            CollectionTimeNanos = row.getSetter("CollectionTimeNanos", long.class);
            AllocatedBytes = row.getSetter("AllocatedBytes", long.class);
            PoolAllocatedBytes = row.getSetter("PoolAllocatedBytes", long.class);
            InputSizeLong = row.getSetter("InputSizeLong", long.class);
            WasInterrupted = row.getSetter("WasInterrupted", Boolean.class);
        }

        @Override
        public void log(final Row.Flags flags, final int operationNumber, final QueryPerformanceNugget nugget)
                throws IOException {
            setRowFlags(flags);
            this.ProcessUniqueId.set(processUniqueId);
            this.EvaluationNumber.setInt(nugget.getEvaluationNumber());
            this.OperationNumber.setInt(operationNumber);
            this.Depth.setInt(nugget.getDepth());
            this.Description.set(nugget.getName());
            this.CallerLine.set(nugget.getCallerLine());
            this.IsTopLevel.setBoolean(nugget.isTopLevel());
            this.IsCompilation.setBoolean(nugget.getName().startsWith("Compile:"));
            this.StartTime.set(DateTimeUtils.epochMillisToInstant(nugget.getStartClockTime()));
            this.EndTime.set(nugget.getTotalTimeNanos() == null
                    ? null
                    : DateTimeUtils.epochMillisToInstant(
                            nugget.getStartClockTime() + DateTimeUtils.nanosToMillis(nugget.getTotalTimeNanos())));
            this.DurationNanos.setLong(
                    nugget.getTotalTimeNanos() == null ? QueryConstants.NULL_LONG : nugget.getTotalTimeNanos());
            this.CpuNanos.setLong(nugget.getCpuNanos());
            this.UserCpuNanos.setLong(nugget.getUserCpuNanos());
            this.FreeMemoryChange.setLong(nugget.getDiffFreeMemory());
            this.TotalMemoryChange.setLong(nugget.getDiffTotalMemory());
            this.Collections.setLong(nugget.getDiffCollections());
            this.CollectionTimeNanos.setLong(nugget.getDiffCollectionTimeNanos());
            this.AllocatedBytes.setLong(nugget.getAllocatedBytes());
            this.PoolAllocatedBytes.setLong(nugget.getPoolAllocatedBytes());
            this.InputSizeLong.setLong(nugget.getInputSize());
            this.WasInterrupted.setBoolean(nugget.wasInterrupted());
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
                .add("EvaluationNumber", int.class)
                .add("OperationNumber", int.class)
                .add("Depth", int.class)
                .add("Description", String.class)
                .add("CallerLine", String.class)
                .add("IsTopLevel", Boolean.class)
                .add("IsCompilation", Boolean.class)
                .add("StartTime", Instant.class)
                .add("EndTime", Instant.class)
                .add("DurationNanos", long.class)
                .add("CpuNanos", long.class)
                .add("UserCpuNanos", long.class)
                .add("FreeMemoryChange", long.class)
                .add("TotalMemoryChange", long.class)
                .add("Collections", long.class)
                .add("CollectionTimeNanos", long.class)
                .add("AllocatedBytes", long.class)
                .add("PoolAllocatedBytes", long.class)
                .add("InputSizeLong", long.class)
                .add("WasInterrupted", Boolean.class);
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getTypes();
    }

    @Override
    protected ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new DirectSetter();
    }

    @Override
    public void log(final int operationNumber, final QueryPerformanceNugget nugget) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, operationNumber, nugget);
    }

    @Override
    public void log(final Row.Flags flags, final int operationNumber, final QueryPerformanceNugget nugget)
            throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, operationNumber, nugget);
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
