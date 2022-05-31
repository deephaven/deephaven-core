/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tablelogger;

import java.io.IOException;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.ColumnsSpecHelper;
import io.deephaven.time.DateTime;
import io.deephaven.tablelogger.*;

import static io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker.IntervalLevelDetails;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;

public class UpdatePerformanceLogLogger
        extends TableLoggerImpl2<UpdatePerformanceLogLogger.ISetter> {

    private static final String TABLE_NAME = "UpdatePerformanceLog";

    private final String processUniqueId;

    public UpdatePerformanceLogLogger(final String processUniqueId) {
        super(TABLE_NAME);
        this.processUniqueId = processUniqueId;
    }

    interface ISetter extends WritableRowContainer {
        void log(Row.Flags flags, IntervalLevelDetails intervalLevelDetails, PerformanceEntry performanceEntry)
                throws IOException;
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    class DirectSetter extends BaseSetter implements ISetter {
        RowSetter<String> ProcessUniqueId;
        RowSetter<Integer> EntryId;
        RowSetter<Integer> EvaluationNumber;
        RowSetter<Integer> OperationNumber;
        RowSetter<String> EntryDescription;
        RowSetter<String> EntryCallerLine;
        RowSetter<DateTime> IntervalStartTime;
        RowSetter<DateTime> IntervalEndTime;
        RowSetter<Long> IntervalDurationNanos;
        RowSetter<Long> EntryIntervalUsage;
        RowSetter<Long> EntryIntervalCpuNanos;
        RowSetter<Long> EntryIntervalUserCpuNanos;
        RowSetter<Long> EntryIntervalAdded;
        RowSetter<Long> EntryIntervalRemoved;
        RowSetter<Long> EntryIntervalModified;
        RowSetter<Long> EntryIntervalShifted;
        RowSetter<Long> EntryIntervalInvocationCount;
        RowSetter<Long> MinFreeMemory;
        RowSetter<Long> MaxTotalMemory;
        RowSetter<Long> Collections;
        RowSetter<Long> CollectionTimeNanos;
        RowSetter<Long> EntryIntervalAllocatedBytes;
        RowSetter<Long> EntryIntervalPoolAllocatedBytes;

        DirectSetter() {
            ProcessUniqueId = row.getSetter("ProcessUniqueId", String.class);
            EntryId = row.getSetter("EntryId", int.class);
            EvaluationNumber = row.getSetter("EvaluationNumber", int.class);
            OperationNumber = row.getSetter("OperationNumber", int.class);
            EntryDescription = row.getSetter("EntryDescription", String.class);
            EntryCallerLine = row.getSetter("EntryCallerLine", String.class);
            IntervalStartTime = row.getSetter("IntervalStartTime", DateTime.class);
            IntervalEndTime = row.getSetter("IntervalEndTime", DateTime.class);
            IntervalDurationNanos = row.getSetter("IntervalDurationNanos", long.class);
            EntryIntervalUsage = row.getSetter("EntryIntervalUsage", long.class);
            EntryIntervalCpuNanos = row.getSetter("EntryIntervalCpuNanos", long.class);
            EntryIntervalUserCpuNanos = row.getSetter("EntryIntervalUserCpuNanos", long.class);
            EntryIntervalAdded = row.getSetter("EntryIntervalAdded", long.class);
            EntryIntervalRemoved = row.getSetter("EntryIntervalRemoved", long.class);
            EntryIntervalModified = row.getSetter("EntryIntervalModified", long.class);
            EntryIntervalShifted = row.getSetter("EntryIntervalShifted", long.class);
            EntryIntervalInvocationCount = row.getSetter("EntryIntervalInvocationCount", long.class);
            MinFreeMemory = row.getSetter("MinFreeMemory", long.class);
            MaxTotalMemory = row.getSetter("MaxTotalMemory", long.class);
            Collections = row.getSetter("Collections", long.class);
            CollectionTimeNanos = row.getSetter("CollectionTimeNanos", long.class);
            EntryIntervalAllocatedBytes = row.getSetter("EntryIntervalAllocatedBytes", long.class);
            EntryIntervalPoolAllocatedBytes = row.getSetter("EntryIntervalPoolAllocatedBytes", long.class);
        }

        @Override
        public void log(final Row.Flags flags, final IntervalLevelDetails intervalLevelDetails,
                final PerformanceEntry performanceEntry) throws IOException {
            setRowFlags(flags);
            this.ProcessUniqueId.set(processUniqueId);
            this.EntryId.setInt(performanceEntry.getId());
            this.EvaluationNumber.setInt(performanceEntry.getEvaluationNumber());
            this.OperationNumber.setInt(performanceEntry.getOperationNumber());
            this.EntryDescription.set(performanceEntry.getDescription());
            this.EntryCallerLine.set(performanceEntry.getCallerLine());
            this.IntervalStartTime.set(DateTimeUtils.millisToTime(intervalLevelDetails.getIntervalStartTimeMillis()));
            this.IntervalEndTime.set(DateTimeUtils.millisToTime(intervalLevelDetails.getIntervalEndTimeMillis()));
            this.IntervalDurationNanos.setLong(intervalLevelDetails.getIntervalDurationNanos());
            this.EntryIntervalUsage.setLong(performanceEntry.getIntervalUsageNanos());
            this.EntryIntervalCpuNanos.setLong(performanceEntry.getIntervalCpuNanos());
            this.EntryIntervalUserCpuNanos.setLong(performanceEntry.getIntervalUserCpuNanos());
            this.EntryIntervalAdded.setLong(performanceEntry.getIntervalAdded());
            this.EntryIntervalRemoved.setLong(performanceEntry.getIntervalRemoved());
            this.EntryIntervalModified.setLong(performanceEntry.getIntervalModified());
            this.EntryIntervalShifted.setLong(performanceEntry.getIntervalShifted());
            this.EntryIntervalInvocationCount.setLong(performanceEntry.getIntervalInvocationCount());
            this.MinFreeMemory.setLong(performanceEntry.getMinFreeMemory());
            this.MaxTotalMemory.setLong(performanceEntry.getMaxTotalMemory());
            this.Collections.setLong(performanceEntry.getCollections());
            this.CollectionTimeNanos.setLong(performanceEntry.getCollectionTimeNanos());
            this.EntryIntervalAllocatedBytes.setLong(performanceEntry.getIntervalAllocatedBytes());
            this.EntryIntervalPoolAllocatedBytes.setLong(performanceEntry.getIntervalPoolAllocatedBytes());
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
                .add("EntryId", int.class)
                .add("EvaluationNumber", int.class)
                .add("OperationNumber", int.class)
                .add("EntryDescription", String.class)
                .add("EntryCallerLine", String.class)

                .add("IntervalStartTime", DateTime.class)
                .add("IntervalEndTime", DateTime.class)

                .add("IntervalDurationNanos", long.class)
                .add("EntryIntervalUsage", long.class)
                .add("EntryIntervalCpuNanos", long.class)
                .add("EntryIntervalUserCpuNanos", long.class)
                .add("EntryIntervalAdded", long.class)
                .add("EntryIntervalRemoved", long.class)
                .add("EntryIntervalModified", long.class)
                .add("EntryIntervalShifted", long.class)

                .add("EntryIntervalInvocationCount", long.class)
                .add("MinFreeMemory", long.class)
                .add("MaxTotalMemory", long.class)
                .add("Collections", long.class)
                .add("CollectionTimeNanos", long.class)
                .add("EntryIntervalAllocatedBytes", long.class)
                .add("EntryIntervalPoolAllocatedBytes", long.class)

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
            final IntervalLevelDetails intervalLevelDetails, final PerformanceEntry performanceEntry)
            throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, intervalLevelDetails, performanceEntry);
    }

    public void log(
            final Row.Flags flags, final IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry performanceEntry)
            throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, intervalLevelDetails, performanceEntry);
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
