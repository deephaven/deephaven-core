/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;

import io.deephaven.db.tablelogger.UpdatePerformanceLogLogger;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.*;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.profiling.ThreadProfiler;
import io.deephaven.internal.log.LoggerFactory;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.db.tables.lang.DBLanguageFunctionUtil.minus;
import static io.deephaven.db.tables.lang.DBLanguageFunctionUtil.plus;

/**
 * This tool is meant to track periodic update events that take place in a LiveTableMonitor. This generally includes (1)
 * LiveTable.refresh() invocations (2) DynamicTable Listener notifications (see InstrumentedListener)
 *
 * Note: Regarding thread safety, this class interacts with a singleton LiveTableMonitor and expects all calls to
 * getEntry(), Entry.onUpdateStart(), and Entry.onUpdateEnd() to be performed while protected by the LTM's live jobs
 * synchronizer.
 */
public class UpdatePerformanceTracker {

    private static final long REPORT_INTERVAL_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            UpdatePerformanceTracker.class, "reportIntervalMillis", 60 * 1000L);
    // aggregate update performance entries less than 500us by default
    private static final QueryPerformanceLogThreshold LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Update", 500_000L);

    private static volatile UpdatePerformanceTracker INSTANCE;
    private static boolean started = false;
    private boolean unitTestMode = false;
    private final Entry aggregatedSmallUpdatesEntry =
            new Entry(QueryConstants.NULL_INT, QueryConstants.NULL_INT, QueryConstants.NULL_INT,
                    "Aggregated Small Updates", null);

    public static UpdatePerformanceTracker getInstance() {
        if (INSTANCE == null) {
            synchronized (UpdatePerformanceTracker.class) {
                if (INSTANCE == null) {
                    final TableDefinition tableDefinition = UpdatePerformanceLogLogger.getTableDefinition();
                    final String processInfoId = MemoryTableLoggers.getInstance().getProcessInfo().getId().value();
                    INSTANCE = new UpdatePerformanceTracker(processInfoId,
                            LoggerFactory.getLogger(UpdatePerformanceTracker.class), tableDefinition);
                }
            }
        }
        return INSTANCE;
    }

    private final Logger logger;
    private final MemoryTableLogger<UpdatePerformanceLogLogger> tableLogger;

    private final AtomicInteger entryIdCounter = new AtomicInteger(1);
    private final Queue<WeakReference<Entry>> entries = new LinkedBlockingDeque<>();

    private UpdatePerformanceTracker(
            @NotNull final String processInfoId,
            @NotNull final Logger logger,
            @NotNull final TableDefinition logTableDefinition) {
        this.logger = logger;
        tableLogger = new MemoryTableLogger<>(
                logger, new UpdatePerformanceLogLogger(processInfoId), logTableDefinition);
    }

    private void startThread() {
        Thread driverThread = new Thread(new Driver(), "UpdatePerformanceTracker.Driver");
        driverThread.setDaemon(true);
        driverThread.start();
    }

    public static void start() {
        synchronized (UpdatePerformanceTracker.class) {
            if (started) {
                return;
            }
            started = true;
        }
        getInstance().startThread();
    }

    private class Driver implements Runnable {
        @Override
        public void run() {
            // noinspection InfiniteLoopStatement
            while (true) {
                final long intervalStartTimeNanos = System.nanoTime();
                final long intervalStartTimeMillis = System.currentTimeMillis();
                try {
                    Thread.sleep(REPORT_INTERVAL_MILLIS);
                } catch (InterruptedException ignore) {
                    // should log, but no logger handy
                    // ignore
                }
                LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(
                        () -> finishInterval(intervalStartTimeMillis,
                                System.currentTimeMillis(),
                                System.nanoTime() - intervalStartTimeNanos));
            }
        }
    }

    public void enableUnitTestMode() {
        unitTestMode = true;
    }

    /**
     * Get a new entry to track the performance characteristics of a single recurring update event.
     *
     * @param description log entry description
     * @return UpdatePerformanceTracker.Entry
     */
    public final Entry getEntry(final String description) {
        final QueryPerformanceRecorder qpr = QueryPerformanceRecorder.getInstance();

        final MutableObject<Entry> entryMu = new MutableObject<>();
        qpr.setQueryData((evaluationNumber, operationNumber, uninstrumented) -> {
            final String effectiveDescription;
            if ((description == null || description.length() == 0) && uninstrumented) {
                effectiveDescription = QueryPerformanceRecorder.UNINSTRUMENTED_CODE_DESCRIPTION;
            } else {
                effectiveDescription = description;
            }
            entryMu.setValue(new Entry(
                    entryIdCounter.getAndIncrement(),
                    evaluationNumber,
                    operationNumber,
                    effectiveDescription,
                    QueryPerformanceRecorder.getCallerLine()));
        });
        final Entry entry = entryMu.getValue();
        if (!unitTestMode) {
            entries.add(new WeakReference<>(entry));
        }

        return entry;
    }

    /**
     * Do entry maintenance, generate an interval performance report table for all active entries, and reset for the
     * next interval. <b>Note:</b> This method is only called under the LiveTableMonitor instance's lock. This ensures
     * exclusive access to the entries, and also prevents any other thread from removing from entries.
     * 
     * @param intervalStartTimeMillis interval start time in millis
     * @param intervalEndTimeMillis interval end time in millis
     * @param intervalDurationNanos interval duration in nanos
     */
    private void finishInterval(final long intervalStartTimeMillis, final long intervalEndTimeMillis,
            final long intervalDurationNanos) {
        /*
         * Visit all entry references. For entries that no longer exist: Remove by index from the entry list. For
         * entries that still exist: If the entry had non-zero usage in this interval, add it to the report. Reset the
         * entry for the next interval.
         */
        final IntervalLevelDetails intervalLevelDetails =
                new IntervalLevelDetails(intervalStartTimeMillis, intervalEndTimeMillis, intervalDurationNanos);

        boolean encounteredErrorLoggingToMemory = false;

        for (final Iterator<WeakReference<Entry>> it = entries.iterator(); it.hasNext();) {
            final WeakReference<Entry> entryReference = it.next();
            final Entry entry = entryReference == null ? null : entryReference.get();
            if (entry == null) {
                it.remove();
                continue;
            }

            if (entry.shouldLogEntryInterval()) {
                encounteredErrorLoggingToMemory =
                        logToMemory(intervalLevelDetails, entry, encounteredErrorLoggingToMemory);
            } else if (entry.intervalInvocationCount > 0) {
                if (entry.totalUsedMemory > aggregatedSmallUpdatesEntry.totalUsedMemory) {
                    aggregatedSmallUpdatesEntry.totalUsedMemory = entry.totalUsedMemory;
                }
                if (aggregatedSmallUpdatesEntry.intervalInvocationCount == 0
                        || aggregatedSmallUpdatesEntry.totalFreeMemory > entry.totalFreeMemory) {
                    aggregatedSmallUpdatesEntry.totalFreeMemory = entry.totalFreeMemory;
                }

                aggregatedSmallUpdatesEntry.intervalUsageNanos += entry.intervalUsageNanos;
                aggregatedSmallUpdatesEntry.intervalInvocationCount += entry.intervalInvocationCount;

                aggregatedSmallUpdatesEntry.intervalCpuNanos =
                        plus(aggregatedSmallUpdatesEntry.intervalCpuNanos, entry.intervalCpuNanos);
                aggregatedSmallUpdatesEntry.intervalUserCpuNanos =
                        plus(aggregatedSmallUpdatesEntry.intervalUserCpuNanos, entry.intervalUserCpuNanos);

                aggregatedSmallUpdatesEntry.intervalAdded += entry.intervalAdded;
                aggregatedSmallUpdatesEntry.intervalRemoved += entry.intervalRemoved;
                aggregatedSmallUpdatesEntry.intervalModified += entry.intervalModified;
                aggregatedSmallUpdatesEntry.intervalShifted += entry.intervalShifted;

                aggregatedSmallUpdatesEntry.intervalAllocatedBytes =
                        plus(aggregatedSmallUpdatesEntry.intervalAllocatedBytes, entry.intervalAllocatedBytes);
                aggregatedSmallUpdatesEntry.intervalPoolAllocatedBytes =
                        plus(aggregatedSmallUpdatesEntry.intervalPoolAllocatedBytes, entry.intervalPoolAllocatedBytes);
            }
            entry.reset();
        }

        if (aggregatedSmallUpdatesEntry.intervalInvocationCount > 0) {
            logToMemory(intervalLevelDetails, aggregatedSmallUpdatesEntry, encounteredErrorLoggingToMemory);
            aggregatedSmallUpdatesEntry.reset();
        }
    }

    private boolean logToMemory(final IntervalLevelDetails intervalLevelDetails,
            final Entry entry,
            final boolean encounteredErrorLoggingToMemory) {
        if (!encounteredErrorLoggingToMemory) {
            try {
                tableLogger.getTableLogger().log(intervalLevelDetails, entry);
            } catch (IOException e) {
                // Don't want to log this more than once in a report
                logger.error().append("Error sending UpdatePerformanceLog data to memory").append(e).endl();
                return true;
            }
        }
        return false;
    }

    /**
     * Holder for logging details that are the same for every Entry in an interval
     */
    public static class IntervalLevelDetails {
        private final long intervalStartTimeMillis;
        private final long intervalEndTimeMillis;
        private final long intervalDurationNanos;

        IntervalLevelDetails(final long intervalStartTimeMillis, final long intervalEndTimeMillis,
                final long intervalDurationNanos) {
            this.intervalStartTimeMillis = intervalStartTimeMillis;
            this.intervalEndTimeMillis = intervalEndTimeMillis;
            this.intervalDurationNanos = intervalDurationNanos;
        }

        public long getIntervalStartTimeMillis() {
            return intervalStartTimeMillis;
        }

        public long getIntervalEndTimeMillis() {
            return intervalEndTimeMillis;
        }

        public long getIntervalDurationNanos() {
            return intervalDurationNanos;
        }
    }

    /**
     * Entry class for tracking the performance characteristics of a single recurring update event.
     */
    public static class Entry implements LogOutputAppendable {
        private final int id;
        private final int evaluationNumber;
        private final int operationNumber;
        private final String description;
        private final String callerLine;

        private long intervalUsageNanos;
        private long intervalInvocationCount;

        private long intervalCpuNanos;
        private long intervalUserCpuNanos;

        private long intervalAdded;
        private long intervalRemoved;
        private long intervalModified;
        private long intervalShifted;

        private long intervalAllocatedBytes;
        private long intervalPoolAllocatedBytes;

        private long startTimeNanos;

        private long startCpuNanos;
        private long startUserCpuNanos;

        private long startAllocatedBytes;
        private long startPoolAllocatedBytes;

        private long totalFreeMemory;
        private long totalUsedMemory;

        private Entry(final int id, final int evaluationNumber, final int operationNumber,
                final String description, final String callerLine) {
            this.id = id;
            this.evaluationNumber = evaluationNumber;
            this.operationNumber = operationNumber;
            this.description = description;
            this.callerLine = callerLine;
        }

        public final void onUpdateStart() {
            ++intervalInvocationCount;

            startAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
            startPoolAllocatedBytes = QueryPerformanceRecorder.getPoolAllocatedBytesForCurrentThread();

            startUserCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadUserTime();
            startCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadCpuTime();
            startTimeNanos = System.nanoTime();
        }

        public final void onUpdateStart(final Index added, final Index removed, final Index modified,
                final IndexShiftData shifted) {
            intervalAdded += added.size();
            intervalRemoved += removed.size();
            intervalModified += modified.size();
            intervalShifted += shifted.getEffectiveSize();

            onUpdateStart();
        }

        public final void onUpdateStart(long added, long removed, long modified, long shifted) {
            intervalAdded += added;
            intervalRemoved += removed;
            intervalModified += modified;
            intervalShifted += shifted;

            onUpdateStart();
        }

        public final void onUpdateEnd() {
            intervalUserCpuNanos = plus(intervalUserCpuNanos,
                    minus(ThreadProfiler.DEFAULT.getCurrentThreadUserTime(), startUserCpuNanos));
            intervalCpuNanos =
                    plus(intervalCpuNanos, minus(ThreadProfiler.DEFAULT.getCurrentThreadCpuTime(), startCpuNanos));

            intervalUsageNanos += System.nanoTime() - startTimeNanos;

            intervalPoolAllocatedBytes = plus(intervalPoolAllocatedBytes,
                    minus(QueryPerformanceRecorder.getPoolAllocatedBytesForCurrentThread(), startPoolAllocatedBytes));
            intervalAllocatedBytes = plus(intervalAllocatedBytes,
                    minus(ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes(), startAllocatedBytes));

            startAllocatedBytes = 0;
            startPoolAllocatedBytes = 0;

            startUserCpuNanos = 0;
            startCpuNanos = 0;
            startTimeNanos = 0;

            final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
            totalFreeMemory = runtimeMemory.freeMemory();
            totalUsedMemory = runtimeMemory.totalMemory();
        }

        private void reset() {
            Assert.eqZero(startTimeNanos, "startTimeNanos");

            intervalUsageNanos = 0;
            intervalInvocationCount = 0;

            intervalCpuNanos = 0;
            intervalUserCpuNanos = 0;

            intervalAdded = 0;
            intervalRemoved = 0;
            intervalModified = 0;
            intervalShifted = 0;

            intervalAllocatedBytes = 0;
            intervalPoolAllocatedBytes = 0;

            totalFreeMemory = 0;
            totalUsedMemory = 0;
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public LogOutput append(final LogOutput logOutput) {
            return logOutput.append("Entry{").append(", id=").append(id).append(", evaluationNumber=")
                    .append(evaluationNumber).append(", operationNumber=").append(operationNumber)
                    .append(", description='").append(description).append('\'').append(", callerLine='")
                    .append(callerLine).append('\'').append(", intervalUsageNanos=").append(intervalUsageNanos)
                    .append(", intervalInvocationCount=").append(intervalInvocationCount).append(", intervalCpuNanos=")
                    .append(intervalCpuNanos).append(", intervalUserCpuNanos=").append(intervalUserCpuNanos)
                    .append(", intervalAdded=").append(intervalAdded).append(", intervalRemoved=")
                    .append(intervalRemoved).append(", intervalModified=").append(intervalModified)
                    .append(", intervalShifted=").append(intervalShifted).append(", intervalAllocatedBytes=")
                    .append(intervalAllocatedBytes).append(", intervalPoolAllocatedBytes=")
                    .append(intervalPoolAllocatedBytes).append(", startCpuNanos=").append(startCpuNanos)
                    .append(", startUserCpuNanos=").append(startUserCpuNanos).append(", startTimeNanos=")
                    .append(startTimeNanos).append(", startAllocatedBytes=").append(startAllocatedBytes)
                    .append(", startPoolAllocatedBytes=").append(startPoolAllocatedBytes).append(", totalUsedMemory=")
                    .append(totalUsedMemory).append(", totalFreeMemory=").append(totalFreeMemory).append('}');
        }

        public int getId() {
            return id;
        }

        public int getEvaluationNumber() {
            return evaluationNumber;
        }

        public int getOperationNumber() {
            return operationNumber;
        }

        public String getDescription() {
            return description;
        }

        public String getCallerLine() {
            return callerLine;
        }

        public long getIntervalUsageNanos() {
            return intervalUsageNanos;
        }

        public long getIntervalCpuNanos() {
            return intervalCpuNanos;
        }

        public long getIntervalUserCpuNanos() {
            return intervalUserCpuNanos;
        }

        public long getIntervalAdded() {
            return intervalAdded;
        }

        public long getIntervalRemoved() {
            return intervalRemoved;
        }

        public long getIntervalModified() {
            return intervalModified;
        }

        public long getIntervalShifted() {
            return intervalShifted;
        }

        public long getTotalFreeMemory() {
            return totalFreeMemory;
        }

        public long getTotalUsedMemory() {
            return totalUsedMemory;
        }

        public long getIntervalAllocatedBytes() {
            return intervalAllocatedBytes;
        }

        public long getIntervalPoolAllocatedBytes() {
            return intervalPoolAllocatedBytes;
        }

        public long getIntervalInvocationCount() {
            return intervalInvocationCount;
        }

        /**
         * Suppress de minimus update entry intervals using the properties defined in the QueryPerformanceNugget class.
         *
         * @return if this nugget is significant enough to be logged, otherwise it is aggregated into the small update
         *         entry
         */
        boolean shouldLogEntryInterval() {
            return intervalInvocationCount > 0 &&
                    LOG_THRESHOLD.shouldLog(getIntervalUsageNanos());
        }
    }

    public QueryTable getQueryTable() {
        return tableLogger.getQueryTable();
    }
}
