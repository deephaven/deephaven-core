/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.perf;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.*;
import io.deephaven.engine.tablelogger.UpdatePerformanceLogLogger;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.util.MemoryTableLogger;
import io.deephaven.engine.table.impl.util.MemoryTableLoggers;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.minus;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.plus;

/**
 * <p>
 * This tool is meant to track periodic update events that take place in an {@link UpdateGraphProcessor}. This generally
 * includes:
 * <ol>
 * <li>Update source {@code run()} invocations</li>
 * <li>{@link Table} {@link ShiftObliviousListener} notifications (see {@link ShiftObliviousInstrumentedListener})</li>
 * <li>{@link Table} {@link TableUpdateListener} notifications (see {@link InstrumentedTableUpdateListener})</li>
 * </ol>
 * (1)
 *
 * @apiNote Regarding thread safety, this class interacts with a singleton UpdateGraphProcessor and expects all calls to
 *          {@link #getEntry(String)}, {@link PerformanceEntry#onUpdateStart()}, and
 *          {@link PerformanceEntry#onUpdateEnd()} to be performed while protected by the UGP's lock.
 */
public class UpdatePerformanceTracker {

    private static final long REPORT_INTERVAL_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            UpdatePerformanceTracker.class, "reportIntervalMillis", 60 * 1000L);
    // aggregate update performance entries less than 500us by default
    static final QueryPerformanceLogThreshold LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Update", 500_000L);

    private static volatile UpdatePerformanceTracker INSTANCE;
    private static boolean started = false;
    private boolean unitTestMode = false;
    private final PerformanceEntry aggregatedSmallUpdatesEntry =
            new PerformanceEntry(QueryConstants.NULL_INT, QueryConstants.NULL_INT, QueryConstants.NULL_INT,
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
    private final Queue<WeakReference<PerformanceEntry>> entries = new LinkedBlockingDeque<>();

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

    public static synchronized void start() {
        if (started) {
            return;
        }
        started = true;
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
                UpdateGraphProcessor.DEFAULT.sharedLock().doLocked(
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
    public final PerformanceEntry getEntry(final String description) {
        final QueryPerformanceRecorder qpr = QueryPerformanceRecorder.getInstance();

        final MutableObject<PerformanceEntry> entryMu = new MutableObject<>();
        qpr.setQueryData((evaluationNumber, operationNumber, uninstrumented) -> {
            final String effectiveDescription;
            if ((description == null || description.length() == 0) && uninstrumented) {
                effectiveDescription = QueryPerformanceRecorder.UNINSTRUMENTED_CODE_DESCRIPTION;
            } else {
                effectiveDescription = description;
            }
            entryMu.setValue(new PerformanceEntry(
                    entryIdCounter.getAndIncrement(),
                    evaluationNumber,
                    operationNumber,
                    effectiveDescription,
                    QueryPerformanceRecorder.getCallerLine()));
        });
        final PerformanceEntry entry = entryMu.getValue();
        if (!unitTestMode) {
            entries.add(new WeakReference<>(entry));
        }

        return entry;
    }

    /**
     * Do entry maintenance, generate an interval performance report table for all active entries, and reset for the
     * next interval. <b>Note:</b> This method is only called under the UpdateGraphProcessor instance's lock. This
     * ensures exclusive access to the entries, and also prevents any other thread from removing from entries.
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

        for (final Iterator<WeakReference<PerformanceEntry>> it = entries.iterator(); it.hasNext();) {
            final WeakReference<PerformanceEntry> entryReference = it.next();
            final PerformanceEntry entry = entryReference == null ? null : entryReference.get();
            if (entry == null) {
                it.remove();
                continue;
            }

            if (entry.shouldLogEntryInterval()) {
                encounteredErrorLoggingToMemory =
                        logToMemory(intervalLevelDetails, entry, encounteredErrorLoggingToMemory);
            } else if (entry.getIntervalInvocationCount() > 0) {
                aggregatedSmallUpdatesEntry.accumulate(entry);
            }
            entry.reset();
        }

        if (aggregatedSmallUpdatesEntry.getIntervalInvocationCount() > 0) {
            logToMemory(intervalLevelDetails, aggregatedSmallUpdatesEntry, encounteredErrorLoggingToMemory);
            aggregatedSmallUpdatesEntry.reset();
        }
    }

    private boolean logToMemory(final IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry entry,
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

    public QueryTable getQueryTable() {
        return tableLogger.getQueryTable();
    }
}
