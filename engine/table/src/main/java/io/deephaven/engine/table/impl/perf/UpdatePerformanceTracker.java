/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListener;
import io.deephaven.engine.tablelogger.EngineTableLoggers;
import io.deephaven.engine.tablelogger.UpdatePerformanceLogLogger;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * This tool is meant to track periodic update events that take place in an {@link PeriodicUpdateGraph}. This generally
 * includes:
 * <ol>
 * <li>Update source {@code run()} invocations</li>
 * <li>{@link Table} {@link ShiftObliviousListener} notifications (see {@link ShiftObliviousInstrumentedListener})</li>
 * <li>{@link Table} {@link TableUpdateListener} notifications (see {@link InstrumentedTableUpdateListener})</li>
 * </ol>
 * (1)
 *
 * @apiNote Regarding thread safety, this class interacts with a singleton PeriodicUpdateGraph and expects all calls to
 *          {@link Driver#getEntry(String)}, {@link PerformanceEntry#onUpdateStart()}, and
 *          {@link PerformanceEntry#onUpdateEnd()} to be performed while protected by the UGP's lock.
 */
public class UpdatePerformanceTracker {

    private static final long REPORT_INTERVAL_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            UpdatePerformanceTracker.class, "reportIntervalMillis", 60 * 1000L);

    private static final Logger log = LoggerFactory.getLogger(UpdatePerformanceTracker.class);

    // aggregate update performance entries less than 500us by default
    static final QueryPerformanceLogThreshold LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Update", 500_000L);

    private static volatile UpdatePerformanceTracker INSTANCE;

    public static UpdatePerformanceTracker getInstance() {
        UpdatePerformanceTracker local;
        if ((local = INSTANCE) == null) {
            synchronized (UpdatePerformanceTracker.class) {
                if ((local = INSTANCE) == null) {
                    INSTANCE = local = new UpdatePerformanceTracker();
                }
            }
        }
        return local;
    }

    public static synchronized void start(final UpdateSourceRegistrar registrar) {
        final UpdatePerformanceTracker upt = getInstance();
        if (upt.adapter != null) {
            return; // already started
        }

        upt.adapter = new StreamToBlinkTableAdapter(
                UpdatePerformanceStreamPublisher.definition(),
                upt.publisher,
                registrar,
                UpdatePerformanceTracker.class.getName());
        upt.blink = upt.adapter.table();
    }

    private final UpdatePerformanceLogLogger tableLogger;
    private final UpdatePerformanceStreamPublisher publisher;
    // Eventually, we can close the StreamToBlinkTableAdapter
    private StreamToBlinkTableAdapter adapter;
    private Table blink;
    private static final AtomicInteger entryIdCounter = new AtomicInteger(1);

    public UpdatePerformanceTracker() {
        this.tableLogger = EngineTableLoggers.get().updatePerformanceLogLogger();
        this.publisher = new UpdatePerformanceStreamPublisher();
    }

    public class Driver implements Runnable {

        private final ExecutionContext context;
        private final PerformanceEntry aggregatedSmallUpdatesEntry;
        private final Queue<WeakReference<PerformanceEntry>> entries = new LinkedBlockingDeque<>();

        private boolean started = false;
        private boolean unitTestMode = false;

        Driver(final UpdateGraph updateGraph) {
            this.context = ExecutionContext.getContext()
                    .withUpdateGraph(Objects.requireNonNull(updateGraph));
            this.aggregatedSmallUpdatesEntry = new PerformanceEntry(
                    QueryConstants.NULL_INT, QueryConstants.NULL_INT, QueryConstants.NULL_INT,
                    "Aggregated Small Updates", null, context.getUpdateGraph().getName());
        }

        public void start() {
            if (started) {
                return;
            }
            if (publisher == null) {
                throw new IllegalStateException("UpdatePerformanceTracker must be started first");
            }
            started = true;

            Thread driverThread = new Thread(this,
                    "UpdatePerformanceTracker.Driver[" + context.getUpdateGraph().getName() + "]");
            driverThread.setDaemon(true);
            driverThread.start();
        }

        @Override
        public void run() {
            try (final SafeCloseable ignored1 = context.open()) {
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
                    try (final SafeCloseable ignored2 = context.getUpdateGraph().sharedLock().lockCloseable()) {
                        finishInterval(intervalStartTimeMillis, System.currentTimeMillis(),
                                System.nanoTime() - intervalStartTimeNanos);
                    }
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
                        QueryPerformanceRecorder.getCallerLine(),
                        context.getUpdateGraph().getName()));
            });
            final PerformanceEntry entry = entryMu.getValue();
            if (!unitTestMode) {
                entries.add(new WeakReference<>(entry));
            }

            return entry;
        }

        /**
         * Do entry maintenance, generate an interval performance report table for all active entries, and reset for the
         * next interval. <b>Note:</b> This method is only called under the PeriodicUpdateGraph instance's shared lock.
         * This ensures that no other thread using the same update graph will mutate individual entries. Concurrent
         * additions to {@link #entries} are supported by the underlying data structure.
         *
         * @param intervalStartTimeMillis interval start time in millis
         * @param intervalEndTimeMillis interval end time in millis
         * @param intervalDurationNanos interval duration in nanos
         */
        private void finishInterval(final long intervalStartTimeMillis, final long intervalEndTimeMillis,
                final long intervalDurationNanos) {
            /*
             * Visit all entry references. For entries that no longer exist: Remove by index from the entry list. For
             * entries that still exist: If the entry had non-zero usage in this interval, add it to the report. Reset
             * the entry for the next interval.
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
                            publish(intervalLevelDetails, entry, encounteredErrorLoggingToMemory);
                } else if (entry.getIntervalInvocationCount() > 0) {
                    aggregatedSmallUpdatesEntry.accumulate(entry);
                }
                entry.reset();
            }

            if (aggregatedSmallUpdatesEntry.getIntervalInvocationCount() > 0) {
                publish(intervalLevelDetails, aggregatedSmallUpdatesEntry, encounteredErrorLoggingToMemory);
                aggregatedSmallUpdatesEntry.reset();
            }
        }
    }

    private boolean publish(
            final IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry entry,
            final boolean encounteredErrorLoggingToMemory) {
        if (!encounteredErrorLoggingToMemory) {
            publisher.add(intervalLevelDetails, entry);
            try {
                tableLogger.log(intervalLevelDetails, entry);
            } catch (IOException e) {
                // Don't want to log this more than once in a report
                log.error().append("Error sending UpdatePerformanceLog data to memory").append(e).endl();
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

    @NotNull
    public QueryTable getQueryTable() {
        if (blink == null) {
            throw new IllegalStateException("UpdatePerformanceTracker must be started first");
        }
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(blink);
    }

    @NotNull
    public static synchronized Driver createDriverFor(final UpdateGraph updateGraph) {
        return getInstance().new Driver(updateGraph);
    }
}
