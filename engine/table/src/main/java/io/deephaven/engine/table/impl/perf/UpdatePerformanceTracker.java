/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.verify.Assert;
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
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.string.StringUtils;
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
 *          {@link #getEntry(String)}, {@link PerformanceEntry#onUpdateStart()}, and
 *          {@link PerformanceEntry#onUpdateEnd()} to be performed while protected by the UGP's lock.
 */
public class UpdatePerformanceTracker {

    public static final long REPORT_INTERVAL_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            UpdatePerformanceTracker.class, "reportIntervalMillis", 60 * 1000L);

    private static final Logger log = LoggerFactory.getLogger(UpdatePerformanceTracker.class);

    // aggregate update performance entries less than 500us by default
    static final QueryPerformanceLogThreshold LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Update", 500_000L);

    private static InternalState INSTANCE;

    private static InternalState getInternalState() {
        InternalState local;
        if ((local = INSTANCE) == null) {
            synchronized (UpdatePerformanceTracker.class) {
                if ((local = INSTANCE) == null) {
                    INSTANCE = local = new InternalState();
                }
            }
        }
        return local;
    }

    private static class InternalState {
        private final UpdatePerformanceLogLogger tableLogger;
        private final UpdatePerformanceStreamPublisher publisher;

        // Eventually, we can close the StreamToBlinkTableAdapter
        @SuppressWarnings("FieldCanBeLocal")
        private final StreamToBlinkTableAdapter adapter;
        private final Table blink;

        private boolean encounteredError = false;

        private InternalState() {
            final UpdateGraph publishingGraph =
                    PeriodicUpdateGraph.getInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
            Assert.neqNull(publishingGraph, "The " + PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME + " UpdateGraph "
                    + "must be created before UpdatePerformanceTracker can be initialized.");
            try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(publishingGraph).open()) {
                tableLogger = EngineTableLoggers.get().updatePerformanceLogLogger();
                publisher = new UpdatePerformanceStreamPublisher();
                adapter = new StreamToBlinkTableAdapter(
                        UpdatePerformanceStreamPublisher.definition(),
                        publisher,
                        publishingGraph,
                        UpdatePerformanceTracker.class.getName());
                blink = adapter.table();
            }
        }

        /**
         * @implNote this method is synchronized to guarantee identical ordering of entries between publisher and
         *           tableLogger; doing so also relieves the requirement that the table logger be thread safe
         */
        private synchronized void publish(
                final IntervalLevelDetails intervalLevelDetails,
                final PerformanceEntry entry) {
            if (!encounteredError) {
                try {
                    publisher.add(intervalLevelDetails, entry);
                    tableLogger.log(intervalLevelDetails, entry);
                } catch (final IOException e) {
                    // Don't want to log this more than once in a report
                    log.error().append("Error publishing ").append(entry).append(" caused by: ").append(e).endl();
                    encounteredError = true;
                }
            }
        }
    }

    private static final AtomicInteger entryIdCounter = new AtomicInteger(1);

    private final UpdateGraph updateGraph;
    private final PerformanceEntry aggregatedSmallUpdatesEntry;
    private final PerformanceEntry flushEntry;
    private final Queue<WeakReference<PerformanceEntry>> entries = new LinkedBlockingDeque<>();

    private boolean unitTestMode = false;

    private long intervalStartTimeMillis = QueryConstants.NULL_LONG;
    private long intervalStartTimeNanos = QueryConstants.NULL_LONG;

    public UpdatePerformanceTracker(final UpdateGraph updateGraph) {
        this.updateGraph = Objects.requireNonNull(updateGraph);
        this.aggregatedSmallUpdatesEntry = new PerformanceEntry(
                QueryConstants.NULL_INT, QueryConstants.NULL_INT, QueryConstants.NULL_INT,
                "Aggregated Small Updates", null, updateGraph.getName());
        this.flushEntry = new PerformanceEntry(
                QueryConstants.NULL_INT, QueryConstants.NULL_INT, QueryConstants.NULL_INT,
                "UpdatePerformanceTracker Flush", null, updateGraph.getName());
    }

    /**
     * Start this UpdatePerformanceTracker, by beginning its first interval.
     */
    public void start() {
        if (intervalStartTimeMillis == QueryConstants.NULL_LONG) {
            intervalStartTimeMillis = System.currentTimeMillis();
            intervalStartTimeNanos = System.nanoTime();
        }
    }

    /**
     * Flush this UpdatePerformanceTracker to the downstream publisher and logger, and begin its next interval.
     */
    public void flush() {
        if (intervalStartTimeMillis == QueryConstants.NULL_LONG || intervalStartTimeNanos == QueryConstants.NULL_LONG) {
            throw new IllegalStateException(String.format("UpdatePerformanceTracker %s was never started",
                    updateGraph.getName()));
        }
        final long intervalEndTimeMillis = System.currentTimeMillis();
        final long intervalEndTimeNanos = System.nanoTime();
        // This happens on the primary refresh thread of this UPT's UpdateGraph. It should already have that UG
        // installed in the ExecutionContext. If we need another UG, that's the responsibility of the publish callbacks.
        try {
            finishInterval(
                    getInternalState(),
                    intervalStartTimeMillis,
                    intervalEndTimeMillis,
                    intervalEndTimeNanos - intervalStartTimeNanos);
        } finally {
            intervalStartTimeMillis = intervalEndTimeMillis;
            intervalStartTimeNanos = intervalEndTimeNanos;
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
            if (StringUtils.isNullOrEmpty(description) && uninstrumented) {
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
                    updateGraph.getName()));
        });
        final PerformanceEntry entry = entryMu.getValue();
        if (!unitTestMode) {
            entries.add(new WeakReference<>(entry));
        }

        return entry;
    }

    /**
     * Do entry maintenance, generate an interval performance report table for all active entries, and reset for the
     * next interval. <b>Note:</b> This method is only called under the PeriodicUpdateGraph instance's shared lock. This
     * ensures that no other thread using the same update graph will mutate individual entries. Concurrent additions to
     * {@link #entries} are supported by the underlying data structure.
     *
     * @param internalState internal publishing state
     * @param intervalStartTimeMillis interval start time in millis
     * @param intervalEndTimeMillis interval end time in millis
     * @param intervalDurationNanos interval duration in nanos
     */
    private void finishInterval(
            final InternalState internalState,
            final long intervalStartTimeMillis,
            final long intervalEndTimeMillis,
            final long intervalDurationNanos) {
        /*
         * Visit all entry references. For entries that no longer exist: Remove by index from the entry list. For
         * entries that still exist: If the entry had non-zero usage in this interval, add it to the report. Reset the
         * entry for the next interval.
         */
        final IntervalLevelDetails intervalLevelDetails =
                new IntervalLevelDetails(intervalStartTimeMillis, intervalEndTimeMillis, intervalDurationNanos);
        if (flushEntry.getIntervalInvocationCount() > 0) {
            internalState.publish(intervalLevelDetails, flushEntry);
        }
        flushEntry.reset();
        flushEntry.onUpdateStart();

        for (final Iterator<WeakReference<PerformanceEntry>> it = entries.iterator(); it.hasNext();) {
            final WeakReference<PerformanceEntry> entryReference = it.next();
            final PerformanceEntry entry = entryReference == null ? null : entryReference.get();
            if (entry == null) {
                it.remove();
                continue;
            }

            if (entry.shouldLogEntryInterval()) {
                internalState.publish(intervalLevelDetails, entry);
            } else if (entry.getIntervalInvocationCount() > 0) {
                aggregatedSmallUpdatesEntry.accumulate(entry);
            }
            entry.reset();
        }

        if (aggregatedSmallUpdatesEntry.getIntervalInvocationCount() > 0) {
            internalState.publish(intervalLevelDetails, aggregatedSmallUpdatesEntry);
        }
        aggregatedSmallUpdatesEntry.reset();
        flushEntry.onUpdateEnd();
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
    public static QueryTable getQueryTable() {
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(getInternalState().blink);
    }
}
