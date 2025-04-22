//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListener;
import io.deephaven.engine.tablelogger.EngineTableLoggers;
import io.deephaven.engine.tablelogger.UpdatePerformanceAncestorLogger;
import io.deephaven.engine.tablelogger.UpdatePerformanceLogLogger;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.BaseUpdateGraph;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * <p>
 * This tool is meant to track periodic update events that take place in an {@link UpdateGraph}. This generally
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

    /**
     * Should all UpdatePerformanceTracker entries be logged once, to permit ancestor discovery even if they would
     * otherwise not qualify for logging based on the {@link #LOG_THRESHOLD log threshold}.
     */
    public static final boolean LOG_ALL_ENTRIES_ONCE = Configuration.getInstance().getBooleanForClassWithDefault(
            UpdatePerformanceTracker.class, "logAllEntriesOnce", false);

    private static final Logger log = LoggerFactory.getLogger(UpdatePerformanceTracker.class);

    // aggregate update performance entries less than 500us by default
    static final QueryPerformanceLogThreshold LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Update", 500_000L);

    // We do not want an UpdatePerformanceTracker's start that occurs within a liveness scope to allow these resources
    // to be freed, the INSTANCE should remain live unless it is discarded.
    private static final SingletonLivenessManager livenessManager = new SingletonLivenessManager();
    private static InternalState INSTANCE;

    /**
     * Retrieves or initializes the singleton instance of {@code InternalState}. If the instance does not exist, it is
     * created.
     *
     * @return the singleton instance of {@code InternalState}
     */
    private static InternalState getInternalState() {
        InternalState local;
        if ((local = INSTANCE) == null) {
            synchronized (UpdatePerformanceTracker.class) {
                if ((local = INSTANCE) == null) {
                    try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                        INSTANCE = local = new InternalState();
                        livenessManager.manage(INSTANCE);
                    }
                }
            }
        }
        return local;
    }

    /**
     * Retrieves the singleton instance of {@code InternalState} if it exists; otherwise, returns {@code null}.
     *
     * @return the singleton {@code InternalState} instance if it is initialized, or {@code null} if it is not
     */
    private static InternalState maybeGetInternalState() {
        // we are satisfied to return null here
        return INSTANCE;
    }

    private static class InternalState extends LivenessArtifact {
        private final UpdatePerformanceLogLogger tableLogger;
        private final UpdatePerformanceStreamPublisher publisher;

        private final UpdatePerformanceAncestorLogger ancestorLogger;
        private final UpdatePerformanceAncestorStreamPublisher ancestorPublisher;

        // Eventually, we can close the StreamToBlinkTableAdapter
        @SuppressWarnings("FieldCanBeLocal")
        private final StreamToBlinkTableAdapter adapter;
        private final Table blink;

        // Eventually, we can close the StreamToBlinkTableAdapter
        @SuppressWarnings("FieldCanBeLocal")
        private final StreamToBlinkTableAdapter ancestorAdapter;
        private final Table ancestorBlink;

        private boolean encounteredError = false;

        private InternalState() {
            final UpdateGraph publishingGraph =
                    BaseUpdateGraph.getInstance(BaseUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
            Assert.neqNull(publishingGraph, "The " + BaseUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME + " UpdateGraph "
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
                /*
                 * When blink (and correspondingly ancestorBlink) is
                 * io.deephaven.engine.liveness.ReferenceCountedLivenessReferent.destroy-ed, the adapter is closed. The
                 * adapter then shuts down the publisher, which flushes and releases the chunks that it holds.
                 */
                manage(blink);

                ancestorLogger = EngineTableLoggers.get().updatePerformanceAncestorLogger();
                ancestorPublisher = new UpdatePerformanceAncestorStreamPublisher();
                ancestorAdapter = new StreamToBlinkTableAdapter(
                        UpdatePerformanceAncestorStreamPublisher.definition(),
                        ancestorPublisher,
                        publishingGraph,
                        UpdatePerformanceTracker.class.getName() + "-Ancestors");
                ancestorBlink = ancestorAdapter.table();
                manage(ancestorBlink);
            }
        }

        /**
         * @implNote this method is synchronized to guarantee identical ordering of entries between publisher and
         *           tableLogger; doing so also relieves the requirement that the table logger be thread safe
         */
        private synchronized void publish(
                final IntervalLevelDetails intervalLevelDetails,
                final PerformanceEntry entry) {
            if (encounteredError) {
                return;
            }
            try {
                publisher.add(intervalLevelDetails, entry);
                tableLogger.log(intervalLevelDetails, entry);
            } catch (final IOException e) {
                // Don't want to log this more than once in a report
                log.error().append("Error publishing ").append(entry).append(" caused by: ").append(e).endl();
                encounteredError = true;
            }
        }

        private synchronized void publishAncestor(String updateGraphName, PerformanceEntry entry, long[] ancestors) {
            if (encounteredError) {
                return;
            }
            try {
                ancestorPublisher.add(updateGraphName, entry.getId(), entry.getDescription(), ancestors);
                ancestorLogger.log(updateGraphName, entry.getId(), entry.getDescription(), ancestors);
            } catch (final IOException e) {
                // Don't want to log this more than once in a report
                log.error().append("Error publishing ancestors for ").append(entry).append(" caused by: ").append(e)
                        .endl();
                encounteredError = true;
            }
        }
    }

    private static final AtomicLong entryIdCounter = new AtomicLong(1);

    private final UpdateGraph updateGraph;
    private final PerformanceEntry aggregatedSmallUpdatesEntry;
    private final PerformanceEntry flushEntry;
    private final Queue<WeakReference<PerformanceEntry>> entries = new LinkedBlockingDeque<>();

    private boolean unitTestMode = false;

    private long intervalStartTimeEpochNanos = QueryConstants.NULL_LONG;

    public UpdatePerformanceTracker(final UpdateGraph updateGraph) {
        this.updateGraph = Objects.requireNonNull(updateGraph);
        this.aggregatedSmallUpdatesEntry = new PerformanceEntry(
                QueryConstants.NULL_LONG, QueryConstants.NULL_LONG, QueryConstants.NULL_INT,
                "Aggregated Small Updates", null, updateGraph.getName());
        this.flushEntry = new PerformanceEntry(
                QueryConstants.NULL_LONG, QueryConstants.NULL_LONG, QueryConstants.NULL_INT,
                "UpdatePerformanceTracker Flush", null, updateGraph.getName());
    }

    /**
     * Start this UpdatePerformanceTracker, by beginning its first interval.
     */
    public void start() {
        if (intervalStartTimeEpochNanos == QueryConstants.NULL_LONG) {
            intervalStartTimeEpochNanos = Clock.system().currentTimeNanos();
        }
    }

    /**
     * Flush this UpdatePerformanceTracker to the downstream publisher and logger, and begin its next interval.
     */
    public void flush() {
        if (intervalStartTimeEpochNanos == QueryConstants.NULL_LONG) {
            throw new IllegalStateException(String.format("UpdatePerformanceTracker %s was never started",
                    updateGraph.getName()));
        }
        final long intervalEndTimeEpochNanos = Clock.system().currentTimeNanos();
        // This happens on the primary refresh thread of this UPT's UpdateGraph. It should already have that UG
        // installed in the ExecutionContext. If we need another UG, that's the responsibility of the publish callbacks.
        try {
            finishInterval(
                    getInternalState(),
                    intervalStartTimeEpochNanos,
                    intervalEndTimeEpochNanos);
        } finally {
            intervalStartTimeEpochNanos = intervalEndTimeEpochNanos;
        }
    }

    /**
     * Log an array of ancestors for the provided entry.
     * 
     * @param entry entry of entry to log for
     * @param ancestors array of ancestor ids
     */
    public void logAncestors(String updateGraphName, PerformanceEntry entry, Supplier<long[]> ancestors) {
        final InternalState state = maybeGetInternalState();
        if (state != null) {
            final long[] ancestorArray = ancestors.get();
            if (ancestorArray != null && ancestorArray.length > 0) {
                state.publishAncestor(updateGraphName, entry, ancestorArray);
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
        qpr.supplyQueryData((evaluationNumber, operationNumber, uninstrumented) -> {
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
     * @param intervalStartTimeEpochNanos interval start time in nanoseconds since the epoch
     * @param intervalEndTimeEpochNanos interval end time in nanoseconds since the epoch
     */
    private void finishInterval(
            final InternalState internalState,
            final long intervalStartTimeEpochNanos,
            final long intervalEndTimeEpochNanos) {
        /*
         * Visit all entry references. For entries that no longer exist: Remove by index from the entry list. For
         * entries that still exist: If the entry had non-zero usage in this interval, add it to the report. Reset the
         * entry for the next interval.
         */
        final IntervalLevelDetails intervalLevelDetails =
                new IntervalLevelDetails(intervalStartTimeEpochNanos, intervalEndTimeEpochNanos);
        if (flushEntry.getInvocationCount() > 0) {
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
            } else if (entry.getInvocationCount() > 0) {
                aggregatedSmallUpdatesEntry.accumulate(entry);
            }
            entry.reset();
        }

        if (aggregatedSmallUpdatesEntry.getInvocationCount() > 0) {
            internalState.publish(intervalLevelDetails, aggregatedSmallUpdatesEntry);
        }
        aggregatedSmallUpdatesEntry.reset();
        flushEntry.onUpdateEnd();
    }

    /**
     * Holder for logging details that are the same for every Entry in an interval
     */
    public static class IntervalLevelDetails {
        private final long intervalStartTimeEpochNanos;
        private final long intervalEndTimeEpochNanos;

        IntervalLevelDetails(final long intervalStartTimeEpochNanos, final long intervalEndTimeEpochNanos) {
            this.intervalStartTimeEpochNanos = intervalStartTimeEpochNanos;
            this.intervalEndTimeEpochNanos = intervalEndTimeEpochNanos;
        }

        public long getIntervalStartTimeEpochNanos() {
            return intervalStartTimeEpochNanos;
        }

        public long getIntervalEndTimeEpochNanos() {
            return intervalEndTimeEpochNanos;
        }
    }

    @NotNull
    public static QueryTable getQueryTable() {
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(getInternalState().blink);
    }

    @NotNull
    public static QueryTable getAncestorTable() {
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(getInternalState().ancestorBlink);
    }

    @TestUseOnly
    public static void resetForUnitTests() {
        synchronized (UpdatePerformanceTracker.class) {
            if (INSTANCE == null) {
                return;
            }
            livenessManager.unmanage(INSTANCE);
            INSTANCE = null;
        }
    }
}
