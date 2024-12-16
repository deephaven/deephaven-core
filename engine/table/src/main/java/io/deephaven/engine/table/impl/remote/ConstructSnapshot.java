//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.remote;

import gnu.trove.TIntCollection;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.ColumnSnapshotUnsuccessfulException;
import io.deephaven.engine.exceptions.SnapshotUnsuccessfulException;
import io.deephaven.engine.table.impl.ForkJoinPoolOperationInitializer;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.updategraph.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.updategraph.NotificationQueue.Dependency;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.io.log.LogEntry;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.internal.log.LoggerFactory;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import io.deephaven.chunk.attributes.Values;

import static io.deephaven.engine.table.impl.QueryTable.ENABLE_PARALLEL_SNAPSHOT;
import static io.deephaven.engine.table.impl.QueryTable.MINIMUM_PARALLEL_SNAPSHOT_ROWS;

/**
 * A Set of static utilities for computing values from a table while avoiding the use of an update graph lock. This
 * class supports snapshots in both position space and key space.
 */
public class ConstructSnapshot {

    public static class NoSnapshotAllowedException extends UnsupportedOperationException {
        public NoSnapshotAllowedException() {
            super();
        }

        public NoSnapshotAllowedException(String reason) {
            super(reason);
        }
    }

    private static final io.deephaven.io.logger.Logger log = LoggerFactory.getLogger(ConstructSnapshot.class);

    /**
     * The maximum number of allowed attempts to construct a snapshot concurrently with {@link PeriodicUpdateGraph} run
     * processing. After this many attempts, we fall back and wait until we can block refreshes.
     */
    private static final int MAX_CONCURRENT_ATTEMPTS =
            Configuration.getInstance().getIntegerWithDefault("ConstructSnapshot.maxConcurrentAttempts", 2);

    /**
     * The maximum duration of an attempt to construct a snapshot concurrently with {@link PeriodicUpdateGraph} run
     * processing. If an unsuccessful attempt takes longer than this timeout, we will fall back and wait until we can
     * block refreshes.
     */
    private static final int MAX_CONCURRENT_ATTEMPT_DURATION_MILLIS = Configuration.getInstance()
            .getIntegerWithDefault("ConstructSnapshot.maxConcurrentAttemptDurationMillis", 5000);

    // TODO (deephaven-core#188): use ChunkPoolConstants.LARGEST_POOL_CHUNK_CAPACITY when JS API allows multiple batches
    // default enables more than 100MB of 8-byte values in a single record batch
    public static final int SNAPSHOT_CHUNK_SIZE = Configuration.getInstance()
            .getIntegerWithDefault("ConstructSnapshot.snapshotChunkSize", 1 << 24);

    public interface State {

        /**
         * Test that determines whether the currently active concurrent snapshot attempt has become inconsistent. Always
         * returns {@code false} if there is no snapshot attempt active, or if there is a locked attempt active
         * (necessarily at lower depth than the lowest concurrent attempt).
         *
         * @return Whether the clock or sources have changed in such a way as to make the currently active concurrent
         *         snapshot attempt inconsistent
         */
        boolean concurrentAttemptInconsistent();

        /**
         * Check that fails if the currently active concurrent snapshot attempt has become inconsistent. This is a no-op
         * if there is no snapshot attempt active, or if there is a locked attempt active (necessarily at lower depth
         * than the lowest concurrent attempt).
         *
         * @throws SnapshotInconsistentException If the currently active concurrent snapshot attempt has become
         *         inconsistent
         */
        void failIfConcurrentAttemptInconsistent();

        /**
         * Wait for a dependency to become satisfied on the current cycle if we're trying to use current values for the
         * currently active concurrent snapshot attempt. This is a no-op if there is no snapshot attempt active, or if
         * there is a locked attempt active (necessarily at lower depth than the lowest concurrent attempt).
         *
         * @param dependency The dependency, which may be null to avoid redundant checks in calling code
         * @throws SnapshotInconsistentException If we cannot wait for this dependency on the current step because the
         *         step changed
         */
        void maybeWaitForSatisfaction(@Nullable NotificationQueue.Dependency dependency);

        /**
         * Return the currently active concurrent snapshot attempt's "before" clock value, or zero if there is no
         * concurrent attempt active.
         *
         * @return The concurrent snapshot attempt's "before" clock value, or zero
         */
        long getConcurrentAttemptClockValue();

        /**
         * Append clock info that pertains to the concurrent attempt state to {@code logOutput}.
         *
         * @param logOutput The {@link LogOutput}
         * @return {@code logOutput}
         */
        LogOutput appendConcurrentAttemptClockInfo(@NotNull LogOutput logOutput);
    }

    /**
     * Holder for thread-local state.
     */
    private static class StateImpl implements State {

        /**
         * ThreadLocal to hold an instance per thread.
         */
        private static final ThreadLocal<StateImpl> threadState = ThreadLocal.withInitial(StateImpl::new);

        /**
         * Get this thread's State instance.
         *
         * @return This thread's State instance
         */
        private static StateImpl get() {
            return threadState.get();
        }

        /**
         * Holder for per-snapshot attempt parameters.
         */
        private static class ConcurrentAttemptParameters {

            /**
             * The {@link SnapshotControl} controlling this snapshot attempt.
             */
            private final SnapshotControl control;

            /**
             * The clock value before this snapshot was attempted.
             */
            private final long beforeClockValue;

            /**
             * Whether this snapshot attempt is using previous values.
             */
            private final boolean usingPreviousValues;

            private ConcurrentAttemptParameters(
                    @NotNull final SnapshotControl control,
                    final long beforeClockValue,
                    final boolean usingPreviousValues) {
                this.control = control;
                this.beforeClockValue = beforeClockValue;
                this.usingPreviousValues = usingPreviousValues;
            }
        }

        /**
         * The {@link UpdateGraph} we're interacting with in the current attempt (concurrent or locked). {@code null} if
         * there are no snapshots in progress on this thread.
         */
        private UpdateGraph updateGraph;

        /**
         * {@link ConcurrentAttemptParameters Per-snapshot attempt parameters} for the lowest-depth concurrent snapshot
         * on this thread. {@code null} if there are no concurrent snapshots in progress at any depth on this thread.
         */
        private ConcurrentAttemptParameters activeConcurrentAttempt;

        /**
         * The depth of nested concurrent snapshots. Used to avoid releasing the update graph lock if it's acquired by a
         * nested snapshot. Zero if there are no concurrent snapshots in progress on this thread.
         */
        private int concurrentSnapshotDepth;

        /**
         * The depth of nested locked snapshots. Used to treat nested locked snapshots as non-concurrent for purposes of
         * consistency checks and to avoid releasing the update graph lock if it's acquired by a nested snapshot. Zero
         * if there are no concurrent snapshots in progress on this thread.
         */
        private int lockedSnapshotDepth;

        /**
         * Whether this thread currently has a permit on the shared update graph lock.
         */
        private boolean acquiredLock;

        /**
         * The last {@link LogicalClock} value that this state has observed.
         */
        private long lastObservedClockValue;

        /**
         * Called before starting a concurrent snapshot in order to increase depth and record per-snapshot attempt
         * parameters.
         *
         * @param control The {@link SnapshotControl control} to record
         * @param beforeClockValue The "before" clock value to record
         * @param usingPreviousValues Whether this attempt will use previous values
         * @return An opaque object describing the enclosing attempt, to be supplied to
         *         {@link #endConcurrentSnapshot(Object)}
         */
        private Object startConcurrentSnapshot(
                @NotNull final SnapshotControl control,
                final long beforeClockValue,
                final boolean usingPreviousValues) {
            checkAndRecordUpdateGraph(control.getUpdateGraph());
            Assert.assertion(!locked(updateGraph) && !acquiredLock, "!locked() && !acquiredLock");
            final Object enclosingAttemptState = activeConcurrentAttempt;
            activeConcurrentAttempt = new ConcurrentAttemptParameters(control, beforeClockValue, usingPreviousValues);
            ++concurrentSnapshotDepth;
            lastObservedClockValue = beforeClockValue;
            return enclosingAttemptState;
        }

        /**
         * Called after finishing a concurrent snapshot attempt in order to record the decrease in depth and restore the
         * enclosing attempt's parameters.
         *
         * @param enclosingAttemptParameters The opaque state object returned from
         *        {@link #startConcurrentSnapshot(SnapshotControl, long, boolean)}
         */
        private void endConcurrentSnapshot(final Object enclosingAttemptParameters) {
            --concurrentSnapshotDepth;
            this.activeConcurrentAttempt = (ConcurrentAttemptParameters) enclosingAttemptParameters;
            maybeClearUpdateGraph();
        }

        /**
         * Called before starting a locked snapshot in order to increase depth and acquire the update graph lock if
         * needed.
         *
         * @param control The {@link SnapshotControl control} to record
         */
        private void startLockedSnapshot(@NotNull final SnapshotControl control) {
            checkAndRecordUpdateGraph(control.getUpdateGraph());
            ++lockedSnapshotDepth;
            maybeAcquireLock();
        }

        /**
         * Called after finishing a concurrent snapshot in order to decrease depth and release the update graph lock if
         * needed.
         */
        private void endLockedSnapshot() {
            --lockedSnapshotDepth;
            maybeReleaseLock();
            maybeClearUpdateGraph();
        }

        private void checkAndRecordUpdateGraph(@NotNull final UpdateGraph updateGraph) {
            if (this.updateGraph == null) {
                this.updateGraph = updateGraph;
                return;
            }
            if (this.updateGraph == updateGraph) {
                return;
            }
            throw new UnsupportedOperationException(String.format(
                    "Cannot nest snapshots that use different update graphs: currently using %s, attempting to use %s",
                    this.updateGraph, updateGraph));
        }

        private void maybeClearUpdateGraph() {
            if (concurrentSnapshotDepth == 0 && lockedSnapshotDepth == 0) {
                this.updateGraph = null;
            }
        }

        /**
         * Is the current lowest-depth snapshot attempt concurrent?
         *
         * @return Whether there is a concurrent snapshot attempt active at the lowest depth
         */
        private boolean concurrentAttemptActive() {
            return concurrentSnapshotDepth > 0 && lockedSnapshotDepth == 0;
        }

        @Override
        public boolean concurrentAttemptInconsistent() {
            if (!concurrentAttemptActive()) {
                return false;
            }
            if (!clockConsistent(
                    activeConcurrentAttempt.beforeClockValue,
                    lastObservedClockValue = updateGraph.clock().currentValue(),
                    activeConcurrentAttempt.usingPreviousValues)) {
                return true;
            }
            return !activeConcurrentAttempt.control.snapshotConsistent(
                    lastObservedClockValue,
                    activeConcurrentAttempt.usingPreviousValues);
        }

        @Override
        public void failIfConcurrentAttemptInconsistent() {
            if (concurrentAttemptInconsistent()) {
                throw new SnapshotInconsistentException();
            }
        }

        @Override
        public void maybeWaitForSatisfaction(@Nullable final NotificationQueue.Dependency dependency) {
            if (!concurrentAttemptActive()
                    || dependency == null
                    || activeConcurrentAttempt.usingPreviousValues
                    || LogicalClock.getState(activeConcurrentAttempt.beforeClockValue) == LogicalClock.State.Idle) {
                // No cycle or dependency to wait for.
                return;
            }
            final long beforeStep = LogicalClock.getStep(activeConcurrentAttempt.beforeClockValue);
            // Wait for satisfaction if necessary
            if (dependency.satisfied(beforeStep)
                    || WaitNotification.waitForSatisfaction(beforeStep, dependency)) {
                return;
            }
            lastObservedClockValue = updateGraph.clock().currentValue();
            // Blow up if we've detected a step change
            if (LogicalClock.getStep(lastObservedClockValue) != beforeStep) {
                throw new SnapshotInconsistentException();
            }
        }

        @Override
        public long getConcurrentAttemptClockValue() {
            return concurrentAttemptActive() ? activeConcurrentAttempt.beforeClockValue : 0;
        }

        @Override
        public LogOutput appendConcurrentAttemptClockInfo(@NotNull final LogOutput logOutput) {
            logOutput.append("concurrent snapshot state: ");
            if (concurrentAttemptActive()) {
                logOutput.append("active, beforeClockValue=").append(activeConcurrentAttempt.beforeClockValue)
                        .append(", usingPreviousValues=").append(activeConcurrentAttempt.usingPreviousValues);
            } else {
                logOutput.append("inactive");
            }
            return logOutput.append(", lastObservedClockValue=").append(lastObservedClockValue);
        }

        /**
         * Check whether this thread currently holds a lock on {@code updateGraph}.
         *
         * @param updateGraph The {@link UpdateGraph}
         * @return Whether this thread currently holds a lock on {@code updateGraph}.
         */
        private static boolean locked(@NotNull final UpdateGraph updateGraph) {
            return updateGraph.sharedLock().isHeldByCurrentThread()
                    || updateGraph.exclusiveLock().isHeldByCurrentThread();
        }

        /**
         * Acquire a shared update graph lock if necessary.
         */
        private void maybeAcquireLock() {
            if (updateGraph.currentThreadProcessesUpdates() || locked(updateGraph)) {
                return;
            }
            updateGraph.sharedLock().lock();
            acquiredLock = true;
        }

        /**
         * Release a shared update graph lock if necessary.
         */
        private void maybeReleaseLock() {
            if (acquiredLock && concurrentSnapshotDepth == 0 && lockedSnapshotDepth == 0) {
                updateGraph.sharedLock().unlock();
                acquiredLock = false;
            }
        }
    }

    /**
     * Exception thrown for "fail-fast" purposes when it's been detected that a snapshot will fail.
     */
    public static class SnapshotInconsistentException extends UncheckedDeephavenException {
    }

    /**
     * Test whether the logical clock has remained sufficiently consistent to allow a snapshot function to succeed. Note
     * that this is a necessary but not sufficient test in most cases; this test is invoked (in short-circuit fashion)
     * before invoking @link SnapshotControl#snapshotConsistent(long, boolean)} (long, boolean)}.
     *
     * @param beforeClockValue The clock value from before the snapshot was attempted
     * @param afterClockValue The clock value from after the snapshot was attempted
     * @param usedPrev Whether the snapshot used previous values
     * @return Whether the snapshot succeeded (based on clock changes)
     */
    private static boolean clockConsistent(
            final long beforeClockValue,
            final long afterClockValue,
            final boolean usedPrev) {
        final boolean stepSame = LogicalClock.getStep(beforeClockValue) == LogicalClock.getStep(afterClockValue);
        final boolean stateSame = LogicalClock.getState(beforeClockValue) == LogicalClock.getState(afterClockValue);
        return stepSame && (stateSame || !usedPrev);
    }

    /**
     * Get the currently active snapshot state.
     *
     * @return the currently active snapshot state
     */
    public static State state() {
        return StateImpl.get();
    }

    /**
     * Test that determines whether the currently active concurrent snapshot attempt has become inconsistent. Always
     * returns {@code false} if there is no snapshot attempt active, or if there is a locked attempt active (necessarily
     * at lower depth than the lowest concurrent attempt).
     *
     * <p>
     * Equivalent to {@code state().concurrentAttemptInconsistent()}.
     *
     * @return Whether the clock or sources have changed in such a way as to make the currently active concurrent
     *         snapshot attempt inconsistent
     * @see State#concurrentAttemptInconsistent()
     */
    public static boolean concurrentAttemptInconsistent() {
        return state().concurrentAttemptInconsistent();
    }

    /**
     * Check that fails if the currently active concurrent snapshot attempt has become inconsistent. This is a no-op if
     * there is no snapshot attempt active, or if there is a locked attempt active (necessarily at lower depth than the
     * lowest concurrent attempt).
     *
     * <p>
     * Equivalent to {@code state().failIfConcurrentAttemptInconsistent()}.
     *
     * @throws SnapshotInconsistentException If the currently active concurrent snapshot attempt has become inconsistent
     * @see State#failIfConcurrentAttemptInconsistent()
     */
    public static void failIfConcurrentAttemptInconsistent() {
        state().failIfConcurrentAttemptInconsistent();
    }

    /**
     * Wait for a dependency to become satisfied on the current cycle if we're trying to use current values for the
     * currently active concurrent snapshot attempt. This is a no-op if there is no snapshot attempt active, or if there
     * is a locked attempt active (necessarily at lower depth than the lowest concurrent attempt).
     *
     * <p>
     * Equivalent to {@code state().maybeWaitForSatisfaction(dependency)}.
     *
     * @param dependency The dependency, which may be null to avoid redundant checks in calling code
     * @throws SnapshotInconsistentException If we cannot wait for this dependency on the current step because the step
     *         changed
     * @see State#maybeWaitForSatisfaction(Dependency)
     */
    public static void maybeWaitForSatisfaction(@Nullable final NotificationQueue.Dependency dependency) {
        state().maybeWaitForSatisfaction(dependency);
    }

    /**
     * Return the currently active concurrent snapshot attempt's "before" clock value, or zero if there is no concurrent
     * attempt active.
     *
     * <p>
     * Equivalent to {@code state().getConcurrentAttemptClockValue()}.
     *
     * @return The concurrent snapshot attempt's "before" clock value, or zero
     * @see State#getConcurrentAttemptClockValue()
     */
    public static long getConcurrentAttemptClockValue() {
        return state().getConcurrentAttemptClockValue();
    }

    /**
     * Append clock info that pertains to the concurrent attempt state to {@code logOutput}.
     *
     * <p>
     * Equivalent to {@code state().appendConcurrentAttemptClockInfo(logOutput)}.
     *
     * @param logOutput The {@link LogOutput}
     * @return {@code logOutput}
     * @see State#appendConcurrentAttemptClockInfo(LogOutput)
     */
    @SuppressWarnings("UnusedReturnValue")
    public static LogOutput appendConcurrentAttemptClockInfo(@NotNull final LogOutput logOutput) {
        return state().appendConcurrentAttemptClockInfo(logOutput);
    }

    /**
     * Create a {@link BarrageMessage snapshot} of the specified table including all columns and rows. Note that this
     * method is notification-oblivious, i.e. it makes no attempt to ensure that notifications are not missed.
     *
     * @param logIdentityObject An object used to prepend to log rows.
     * @param table the table to snapshot.
     * @return a snapshot of the entire base table.
     */
    public static BarrageMessage constructBackplaneSnapshot(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table) {
        return constructBackplaneSnapshotInPositionSpace(logIdentityObject, table, null, null, null);
    }

    /**
     * Create a {@link BarrageMessage snapshot} of the specified table using a set of requested columns and positions.
     * Note that this method uses a RowSet that is in position space, and that it is notification-oblivious, i.e. it
     * makes no attempt to ensure that notifications are not missed.
     *
     * @param logIdentityObject An object used to prepend to log rows.
     * @param table the table to snapshot.
     * @param columnsToSnapshot A {@link BitSet} of columns to include, null for all
     * @param positionsToSnapshot RowSet of positions within the table to include, null for all
     * @return a snapshot of the entire base table.
     */
    public static BarrageMessage constructBackplaneSnapshotInPositionSpace(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSnapshot,
            @Nullable final RowSequence positionsToSnapshot,
            @Nullable final RowSequence reversePositionsToSnapshot) {
        return constructBackplaneSnapshotInPositionSpace(logIdentityObject, table, columnsToSnapshot,
                positionsToSnapshot, reversePositionsToSnapshot,
                makeSnapshotControl(false, table.isRefreshing(), table));
    }

    /**
     * Create a {@link BarrageMessage snapshot} of the specified table using a set of requested columns and positions.
     * Note that this method uses a RowSet that is in position space.
     *
     * @param logIdentityObject An object used to prepend to log rows.
     * @param table the table to snapshot.
     * @param columnsToSnapshot A {@link BitSet} of columns to include, null for all
     * @param positionsToSnapshot A RowSequence of positions within the table to include, null for all
     * @param reversePositionsToSnapshot A RowSequence of reverse positions within the table to include, null for all
     * @param control A {@link SnapshotControl} to define the parameters and consistency for this snapshot
     * @return a snapshot of the entire base table.
     */
    public static BarrageMessage constructBackplaneSnapshotInPositionSpace(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSnapshot,
            @Nullable final RowSequence positionsToSnapshot,
            @Nullable final RowSequence reversePositionsToSnapshot,
            @NotNull final SnapshotControl control) {
        final UpdateGraph updateGraph = table.getUpdateGraph();
        final MutableObject<BarrageMessage> snapshotMsg = new MutableObject<>();
        // Use Fork-Join thread pool for parallel snapshotting
        try (final SafeCloseable ignored1 = ExecutionContext.getContext()
                .withUpdateGraph(updateGraph)
                .withOperationInitializer(ForkJoinPoolOperationInitializer.fromCommonPool())
                .open()) {
            final SnapshotFunction doSnapshot = (usePrev, beforeClockValue) -> {
                final BarrageMessage snapshot = new BarrageMessage();
                snapshot.isSnapshot = true;
                snapshot.shifted = RowSetShiftData.EMPTY;
                final RowSet keysToSnapshot;
                if (positionsToSnapshot == null && reversePositionsToSnapshot == null) {
                    keysToSnapshot = null;
                } else {
                    final RowSet rowSetToUse = usePrev ? table.getRowSet().prev() : table.getRowSet();
                    final WritableRowSet forwardKeys =
                            positionsToSnapshot == null ? null : rowSetToUse.subSetForPositions(positionsToSnapshot);
                    final RowSet reverseKeys = reversePositionsToSnapshot == null ? null
                            : rowSetToUse.subSetForReversePositions(reversePositionsToSnapshot);
                    if (forwardKeys != null) {
                        if (reverseKeys != null) {
                            forwardKeys.insert(reverseKeys);
                            reverseKeys.close();
                        }
                        keysToSnapshot = forwardKeys;
                    } else {
                        keysToSnapshot = reverseKeys;
                    }
                }
                try (final RowSet ignored = keysToSnapshot) {
                    final boolean success = snapshotAllTable(usePrev, snapshot, table, logIdentityObject,
                            columnsToSnapshot, keysToSnapshot);
                    if (success) {
                        snapshotMsg.setValue(snapshot);
                    } else {
                        snapshot.close();
                    }
                    return success;
                } catch (final Throwable e) {
                    snapshot.close();
                    throw e;
                }
            };
            final long clockStep =
                    callDataSnapshotFunction(System.identityHashCode(logIdentityObject), control, doSnapshot);
            final BarrageMessage snapshot = snapshotMsg.getValue();
            snapshot.firstSeq = snapshot.lastSeq = clockStep;
            return snapshot;
        }
    }

    @FunctionalInterface
    public interface SnapshotFunction {
        /**
         * A function that would like to take a snapshot of data guarded by a retry loop with data consistency tests.
         *
         * @param usePrev Whether data from the previous cycle should be used (otherwise use this cycle)
         * @param beforeClockValue The clock value that we captured before the function began; the function can use this
         *        value to bail out early if it notices something has gone wrong; {@value LogicalClock#NULL_CLOCK_VALUE}
         *        for static snapshots
         * @return true if the function was successful, false if it should be retried
         */
        boolean call(boolean usePrev, long beforeClockValue);
    }

    /**
     * Interface for {@link #usePreviousValues(long)}.
     */
    @FunctionalInterface
    public interface UsePreviousValues {
        /**
         * <p>
         * Determine if previous values should be used in table data access for the given {@link LogicalClock clock}
         * value.
         * <p>
         * Expected to never request previous values during the idle phase of a cycle.
         * <p>
         * Must never request previous values for a source that has already been updated on the current cycle, unless it
         * can be proven that that source was not instantiated on the current cycle.
         * <p>
         * Must be safe to call more than once, exactly once per snapshot attempt.
         *
         * @param beforeClockValue The current clock value before the snapshot function will be invoked
         * @return A {@link Boolean} with the following meaning:
         *         <ul>
         *         <li>{@code true} if previous values should be used</li>
         *         <li>{@code false} if they should not</li>
         *         <li>{@code null} if a clock discrepancy was detected and we must retry with a new
         *         {@code beforeClockValue}</li>
         *         </ul>
         */
        Boolean usePreviousValues(long beforeClockValue);
    }

    /**
     * Interface for {@link #snapshotConsistent(long, boolean)}.
     */
    @FunctionalInterface
    public interface SnapshotConsistent {
        /**
         * <p>
         * Determine (from within a snapshot function) if the snapshot appears to still be consistent.
         * <p>
         * This should be no more restrictive than the associated {@link SnapshotCompletedConsistently}.
         * <p>
         * Can assume as a precondition that the clock step has not been observed to change since the last time the
         * associated {@link UsePreviousValues#usePreviousValues(long)} was invoked, and that the clock state has not
         * been observed to change if previous values were used. See {@link #clockConsistent(long, long, boolean)}.
         *
         * @param currentClockValue The current clock value
         * @param usingPreviousValues Whether the snapshot function is using previous values
         * @return True if we can no longer expect that the snapshot function's result will be consistent
         */
        boolean snapshotConsistent(long currentClockValue, boolean usingPreviousValues);
    }

    /**
     * Interface for {@link #snapshotCompletedConsistently(long, boolean)}.
     */
    @FunctionalInterface
    public interface SnapshotCompletedConsistently {
        /**
         * <p>
         * Determine if a snapshot was consistent according to the clock cycle. Intended to be paired with a
         * {@link UsePreviousValues} function.
         * <p>
         * Can assume as a precondition that the clock step has not been observed to change since the last time the
         * associated {@link UsePreviousValues#usePreviousValues(long)} was invoked, and that the clock state has not
         * been observed to change if previous values were used. See {@link #clockConsistent(long, long, boolean)}.
         * <p>
         * Will be called at most once per snapshot attempt. Will be called for all possibly-successful snapshot
         * attempts. May be called after unsuccessful concurrent snapshot attempts.
         *
         * @param afterClockValue The current clock value after the snapshot function was invoked
         * @param usedPreviousValues If previous values were used
         * @return Whether the snapshot is provably consistent
         * @throws RuntimeException If the snapshot was consistent but the snapshot function failed to satisfy this
         *         function's expectations; this will be treated as a failure of the snapshot function
         */
        boolean snapshotCompletedConsistently(long afterClockValue, boolean usedPreviousValues);
    }

    /**
     * Interface used to control snapshot behavior, including previous value usage and consistency testing.
     */
    public interface SnapshotControl extends UsePreviousValues, SnapshotConsistent, SnapshotCompletedConsistently {

        @Override
        default boolean snapshotCompletedConsistently(final long afterClockValue, final boolean usedPreviousValues) {
            return snapshotConsistent(afterClockValue, usedPreviousValues);
        }

        /**
         * @return The {@link UpdateGraph} that applies for this snapshot; {@code null} for snapshots of static data,
         *         which can skip all consistency-related considerations
         */
        UpdateGraph getUpdateGraph();
    }

    /**
     * An implementation of {@link SnapshotControl} for sources that cannot update.
     */
    public static final class StaticSnapshotControl implements SnapshotControl {

        public static final SnapshotControl INSTANCE = new StaticSnapshotControl();

        private StaticSnapshotControl() {}

        @Override
        public Boolean usePreviousValues(long beforeClockValue) {
            // noinspection AutoBoxing
            return false;
        }

        @Override
        public boolean snapshotConsistent(long currentClockValue, boolean usingPreviousValues) {
            return true;
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return null;
        }
    }

    /**
     * Make a {@link SnapshotControl} from individual function objects.
     *
     * @param updateGraph The {@link UpdateGraph} for the snapshot
     * @param usePreviousValues The {@link UsePreviousValues} to use
     * @param snapshotConsistent The {@link SnapshotConsistent} to use
     * @param snapshotCompletedConsistently The {@link SnapshotCompletedConsistently} to use, or null to use {@code
     * snapshotConsistent}
     */
    public static SnapshotControl makeSnapshotControl(
            @NotNull final UpdateGraph updateGraph,
            @NotNull final UsePreviousValues usePreviousValues,
            @NotNull final SnapshotConsistent snapshotConsistent,
            @Nullable final SnapshotCompletedConsistently snapshotCompletedConsistently) {
        return snapshotCompletedConsistently == null
                ? new SnapshotControlAdapter(
                        updateGraph, usePreviousValues, snapshotConsistent, snapshotConsistent::snapshotConsistent)
                : new SnapshotControlAdapter(
                        updateGraph, usePreviousValues, snapshotConsistent, snapshotCompletedConsistently);
    }

    /**
     * Adapter to combine the individual component functions of {@link SnapshotControl} into a valid snapshot control.
     */
    private static class SnapshotControlAdapter implements SnapshotControl {

        private final UpdateGraph updateGraph;
        private final UsePreviousValues usePreviousValues;
        private final SnapshotConsistent snapshotConsistent;
        private final SnapshotCompletedConsistently snapshotCompletedConsistently;

        /**
         * Make a SnapshotControlAdapter.
         *
         * @param updateGraph The {@link UpdateGraph} to use
         * @param usePreviousValues The {@link UsePreviousValues} to use
         * @param snapshotConsistent The {@link SnapshotConsistent} to use
         * @param snapshotCompletedConsistently The {@link SnapshotCompletedConsistently} to use
         */
        private SnapshotControlAdapter(
                @NotNull final UpdateGraph updateGraph,
                @NotNull final UsePreviousValues usePreviousValues,
                @NotNull final SnapshotConsistent snapshotConsistent,
                @NotNull final SnapshotCompletedConsistently snapshotCompletedConsistently) {
            this.updateGraph = updateGraph;
            this.usePreviousValues = usePreviousValues;
            this.snapshotConsistent = snapshotConsistent;
            this.snapshotCompletedConsistently = snapshotCompletedConsistently;
        }

        @Override
        public Boolean usePreviousValues(final long beforeClockValue) {
            return usePreviousValues.usePreviousValues(beforeClockValue);
        }

        @Override
        public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
            return snapshotConsistent.snapshotConsistent(currentClockValue, usingPreviousValues);
        }

        @Override
        public boolean snapshotCompletedConsistently(final long afterClockValue, final boolean usedPreviousValues) {
            return snapshotCompletedConsistently.snapshotCompletedConsistently(afterClockValue, usedPreviousValues);
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return updateGraph;
        }
    }

    /**
     * Make a default {@link SnapshotControl} for a single source.
     *
     * @param notificationAware Whether the result should be concerned with not missing notifications
     * @param refreshing Whether the data source (usually a {@link Table} table) is refreshing (vs static)
     * @param source The source
     * @return An appropriate {@link SnapshotControl}
     */
    public static SnapshotControl makeSnapshotControl(
            final boolean notificationAware,
            final boolean refreshing,
            @NotNull final NotificationStepSource source) {
        return refreshing
                ? notificationAware
                        ? new NotificationAwareSingleSourceSnapshotControl(source)
                        : new NotificationObliviousSingleSourceSnapshotControl(source)
                : StaticSnapshotControl.INSTANCE;
    }

    /**
     * Make a default {@link SnapshotControl} for one or more sources.
     *
     * @param notificationAware Whether the result should be concerned with not missing notifications
     * @param refreshing Whether any of the data sources (usually {@link Table tables}) are refreshing (vs static)
     * @param sources The sources
     * @return An appropriate {@link SnapshotControl}
     */
    public static SnapshotControl makeSnapshotControl(
            final boolean notificationAware,
            final boolean refreshing,
            @NotNull final NotificationStepSource... sources) {
        if (sources.length == 1) {
            return makeSnapshotControl(notificationAware, refreshing, sources[0]);
        }
        return refreshing
                ? notificationAware
                        ? new NotificationAwareMultipleSourceSnapshotControl(sources)
                        : new NotificationObliviousMultipleSourceSnapshotControl(sources)
                : StaticSnapshotControl.INSTANCE;
    }

    /**
     * Base class for SnapshotControl implementations driven by a single data source.
     */
    private static abstract class SingleSourceSnapshotControl implements ConstructSnapshot.SnapshotControl {

        protected final NotificationStepSource source;

        public SingleSourceSnapshotControl(@NotNull final NotificationStepSource source) {
            this.source = source;
        }

        @Override
        public Boolean usePreviousValues(final long beforeClockValue) {
            final long beforeStep = LogicalClock.getStep(beforeClockValue);
            final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);

            try {
                // noinspection AutoBoxing
                return beforeState == LogicalClock.State.Updating
                        && source.getLastNotificationStep() != beforeStep
                        && !source.satisfied(beforeStep);
            } catch (ClockInconsistencyException e) {
                return null;
            }
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return source.getUpdateGraph();
        }
    }

    /**
     * A SnapshotControl implementation driven by a single data source for use cases when the snapshot function must not
     * miss a notification. For use when instantiating concurrent consumers of all updates from a source.
     */
    private static class NotificationAwareSingleSourceSnapshotControl extends SingleSourceSnapshotControl {

        private NotificationAwareSingleSourceSnapshotControl(@NotNull final NotificationStepSource source) {
            super(source);
        }

        @Override
        public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
            if (!usingPreviousValues) {
                // Cycle was Idle or we had already ticked, and so we're using current values on the current cycle:
                // Success
                return true;
            }
            // If we didn't miss an update then we're succeeding using previous values, else we've failed
            return source.getLastNotificationStep() != LogicalClock.getStep(currentClockValue);
        }
    }

    /**
     * A SnapshotControl implementation driven by a single data source for use cases when the snapshot function doesn't
     * care if it misses a notification. For use by consistent consumers of consistent current state.
     */
    private static class NotificationObliviousSingleSourceSnapshotControl extends SingleSourceSnapshotControl {

        private NotificationObliviousSingleSourceSnapshotControl(@NotNull final NotificationStepSource source) {
            super(source);
        }

        @Override
        public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
            return true;
        }
    }

    /**
     * Base class for SnapshotControl implementations driven by multiple data sources.
     */
    private static abstract class MultipleSourceSnapshotControl implements ConstructSnapshot.SnapshotControl {

        protected final NotificationStepSource[] sources;

        private final UpdateGraph updateGraph;

        private MultipleSourceSnapshotControl(@NotNull final NotificationStepSource... sources) {
            this.sources = sources;
            // Note that we can avoid copying a suffix of the sources array by just effectively passing source[0] twice.
            updateGraph = sources[0].getUpdateGraph(sources);
        }

        @SuppressWarnings("AutoBoxing")
        @Override
        public Boolean usePreviousValues(final long beforeClockValue) {
            if (LogicalClock.getState(beforeClockValue) != LogicalClock.State.Updating) {
                return false;
            }
            final long beforeStep = LogicalClock.getStep(beforeClockValue);
            final NotificationStepSource[] notYetSatisfied;
            try {
                notYetSatisfied = Stream.of(sources)
                        .filter((final NotificationStepSource source) -> source.getLastNotificationStep() != beforeStep
                                && !source.satisfied(beforeStep))
                        .toArray(NotificationStepSource[]::new);
            } catch (ClockInconsistencyException e) {
                return null;
            }
            if (notYetSatisfied.length == sources.length) {
                return true;
            }
            if (notYetSatisfied.length > 0 && !WaitNotification.waitForSatisfaction(beforeStep, notYetSatisfied)) {
                if (updateGraph.clock().currentStep() != beforeStep) {
                    // If we missed a step change, we've already failed, request a do-over.
                    return null;
                }
            }
            return false;
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return updateGraph;
        }
    }

    /**
     * A SnapshotControl implementation driven by multiple data sources for use cases when the snapshot function must
     * not miss a notification. Waits for all sources to be notified on this cycle if any has been notified on this
     * cycle. For use when instantiating concurrent consumers of all updates from a set of sources.
     */
    private static class NotificationAwareMultipleSourceSnapshotControl extends MultipleSourceSnapshotControl {

        private NotificationAwareMultipleSourceSnapshotControl(@NotNull final NotificationStepSource... sources) {
            super(sources);
        }

        @Override
        public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
            if (!usingPreviousValues) {
                // Cycle was Idle or we had already ticked, and so we're using current values on the current cycle:
                // Success
                return true;
            }
            // If we didn't miss an update then we're succeeding using previous values, else we've failed
            final long currentStep = LogicalClock.getStep(currentClockValue);
            return Stream.of(sources)
                    .allMatch((final NotificationStepSource source) -> source.getLastNotificationStep() != currentStep);
        }
    }

    /**
     * A SnapshotControl implementation driven by multiple data sources for use cases when the snapshot function must
     * not miss a notification. Waits for all sources to be notified on this cycle if any has been notified on this
     * cycle. For use by consistent consumers of consistent current state.
     */
    private static class NotificationObliviousMultipleSourceSnapshotControl extends MultipleSourceSnapshotControl {

        private NotificationObliviousMultipleSourceSnapshotControl(@NotNull final NotificationStepSource... sources) {
            super(sources);
        }

        @Override
        public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
            return true;
        }
    }

    private static long callDataSnapshotFunction(
            final int logInt,
            @NotNull final SnapshotControl control,
            @NotNull final SnapshotFunction function) {
        return callDataSnapshotFunction(logOutput -> logOutput.append(logInt), control, function);
    }

    /**
     * Invokes the snapshot function in a loop until it succeeds with provably consistent results, or until
     * {@code MAX_CONCURRENT_ATTEMPTS} or {@code MAX_CONCURRENT_ATTEMPT_DURATION_MILLIS} are exceeded. Falls back to
     * acquiring a shared update graph lock for a final attempt.
     *
     * @param logPrefix A prefix for our log messages
     * @param control A {@link SnapshotControl} to define the parameters and consistency for this snapshot
     * @param function The function to execute
     * @return The logical clock step that applied to this snapshot
     */
    public static long callDataSnapshotFunction(
            @NotNull final String logPrefix,
            @NotNull final SnapshotControl control,
            @NotNull final SnapshotFunction function) {
        return callDataSnapshotFunction((final LogOutput logOutput) -> logOutput.append(logPrefix), control, function);
    }

    /**
     * Invokes the snapshot function in a loop until it succeeds with provably consistent results, or until
     * {@code MAX_CONCURRENT_ATTEMPTS} or {@code MAX_CONCURRENT_ATTEMPT_DURATION_MILLIS} are exceeded. Falls back to
     * acquiring a shared update graph lock for a final attempt.
     * <p>
     * The supplied {@link SnapshotControl}'s {@link SnapshotControl#usePreviousValues usePreviousValues} will be
     * invoked at the start of any snapshot attempt, and its {@link SnapshotControl#snapshotCompletedConsistently
     * snapshotCompletedConsistently} will be invoked at the end of any snapshot attempt that is not provably
     * inconsistent.
     * <p>
     * If the supplied {@link SnapshotControl} provides a null {@link SnapshotControl#getUpdateGraph UpdateGraph}, then
     * this method will perform a static snapshot without locks or retrying. In this case, the {@link SnapshotControl}'s
     * {@link SnapshotControl#usePreviousValues usePreviousValues} must return {@code false},
     * {@link SnapshotControl#snapshotCompletedConsistently snapshotCompletedConsistently} must return {@code true}, and
     * the {@link LogicalClock#NULL_CLOCK_VALUE NULL_CLOCK_VALUE} will be supplied to {@code usePreviousValues} and
     * {@code snapshotCompletedConsistently}.
     *
     * @param logPrefix A prefix for our log messages
     * @param control A {@link SnapshotControl} to define the parameters and consistency for this snapshot
     * @param function The function to execute
     * @return The logical clock step that applied to this snapshot; {@value LogicalClock#NULL_CLOCK_VALUE} for static
     *         snapshots
     */
    public static long callDataSnapshotFunction(
            @NotNull final LogOutputAppendable logPrefix,
            @NotNull final SnapshotControl control,
            @NotNull final SnapshotFunction function) {
        final long overallStart = System.currentTimeMillis();
        final StateImpl state = StateImpl.get();
        final UpdateGraph updateGraph = control.getUpdateGraph();

        if (updateGraph == null) {
            // This is a snapshot of static data. Just call the function with no frippery.
            final boolean controlUsePrev = control.usePreviousValues(LogicalClock.NULL_CLOCK_VALUE);
            if (controlUsePrev) {
                throw new SnapshotUnsuccessfulException("Static snapshot requested previous values");
            }

            final boolean functionSuccessful = function.call(false, LogicalClock.NULL_CLOCK_VALUE);
            if (!functionSuccessful) {
                throw new SnapshotUnsuccessfulException("Static snapshot failed to execute snapshot function");
            }
            if (log.isDebugEnabled()) {
                final long duration = System.currentTimeMillis() - overallStart;
                log.debug().append(logPrefix)
                        .append(" Static snapshot function elapsed time ").append(duration).append(" ms").endl();
            }

            // notify control of successful snapshot
            final boolean controlSuccessful =
                    control.snapshotCompletedConsistently(LogicalClock.NULL_CLOCK_VALUE, false);
            if (!controlSuccessful) {
                throw new SnapshotUnsuccessfulException("Static snapshot function succeeded but control failed");
            }
            return LogicalClock.NULL_CLOCK_VALUE;
        }

        final boolean onUpdateThread = updateGraph.currentThreadProcessesUpdates();
        final boolean alreadyLocked = StateImpl.locked(updateGraph);

        boolean snapshotSuccessful = false;
        boolean functionSuccessful = false;
        Exception caughtException = null;
        long step = 0;
        int delay = 10;
        int numConcurrentAttempts = 0;

        final LivenessManager initialLivenessManager = LivenessScopeStack.peek();
        while (numConcurrentAttempts < MAX_CONCURRENT_ATTEMPTS && !alreadyLocked && !onUpdateThread) {
            ++numConcurrentAttempts;
            final long beforeClockValue = updateGraph.clock().currentValue();
            final long attemptStart = System.currentTimeMillis();

            final Boolean previousValuesRequested = control.usePreviousValues(beforeClockValue);
            if (previousValuesRequested == null) {
                // usePreviousValues detected a step change; we should try again
                continue;
            }
            // noinspection AutoUnboxing
            final boolean usePrev = previousValuesRequested;
            if (LogicalClock.getState(beforeClockValue) == LogicalClock.State.Idle && usePrev) {
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted("Previous values requested while not updating: " + beforeClockValue);
            }

            final long attemptDurationMillis;

            final LivenessScope snapshotLivenessScope = new LivenessScope();
            try (final SafeCloseable ignored = LivenessScopeStack.open(snapshotLivenessScope, true)) {
                final Object startObject = state.startConcurrentSnapshot(control, beforeClockValue, usePrev);
                try {
                    functionSuccessful = function.call(usePrev, beforeClockValue);
                } catch (NoSnapshotAllowedException ex) {
                    // Breaking here will force an update graph lock acquire.
                    // TODO: Optimization. If this exception is only used for cases when we can't use previous values,
                    // then we could simply wait for the source to become satisfied on this cycle, rather than
                    // waiting for the update graph lock. Likely requires work for all code that uses this pattern.
                    if (log.isDebugEnabled()) {
                        log.debug().append(logPrefix).append(" Disallowed concurrent snapshot function took ")
                                .append(System.currentTimeMillis() - attemptStart).append("ms")
                                .append(", beforeClockValue=").append(beforeClockValue)
                                .append(", afterClockValue=").append(updateGraph.clock().currentValue())
                                .append(", usePrev=").append(usePrev)
                                .endl();
                    }
                    break;
                } catch (SnapshotUnsuccessfulException sue) {
                    // The snapshot function below detected a failure despite a consistent state. We should not
                    // re-attempt, or re-check the consistency ourselves.
                    throw sue;
                } catch (Exception e) {
                    functionSuccessful = false;
                    caughtException = e;
                } finally {
                    state.endConcurrentSnapshot(startObject);
                }

                final long afterClockValue = updateGraph.clock().currentValue();
                try {
                    snapshotSuccessful = clockConsistent(beforeClockValue, afterClockValue, usePrev)
                            && control.snapshotCompletedConsistently(afterClockValue, usePrev);
                } catch (Exception e) {
                    if (functionSuccessful) {
                        // Treat this exception as a snapshot function failure despite consistent snapshot
                        functionSuccessful = false;
                        caughtException = e;
                        snapshotSuccessful = true;
                    } else if (log.isDebugEnabled()) {
                        log.debug().append(logPrefix)
                                .append(" Suppressed exception from snapshot success function: ").append(e).endl();
                    }
                }
                attemptDurationMillis = System.currentTimeMillis() - attemptStart;
                if (log.isDebugEnabled()) {
                    log.debug().append(logPrefix).append(" Concurrent snapshot function took ")
                            .append(attemptDurationMillis).append("ms")
                            .append(", snapshotSuccessful=").append(snapshotSuccessful)
                            .append(", functionSuccessful=").append(functionSuccessful)
                            .append(", beforeClockValue=").append(beforeClockValue)
                            .append(", afterClockValue=").append(afterClockValue)
                            .append(", usePrev=").append(usePrev)
                            .endl();
                }
                if (snapshotSuccessful) {
                    if (functionSuccessful) {
                        step = LogicalClock.getStep(beforeClockValue) - (usePrev ? 1 : 0);
                        snapshotLivenessScope.transferTo(initialLivenessManager);
                    }
                    break;
                }
            }
            if (attemptDurationMillis > MAX_CONCURRENT_ATTEMPT_DURATION_MILLIS) {
                if (log.isDebugEnabled()) {
                    log.debug().append(logPrefix).append(" Failed concurrent execution exceeded maximum duration (")
                            .append(attemptDurationMillis).append(" ms > ")
                            .append(MAX_CONCURRENT_ATTEMPT_DURATION_MILLIS).append(" ms)").endl();
                }
                break;
            } else {
                try {
                    Thread.sleep(delay);
                    delay *= 2;
                } catch (InterruptedException interruptIsCancel) {
                    throw new CancellationException("Interrupt detected", interruptIsCancel);
                }
            }
        }

        if (snapshotSuccessful) {
            state.maybeReleaseLock();
            if (!functionSuccessful) {
                final String message = "Failed to execute function concurrently despite consistent state";
                if (caughtException != null) {
                    throw new SnapshotUnsuccessfulException(message, caughtException);
                } else {
                    throw new SnapshotUnsuccessfulException(message);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                if (numConcurrentAttempts == 0) {
                    log.debug().append(logPrefix).append(" Already held lock, proceeding to locked snapshot").endl();
                } else {
                    log.debug().append(logPrefix)
                            .append(" Failed to obtain clean execution without blocking run processing").endl();
                }
            }
            state.startLockedSnapshot(control);
            final LivenessScope snapshotLivenessScope = new LivenessScope();
            try (final SafeCloseable ignored = LivenessScopeStack.open(snapshotLivenessScope, true)) {
                final long beforeClockValue = updateGraph.clock().currentValue();

                final Boolean previousValuesRequested = control.usePreviousValues(beforeClockValue);
                if (!Boolean.FALSE.equals(previousValuesRequested)) {
                    Assert.statementNeverExecuted(String.format(
                            "Previous values requested or inconsistent %s: beforeClockValue=%d, previousValuesRequested=%s",
                            onUpdateThread ? "from update-processing thread" : "while locked",
                            beforeClockValue, previousValuesRequested));
                }

                final long attemptStart = System.currentTimeMillis();
                functionSuccessful = function.call(false, beforeClockValue);
                Assert.assertion(functionSuccessful, "functionSuccessful");

                final long afterClockValue = updateGraph.clock().currentValue();

                Assert.eq(beforeClockValue, "beforeClockValue", afterClockValue, "afterClockValue");

                final boolean consistent = control.snapshotCompletedConsistently(afterClockValue, false);
                if (!consistent) {
                    Assert.statementNeverExecuted(String.format(
                            "Consistent execution not achieved %s",
                            onUpdateThread ? "from update-processing thread" : "while locked"));
                }

                if (log.isDebugEnabled()) {
                    log.debug().append(logPrefix).append(" Non-concurrent Snapshot Function took ")
                            .append(System.currentTimeMillis() - attemptStart).append("ms").endl();
                }
                if (functionSuccessful && consistent) {
                    snapshotLivenessScope.transferTo(initialLivenessManager);
                }
                step = LogicalClock.getStep(afterClockValue);
            } finally {
                state.endLockedSnapshot();
            }
        }
        if (log.isDebugEnabled()) {
            final long duration = System.currentTimeMillis() - overallStart;
            log.debug().append(logPrefix).append(" Total snapshot function elapsed time ")
                    .append(duration).append(" ms").append(", step=").append(step).endl();
        }
        return step;
    }

    /**
     * <p>
     * Populate a BarrageMessage with the specified positions to snapshot and columns.
     * <p>
     * Note that care must be taken while using this method to ensure the underlying table is locked or does not change,
     * otherwise the resulting snapshot may be inconsistent. In general users should instead use
     * {@link #constructBackplaneSnapshot} for simple use cases or {@link #callDataSnapshotFunction} for more advanced
     * uses.
     *
     * @param usePrev Use previous values?
     * @param snapshot The snapshot to populate
     * @param logIdentityObject an object for use with log() messages
     * @param columnsToSnapshot A {@link BitSet} of columns to include, null for all
     * @param keysToSnapshot A RowSet of keys within the table to include, null for all
     * @return true if the snapshot was computed with an unchanged clock, false otherwise.
     */
    private static boolean snapshotAllTable(
            final boolean usePrev,
            @NotNull final BarrageMessage snapshot,
            @NotNull final BaseTable<?> table,
            @NotNull final Object logIdentityObject,
            @Nullable final BitSet columnsToSnapshot,
            @Nullable final RowSet keysToSnapshot) {

        snapshot.rowsAdded = (usePrev ? table.getRowSet().prev() : table.getRowSet()).copy();
        snapshot.tableSize = snapshot.rowsAdded.size();
        snapshot.rowsRemoved = RowSetFactory.empty();
        snapshot.addColumnData = new BarrageMessage.AddColumnData[table.getColumnSources().size()];

        // TODO (core#412): when sending app metadata; this can be reduced to a zero-len array
        snapshot.modColumnData = new BarrageMessage.ModColumnData[table.getColumnSources().size()];

        if (keysToSnapshot != null) {
            snapshot.rowsIncluded = snapshot.rowsAdded.intersect(keysToSnapshot);
        } else {
            snapshot.rowsIncluded = snapshot.rowsAdded.copy();
        }

        final String[] columnSources = table.getDefinition().getColumnNamesArray();

        // Snapshot empty columns serially, and collect indices of non-empty columns
        final int numColumnsToSnapshot =
                columnsToSnapshot != null ? columnsToSnapshot.cardinality() : columnSources.length;
        final TIntList nonEmptyColumnIndices = new TIntArrayList(numColumnsToSnapshot);
        final List<ColumnSource<?>> nonEmptyColumnSources = new ArrayList<>(numColumnsToSnapshot);
        if (!snapshotEmptyColumns(columnSources, columnsToSnapshot, table, logIdentityObject, snapshot,
                nonEmptyColumnIndices, nonEmptyColumnSources)) {
            return false;
        }
        boolean canParallelize = false;
        final int numNonEmptyColumns = nonEmptyColumnIndices.size();
        if (numNonEmptyColumns != 0) {
            final ExecutionContext executionContext = ExecutionContext.getContext();
            canParallelize = ENABLE_PARALLEL_SNAPSHOT &&
                    executionContext.getOperationInitializer().canParallelize() &&
                    numNonEmptyColumns > 1 &&
                    (snapshot.rowsIncluded.size() >= MINIMUM_PARALLEL_SNAPSHOT_ROWS ||
                            !allColumnSourcesInMemory(nonEmptyColumnSources));
            if (canParallelize) {
                if (!snapshotColumnsParallel(nonEmptyColumnIndices, nonEmptyColumnSources, usePrev, executionContext,
                        snapshot)) {
                    return false;
                }
            } else {
                // Snapshot all non-empty columns serially
                snapshotColumnsSerial(nonEmptyColumnIndices, nonEmptyColumnSources, usePrev, snapshot);
            }
        }
        if (log.isDebugEnabled()) {
            final LogEntry logEntry = log.debug().append(System.identityHashCode(logIdentityObject))
                    .append(": Snapshot candidate step=")
                    .append((usePrev ? -1 : 0) + LogicalClock.getStep(getConcurrentAttemptClockValue()))
                    .append(", canParallelize=").append(canParallelize)
                    .append(", rows=").append(snapshot.rowsIncluded).append("/").append(keysToSnapshot)
                    .append(", cols=");
            if (columnsToSnapshot == null) {
                logEntry.append("ALL");
            } else {
                logEntry.append(FormatBitSet.formatBitSet(columnsToSnapshot));
            }
            logEntry.append(", usePrev=").append(usePrev).endl();
        }
        return true;
    }

    /**
     * Check if all the required column sources are in memory and should allow efficient access.
     */
    private static boolean allColumnSourcesInMemory(@NotNull final Iterable<ColumnSource<?>> columnSources) {
        for (final ColumnSource<?> columnSource : columnSources) {
            if (!isColumnSourceInMemory(columnSource)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if the column source is in-memory or redirected to an in-memory column source.
     */
    private static boolean isColumnSourceInMemory(ColumnSource<?> columnSource) {
        do {
            if (columnSource instanceof InMemoryColumnSource) {
                return ((InMemoryColumnSource) columnSource).isInMemory();
            }
            if (!(columnSource instanceof RedirectedColumnSource)) {
                return false;
            }
            columnSource = ((RedirectedColumnSource<?>) columnSource).getInnerSource();
        } while (true);
    }

    /**
     * Snapshot the specified columns in parallel.
     */
    private static boolean snapshotColumnsParallel(
            @NotNull final TIntList columnIndices,
            @NotNull final List<ColumnSource<?>> columnSources,
            final boolean usePrev,
            final ExecutionContext executionContext,
            @NotNull final BarrageMessage snapshot) {
        final JobScheduler jobScheduler = new OperationInitializerJobScheduler();
        final CompletableFuture<Void> waitForParallelSnapshot = new CompletableFuture<>();
        jobScheduler.iterateParallel(
                executionContext,
                logOutput -> logOutput.append("snapshotColumnsParallel"),
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, columnIndices.size(),
                (context, colRank, nestedErrorConsumer) -> snapshotColumnsSerial(
                        new TIntArrayList(new int[] {columnIndices.get(colRank)}),
                        columnSources.subList(colRank, colRank + 1),
                        usePrev, snapshot),
                () -> waitForParallelSnapshot.complete(null),
                waitForParallelSnapshot::completeExceptionally);
        try {
            waitForParallelSnapshot.get();
        } catch (final InterruptedException e) {
            throw new CancellationException("Interrupted during parallel column snapshot");
        } catch (final ExecutionException e) {
            throw new ColumnSnapshotUnsuccessfulException(
                    "Exception occurred during parallel column snapshot", e.getCause());
        }
        return true;
    }

    /**
     * Allocate add and mod column data for each column, and populate the snapshot data for empty columns. Also, collect
     * the indices and column sources of non-empty columns, for which we will populate snapshot data later.
     * <p>
     * This method is intended to be called from the thread that initiated the column snapshot.
     */
    private static boolean snapshotEmptyColumns(
            final String[] columnSources,
            @Nullable final BitSet columnsToSnapshot,
            @NotNull final Table table,
            @NotNull final Object logIdentityObject,
            @NotNull final BarrageMessage snapshot,
            @NotNull final TIntCollection nonEmptyColumnsIndices,
            @NotNull final Collection<ColumnSource<?>> nonEmptyColumnSources) {
        final boolean rowsetIsEmpty = snapshot.rowsIncluded.isEmpty();
        for (int colIdx = 0; colIdx < columnSources.length; ++colIdx) {
            if (concurrentAttemptInconsistent()) {
                if (log.isDebugEnabled()) {
                    final LogEntry logEntry = log.debug().append(System.identityHashCode(logIdentityObject))
                            .append(" Bad snapshot before column ").append(columnSources[colIdx])
                            .append(" at idx ").append(colIdx);
                    appendConcurrentAttemptClockInfo(logEntry);
                    logEntry.endl();
                }
                return false;
            }

            final ColumnSource<?> columnSource = table.getColumnSource(columnSources[colIdx]);

            final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
            snapshot.addColumnData[colIdx] = acd;
            final boolean columnIsEmpty = rowsetIsEmpty ||
                    (columnsToSnapshot != null && !columnsToSnapshot.get(colIdx));
            final ColumnSource<?> sourceToUse = ReinterpretUtils.maybeConvertToPrimitive(columnSource);
            if (columnIsEmpty) {
                acd.data = List.of();
            } else {
                acd.data = new ArrayList<>(); // To be populated later
                nonEmptyColumnsIndices.add(colIdx);
                nonEmptyColumnSources.add(sourceToUse);
            }
            acd.type = columnSource.getType();
            acd.componentType = columnSource.getComponentType();
            acd.chunkType = sourceToUse.getChunkType();

            final BarrageMessage.ModColumnData mcd = new BarrageMessage.ModColumnData();
            snapshot.modColumnData[colIdx] = mcd;
            mcd.rowsModified = RowSetFactory.empty();
            mcd.data = List.of();
            mcd.type = acd.type;
            mcd.componentType = acd.componentType;
            mcd.chunkType = sourceToUse.getChunkType();
        }
        return true;
    }

    private static void snapshotColumnsSerial(
            @NotNull final TIntList columnIndices,
            @NotNull final List<ColumnSource<?>> columnSources,
            final boolean usePrev,
            @NotNull final BarrageMessage snapshot) {
        final RowSet rowSet = snapshot.rowsIncluded;
        final long numRows = rowSet.size();
        // The caller should handle empty tables
        Assert.assertion(numRows > 0, "numRows > 0");
        final int numCols = columnIndices.size();
        final int maxChunkSize = (int) Math.min(numRows, SNAPSHOT_CHUNK_SIZE);
        final ColumnSource.FillContext[] fillContexts = new ColumnSource.FillContext[numCols];
        try (final SharedContext sharedContext = numCols > 1 ? SharedContext.makeSharedContext() : null;
                final SafeCloseableArray<ColumnSource.FillContext> ignored = new SafeCloseableArray<>(fillContexts);
                final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            for (int colRank = 0; colRank < numCols; ++colRank) {
                fillContexts[colRank] = columnSources.get(colRank).makeFillContext(maxChunkSize, sharedContext);
            }
            while (it.hasMore()) {
                final RowSequence reducedRowSet = it.getNextRowSequenceWithLength(maxChunkSize);
                // Populate the snapshot data for each column for the current chunk of rows
                for (int colRank = 0; colRank < numCols; ++colRank) {
                    final int colIdx = columnIndices.get(colRank);
                    final ColumnSource<?> columnSource = columnSources.get(colRank);
                    final ColumnSource.FillContext fillContext = fillContexts[colRank];
                    final WritableChunk<Values> currentChunk =
                            columnSource.getChunkType().makeWritableChunk(reducedRowSet.intSize());
                    if (usePrev) {
                        columnSource.fillPrevChunk(fillContext, currentChunk, reducedRowSet);
                    } else {
                        columnSource.fillChunk(fillContext, currentChunk, reducedRowSet);
                    }
                    snapshot.addColumnData[colIdx].data.add(currentChunk);
                }
                if (sharedContext != null) {
                    sharedContext.reset();
                }
            }
        }
    }
}
