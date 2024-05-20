//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.remote;

import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.SnapshotUnsuccessfulException;
import io.deephaven.engine.updategraph.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.updategraph.NotificationQueue.Dependency;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.io.log.LogEntry;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.datastructures.LongSizedDataStructure;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Stream;

import io.deephaven.chunk.attributes.Values;

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
         * Test that determines whether the currently-active concurrent snapshot attempt has become inconsistent. Always
         * returns {@code false} if there is no snapshot attempt active, or if there is a locked attempt active
         * (necessarily at lower depth than the lowest concurrent attempt).
         *
         * @return Whether the clock or sources have changed in such a way as to make the currently-active concurrent
         *         snapshot attempt inconsistent
         */
        boolean concurrentAttemptInconsistent();

        /**
         * Check that fails if the currently-active concurrent snapshot attempt has become inconsistent. source. This is
         * a no-op if there is no snapshot attempt active, or if there is a locked attempt active (necessarily at lower
         * depth than the lowest concurrent attempt).
         *
         * @throws SnapshotInconsistentException If the currently-active concurrent snapshot attempt has become
         *         inconsistent
         */
        void failIfConcurrentAttemptInconsistent();

        /**
         * Wait for a dependency to become satisfied on the current cycle if we're trying to use current values for the
         * currently-active concurrent snapshot attempt. This is a no-op if there is no snapshot attempt active, or if
         * there is a locked attempt active (necessarily at lower depth than the lowest concurrent attempt).
         *
         * @param dependency The dependency, which may be null in order to avoid redundant checks in calling code
         * @throws SnapshotInconsistentException If we cannot wait for this dependency on the current step because the
         *         step changed
         */
        void maybeWaitForSatisfaction(@Nullable NotificationQueue.Dependency dependency);

        /**
         * Return the currently-active concurrent snapshot attempt's "before" clock value, or zero if there is no
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
     * Get the currently-active snapshot state.
     *
     * @return the currently-active snapshot state
     */
    public static State state() {
        return StateImpl.get();
    }

    /**
     * Test that determines whether the currently-active concurrent snapshot attempt has become inconsistent. Always
     * returns {@code false} if there is no snapshot attempt active, or if there is a locked attempt active (necessarily
     * at lower depth than the lowest concurrent attempt).
     *
     * <p>
     * Equivalent to {@code state().concurrentAttemptInconsistent()}.
     *
     * @return Whether the clock or sources have changed in such a way as to make the currently-active concurrent
     *         snapshot attempt inconsistent
     * @see State#concurrentAttemptInconsistent()
     */
    public static boolean concurrentAttemptInconsistent() {
        return state().concurrentAttemptInconsistent();
    }

    /**
     * Check that fails if the currently-active concurrent snapshot attempt has become inconsistent. source. This is a
     * no-op if there is no snapshot attempt active, or if there is a locked attempt active (necessarily at lower depth
     * than the lowest concurrent attempt).
     *
     * <p>
     * Equivalent to {@code state().failIfConcurrentAttemptInconsistent()}.
     *
     * @throws SnapshotInconsistentException If the currently-active concurrent snapshot attempt has become inconsistent
     * @see State#failIfConcurrentAttemptInconsistent()
     */
    public static void failIfConcurrentAttemptInconsistent() {
        state().failIfConcurrentAttemptInconsistent();
    }

    /**
     * Wait for a dependency to become satisfied on the current cycle if we're trying to use current values for the
     * currently-active concurrent snapshot attempt. This is a no-op if there is no snapshot attempt active, or if there
     * is a locked attempt active (necessarily at lower depth than the lowest concurrent attempt).
     *
     * <p>
     * Equivalent to {@code state().maybeWaitForSatisfaction(dependency)}.
     *
     * @param dependency The dependency, which may be null in order to avoid redundant checks in calling code
     * @throws SnapshotInconsistentException If we cannot wait for this dependency on the current step because the step
     *         changed
     * @see State#maybeWaitForSatisfaction(Dependency)
     */
    public static void maybeWaitForSatisfaction(@Nullable final NotificationQueue.Dependency dependency) {
        state().maybeWaitForSatisfaction(dependency);
    }

    /**
     * Return the currently-active concurrent snapshot attempt's "before" clock value, or zero if there is no concurrent
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
     * Create a {@link InitialSnapshot snapshot} of the entire table specified. Note that this method is
     * notification-oblivious, i.e. it makes no attempt to ensure that notifications are not missed.
     *
     * @param logIdentityObject An object used to prepend to log rows.
     * @param table the table to snapshot.
     * @return a snapshot of the entire base table.
     */
    public static InitialSnapshot constructInitialSnapshot(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table) {
        return constructInitialSnapshot(logIdentityObject, table, null, null);
    }

    /**
     * Create a {@link InitialSnapshot snapshot} of the specified table using a set of requested columns and keys. Note
     * that this method uses a RowSet that is in key space, and that it is notification-oblivious, i.e. it makes no
     * attempt to ensure that notifications are not missed.
     *
     * @param logIdentityObject An object used to prepend to log rows.
     * @param table the table to snapshot.
     * @param columnsToSerialize A {@link BitSet} of columns to include, null for all
     * @param keysToSnapshot An RowSet of keys within the table to include, null for all
     * @return a snapshot of the entire base table.
     */
    public static InitialSnapshot constructInitialSnapshot(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSet keysToSnapshot) {
        return constructInitialSnapshot(logIdentityObject, table, columnsToSerialize, keysToSnapshot,
                makeSnapshotControl(false, table.isRefreshing(), table));
    }

    static InitialSnapshot constructInitialSnapshot(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSet keysToSnapshot,
            @NotNull final SnapshotControl control) {
        final UpdateGraph updateGraph = table.getUpdateGraph();
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            final InitialSnapshot snapshot = new InitialSnapshot();

            final SnapshotFunction doSnapshot = (usePrev, beforeClockValue) -> serializeAllTable(
                    usePrev, snapshot, table, logIdentityObject, columnsToSerialize, keysToSnapshot);

            snapshot.step = callDataSnapshotFunction(System.identityHashCode(logIdentityObject), control, doSnapshot);

            return snapshot;
        }
    }

    /**
     * Create a {@link InitialSnapshot snapshot} of the specified table using a set of requested columns and positions.
     * Note that this method uses a RowSet that is in position space, and that it is notification-oblivious, i.e. it
     * makes no attempt to ensure that notifications are not missed.
     *
     * @param logIdentityObject An object used to prepend to log rows.
     * @param table the table to snapshot.
     * @param columnsToSerialize A {@link BitSet} of columns to include, null for all
     * @param positionsToSnapshot An RowSet of positions within the table to include, null for all
     * @return a snapshot of the entire base table.
     */
    public static InitialSnapshot constructInitialSnapshotInPositionSpace(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSet positionsToSnapshot) {
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(
                table.getUpdateGraph()).open()) {
            return constructInitialSnapshotInPositionSpace(logIdentityObject, table, columnsToSerialize,
                    positionsToSnapshot, makeSnapshotControl(false, table.isRefreshing(), table));
        }
    }

    static InitialSnapshot constructInitialSnapshotInPositionSpace(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSet positionsToSnapshot,
            @NotNull final SnapshotControl control) {
        final InitialSnapshot snapshot = new InitialSnapshot();

        final SnapshotFunction doSnapshot = (usePrev, beforeClockValue) -> {
            final RowSet keysToSnapshot;
            if (positionsToSnapshot == null) {
                keysToSnapshot = null;
            } else {
                keysToSnapshot = (usePrev ? table.getRowSet().prev() : table.getRowSet())
                        .subSetForPositions(positionsToSnapshot);
            }
            return serializeAllTable(usePrev, snapshot, table, logIdentityObject, columnsToSerialize, keysToSnapshot);
        };

        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(
                table.getUpdateGraph()).open()) {
            snapshot.step = callDataSnapshotFunction(System.identityHashCode(logIdentityObject), control, doSnapshot);
        }
        return snapshot;
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
     * @param columnsToSerialize A {@link BitSet} of columns to include, null for all
     * @param positionsToSnapshot An RowSet of positions within the table to include, null for all
     * @return a snapshot of the entire base table.
     */
    public static BarrageMessage constructBackplaneSnapshotInPositionSpace(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSequence positionsToSnapshot,
            @Nullable final RowSequence reversePositionsToSnapshot) {
        return constructBackplaneSnapshotInPositionSpace(logIdentityObject, table, columnsToSerialize,
                positionsToSnapshot, reversePositionsToSnapshot,
                makeSnapshotControl(false, table.isRefreshing(), table));
    }

    /**
     * Create a {@link BarrageMessage snapshot} of the specified table using a set of requested columns and positions.
     * Note that this method uses a RowSet that is in position space.
     *
     * @param logIdentityObject An object used to prepend to log rows.
     * @param table the table to snapshot.
     * @param columnsToSerialize A {@link BitSet} of columns to include, null for all
     * @param positionsToSnapshot A RowSequence of positions within the table to include, null for all
     * @param reversePositionsToSnapshot A RowSequence of reverse positions within the table to include, null for all
     * @param control A {@link SnapshotControl} to define the parameters and consistency for this snapshot
     * @return a snapshot of the entire base table.
     */
    public static BarrageMessage constructBackplaneSnapshotInPositionSpace(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?> table,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSequence positionsToSnapshot,
            @Nullable final RowSequence reversePositionsToSnapshot,
            @NotNull final SnapshotControl control) {

        final UpdateGraph updateGraph = table.getUpdateGraph();
        try (final SafeCloseable ignored1 = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            final BarrageMessage snapshot = new BarrageMessage();
            snapshot.isSnapshot = true;
            snapshot.shifted = RowSetShiftData.EMPTY;

            final SnapshotFunction doSnapshot = (usePrev, beforeClockValue) -> {
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
                    return serializeAllTable(usePrev, snapshot, table, logIdentityObject, columnsToSerialize,
                            keysToSnapshot);
                }
            };

            snapshot.step = callDataSnapshotFunction(System.identityHashCode(logIdentityObject), control, doSnapshot);
            snapshot.firstSeq = snapshot.lastSeq = snapshot.step;

            return snapshot;
        }
    }

    /**
     * Constructs {@link InitialSnapshot}s for the entirety of the tables. Note that this method is
     * notification-oblivious, i.e. it makes no attempt to ensure that notifications are not missed.
     *
     * @param logIdentityObject identifier prefixing the log message
     * @param tables tables to snapshot
     * @return list of the resulting {@link InitialSnapshot}s
     */
    public static List<InitialSnapshot> constructInitialSnapshots(
            @NotNull final Object logIdentityObject,
            @NotNull final BaseTable<?>... tables) {
        if (tables.length == 0) {
            return Collections.emptyList();
        }
        final UpdateGraph updateGraph = NotificationQueue.Dependency.getUpdateGraph(null, tables);
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            final List<InitialSnapshot> snapshots = new ArrayList<>();

            final SnapshotControl snapshotControl = tables.length == 1
                    ? makeSnapshotControl(false, tables[0].isRefreshing(), tables[0])
                    : makeSnapshotControl(false, Arrays.stream(tables).anyMatch(BaseTable::isRefreshing), tables);

            final SnapshotFunction doSnapshot =
                    (usePrev, beforeClockValue) -> serializeAllTables(usePrev, snapshots, tables, logIdentityObject);

            callDataSnapshotFunction(System.identityHashCode(logIdentityObject), snapshotControl, doSnapshot);

            return snapshots;
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
     * Populate an {@link InitialSnapshot} with the specified keys and columns to snapshot.
     * <p>
     * Note that care must be taken while using this method to ensure the underlying table is locked or does not change,
     * otherwise the resulting snapshot may be inconsistent. In general users should instead use
     * {@link #constructInitialSnapshot} for simple use cases or {@link #callDataSnapshotFunction} for more advanced
     * uses.
     *
     * @param usePrev Whether to use previous values
     * @param snapshot The snapshot to populate
     * @param logIdentityObject An object for use with log() messages
     * @param columnsToSerialize A {@link BitSet} of columns to include, null for all
     * @param keysToSnapshot A RowSet of keys within the table to include, null for all
     *
     * @return Whether the snapshot succeeded
     */
    private static boolean serializeAllTable(
            final boolean usePrev,
            @NotNull final InitialSnapshot snapshot,
            @NotNull final BaseTable<?> table,
            @NotNull final Object logIdentityObject,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSet keysToSnapshot) {
        snapshot.rowSet = (usePrev ? table.getRowSet().prev() : table.getRowSet()).copy();

        if (keysToSnapshot != null) {
            snapshot.rowsIncluded = snapshot.rowSet.intersect(keysToSnapshot);
        } else {
            snapshot.rowsIncluded = snapshot.rowSet;
        }

        LongSizedDataStructure.intSize("construct snapshot", snapshot.rowsIncluded.size());
        final String[] columnSources = table.getDefinition().getColumnNamesArray();

        snapshot.dataColumns = new Object[columnSources.length];
        try (final SharedContext sharedContext =
                (columnSources.length > 1) ? SharedContext.makeSharedContext() : null) {
            for (int ii = 0; ii < columnSources.length; ii++) {
                if (columnsToSerialize != null && !columnsToSerialize.get(ii)) {
                    continue;
                }

                if (concurrentAttemptInconsistent()) {
                    if (log.isDebugEnabled()) {
                        final LogEntry logEntry = log.debug().append(System.identityHashCode(logIdentityObject))
                                .append(" Bad snapshot before column ").append(ii);
                        appendConcurrentAttemptClockInfo(logEntry);
                        logEntry.endl();
                    }
                    return false;
                }

                final ColumnSource<?> columnSource = table.getColumnSource(columnSources[ii]);
                snapshot.dataColumns[ii] = getSnapshotData(columnSource, sharedContext, snapshot.rowsIncluded, usePrev);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug().append(System.identityHashCode(logIdentityObject))
                    .append(": Snapshot candidate step=")
                    .append((usePrev ? -1 : 0) + LogicalClock.getStep(getConcurrentAttemptClockValue()))
                    .append(", rows=").append(snapshot.rowsIncluded).append("/").append(keysToSnapshot)
                    .append(", cols=").append(FormatBitSet.arrayToLog(snapshot.dataColumns)).append("/")
                    .append((columnsToSerialize != null) ? FormatBitSet.formatBitSet(columnsToSerialize)
                            : FormatBitSet.arrayToLog(snapshot.dataColumns))
                    .append(", usePrev=").append(usePrev).endl();
        }
        return true;
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
     * @param columnsToSerialize A {@link BitSet} of columns to include, null for all
     * @param keysToSnapshot A RowSet of keys within the table to include, null for all
     * @return true if the snapshot was computed with an unchanged clock, false otherwise.
     */
    private static boolean serializeAllTable(
            final boolean usePrev,
            @NotNull final BarrageMessage snapshot,
            @NotNull final BaseTable<?> table,
            @NotNull final Object logIdentityObject,
            @Nullable final BitSet columnsToSerialize,
            @Nullable final RowSet keysToSnapshot) {

        snapshot.rowsAdded = (usePrev ? table.getRowSet().prev() : table.getRowSet()).copy();
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

        try (final SharedContext sharedContext =
                (columnSources.length > 1) ? SharedContext.makeSharedContext() : null) {
            for (int ii = 0; ii < columnSources.length; ++ii) {
                if (concurrentAttemptInconsistent()) {
                    if (log.isDebugEnabled()) {
                        final LogEntry logEntry = log.debug().append(System.identityHashCode(logIdentityObject))
                                .append(" Bad snapshot before column ").append(ii);
                        appendConcurrentAttemptClockInfo(logEntry);
                        logEntry.endl();
                    }
                    return false;
                }

                final ColumnSource<?> columnSource = table.getColumnSource(columnSources[ii]);

                final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
                snapshot.addColumnData[ii] = acd;
                final boolean columnIsEmpty = columnsToSerialize != null && !columnsToSerialize.get(ii);
                final RowSet rows = columnIsEmpty ? RowSetFactory.empty() : snapshot.rowsIncluded;
                // Note: cannot use shared context across several calls of differing lengths and no sharing necessary
                // when empty
                final ColumnSource<?> sourceToUse = ReinterpretUtils.maybeConvertToPrimitive(columnSource);
                acd.data = getSnapshotDataAsChunkList(sourceToUse, columnIsEmpty ? null : sharedContext, rows, usePrev);
                acd.type = columnSource.getType();
                acd.componentType = columnSource.getComponentType();
                acd.chunkType = sourceToUse.getChunkType();

                final BarrageMessage.ModColumnData mcd = new BarrageMessage.ModColumnData();
                snapshot.modColumnData[ii] = mcd;
                mcd.rowsModified = RowSetFactory.empty();
                mcd.data = getSnapshotDataAsChunkList(sourceToUse, null, RowSetFactory.empty(), usePrev);
                mcd.type = acd.type;
                mcd.componentType = acd.componentType;
                mcd.chunkType = sourceToUse.getChunkType();
            }
        }

        if (log.isDebugEnabled()) {
            final LogEntry logEntry = log.debug().append(System.identityHashCode(logIdentityObject))
                    .append(": Snapshot candidate step=")
                    .append((usePrev ? -1 : 0) + LogicalClock.getStep(getConcurrentAttemptClockValue()))
                    .append(", rows=").append(snapshot.rowsIncluded).append("/").append(keysToSnapshot)
                    .append(", cols=");
            if (columnsToSerialize == null) {
                logEntry.append("ALL");
            } else {
                logEntry.append(FormatBitSet.formatBitSet(columnsToSerialize));
            }
            logEntry.append(", usePrev=").append(usePrev).endl();
        }

        return true;
    }

    private static boolean serializeAllTables(
            final boolean usePrev,
            @NotNull final List<InitialSnapshot> snapshots,
            @NotNull final BaseTable<?>[] tables,
            @NotNull final Object logIdentityObject) {
        snapshots.clear();

        for (final BaseTable<?> table : tables) {
            final InitialSnapshot snapshot = new InitialSnapshot();
            snapshots.add(snapshot);
            if (!serializeAllTable(usePrev, snapshot, table, logIdentityObject, null, null)) {
                snapshots.clear();
                return false;
            }
        }

        return true;
    }

    private static <T> Object getSnapshotData(
            @NotNull final ColumnSource<T> columnSource,
            @Nullable final SharedContext sharedContext,
            @NotNull final RowSet rowSet,
            final boolean usePrev) {
        final ColumnSource<?> sourceToUse = ReinterpretUtils.maybeConvertToPrimitive(columnSource);
        final Class<?> type = sourceToUse.getType();
        final int size = rowSet.intSize();
        try (final ColumnSource.FillContext context = sourceToUse.makeFillContext(size, sharedContext)) {
            final ChunkType chunkType = sourceToUse.getChunkType();
            final Object resultArray = chunkType.makeArray(size);
            final WritableChunk<Values> result = chunkType.writableChunkWrap(resultArray, 0, size);
            if (usePrev) {
                sourceToUse.fillPrevChunk(context, result, rowSet);
            } else {
                sourceToUse.fillChunk(context, result, rowSet);
            }
            if (chunkType == ChunkType.Object) {
                // noinspection unchecked
                final T[] values = (T[]) Array.newInstance(type, size);
                for (int ii = 0; ii < values.length; ++ii) {
                    // noinspection unchecked
                    values[ii] = (T) result.asObjectChunk().get(ii);
                }
                return values;

            }
            return resultArray;
        }
    }

    private static <T> ArrayList<Chunk<Values>> getSnapshotDataAsChunkList(
            @NotNull final ColumnSource<T> columnSource,
            @Nullable final SharedContext sharedContext,
            @NotNull final RowSet rowSet,
            final boolean usePrev) {
        long offset = 0;
        final long size = rowSet.size();
        final ArrayList<Chunk<Values>> result = new ArrayList<>();

        if (size == 0) {
            return result;
        }

        final int maxChunkSize = (int) Math.min(size, SNAPSHOT_CHUNK_SIZE);

        try (final ColumnSource.FillContext context = columnSource.makeFillContext(maxChunkSize, sharedContext);
                final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            int chunkSize = maxChunkSize;
            while (it.hasMore()) {
                final RowSequence reducedRowSet = it.getNextRowSequenceWithLength(chunkSize);
                final ChunkType chunkType = columnSource.getChunkType();

                // create a new chunk
                WritableChunk<Values> currentChunk = chunkType.makeWritableChunk(chunkSize);

                if (usePrev) {
                    columnSource.fillPrevChunk(context, currentChunk, reducedRowSet);
                } else {
                    columnSource.fillChunk(context, currentChunk, reducedRowSet);
                }

                // add the chunk to the current list
                result.add(currentChunk);

                // increment the offset for the next chunk (using the actual values written)
                offset += currentChunk.size();

                // recompute the size of the next chunk
                if (size - offset > maxChunkSize) {
                    chunkSize = maxChunkSize;
                } else {
                    chunkSize = (int) (size - offset);
                }

                if (sharedContext != null) {
                    // a shared context is good for only one chunk of rows
                    sharedContext.reset();
                }
            }
        }
        return result;
    }

    /**
     * Estimate the size of a complete table snapshot in bytes.
     *
     * @param table the table to estimate
     * @return the estimated snapshot size in bytes.
     */
    public static long estimateSnapshotSize(@NotNull final Table table) {
        final BitSet columns = new BitSet(table.numColumns());
        columns.set(0, table.numColumns());
        return estimateSnapshotSize(table.getDefinition(), columns, table.size());
    }

    /**
     * Make a rough guess at the size of a snapshot, using the column types and common column names. The use case is
     * when a user requests something from the GUI; we'd like to know if it is ridiculous before actually doing it.
     *
     * @param tableDefinition the table definition
     * @param columns a bitset indicating which columns are included
     * @param rowCount how many rows of this data we'll be snapshotting
     * @return the estimated size of the snapshot
     */
    public static long estimateSnapshotSize(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final BitSet columns,
            final long rowCount) {
        long sizePerRow = 0;
        long totalSize = 0;

        final int numColumns = tableDefinition.numColumns();
        final List<ColumnDefinition<?>> columnDefinitions = tableDefinition.getColumns();
        for (int ii = 0; ii < numColumns; ++ii) {
            if (!columns.get(ii)) {
                continue;
            }

            totalSize += 44; // for an array

            final ColumnDefinition<?> definition = columnDefinitions.get(ii);
            if (definition.getDataType() == byte.class || definition.getDataType() == char.class
                    || definition.getDataType() == Boolean.class) {
                sizePerRow += 1;
            } else if (definition.getDataType() == short.class) {
                sizePerRow += 2;
            } else if (definition.getDataType() == int.class || definition.getDataType() == float.class) {
                sizePerRow += 4;
            } else if (definition.getDataType() == long.class || definition.getDataType() == double.class
                    || definition.getDataType() == Instant.class || definition.getDataType() == ZonedDateTime.class) {
                sizePerRow += 8;
            } else {
                switch (definition.getName()) {
                    case "Date":
                        sizePerRow += 5;
                        totalSize += 10;
                        break;
                    case "USym":
                        sizePerRow += 5;
                        totalSize += Math.min(rowCount, 10000) * 10;
                        break;
                    case "Sym":
                        sizePerRow += 5;
                        totalSize += Math.min(rowCount, 1000000) * 30;
                        break;
                    case "Parity":
                        sizePerRow += 5;
                        totalSize += 30;
                        break;
                    case "SecurityType":
                        sizePerRow += 5;
                        totalSize += 100;
                        break;
                    case "Exchange":
                        sizePerRow += 5;
                        totalSize += 130;
                        break;
                    default:
                        sizePerRow += (42 + 8); // how big a dummy object was on a test + a pointer
                }
            }
        }

        return totalSize + (sizePerRow * rowCount);
    }
}
