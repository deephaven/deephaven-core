//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.DataIndexOptions;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.NotificationStepReceiver;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.datastructures.ArrayWeakReferenceManager;
import io.deephaven.util.datastructures.WeakReferenceManager;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.LongStream;

/**
 * The set state shared by a {@link DynamicWhereFilter} and every copy of it: the set table, the {@link SetKernel} built
 * from it, and the listener that maintains that kernel.
 * <p>
 * A where filter is copied for each operation that uses it, and a {@code PartitionedTable} proxy copies it once per
 * constituent table, so a single filter can be copied many times over. Building a kernel and subscribing a listener for
 * each copy would repeat that work per copy and leave one listener per copy attached to the set table, so copies share
 * one instance of this class instead.
 * <p>
 * The kernel is inclusion-agnostic: {@link DynamicWhereFilter} always supplies its own inclusion when matching values,
 * so one shared set serves both {@code whereIn} and {@code whereNotIn} filters over the same keys.
 */
final class SharedSetKernel extends LivenessArtifact implements NotificationQueue.Dependency {

    private final UpdateGraph updateGraph;
    private final MatchPair[] sourceToSetColumnNamePairs;

    /** The set table, or {@code null} if the set is static and needs no maintenance. */
    private final QueryTable setTable;
    /** The kernel; owned by {@link #setUpdateListener} when the set is refreshing. */
    private final SetKernel kernel;
    private final Class<?> @NotNull [] setKeyTypes;

    @SuppressWarnings("FieldCanBeLocal")
    @ReferentialIntegrity
    private final SetUpdateListener setUpdateListener;

    /**
     * The step on which {@link #setUpdateListener} began changing {@link #kernel} or recorded a {@link #failure},
     * published before the change and under the {@link #filters} registry monitor, so that
     * {@link #addFilter(DynamicWhereFilter, long)} can check it atomically against that change.
     */
    private volatile long lastStateChangeStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

    /**
     * The failure that ended this set, or {@code null} while it is alive. Written once under the {@link #filters}
     * registry monitor, and volatile so that {@link #throwIfFailed()} can check it without taking that monitor.
     */
    private volatile Throwable failure;

    /**
     * Incremented by {@link #setUpdateListener} immediately before and immediately after it mutates {@link #kernel}, so
     * that it is odd for exactly as long as a mutation is in progress, and any reader that observes an effect of a
     * mutation is guaranteed to observe the leading increment. Readers capture it with {@link #beginRead()} and pass it
     * back through {@link #failIfChangedSince(long)} as they go; see {@link #kernel()}.
     */
    private volatile long generation;

    /**
     * The filters sharing this set, weakly referenced so that a filter, and the result table that reaches it, remain
     * collectable. Registration is guarded by this manager's monitor; delivery deliberately is not, so that a filter's
     * recompute handling cannot deadlock against a concurrent registration.
     */
    private final WeakReferenceManager<DynamicWhereFilter> filters = new ArrayWeakReferenceManager<>(true);

    /**
     * Create the shared set for {@code setTable}.
     */
    static SharedSetKernel create(
            @NotNull final Table setTable,
            @NotNull final MatchPair[] sourceToSetColumnNamePairs) {
        return new SharedSetKernel(setTable, sourceToSetColumnNamePairs);
    }

    private SharedSetKernel(
            @NotNull final Table setTable,
            @NotNull final MatchPair[] sourceToSetColumnNamePairs) {
        this.updateGraph = ExecutionContext.getContext().getUpdateGraph();
        this.sourceToSetColumnNamePairs = sourceToSetColumnNamePairs;

        // The kernel tolerates duplicate keys, counting the rows that hold each, so any of these will do.
        final QueryTable setTableToUse;
        final boolean setRefreshing = setTable.isRefreshing();

        final String[] setColumnNames = MatchPair.getRightColumns(sourceToSetColumnNamePairs);
        final DataIndex setIndex = DataIndexer.getDataIndex(setTable, setColumnNames);
        if (setIndex != null) {
            // We have a distinct index table, let's use it. Only its keys are read, so the partial table will do.
            setTableToUse = (QueryTable) setIndex.table(DataIndexOptions.USING_PARTIAL_TABLE);
        } else if (setRefreshing) {
            setTableToUse = (QueryTable) setTable.coalesce();
        } else {
            final TableDefinition setDef = setTable.getDefinition();
            final boolean allPartitioning =
                    Arrays.stream(setColumnNames).allMatch(cn -> setDef.getColumn(cn).isPartitioning());
            if (allPartitioning) {
                // A partition-aware source table answers this from its location table, without reading any data.
                setTableToUse = (QueryTable) setTable.selectDistinct(setColumnNames);
            } else {
                setTableToUse = (QueryTable) setTable.coalesce();
            }
        }

        // Use reinterpreted column sources for the set table's keys.
        final ColumnSource<?>[] setColumns = Arrays.stream(sourceToSetColumnNamePairs)
                .map(mp -> setTableToUse.getColumnSource(mp.rightColumn()))
                .map(ReinterpretUtils::maybeConvertToPrimitive)
                .toArray(ColumnSource[]::new);
        setKeyTypes = Arrays.stream(setColumns).map(ColumnSource::getType).toArray(Class[]::new);

        if (!setRefreshing) {
            this.setTable = null;
            this.setUpdateListener = null;
            this.kernel = SetKernel.create(setColumns, setTableToUse.getRowSet(), false);
            return;
        }

        this.setTable = setTableToUse;
        // This set outlives the caller's liveness scope, because the filters sharing it do.
        manage(setTableToUse);

        final Mutable<SetUpdateListener> resultListenerHolder = new MutableObject<>();
        snapshotAndCreate(setTableToUse, setColumns, resultListenerHolder);
        this.setUpdateListener = resultListenerHolder.getValue();
        this.kernel = setUpdateListener.kernel;
    }

    /**
     * Create and populate the kernel from the set table's key columns.
     */
    private static @NotNull SetKernel createKernel(
            @NotNull final QueryTable setTable,
            @NotNull final ColumnSource<?>[] setColumns,
            final boolean usePrev) {
        final RowSet rsToUse = usePrev ? setTable.getRowSet().prev() : setTable.getRowSet();
        return SetKernel.create(setColumns, rsToUse, usePrev);
    }

    /**
     * Create and populate the kernel and install the update listener on the same step of the cycle.
     */
    private void snapshotAndCreate(
            @NotNull final QueryTable setTable,
            @NotNull final ColumnSource<?>[] setColumns,
            final Mutable<SetUpdateListener> resultListenerHolder) {
        final String[] setColumnNames = MatchPair.getRightColumns(sourceToSetColumnNamePairs);
        final ModifiedColumnSet setColumnsMCS = setTable.newModifiedColumnSet(setColumnNames);
        final String humanReadablePrefix = "DynamicWhereFilter(" + Arrays.toString(sourceToSetColumnNamePairs) + ")";

        ConstructSnapshot.callDataSnapshotFunction("SharedSetKernel-createKernel",
                ConstructSnapshot.makeSnapshotControl(true, true, setTable),
                (usePrev, beforeClockUnused) -> {
                    // This function is re-invoked for every snapshot attempt. An attempt that proves inconsistent
                    // leaves behind a subscribed, managed listener. If we don't clean it up, that listener will
                    // stay attached to the set table and continue processing every set table update for the life
                    // of this SharedSetKernel, causing duplicate notifications and wasted work. We remove and
                    // unmanage any stale listener from a previous failed attempt before creating a new one.
                    final SetUpdateListener staleListener = resultListenerHolder.getValue();
                    if (staleListener != null) {
                        resultListenerHolder.setValue(null);
                        // A notification already queued for it still runs; make that run a no-op.
                        staleListener.superseded = true;
                        unmanage(staleListener);
                        setTable.removeUpdateListener(staleListener);
                    }

                    final SetUpdateListener localListener = new SetUpdateListener(humanReadablePrefix, setTable,
                            createKernel(setTable, setColumns, usePrev), setColumnsMCS);
                    resultListenerHolder.setValue(localListener);
                    manage(localListener);
                    setTable.addUpdateListener(localListener);
                    return true;
                });
    }

    /**
     * The kernel. Readers use it with no synchronization at all, even though {@link #setUpdateListener} may be mutating
     * it on the update graph thread at the same time; they capture {@link #generation()} first and call
     * {@link #failIfChangedSince(long)} as they go, so that a read overtaken by a mutation is abandoned early.
     * <p>
     * Reading without synchronization is safe because a concurrent read of the fastutil open hash map behind every
     * kernel can return a wrong answer or throw, but cannot hang. A wrong answer is rejected by the snapshot control,
     * which compares {@link #lastStateChangeStep()} against its step, or by the snapshot clock, and the attempt is
     * retried; the snapshot machinery retries on an exception, so neither reaches a caller. Termination follows from
     * the map's shape: {@code containsKey} reads the {@code key} array once and {@code mask} on each probe, and
     * {@code rehash} assigns {@code key} last, so a reader can see a torn pair, but every such pair either indexes out
     * of bounds (an exception) or probes a region that still holds a free slot, because a doubled table is at most
     * three quarters full and a table is only ever halved when under a fifth full; in-place mutation never fills the
     * table; and the iterator's position only decreases. A compound kernel's {@code Hash.Strategy} only reads the probe
     * and the stored tuples, which are immutable. This depends on every kernel that holds keys being fastutil-backed,
     * including the Object kernel, which is why that one uses {@code Object2LongOpenHashMap} rather than
     * {@code HashMap}; the kernel for a key of no columns holds only a row count.
     */
    SetKernel kernel() {
        return kernel;
    }

    /**
     * Begin a read of {@link #kernel()}, abandoning the enclosing concurrent snapshot attempt at once if a mutation is
     * already in progress.
     *
     * @return The generation to pass to {@link #failIfChangedSince(long)} during the read
     * @throws ConstructSnapshot.SnapshotInconsistentException If a mutation is in progress during a concurrent snapshot
     *         attempt
     */
    long beginRead() {
        final long generation = this.generation;
        if ((generation & 1) != 0) {
            // The listener is between its two increments, so the kernel is being mutated right now.
            abandonAttempt();
        }
        return generation;
    }

    /**
     * Abandon the enclosing concurrent snapshot attempt if a mutation has begun since {@link #beginRead()}. Such a read
     * is going to be rejected by the snapshot control regardless, so finishing it is wasted work; the check is one
     * volatile read, so callers make it once per chunk of work rather than per key.
     *
     * @param generation The value returned by {@link #beginRead()}
     * @throws ConstructSnapshot.SnapshotInconsistentException If the set changed during a concurrent snapshot attempt
     */
    void failIfChangedSince(final long generation) {
        // Guarantees that the kernel reads *precede* the generation check.
        VarHandle.loadLoadFence();
        if (this.generation != generation) {
            abandonAttempt();
        }
    }

    private void abandonAttempt() {
        // Only a concurrent snapshot attempt can race setUpdateListener's mutation; a reader on the update graph
        // thread runs after it, so a change observed there would be a bug.
        Assert.eqFalse(updateGraph.currentThreadProcessesUpdates(), "updateGraph.currentThreadProcessesUpdates()");
        throw new ConstructSnapshot.SnapshotInconsistentException();
    }

    Class<?> @NotNull [] setKeyTypes() {
        return setKeyTypes;
    }

    boolean isRefreshing() {
        return setUpdateListener != null;
    }

    /**
     * Refuse an operation that cannot proceed because this set has already failed, after which no further set
     * notification is coming.
     *
     * @throws TableAlreadyFailedException If maintaining this set has failed
     */
    void throwIfFailed() {
        final Throwable localFailure = failure;
        if (localFailure != null) {
            throw new TableAlreadyFailedException("Can not filter with an already-failed set table", localFailure);
        }
    }

    /**
     * The step on which the shared keys last began changing; delegated here by {@link DynamicWhereFilter}.
     */
    long lastStateChangeStep() {
        return lastStateChangeStep;
    }

    /**
     * Register {@code filter} to be told when the shared keys change, for a snapshot attempt that is committing, if the
     * keys have not changed since that attempt read them. Registration is weak, so a filter that becomes unreachable
     * stops being notified without any explicit removal.
     * <p>
     * The check shares the monitor under which a change publishes its step, so a change is either delivered to
     * {@code filter} or is the reason it is refused, with no window in between.
     *
     * @param filter The filter to register
     * @param requiredLastStateChangeStep The {@link #lastStateChangeStep()} read when the attempt began
     * @return Whether {@code filter} was registered
     * @throws TableAlreadyFailedException If maintaining this set has failed, so that no attempt can ever commit
     */
    boolean addFilter(@NotNull final DynamicWhereFilter filter, final long requiredLastStateChangeStep) {
        synchronized (filters) {
            throwIfFailed();
            if (lastStateChangeStep != requiredLastStateChangeStep) {
                // We thought we were consistent, but the set has begun changing since we read it.
                // Must refuse this filter addition (and the current snapshot attempt).
                return false;
            }
            // Previous attempts might have added this filter already, remove then add.
            filters.remove(filter);
            filters.add(filter);
            return true;
        }
    }

    void removeFilter(@NotNull final DynamicWhereFilter filter) {
        synchronized (filters) {
            filters.remove(filter);
        }
    }

    /**
     * @return The number of filters currently registered for set change notifications
     */
    @VisibleForTesting
    int registeredFilterCount() {
        final MutableInt count = new MutableInt();
        filters.forEachValidReference(filter -> count.increment());
        return count.get();
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    @Override
    public boolean satisfied(final long step) {
        return setUpdateListener == null || setUpdateListener.satisfied(step);
    }

    LongStream parentPerformanceEntryIds() {
        if (setUpdateListener == null) {
            return LongStream.empty();
        }
        final PerformanceEntry entry = setUpdateListener.getEntry();
        return entry == null ? LongStream.empty() : LongStream.of(entry.getId());
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append("SharedSetKernel(")
                .append(MatchPair.MATCH_PAIR_ARRAY_FORMATTER, sourceToSetColumnNamePairs)
                .append(')');
    }

    /**
     * Maintains one snapshot attempt's kernel as the set table ticks, and asks the sharing filters to re-evaluate.
     * <p>
     * Each attempt builds its own listener, owning its own kernel. Removing a discarded attempt's listener does not
     * cancel a callback already running on it, so a kernel shared across attempts could be mutated by that callback
     * after the committed attempt read it; owning one per attempt makes that harmless.
     */
    private final class SetUpdateListener extends InstrumentedTableUpdateListenerAdapter {

        /** The kernel this listener maintains, mutated in place; see {@link SharedSetKernel#kernel()}. */
        private final SetKernel kernel;

        private final boolean isBlink;
        private final ModifiedColumnSet setColumnsMCS;

        /**
         * Set when a later snapshot attempt replaced this listener; a notification already queued for it then no-ops.
         * <p>
         * This is an optimization: allows early exit for superseded listeners without affecting correctness.
         */
        private volatile boolean superseded;

        private SetUpdateListener(
                @NotNull final String description,
                @NotNull final QueryTable setTable,
                @NotNull final SetKernel kernel,
                @NotNull final ModifiedColumnSet setColumnsMCS) {
            super(description, setTable, false);
            this.kernel = kernel;
            // A blink set accumulates every key it has ever held, as a selectDistinct of it would.
            this.isBlink = setTable.isBlink();
            this.setColumnsMCS = setColumnsMCS;
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            if (superseded) {
                // Nothing we do matters, so avoid wasted work.
                return;
            }
            final boolean hasAdds = upstream.added().isNonempty();
            final boolean hasRemoves = !isBlink && upstream.removed().isNonempty();
            final boolean hasModifies = upstream.modified().isNonempty()
                    && upstream.modifiedColumnSet().containsAny(setColumnsMCS);
            if (!hasAdds && !hasRemoves && !hasModifies) {
                // The kernel is unchanged, so a concurrent snapshot reading it remains consistent;
                // deliberately do not record a state change step.
                return;
            }

            // We are changing the set during this step. Publish the step before publishing the new
            // kernel, never after, so that a reader which observes the change is guaranteed to
            // observe this step and reject what it read. Publishing under the registry monitor is
            // what makes addFilter's check atomic against this change.
            // A modified row's key is removed and then added back, so the kernel changes even when the
            // row's key does not, and the step is recorded for it.
            synchronized (filters) {
                lastStateChangeStep = getUpdateGraph().clock().currentStep();
            }

            // Mark a mutation in progress, so concurrent readers abandon their attempts; see kernel() for why they
            // need no more protection than that. Incremented before mutating, and again after, so that the value
            // is odd for exactly as long as the kernel is inconsistent, which is what beginRead() relies on.
            ++generation;
            Assert.neqZero(generation & 1, "generation & 1 (must be odd while mutating)");
            // Guarantees that the kernel mutation *follows* the generation increment.
            VarHandle.storeStoreFence();

            // Every removal precedes every addition, so that a key which leaves the set and comes back within this
            // update is revived rather than replaced, and is not reported as either.
            kernel.beginUpdate();
            if (hasRemoves) {
                kernel.remove(upstream.removed());
            }
            if (hasModifies) {
                kernel.remove(upstream.getModifiedPreShift());
                kernel.add(upstream.modified());
            }
            if (hasAdds) {
                kernel.add(upstream.added());
            }
            if (hasRemoves) {
                kernel.finishRemove(upstream.removed());
            }
            if (hasModifies) {
                kernel.finishRemove(upstream.getModifiedPreShift());
            }
            ++generation;
            Assert.eqZero(generation & 1, "generation & 1 (must be even once mutation is complete)");

            final boolean added = kernel.keysAdded();
            final boolean removed = kernel.keysRemoved();
            if (!added && !removed) {
                return;
            }

            // Every filter sharing this set must re-evaluate against the updated keys. Each applies
            // its own inclusion, so exclusion filters invert the requests.
            filters.forEachValidReference(filter -> filter.onSetChanged(added, removed));
        }

        @Override
        public void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
            if (superseded) {
                return;
            }

            // A failure changes the state too. Record it under the registry monitor, publishing the step there as
            // onUpdate does, so that
            // a filter committing concurrently either registers in time to be told or is refused outright.
            synchronized (filters) {
                lastStateChangeStep = getUpdateGraph().clock().currentStep();
                if (failure == null) {
                    failure = originalException;
                }
            }

            // No lock needed, no more adds allowed after a failure.
            filters.forEachValidReference(filter -> filter.onSetError(originalException, sourceEntry));
        }
    }
}
