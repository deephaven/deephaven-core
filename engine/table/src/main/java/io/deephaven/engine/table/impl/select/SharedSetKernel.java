//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.NotificationAwareDependency;
import io.deephaven.engine.table.impl.NotificationStepReceiver;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.setinclusion.SetInclusionKernel;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.datastructures.ArrayWeakReferenceManager;
import io.deephaven.util.datastructures.WeakReferenceManager;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.LongStream;

/**
 * The set state shared by a {@link DynamicWhereFilter} and every copy of it: the distinct set table, the
 * {@link SetInclusionKernel} built from it, and the listener that maintains that kernel.
 * <p>
 * A where filter is copied for each operation that uses it, and a {@code PartitionedTable} proxy copies it once per
 * constituent table, so a single filter can be copied many times over. Building a kernel and subscribing a listener for
 * each copy would repeat that work per copy and leave one listener per copy attached to the set table, so copies share
 * one instance of this class instead.
 * <p>
 * The kernel is inclusion-agnostic: {@link DynamicWhereFilter} always supplies its own inclusion when matching values,
 * so one shared set serves both {@code whereIn} and {@code whereNotIn} filters over the same keys.
 */
final class SharedSetKernel extends LivenessArtifact implements NotificationAwareDependency {

    private static final int CHUNK_SIZE = 1 << 16;

    private final UpdateGraph updateGraph;
    private final MatchPair[] sourceToSetColumnNamePairs;

    /** The distinct set table, or {@code null} if the set is static and needs no maintenance. */
    private final QueryTable setTable;
    /** The kernel; owned by {@link #setUpdateListener} when the set is refreshing. */
    private final SetInclusionKernel kernel;
    private final Class<?> @NotNull [] setKeyTypes;

    @SuppressWarnings("FieldCanBeLocal")
    @ReferentialIntegrity
    private final SetUpdateListener setUpdateListener;

    /**
     * The step on which {@link #setUpdateListener} began changing {@link #kernel}, published before the change. See
     * {@link NotificationAwareDependency}.
     */
    private volatile long lastStateChangeStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

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
     * Create the shared set for {@code setTable}, reducing it to distinct key values first.
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

        // Ensure that only distinct values are passed to the kernel
        final QueryTable setTableToUse;
        final boolean setRefreshing = setTable.isRefreshing();

        final String[] setColumnNames = MatchPair.getRightColumns(sourceToSetColumnNamePairs);
        final DataIndex setIndex = DataIndexer.getDataIndex(setTable, setColumnNames);
        if (setIndex != null) {
            // We have a distinct index table, let's use it.
            setTableToUse = (QueryTable) setIndex.table();
        } else if (setRefreshing) {
            setTableToUse = (QueryTable) setTable.selectDistinct(setColumnNames);
        } else {
            final TableDefinition setDef = setTable.getDefinition();
            final boolean allPartitioning =
                    Arrays.stream(setColumnNames).allMatch(cn -> setDef.getColumn(cn).isPartitioning());
            if (allPartitioning) {
                setTableToUse = (QueryTable) setTable.selectDistinct(setColumnNames);
            } else {
                setTableToUse = (QueryTable) setTable.coalesce();
            }
        }

        // Use reinterpreted column sources for the set table tuple source.
        final ColumnSource<?>[] setColumns = Arrays.stream(sourceToSetColumnNamePairs)
                .map(mp -> setTableToUse.getColumnSource(mp.rightColumn()))
                .map(ReinterpretUtils::maybeConvertToPrimitive)
                .toArray(ColumnSource[]::new);
        setKeyTypes = Arrays.stream(setColumns).map(ColumnSource::getType).toArray(Class[]::new);
        final TupleSource<?> setKeySource = TupleSourceFactory.makeTupleSource(setColumns);

        if (!setRefreshing) {
            this.setTable = null;
            this.setUpdateListener = null;
            this.kernel = createKernel(setTableToUse, setKeySource, false);
            return;
        }

        this.setTable = setTableToUse;
        // This set outlives the caller's liveness scope, because the filters sharing it do.
        manage(setTableToUse);

        final Mutable<SetUpdateListener> resultListenerHolder = new MutableObject<>();
        snapshotAndCreate(setTableToUse, setKeySource, resultListenerHolder);
        this.setUpdateListener = resultListenerHolder.getValue();
        this.kernel = setUpdateListener.kernel;
    }

    /**
     * Create and populate the kernel from the set table and set key source.
     */
    private static @NotNull SetInclusionKernel createKernel(
            @NotNull final QueryTable setTable,
            @NotNull final TupleSource<?> setKeySource,
            final boolean usePrev) {
        // Inclusion is supplied per match by the filter, so the kernel's own inclusion is immaterial.
        final SetInclusionKernel localKernel = SetInclusionKernel.makeKernel(setKeySource.getChunkType(), true);

        final RowSet rsToUse = usePrev ? setTable.getRowSet().prev() : setTable.getRowSet();
        final ChunkSource<?> sourceToUse = usePrev ? setKeySource.getPrevSource() : setKeySource;
        if (rsToUse.isNonempty()) {
            try (final CloseableIterator<?> initialKeysIterator = ChunkedColumnIterator.make(
                    sourceToUse, rsToUse, getChunkSize(rsToUse))) {
                initialKeysIterator.forEachRemaining(localKernel::add);
            }
        }
        return localKernel;
    }

    /**
     * Create and populate the kernel and install the update listener on the same step of the cycle.
     */
    private void snapshotAndCreate(
            @NotNull final QueryTable setTable,
            @NotNull final TupleSource<?> setKeySource,
            final Mutable<SetUpdateListener> resultListenerHolder) {

        ConstructSnapshot.callDataSnapshotFunction("SharedSetKernel-createKernel",
                ConstructSnapshot.makeSnapshotControl(true, true, setTable),
                (usePrev, beforeClockUnused) -> {
                    // This function is re-invoked for every snapshot attempt. An attempt that proves inconsistent
                    // leaves behind a subscribed, managed listener, which would otherwise stay attached to the set
                    // table for the life of this set and redundantly process every set table update.
                    final SetUpdateListener staleListener = resultListenerHolder.getValue();
                    if (staleListener != null) {
                        resultListenerHolder.setValue(null);
                        unmanage(staleListener);
                        setTable.removeUpdateListener(staleListener);
                    }

                    final String[] setColumnNames = Arrays.stream(sourceToSetColumnNamePairs)
                            .map(MatchPair::rightColumn).toArray(String[]::new);
                    final ModifiedColumnSet setColumnsMCS = setTable.newModifiedColumnSet(setColumnNames);

                    final String humanReadablePrefix =
                            "DynamicWhereFilter(" + Arrays.toString(sourceToSetColumnNamePairs) + ")";
                    final SetUpdateListener localListener = new SetUpdateListener(humanReadablePrefix, setTable,
                            createKernel(setTable, setKeySource, usePrev), setKeySource, setColumnsMCS);
                    resultListenerHolder.setValue(localListener);
                    manage(localListener);
                    setTable.addUpdateListener(localListener);
                    return true;
                });
    }

    /**
     * Remove a key from {@code kernel}. Called only from the set update listener, on the update graph thread.
     * <p>
     * The kernel is supplied rather than read from {@link #kernel}, which is assigned only once the snapshot that
     * creates it has committed. A listener maintains the kernel it was created alongside, so that a listener belonging
     * to a discarded snapshot attempt cannot reach the committed one.
     */
    private static void removeKey(@NotNull final SetInclusionKernel kernel, final Object key) {
        if (!kernel.remove(key)) {
            throw new RuntimeException("Inconsistent state, key not found in set: " + key);
        }
    }

    /**
     * Add a key to {@code kernel}. See {@link #removeKey(SetInclusionKernel, Object)} for why the kernel is supplied.
     */
    private static void addKey(@NotNull final SetInclusionKernel kernel, final Object key) {
        if (!kernel.add(key)) {
            throw new RuntimeException("Inconsistent state, key already in set:" + key);
        }
    }

    private static int getChunkSize(@NotNull final RowSet selection) {
        return (int) Math.min(selection.size(), CHUNK_SIZE);
    }

    /**
     * The kernel. Readers use it with no synchronization at all, even though {@link #setUpdateListener} may be mutating
     * it on the update graph thread at the same time; they capture {@link #generation()} first and call
     * {@link #failIfChangedSince(long)} as they go, so that a read overtaken by a mutation is abandoned early.
     * <p>
     * Reading without synchronization is safe because a concurrent read of the fastutil open hash set behind every
     * kernel can return a wrong answer or throw, but cannot hang. A wrong answer is rejected by
     * {@link #stateChangedOnStep} or the snapshot clock and the attempt is retried, and the snapshot machinery retries
     * on an exception, so neither reaches a caller. Termination follows from the set's shape: {@code contains} reads
     * the {@code key} array once and {@code mask} on each probe, and {@code rehash} assigns {@code key} last, so a
     * reader can see a torn pair, but every such pair either indexes out of bounds (an exception) or probes a region
     * that still holds a free slot, because a doubled table is at most three quarters full and a table is only ever
     * halved when under a fifth full; in-place mutation never fills the table; and the iterator's position only
     * decreases. This depends on every kernel being fastutil-backed, including the object kernel, which is why that one
     * uses {@code ObjectOpenHashSet} rather than {@code HashSet}.
     */
    SetInclusionKernel kernel() {
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
        if (this.generation != generation) {
            abandonAttempt();
        }
    }

    private static void abandonAttempt() {
        // Outside a concurrent snapshot attempt, a reader runs on or downstream of the update graph thread, which
        // cannot be mutating the set at the same time; a change there is an invariant violation, not a retry.
        Assert.neqZero(ConstructSnapshot.getConcurrentAttemptClockValue(), "concurrent snapshot attempt clock value");
        throw new ConstructSnapshot.SnapshotInconsistentException();
    }

    Class<?> @NotNull [] setKeyTypes() {
        return setKeyTypes;
    }

    boolean isRefreshing() {
        return setUpdateListener != null;
    }

    /**
     * Register {@code filter} to be told when the shared keys change. Registration is idempotent for a given filter,
     * and weak, so a filter that becomes unreachable stops being notified without any explicit removal.
     */
    void addFilter(@NotNull final DynamicWhereFilter filter) {
        synchronized (filters) {
            // A failed snapshot attempt can register the same filter again; remove first to avoid a double notify.
            filters.remove(filter);
            filters.add(filter);
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

    @Override
    public boolean stateChangedOnStep(final long step) {
        return lastStateChangeStep == step;
    }

    LongStream parentPerformanceEntryIds() {
        if (setUpdateListener == null) {
            return LongStream.empty();
        }
        final PerformanceEntry entry = setUpdateListener.getEntry();
        return entry == null ? LongStream.empty() : LongStream.of(entry.getId());
    }

    @Nullable
    QueryTable setTable() {
        return setTable;
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
        private final SetInclusionKernel kernel;

        private final TupleSource<?> setKeySource;
        private final ModifiedColumnSet setColumnsMCS;

        private SetUpdateListener(
                @NotNull final String description,
                @NotNull final QueryTable setTable,
                @NotNull final SetInclusionKernel kernel,
                @NotNull final TupleSource<?> setKeySource,
                @NotNull final ModifiedColumnSet setColumnsMCS) {
            super(description, setTable, false);
            this.kernel = kernel;
            this.setKeySource = setKeySource;
            this.setColumnsMCS = setColumnsMCS;
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            final boolean hasAdds = upstream.added().isNonempty();
            final boolean hasRemoves = upstream.removed().isNonempty();
            final boolean hasModifies = upstream.modified().isNonempty()
                    && upstream.modifiedColumnSet().containsAny(setColumnsMCS);
            if (!hasAdds && !hasRemoves && !hasModifies) {
                // The kernel is unchanged, so a concurrent snapshot reading it remains consistent;
                // deliberately do not record a state change step.
                return;
            }

            // We are changing the set during this step. Publish the step before publishing the new
            // kernel, never after, so that a reader which observes the change is guaranteed to
            // observe this step and reject what it read.
            // Note that a modifies-only update whose keys all compare equal below would over-report,
            // failing concurrent previous-value snapshots that in fact read this set consistently.
            // The set table is always a selectDistinct or a data index table, which produce only adds
            // and removes for a key change, so no such update arises today.
            lastStateChangeStep = getUpdateGraph().clock().currentStep();

            // Mark a mutation in progress, so concurrent readers abandon their attempts; see kernel() for why they
            // need no more protection than that. Incremented before mutating, and again after, so that the value
            // is odd for exactly as long as the kernel is inconsistent.
            ++generation;

            boolean trueModification = false;
            // Remove removed keys
            if (hasRemoves) {
                try (final CloseableIterator<?> removedKeysIterator = ChunkedColumnIterator.make(
                        setKeySource.getPrevSource(), upstream.removed(),
                        getChunkSize(upstream.removed()))) {
                    removedKeysIterator.forEachRemaining(key -> removeKey(kernel, key));
                }
            }

            // Update modified keys
            if (hasModifies) {
                try (final CloseableIterator<?> preModifiedKeysIterator =
                        ChunkedColumnIterator.make(
                                setKeySource.getPrevSource(), upstream.getModifiedPreShift(),
                                getChunkSize(upstream.getModifiedPreShift()));
                        final CloseableIterator<?> postModifiedKeysIterator =
                                ChunkedColumnIterator.make(
                                        setKeySource, upstream.modified(),
                                        getChunkSize(upstream.modified()))) {
                    while (preModifiedKeysIterator.hasNext()) {
                        Assert.assertion(postModifiedKeysIterator.hasNext(),
                                "Pre and post modified row sets must be the same size; post is exhausted, but pre is not");
                        final Object oldKey = preModifiedKeysIterator.next();
                        final Object newKey = postModifiedKeysIterator.next();
                        if (!Objects.equals(oldKey, newKey)) {
                            trueModification = true;
                            removeKey(kernel, oldKey);
                            addKey(kernel, newKey);
                        }
                    }
                    Assert.assertion(!postModifiedKeysIterator.hasNext(),
                            "Pre and post modified row sets must be the same size; pre is exhausted, but post is not");
                }
            }

            // Add added keys
            if (hasAdds) {
                try (final CloseableIterator<?> addedKeysIterator = ChunkedColumnIterator.make(
                        setKeySource, upstream.added(), getChunkSize(upstream.added()))) {
                    addedKeysIterator.forEachRemaining(key -> addKey(kernel, key));
                }
            }
            ++generation;

            // Every filter sharing this set must re-evaluate against the updated keys. Each applies
            // its own inclusion, so exclusion filters invert the requests.
            final boolean added = hasAdds || trueModification;
            final boolean removed = hasRemoves || trueModification;
            filters.forEachValidReference(filter -> filter.onSetChanged(added, removed));
        }

        @Override
        public void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
            filters.forEachValidReference(
                    filter -> filter.onSetError(originalException, sourceEntry));
        }
    }
}
