/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class UnionSourceManager {

    private final UnionColumnSource<?>[] sources;
    private final WritableRowSet rowSet;
    private final List<ModifiedColumnSet.Transformer> modColumnTransformers = new ArrayList<>();
    private final ModifiedColumnSet modifiedColumnSet;

    private final String[] names;
    private final NotificationQueue.Dependency parentDependency;
    private final UnionRedirection unionRedirection = new UnionRedirection();
    private final List<Table> tables = new ArrayList<>();
    private final List<UnionListenerRecorder> listeners = Collections.synchronizedList(new ArrayList<>());
    private final MergedListener mergedListener;
    private final QueryTable result;

    private boolean refreshing = false;
    private boolean disallowReinterpret = false;
    private boolean isUsingComponentsSafe = true;

    private UpdateCommitter<UnionSourceManager> prevFlusher = null;

    public UnionSourceManager(TableDefinition tableDefinition,
            @Nullable NotificationQueue.Dependency parentDependency) {
        sources = tableDefinition.getColumnList().stream()
                .map((cd) -> new UnionColumnSource<>(cd.getDataType(), cd.getComponentType(), unionRedirection, this))
                .toArray(UnionColumnSource[]::new);
        names = tableDefinition.getColumnList().stream().map(ColumnDefinition::getName).toArray(String[]::new);
        this.parentDependency = parentDependency;

        result = new QueryTable(RowSetFactory.empty().toTracking(), getColumnSources());
        rowSet = result.getRowSet().writableCast();
        modifiedColumnSet = result.newModifiedColumnSet(names);

        mergedListener = new MergedUnionListener(listeners, "TableTools.merge()", result);
    }

    /**
     * Ensure that this UnionSourceManager will be refreshing. Should be called proactively if it is expected that
     * refreshing DynamicTables may be added *after* the initial set, in order to ensure that children of the result
     * table are correctly setup to listen and run.
     */
    public void setRefreshing() {
        if (refreshing) {
            return;
        }
        refreshing = true;
        result.addParentReference(mergedListener);
        prevFlusher = new UpdateCommitter<>(this, UnionSourceManager::swapPrevStartOfIndices);
    }

    /**
     * Specify that columns sources using this manager should not allow reinterpret.
     */
    public void setDisallowReinterpret() {
        disallowReinterpret = true;
    }

    /**
     * Determine whether column sources that use this manager should allow reinterpret.
     *
     * @return If {@link ColumnSource#reinterpret(Class)} is allowed
     */
    public boolean allowsReinterpret() {
        return !disallowReinterpret;
    }

    /**
     * Note that this UnionSourceManager might have tables added dynamically throughout its lifetime.
     */
    public void noteUsingComponentsIsUnsafe() {
        isUsingComponentsSafe = false;
    }

    /**
     * Determine whether using the component tables directly in a subsequent merge will affect the correctness of the
     * merge.
     *
     * @return If using the component tables is allowed.
     */
    public boolean isUsingComponentsSafe() {
        return isUsingComponentsSafe;
    }

    /**
     * Adds a table to the managed constituents.
     *
     * @param table the new table
     * @param onNewTableMapKey whether this table is being added after the initial setup
     */
    public synchronized void addTable(@NotNull final QueryTable table, final boolean onNewTableMapKey) {
        final Map<String, ? extends ColumnSource<?>> sources = table.getColumnSourceMap();
        if (onNewTableMapKey) {
            Require.requirement(!isUsingComponentsSafe(), "!isUsingComponentsSafe()");
        }
        Require.requirement(sources.size() == this.sources.length,
                "sources.size() == this.sources.length", sources.size(),
                "sources.size()", this.sources.length, "this.sources.length");
        unionRedirection.appendTable(table.getRowSet().lastRowKey());

        for (int i = 0; i < this.sources.length; i++) {
            final ColumnSource<?> sourceToAdd = sources.get(names[i]);
            Assert.assertion(sourceToAdd != null, "sources.get(names[i]) != null", names[i],
                    "names[i]");
            // noinspection unchecked,rawtypes
            this.sources[i].appendColumnSource((ColumnSource) sourceToAdd);
        }
        final int tableId = tables.size();
        tables.add(table);

        if (onNewTableMapKey && !disallowReinterpret) {
            // if we allow new tables to be added, then we have concurrency concerns about doing reinterpretations off
            // of the UGP thread
            throw new IllegalStateException("Can not add new tables when reinterpretation is enabled!");
        }

        if (table.isRefreshing()) {
            setRefreshing();
            final UnionListenerRecorder listener = new UnionListenerRecorder("TableTools.merge",
                    table, tableId);
            listeners.add(listener);

            modColumnTransformers.add(table.newModifiedColumnSetTransformer(result, names));

            table.listenForUpdates(listener);
            if (onNewTableMapKey) {
                // synthetically invoke onUpdate lest our MergedUnionListener#process never fires.
                final TableUpdate update = new TableUpdateImpl(
                        table.getRowSet().copy(), RowSetFactory.empty(), RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.ALL);
                listener.onUpdate(update);
                update.release();
            }
        }

        try (final RowSet shifted = getShiftedIndex(table.getRowSet(), tableId)) {
            rowSet.insert(shifted);
        }
    }

    public Map<String, UnionColumnSource<?>> getColumnSources() {
        final Map<String, UnionColumnSource<?>> result = new LinkedHashMap<>();
        for (int i = 0; i < sources.length; i++) {
            result.put(names[i], sources[i]);
        }
        return result;
    }

    private RowSet getShiftedIndex(final RowSet rowSet, final int tableId) {
        return rowSet.shift(unionRedirection.startOfIndices[tableId]);
    }

    private RowSet getShiftedPrevIndex(final RowSet rowSet, final int tableId) {
        return rowSet.shift(unionRedirection.prevStartOfIndices[tableId]);
    }

    public Collection<Table> getComponentTables() {
        return tables;
    }

    @NotNull
    public QueryTable getResult() {
        return result;
    }

    private void swapPrevStartOfIndices() {
        final long[] tmp = unionRedirection.prevStartOfIndices;
        unionRedirection.prevStartOfIndices = unionRedirection.prevStartOfIndicesAlt;
        unionRedirection.prevStartOfIndicesAlt = tmp;
    }

    class UnionListenerRecorder extends ListenerRecorder {
        final int tableId;

        UnionListenerRecorder(String description, Table parent, int tableId) {
            super(description, parent, result);
            this.tableId = tableId;
            setMergedListener(mergedListener);
        }
    }

    class MergedUnionListener extends MergedListener {
        MergedUnionListener(Collection<UnionListenerRecorder> recorders, String listenerDescription,
                QueryTable result) {
            super(recorders, Collections.emptyList(), listenerDescription, result);
        }

        @Override
        protected void process() {
            modifiedColumnSet.clear();
            final TableUpdateImpl update = new TableUpdateImpl();
            final RowSetShiftData.Builder shiftedBuilder = new RowSetShiftData.Builder();

            final long currentStep = LogicalClock.DEFAULT.currentStep();

            long accumulatedShift = 0;
            int firstShiftingTable = tables.size();
            for (int tableId = 0; tableId < tables.size(); ++tableId) {
                final long newShift =
                        unionRedirection.computeShiftIfNeeded(tableId, tables.get(tableId).getRowSet().lastRowKey());
                unionRedirection.prevStartOfIndicesAlt[tableId] =
                        unionRedirection.startOfIndices[tableId] += accumulatedShift;
                accumulatedShift += newShift;
                if (newShift > 0 && tableId + 1 < firstShiftingTable) {
                    firstShiftingTable = tableId + 1;
                }
            }
            // note: prevStart must be set irregardless of whether accumulatedShift is non-zero or not.
            unionRedirection.prevStartOfIndicesAlt[tables.size()] =
                    unionRedirection.startOfIndices[tables.size()] += accumulatedShift;

            if (accumulatedShift > 0) {
                final int maxTableId = tables.size() - 1;

                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                rowSet.removeRange(unionRedirection.prevStartOfIndices[firstShiftingTable], Long.MAX_VALUE);

                for (int tableId = firstShiftingTable; tableId <= maxTableId; ++tableId) {
                    final long startOfShift = unionRedirection.startOfIndices[tableId];
                    builder.appendRowSequenceWithOffset(tables.get(tableId).getRowSet(), startOfShift);
                }

                rowSet.insert(builder.build());
            }

            final RowSetBuilderSequential updateAddedBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential shiftAddedBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential shiftRemoveBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential updateRemovedBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential updateModifiedBuilder = RowSetFactory.builderSequential();

            // listeners should be quiescent by the time we are processing this notification, because of the dependency
            // tracking
            int nextListenerId = 0;
            for (int tableId = 0; tableId < tables.size(); ++tableId) {
                final long offset = unionRedirection.prevStartOfIndices[tableId];
                final long currOffset = unionRedirection.startOfIndices[tableId];
                final long shiftDelta = currOffset - offset;

                // Listeners only contains ticking tables. However, we might need to shift tables that do not tick.
                final ListenerRecorder listener =
                        (nextListenerId < listeners.size() && listeners.get(nextListenerId).tableId == tableId)
                                ? listeners.get(nextListenerId++)
                                : null;

                if (listener == null || listener.getNotificationStep() != currentStep) {
                    if (shiftDelta != 0) {
                        shiftedBuilder.shiftRange(unionRedirection.prevStartOfIndices[tableId],
                                unionRedirection.prevStartOfIndices[tableId + 1] - 1, shiftDelta);
                    }
                    continue;
                }

                // Mark all dirty columns in this source table as dirty in aggregate.
                modColumnTransformers.get(nextListenerId - 1).transform(listener.getModifiedColumnSet(),
                        modifiedColumnSet);

                final RowSetShiftData shiftData = listener.getShifted();

                updateAddedBuilder.appendRowSequenceWithOffset(listener.getAdded(),
                        unionRedirection.startOfIndices[tableId]);
                updateModifiedBuilder.appendRowSequenceWithOffset(listener.getModified(),
                        unionRedirection.startOfIndices[tableId]);

                if (shiftDelta == 0) {
                    try (final RowSet newRemoved = getShiftedPrevIndex(listener.getRemoved(), tableId)) {
                        updateRemovedBuilder.appendRowSequence(newRemoved);
                        rowSet.remove(newRemoved);
                    }
                } else {
                    // If the shiftDelta is non-zero we have already updated the RowSet above (because we used the new
                    // RowSet),
                    // otherwise we need to apply the removals (adjusted by the table's starting key)
                    updateRemovedBuilder.appendRowSequenceWithOffset(listener.getRemoved(),
                            unionRedirection.prevStartOfIndices[tableId]);
                }

                // Apply and process shifts.
                final long firstTableKey = unionRedirection.startOfIndices[tableId];
                final long lastTableKey = unionRedirection.startOfIndices[tableId + 1] - 1;
                if (shiftData.nonempty() && rowSet.overlapsRange(firstTableKey, lastTableKey)) {
                    final long prevCardinality = unionRedirection.prevStartOfIndices[tableId + 1] - offset;
                    final long currCardinality = unionRedirection.startOfIndices[tableId + 1] - currOffset;
                    shiftedBuilder.appendShiftData(shiftData, offset, prevCardinality, currOffset, currCardinality);

                    // if the entire table was shifted, we've already applied the RowSet update
                    if (shiftDelta == 0) {
                        // it is possible that shifts occur outside of our reserved keyspace for this table; we must
                        // protect from shifting keys that belong to other tables by clipping the shift space
                        final long lastLegalKey = unionRedirection.prevStartOfIndices[tableId + 1] - 1;

                        try (RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator()) {
                            for (int idx = 0; idx < shiftData.size(); ++idx) {
                                final long beginRange = shiftData.getBeginRange(idx) + offset;
                                if (beginRange > lastLegalKey) {
                                    break;
                                }
                                final long endRange = Math.min(shiftData.getEndRange(idx) + offset, lastLegalKey);
                                final long rangeDelta = shiftData.getShiftDelta(idx);

                                if (!rsIt.advance(beginRange)) {
                                    break;
                                }
                                Assert.leq(beginRange, "beginRange", endRange, "endRange");
                                shiftRemoveBuilder.appendRange(beginRange, endRange);
                                rsIt.getNextRowSequenceThrough(endRange).forAllRowKeyRanges(
                                        (s, e) -> shiftAddedBuilder.appendRange(s + rangeDelta, e + rangeDelta));
                            }
                        }
                    }
                } else if (shiftDelta != 0) {
                    // shift entire thing
                    shiftedBuilder.shiftRange(unionRedirection.prevStartOfIndices[tableId],
                            unionRedirection.prevStartOfIndices[tableId + 1] - 1, shiftDelta);
                }
            }

            if (accumulatedShift > 0 && prevFlusher != null) {
                prevFlusher.maybeActivate();
            }

            update.modifiedColumnSet = modifiedColumnSet;
            update.added = updateAddedBuilder.build();
            update.removed = updateRemovedBuilder.build();
            update.modified = updateModifiedBuilder.build();
            update.shifted = shiftedBuilder.build();

            // Finally add the new keys to the RowSet in post-shift key-space.
            try (RowSet shiftRemoveRowSet = shiftRemoveBuilder.build();
                    RowSet shiftAddedRowSet = shiftAddedBuilder.build()) {
                rowSet.remove(shiftRemoveRowSet);
                rowSet.insert(shiftAddedRowSet);
            }
            rowSet.insert(update.added());

            result.notifyListeners(update);
        }

        @Override
        protected boolean canExecute(final long step) {
            if (parentDependency != null && !parentDependency.satisfied(step)) {
                return false;
            }
            synchronized (listeners) {
                return listeners.stream().allMatch((final UnionListenerRecorder recorder) -> recorder.satisfied(step));
            }
        }
    }
}
