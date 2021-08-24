/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ListenerRecorder;
import io.deephaven.db.v2.MergedListener;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.UpdateCommitter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class UnionSourceManager {

    private final UnionColumnSource[] sources;
    private final Index index;
    private final List<ModifiedColumnSet.Transformer> modColumnTransformers = new ArrayList<>();
    private final ModifiedColumnSet modifiedColumnSet;

    private final String[] names;
    private final NotificationQueue.Dependency parentDependency;
    private final UnionRedirection unionRedirection = new UnionRedirection();
    private final List<Table> tables = new ArrayList<>();
    private final List<UnionListenerRecorder> listeners =
        Collections.synchronizedList(new ArrayList<>());
    private final MergedListener mergedListener;
    private final QueryTable result;

    private boolean refreshing = false;
    private boolean disallowReinterpret = false;
    private boolean isUsingComponentsSafe = true;

    private UpdateCommitter<UnionSourceManager> prevFlusher = null;

    public UnionSourceManager(TableDefinition tableDefinition,
        @Nullable NotificationQueue.Dependency parentDependency) {
        // noinspection unchecked
        sources = tableDefinition.getColumnList().stream()
            .map((cd) -> new UnionColumnSource(cd.getDataType(), cd.getComponentType(),
                unionRedirection, this))
            .toArray(UnionColumnSource[]::new);
        names = tableDefinition.getColumnList().stream().map(ColumnDefinition::getName)
            .toArray(String[]::new);
        this.parentDependency = parentDependency;

        index = Index.FACTORY.getEmptyIndex();
        result = new QueryTable(index, getColumnSources());
        modifiedColumnSet = result.newModifiedColumnSet(names);

        mergedListener = new MergedUnionListener(listeners, "TableTools.merge()", result);
    }

    /**
     * Ensure that this UnionSourceManager will be refreshing. Should be called proactively if it is
     * expected that refreshing DynamicTables may be added *after* the initial set, in order to
     * ensure that children of the result table are correctly setup to listen and refresh.
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
     * Note that this UnionSourceManager might have tables added dynamically throughout its
     * lifetime.
     */
    public void noteUsingComponentsIsUnsafe() {
        isUsingComponentsSafe = false;
    }

    /**
     * Determine whether using the component tables directly in a subsequent merge will affect the
     * correctness of the merge.
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
    public synchronized void addTable(@NotNull final Table table, final boolean onNewTableMapKey) {
        final Map<String, ? extends ColumnSource> sources = table.getColumnSourceMap();
        if (onNewTableMapKey) {
            Require.requirement(!isUsingComponentsSafe(), "!isUsingComponentsSafe()");
        }
        Require.requirement(sources.size() == this.sources.length,
            "sources.size() == this.sources.length", sources.size(),
            "sources.size()", this.sources.length, "this.sources.length");
        unionRedirection.appendTable(table.getIndex().lastKey());

        for (int i = 0; i < this.sources.length; i++) {
            final ColumnSource sourceToAdd = sources.get(names[i]);
            Assert.assertion(sourceToAdd != null, "sources.get(names[i]) != null", names[i],
                "names[i]");
            // noinspection unchecked
            this.sources[i].appendColumnSource(sourceToAdd);
        }
        final int tableId = tables.size();
        tables.add(table);

        if (onNewTableMapKey && !disallowReinterpret) {
            // if we allow new tables to be added, then we have concurrency concerns about doing
            // reinterpretations off
            // of the LTM thread
            throw new IllegalStateException(
                "Can not add new tables when reinterpretation is enabled!");
        }

        if (table.isLive()) {
            setRefreshing();
            final DynamicTable dynTable = (DynamicTable) table;
            final UnionListenerRecorder listener = new UnionListenerRecorder("TableTools.merge",
                dynTable, tableId);
            listeners.add(listener);

            modColumnTransformers.add(dynTable.newModifiedColumnSetTransformer(result, names));

            dynTable.listenForUpdates(listener);
            if (onNewTableMapKey) {
                // synthetically invoke onUpdate lest our MergedUnionListener#process never fires.
                final ShiftAwareListener.Update update = new ShiftAwareListener.Update(
                    table.getIndex().clone(), Index.FACTORY.getEmptyIndex(),
                    Index.FACTORY.getEmptyIndex(),
                    IndexShiftData.EMPTY, ModifiedColumnSet.ALL);
                listener.onUpdate(update);
                update.release();
            }
        }

        try (final Index shifted = getShiftedIndex(table.getIndex(), tableId)) {
            index.insert(shifted);
        }
    }

    public Map<String, UnionColumnSource> getColumnSources() {
        final Map<String, UnionColumnSource> result = new LinkedHashMap<>();
        for (int i = 0; i < sources.length; i++) {
            result.put(names[i], sources[i]);
        }
        return result;
    }

    private Index getShiftedIndex(final Index index, final int tableId) {
        return index.shift(unionRedirection.startOfIndices[tableId]);
    }

    private Index getShiftedPrevIndex(final Index index, final int tableId) {
        return index.shift(unionRedirection.prevStartOfIndices[tableId]);
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

        UnionListenerRecorder(String description, DynamicTable parent, int tableId) {
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
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            final IndexShiftData.Builder shiftedBuilder = new IndexShiftData.Builder();

            final long currentStep = LogicalClock.DEFAULT.currentStep();

            long accumulatedShift = 0;
            int firstShiftingTable = tables.size();
            for (int tableId = 0; tableId < tables.size(); ++tableId) {
                final long newShift = unionRedirection.computeShiftIfNeeded(tableId,
                    tables.get(tableId).getIndex().lastKey());
                unionRedirection.prevStartOfIndicesAlt[tableId] =
                    unionRedirection.startOfIndices[tableId] += accumulatedShift;
                accumulatedShift += newShift;
                if (newShift > 0 && tableId + 1 < firstShiftingTable) {
                    firstShiftingTable = tableId + 1;
                }
            }
            // note: prevStart must be set irregardless of whether accumulatedShift is non-zero or
            // not.
            unionRedirection.prevStartOfIndicesAlt[tables.size()] =
                unionRedirection.startOfIndices[tables.size()] += accumulatedShift;

            if (accumulatedShift > 0) {
                final int maxTableId = tables.size() - 1;

                final Index.SequentialBuilder builder =
                    Index.CURRENT_FACTORY.getSequentialBuilder();
                index.removeRange(unionRedirection.prevStartOfIndices[firstShiftingTable],
                    Long.MAX_VALUE);

                for (int tableId = firstShiftingTable; tableId <= maxTableId; ++tableId) {
                    final long startOfShift = unionRedirection.startOfIndices[tableId];
                    builder.appendIndexWithOffset(tables.get(tableId).getIndex(), startOfShift);
                }

                index.insert(builder.getIndex());
            }

            final Index.SequentialBuilder updateAddedBuilder = Index.FACTORY.getSequentialBuilder();
            final Index.SequentialBuilder shiftAddedBuilder = Index.FACTORY.getSequentialBuilder();
            final Index.SequentialBuilder shiftRemoveBuilder = Index.FACTORY.getSequentialBuilder();
            final Index.SequentialBuilder updateRemovedBuilder =
                Index.FACTORY.getSequentialBuilder();
            final Index.SequentialBuilder updateModifiedBuilder =
                Index.FACTORY.getSequentialBuilder();

            // listeners should be quiescent by the time we are processing this notification,
            // because of the dependency tracking
            int nextListenerId = 0;
            for (int tableId = 0; tableId < tables.size(); ++tableId) {
                final long offset = unionRedirection.prevStartOfIndices[tableId];
                final long currOffset = unionRedirection.startOfIndices[tableId];
                final long shiftDelta = currOffset - offset;

                // Listeners only contains ticking tables. However, we might need to shift tables
                // that do not tick.
                final ListenerRecorder listener =
                    (nextListenerId < listeners.size()
                        && listeners.get(nextListenerId).tableId == tableId)
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
                modColumnTransformers.get(nextListenerId - 1)
                    .transform(listener.getModifiedColumnSet(), modifiedColumnSet);

                final IndexShiftData shiftData = listener.getShifted();

                updateAddedBuilder.appendIndexWithOffset(listener.getAdded(),
                    unionRedirection.startOfIndices[tableId]);
                updateModifiedBuilder.appendIndexWithOffset(listener.getModified(),
                    unionRedirection.startOfIndices[tableId]);

                if (shiftDelta == 0) {
                    try (final Index newRemoved =
                        getShiftedPrevIndex(listener.getRemoved(), tableId)) {
                        updateRemovedBuilder.appendIndex(newRemoved);
                        index.remove(newRemoved);
                    }
                } else {
                    // If the shiftDelta is non-zero we have already updated the index above
                    // (because we used the new index),
                    // otherwise we need to apply the removals (adjusted by the table's starting
                    // key)
                    updateRemovedBuilder.appendIndexWithOffset(listener.getRemoved(),
                        unionRedirection.prevStartOfIndices[tableId]);
                }

                // Apply and process shifts.
                final long firstTableKey = unionRedirection.startOfIndices[tableId];
                final long lastTableKey = unionRedirection.startOfIndices[tableId + 1] - 1;
                if (shiftData.nonempty() && index.overlapsRange(firstTableKey, lastTableKey)) {
                    final long prevCardinality =
                        unionRedirection.prevStartOfIndices[tableId + 1] - offset;
                    final long currCardinality =
                        unionRedirection.startOfIndices[tableId + 1] - currOffset;
                    shiftedBuilder.appendShiftData(shiftData, offset, prevCardinality, currOffset,
                        currCardinality);

                    // if the entire table was shifted, we've already applied the index update
                    if (shiftDelta == 0) {
                        // it is possible that shifts occur outside of our reserved keyspace for
                        // this table; we must
                        // protect from shifting keys that belong to other tables by clipping the
                        // shift space
                        final long lastLegalKey =
                            unionRedirection.prevStartOfIndices[tableId + 1] - 1;

                        try (OrderedKeys.Iterator okIt = index.getOrderedKeysIterator()) {
                            for (int idx = 0; idx < shiftData.size(); ++idx) {
                                final long beginRange = shiftData.getBeginRange(idx) + offset;
                                if (beginRange > lastLegalKey) {
                                    break;
                                }
                                final long endRange =
                                    Math.min(shiftData.getEndRange(idx) + offset, lastLegalKey);
                                final long rangeDelta = shiftData.getShiftDelta(idx);

                                if (!okIt.advance(beginRange)) {
                                    break;
                                }
                                Assert.leq(beginRange, "beginRange", endRange, "endRange");
                                shiftRemoveBuilder.appendRange(beginRange, endRange);
                                okIt.getNextOrderedKeysThrough(endRange)
                                    .forAllLongRanges((s, e) -> shiftAddedBuilder
                                        .appendRange(s + rangeDelta, e + rangeDelta));
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
            update.added = updateAddedBuilder.getIndex();
            update.removed = updateRemovedBuilder.getIndex();
            update.modified = updateModifiedBuilder.getIndex();
            update.shifted = shiftedBuilder.build();

            // Finally add the new keys to the index in post-shift key-space.
            try (Index shiftRemoveIndex = shiftRemoveBuilder.getIndex();
                Index shiftAddedIndex = shiftAddedBuilder.getIndex()) {
                index.remove(shiftRemoveIndex);
                index.insert(shiftAddedIndex);
            }
            index.insert(update.added);

            result.notifyListeners(update);
        }

        @Override
        protected boolean canExecute(final long step) {
            if (parentDependency != null && !parentDependency.satisfied(step)) {
                return false;
            }
            synchronized (listeners) {
                return listeners.stream()
                    .allMatch((final UnionListenerRecorder recorder) -> recorder.satisfied(step));
            }
        }
    }
}
