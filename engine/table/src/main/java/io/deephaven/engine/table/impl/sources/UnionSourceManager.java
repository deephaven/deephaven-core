/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.partitioned.TableTransformationColumn;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.*;
import io.deephaven.util.SafeCloseableList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class UnionSourceManager {

    private final boolean constituentChangesPermitted;
    private final String[] columnNames;

    private final TrackingRowSet constituentRows;
    private final ColumnSource<Table> constituentTables;

    private final TrackingWritableRowSet resultRows;
    private final UnionRedirection unionRedirection;
    private final UnionColumnSource<?>[] resultColumnSources;
    private final QueryTable resultTable;
    private final ModifiedColumnSet modifiedColumnSet;

    /**
     * The ListenerRecorders our MergedListener depends on. The first entry is a basic recorder for coalesced
     * partitions. Subsequent entries are for individual parent tables that occupy our slots.
     */
    private final List<ListenerRecorder> listenerRecorders;
    private final MergedListener mergedListener;
    private final ListenerRecorder coalescedPartitionsListener;
    private final UpdateCommitter<UnionSourceManager> updateCommitter;

    public UnionSourceManager(@NotNull final PartitionedTable partitionedTable) {
        constituentChangesPermitted = partitionedTable.constituentChangesPermitted();
        columnNames = partitionedTable.constituentDefinition().getColumnNamesArray();

        final Table coalescedPartitions = partitionedTable.table().coalesce().select(
                new TableTransformationColumn(partitionedTable.constituentColumnName(), Table::coalesce));
        constituentRows = coalescedPartitions.getRowSet();
        constituentTables = coalescedPartitions.getColumnSource(partitionedTable.constituentColumnName());

        final boolean refreshing = coalescedPartitions.isRefreshing();
        final int initialNumSlots = constituentRows.intSize();

        // noinspection resource
        resultRows = RowSetFactory.empty().toTracking();
        unionRedirection = new UnionRedirection(initialNumSlots, refreshing);
        // noinspection unchecked
        resultColumnSources = partitionedTable.constituentDefinition().getColumnStream()
                .map(cd -> new UnionColumnSource<>(cd.getDataType(), cd.getComponentType(), this, unionRedirection,
                        new TableSourceLookup(cd.getName())))
                .toArray(UnionColumnSource[]::new);
        resultTable = new QueryTable(resultRows, getColumnSources());
        modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();

        if (refreshing) {
            listenerRecorders = Collections.synchronizedList(new ArrayList<>(initialNumSlots + 1));
            mergedListener = new MergedUnionListener(listenerRecorders, resultTable);
            resultTable.addParentReference(mergedListener);

            coalescedPartitionsListener = new ListenerRecorder(
                    "PartitionedTable.merge() Partitions Listener", coalescedPartitions, mergedListener);
            listenerRecorders.add(coalescedPartitionsListener);

            updateCommitter = new UpdateCommitter<>(this, usm -> usm.unionRedirection.copyCurrToPrev());
        } else {
            listenerRecorders = null;
            mergedListener = null;
            coalescedPartitionsListener = null;
            updateCommitter = null;
        }

        currConstituents().forEach((final Table constituent) -> {
            final long shiftAmount = unionRedirection.appendInitialTable(constituent.getRowSet().lastRowKey());
            resultRows.insertWithShift(shiftAmount, constituent.getRowSet());
            if (constituent.isRefreshing()) {
                assert refreshing;
                listenerRecorders.add(new UnionListenerRecorder(constituent));
            }
        });
        if (refreshing) {
            unionRedirection.copyCurrToPrev();
        }
    }

    /**
     * Determine whether using the component tables directly in a subsequent merge will affect the correctness of that
     * merge. This is {@code true} iff constituents cannot be changed.
     *
     * @return If using the component tables is allowed
     */
    public boolean isUsingComponentsSafe() {
        return !constituentChangesPermitted;
    }

    public Collection<Table> getComponentTables() {
        if (!isUsingComponentsSafe()) {
            throw new UnsupportedOperationException("Cannot get component tables if constituent changes not permitted");
        }
        return currConstituents().collect(Collectors.toList());
    }

    public Map<String, UnionColumnSource<?>> getColumnSources() {
        final int numColumns = columnNames.length;
        final Map<String, UnionColumnSource<?>> columnSourcesMap = new LinkedHashMap<>(numColumns);
        for (int ci = 0; ci < numColumns; ci++) {
            columnSourcesMap.put(columnNames[ci], resultColumnSources[ci]);
        }
        return columnSourcesMap;
    }

    private RowSet applyCurrShift(final RowSet constituentRowSet, final int slot) {
        return constituentRowSet.shift(unionRedirection.currFirstRowKeyForSlot(slot));
    }

    private RowSet applyPreVShift(final RowSet constituentRowSet, final int slot) {
        return constituentRowSet.shift(unionRedirection.prevFirstRowKeyForSlot(slot));
    }

    @NotNull
    public QueryTable getResult() {
        return resultTable;
    }

    private final class UnionListenerRecorder extends ListenerRecorder {

        private final ModifiedColumnSet.Transformer modifiedColumnsTransformer;

        UnionListenerRecorder(@NotNull final Table parent) {
            super("PartitionedTable.merge() Constituent", parent, mergedListener);
            modifiedColumnsTransformer =
                    ((QueryTable) parent).newModifiedColumnSetTransformer(resultTable, columnNames);
            setMergedListener(mergedListener);
        }

        @Override
        public Table getParent() {
            return super.getParent();
        }
    }

    private final class MergedUnionListener extends MergedListener {

        private MergedUnionListener(
                @NotNull final List<ListenerRecorder> listenerRecorders,
                @NotNull final QueryTable resultTable) {
            super(listenerRecorders, List.of(), "PartitionedTable.merge()", resultTable);
        }

        @Override
        protected void process() {
            final TableUpdate constituentChanges = coalescedPartitionsListener.getUpdate();
            final boolean didConstituentsChange = constituentChanges != null && !constituentChanges.empty();
            if (!constituentChangesPermitted && didConstituentsChange) {
                throw new IllegalStateException(
                        "Constituent changes not permitted, but received update " + constituentChanges);
            }

            final long currentStep = LogicalClock.DEFAULT.currentStep();

            final int currNumSlots = constituentRows.intSize();
            unionRedirection.updateCurrSize(currNumSlots);
            final long[] currFirstRowKeys = unionRedirection.getCurrFirstRowKeysForUpdate();

            try (final SafeCloseableList toClose = new SafeCloseableList()) {
                // Get our previous constituent rows
                final RowSet previousConstituentRows = toClose.add(constituentRows.copyPrev());

                // Compute the row sets of constituents that have been added or removed
                final WritableRowSet addedConstituentRows = toClose.add(
                        didConstituentsChange ? RowSetFactory.empty() : constituentChanges.added().copy();
                final WritableRowSet removedConstituentRows = toClose.add(
                        didConstituentsChange ? RowSetFactory.empty() : constituentChanges.removed().copy();
                convertModifies(constituentChanges, addedConstituentRows, removedConstituentRows);

                // Convert the row sets of constituents that have been added or removed to slots
                final RowSet addedSlots = toClose.add(constituentRows.invert(addedConstituentRows));
                final RowSet removedSlots = toClose.add(previousConstituentRows.invert(removedConstituentRows));

//                final RowSet preservedCurrent = addedConstituentRows.isEmpty() ? constituentRows :
//                        toClose.add(constituentRows.minus(addedConstituentRows));
//                final RowSet preservedPrevious = removedConstituentRows.isEmpty() ? previousConstituentRows :
//                        toClose.add(previousConstituentRows.minus(removedConstituentRows));

                final RowSetShiftData.Builder shiftedBuilder = new RowSetShiftData.Builder();
                final WritableRowSet downstreamAdded = RowSetFactory.empty();
                final WritableRowSet downstreamRemoved = RowSetFactory.empty();

                try (final RowSet.Iterator addedSlotsIter = addedSlots.iterator();
                     final RowSet.Iterator removedSlotsIter = removedSlots.iterator();
                     final ObjectColumnIterator<Table> currValues = currConstituentIter(constituentRows);
                     final ObjectColumnIterator<Table> prevValues = prevConstituentIter(previousConstituentRows)) {
                    int addedSlot = addedSlotsIter.hasNext() ? (int) addedSlotsIter.nextLong() : -1;
                    int removedSlot = removedSlotsIter.hasNext() ? (int) removedSlotsIter.nextLong() : -1;
                    for (int slot = 0; slot < currSizwe; ++slot) {
                        final Table currValue = currValues.next();
                        final Table prevValue = currValues.next();
                        // TODO-RWC: Figure out how to integrate old logic here
                        if (slot == removedSlot)
                    }
                }

                long accumulatedShift = 0;
                int firstShiftingSlot = constituentRows.intSize();
                for (int tableId = 0; tableId < tables.size(); ++tableId) {
                    final long newShift =
                            unionRedirection.computeShiftIfNeeded(tableId, tables.get(tableId).getRowSet().lastRowKey());
                    unionRedirection.prevStartOfIndicesAlt[tableId] =
                            unionRedirection.currFirstRowKeys[tableId] += accumulatedShift;
                    accumulatedShift += newShift;
                    if (newShift > 0 && tableId + 1 < firstShiftingTable) {
                        firstShiftingTable = tableId + 1;
                    }
                }
                // note: prevStart must be set regardless of whether accumulatedShift is non-zero or not.
                unionRedirection.prevStartOfIndicesAlt[tables.size()] =
                        unionRedirection.currFirstRowKeys[tables.size()] += accumulatedShift;

                if (accumulatedShift > 0) {
                    final int maxTableId = tables.size() - 1;

                    final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                    resultRows.removeRange(unionRedirection.prevFirstRowKeys[firstShiftingTable], Long.MAX_VALUE);

                    for (int tableId = firstShiftingTable; tableId <= maxTableId; ++tableId) {
                        final long startOfShift = unionRedirection.currFirstRowKeys[tableId];
                        builder.appendRowSequenceWithOffset(tables.get(tableId).getRowSet(), startOfShift);
                    }

                    resultRows.insert(builder.build());
                }

                modifiedColumnSet.clear();
                final RowSetBuilderSequential updateAddedBuilder = RowSetFactory.builderSequential();
                final RowSetBuilderSequential shiftAddedBuilder = RowSetFactory.builderSequential();
                final RowSetBuilderSequential shiftRemoveBuilder = RowSetFactory.builderSequential();
                final RowSetBuilderSequential updateRemovedBuilder = RowSetFactory.builderSequential();
                final RowSetBuilderSequential updateModifiedBuilder = RowSetFactory.builderSequential();

                // listeners should be quiescent by the time we are processing this notification, because of the dependency
                // tracking
                int nextListenerId = 0;
                for (int tableId = 0; tableId < tables.size(); ++tableId) {
                    final long offset = unionRedirection.prevFirstRowKeys[tableId];
                    final long currOffset = unionRedirection.currFirstRowKeys[tableId];
                    final long shiftDelta = currOffset - offset;

                    // Listeners only contains ticking tables. However, we might need to shift tables that do not tick.
                    final ListenerRecorder listener =
                            (nextListenerId < constituentListeners.size()
                                    && constituentListeners.get(nextListenerId).tableId == tableId)
                                    ? constituentListeners.get(nextListenerId++)
                                    : null;

                    if (listener == null || listener.getNotificationStep() != currentStep) {
                        if (shiftDelta != 0) {
                            shiftedBuilder.shiftRange(unionRedirection.prevFirstRowKeys[tableId],
                                    unionRedirection.prevFirstRowKeys[tableId + 1] - 1, shiftDelta);
                        }
                        continue;
                    }

                    // Mark all dirty columns in this source table as dirty in aggregate.
                    modColumnTransformers.get(nextListenerId - 1).transform(listener.getModifiedColumnSet(),
                            modifiedColumnSet);

                    final RowSetShiftData shiftData = listener.getShifted();

                    updateAddedBuilder.appendRowSequenceWithOffset(listener.getAdded(),
                            unionRedirection.currFirstRowKeys[tableId]);
                    updateModifiedBuilder.appendRowSequenceWithOffset(listener.getModified(),
                            unionRedirection.currFirstRowKeys[tableId]);

                    if (shiftDelta == 0) {
                        try (final RowSet newRemoved = getShiftedPrevIndex(listener.getRemoved(), tableId)) {
                            updateRemovedBuilder.appendRowSequence(newRemoved);
                            resultRows.remove(newRemoved);
                        }
                    } else {
                        // If the shiftDelta is non-zero we have already updated the RowSet above (because we used the new
                        // RowSet), otherwise we need to apply the removals (adjusted by the table's starting key)
                        updateRemovedBuilder.appendRowSequenceWithOffset(listener.getRemoved(),
                                unionRedirection.prevFirstRowKeys[tableId]);
                    }

                    // Apply and process shifts.
                    final long firstTableKey = unionRedirection.currFirstRowKeys[tableId];
                    final long lastTableKey = unionRedirection.currFirstRowKeys[tableId + 1] - 1;
                    if (shiftData.nonempty() && resultRows.overlapsRange(firstTableKey, lastTableKey)) {
                        final long prevCardinality = unionRedirection.prevFirstRowKeys[tableId + 1] - offset;
                        final long currCardinality = unionRedirection.currFirstRowKeys[tableId + 1] - currOffset;
                        shiftedBuilder.appendShiftData(shiftData, offset, prevCardinality, currOffset, currCardinality);

                        // if the entire table was shifted, we've already applied the RowSet update
                        if (shiftDelta == 0) {
                            // it is possible that shifts occur outside of our reserved keyspace for this table; we must
                            // protect from shifting keys that belong to other tables by clipping the shift space
                            final long lastLegalKey = unionRedirection.prevFirstRowKeys[tableId + 1] - 1;

                            try (RowSequence.Iterator rsIt = resultRows.getRowSequenceIterator()) {
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
                        shiftedBuilder.shiftRange(unionRedirection.prevFirstRowKeys[tableId],
                                unionRedirection.prevFirstRowKeys[tableId + 1] - 1, shiftDelta);
                    }
                }

                if (accumulatedShift > 0 && updateCommitter != null) {
                    updateCommitter.maybeActivate();
                }

                final TableUpdateImpl downstream = new TableUpdateImpl();
                downstream.added = updateAddedBuilder.build();
                downstream.removed = updateRemovedBuilder.build();
                downstream.modified = updateModifiedBuilder.build();
                downstream.shifted = shiftedBuilder.build();
                downstream.modifiedColumnSet = modifiedColumnSet;

                // Finally add the new keys to the RowSet in post-shift key-space.
                try (RowSet shiftRemoveRowSet = shiftRemoveBuilder.build();
                     RowSet shiftAddedRowSet = shiftAddedBuilder.build()) {
                    resultRows.remove(shiftRemoveRowSet);
                    resultRows.insert(shiftAddedRowSet);
                }
                resultRows.insert(downstream.added());

                result.notifyListeners(downstream);
            }
        }

        @Override
        protected boolean canExecute(final long step) {
            synchronized (listenerRecorders) {
                return listenerRecorders.stream().allMatch(lr -> lr.satisfied(step));
            }
        }
    }

    /**
     * Examine all modified constituent tables. For any that have actually changed (according to reference equality),
     * insert the modified row key into {@code addedConstituentRows} (post-shift) and {@code removedConstituentRows}
     * (pre-shift).
     *
     * @param constituentChanges The upstream constituent changes to process
     * @param addedConstituentRows Added constituent rows to insert into
     * @param removedConstituentRows Removed constituent rows to insert into
     */
    private void convertModifies(
            @Nullable final TableUpdate constituentChanges,
            @NotNull final WritableRowSet addedConstituentRows,
            @NotNull final WritableRowSet removedConstituentRows) {
        //noinspection resource
        if (constituentChanges == null || constituentChanges.modified().isEmpty()) {
            return;
        }
        final RowSetBuilderSequential modifiesAsAddsBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential modifiesAsRemovesBuilder = RowSetFactory.builderSequential();
        // @formatter:off
        //noinspection resource
        try (final RowSet.Iterator modifiedCurrentRowKeys = constituentChanges.modified().iterator();
             final RowSet.Iterator modifiedPreviousRowKeys = constituentChanges.getModifiedPreShift().iterator();
             final ObjectColumnIterator<Table> modifiedCurrentValues =
                     currConstituentIter(constituentChanges.modified());
             final ObjectColumnIterator<Table> modifiedPreviousValues =
                     prevConstituentIter(constituentChanges.getModifiedPreShift())) {
            // @formatter:on
            while (modifiedCurrentRowKeys.hasNext()) {
                final long currentKey = modifiedCurrentRowKeys.nextLong();
                final long previousKey = modifiedPreviousRowKeys.nextLong();
                final Table currentValue = modifiedCurrentValues.next();
                final Table previousValue = modifiedPreviousValues.next();
                if (currentValue != previousValue) {
                    modifiesAsAddsBuilder.appendKey(currentKey);
                    modifiesAsRemovesBuilder.appendKey(previousKey);
                }
            }
        }
        try (final RowSet modifiesAsAdds = modifiesAsAddsBuilder.build()) {
            addedConstituentRows.insert(modifiesAsAdds);
        }
        try (final RowSet modifiesAsRemoves = modifiesAsRemovesBuilder.build()) {
            removedConstituentRows.insert(modifiesAsRemoves);
        }
    }

    /**
     * Get a stream over all current constituent tables.
     *
     * @return The stream
     */
    private Stream<Table> currConstituents() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        currConstituentIter(constituentRows),
                        constituentRows.size(),
                        Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED),
                false);
    }

    /**
     * Make an {@link ObjectColumnIterator} over the current constituent tables designated by {@code rows}.
     *
     * @param rows The row keys designating the constituents to iterate
     * @return The iterator
     */
    private ObjectColumnIterator<Table> currConstituentIter(@NotNull final RowSequence rows) {
        return new ObjectColumnIterator<>(constituentTables, rows);
    }

    /**
     * Make an {@link ObjectColumnIterator} over the previous constituent tables designated by {@code rows}.
     *
     * @param rows The row keys designating the constituents to iterate
     * @return The iterator
     */
    private ObjectColumnIterator<Table> prevConstituentIter(@NotNull final RowSequence rows) {
        return new ObjectColumnIterator<>(constituentTables.getPrevSource(), rows);
    }

    /**
     * ConstituentSourceLookup backed by our {@code constituentTables} and {@code constituentRows}.
     */
    private final class TableSourceLookup<T> implements UnionColumnSource.ConstituentSourceLookup<T> {

        private final String columnName;

        private TableSourceLookup(@NotNull final String columnName) {
            this.columnName = columnName;
        }

        @Override
        public ColumnSource<T> slotToCurrSource(final int slot) {
            return sourceFromTable(constituentTables.get(constituentRows.get(slot)));
        }

        @Override
        public ColumnSource<T> slotToPrevSource(final int slot) {
            return sourceFromTable(constituentTables.getPrev(constituentRows.getPrev(slot)));
        }

        @Override
        public Stream<ColumnSource<T>> currSources() {
            Assert.eqFalse(constituentChangesPermitted, "constituentChangesPermitted");
            return currConstituents().map(this::sourceFromTable);
        }

        private ColumnSource<T> sourceFromTable(@NotNull final Table table) {
            return table.getColumnSource(columnName);
        }
    }
}
