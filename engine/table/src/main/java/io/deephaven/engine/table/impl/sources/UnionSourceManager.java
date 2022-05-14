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
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.*;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class UnionSourceManager {

    private final boolean constituentChangesPermitted;
    private final String[] columnNames;

    private final Table coalescedPartitions;
    private final TrackingRowSet constituentSlots;
    private final ColumnSource<Table> constituentTables;

    private final TrackingWritableRowSet resultRowSet;
    private final UnionRedirection unionRedirection;
    private final UnionColumnSource<?>[] resultColumnSources;
    private final QueryTable resultTable;
    private final ModifiedColumnSet modifiedColumnSet;

    /**
     * The ListenerRecorders our MergedListener depends on. The first entry is a basic recorder for
     * {@link #coalescedPartitions}. Subsequent entries are for individual parent tables that occupy our slots.
     */
    private final List<ListenerRecorder> listenerRecorders;
    private final MergedListener mergedListener;
    private final ListenerRecorder coalescedPartitionsListener;
    private final UpdateCommitter<UnionSourceManager> updateCommitter;

    public UnionSourceManager(@NotNull final PartitionedTable partitionedTable) {
        constituentChangesPermitted = partitionedTable.constituentChangesPermitted();
        columnNames = partitionedTable.constituentDefinition().getColumnNamesArray();

        coalescedPartitions = partitionedTable.table().coalesce().select(
                new TableTransformationColumn(partitionedTable.constituentColumnName(), Table::coalesce));
        constituentSlots = coalescedPartitions.getRowSet();
        constituentTables = coalescedPartitions.getColumnSource(partitionedTable.constituentColumnName());

        final boolean refreshing = coalescedPartitions.isRefreshing();
        final int initialNumSlots = constituentSlots.intSize();

        // noinspection resource
        resultRowSet = RowSetFactory.empty().toTracking();
        unionRedirection = new UnionRedirection(initialNumSlots, refreshing);
        // noinspection unchecked
        resultColumnSources = partitionedTable.constituentDefinition().getColumnStream()
                .map(cd -> new UnionColumnSource<>(cd.getDataType(), cd.getComponentType(), this, unionRedirection,
                        new TableSourceLookup(cd.getName())))
                .toArray(UnionColumnSource[]::new);
        resultTable = new QueryTable(resultRowSet, getColumnSources());
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
            resultRowSet.insertWithShift(shiftAmount, constituent.getRowSet());
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
                resultRowSet.removeRange(unionRedirection.prevFirstRowKeys[firstShiftingTable], Long.MAX_VALUE);

                for (int tableId = firstShiftingTable; tableId <= maxTableId; ++tableId) {
                    final long startOfShift = unionRedirection.currFirstRowKeys[tableId];
                    builder.appendRowSequenceWithOffset(tables.get(tableId).getRowSet(), startOfShift);
                }

                resultRowSet.insert(builder.build());
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
                        resultRowSet.remove(newRemoved);
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
                if (shiftData.nonempty() && resultRowSet.overlapsRange(firstTableKey, lastTableKey)) {
                    final long prevCardinality = unionRedirection.prevFirstRowKeys[tableId + 1] - offset;
                    final long currCardinality = unionRedirection.currFirstRowKeys[tableId + 1] - currOffset;
                    shiftedBuilder.appendShiftData(shiftData, offset, prevCardinality, currOffset, currCardinality);

                    // if the entire table was shifted, we've already applied the RowSet update
                    if (shiftDelta == 0) {
                        // it is possible that shifts occur outside of our reserved keyspace for this table; we must
                        // protect from shifting keys that belong to other tables by clipping the shift space
                        final long lastLegalKey = unionRedirection.prevFirstRowKeys[tableId + 1] - 1;

                        try (RowSequence.Iterator rsIt = resultRowSet.getRowSequenceIterator()) {
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

            update.modifiedColumnSet = modifiedColumnSet;
            update.added = updateAddedBuilder.build();
            update.removed = updateRemovedBuilder.build();
            update.modified = updateModifiedBuilder.build();
            update.shifted = shiftedBuilder.build();

            // Finally add the new keys to the RowSet in post-shift key-space.
            try (RowSet shiftRemoveRowSet = shiftRemoveBuilder.build();
                    RowSet shiftAddedRowSet = shiftAddedBuilder.build()) {
                resultRowSet.remove(shiftRemoveRowSet);
                resultRowSet.insert(shiftAddedRowSet);
            }
            resultRowSet.insert(update.added());

            result.notifyListeners(update);
        }

        @Override
        protected boolean canExecute(final long step) {
            synchronized (listenerRecorders) {
                return listenerRecorders.stream().allMatch(lr -> lr.satisfied(step));
            }
        }
    }

    private Stream<Table> currConstituents() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        new ObjectColumnIterator<>(
                                constituentTables, constituentSlots, ColumnIterator.DEFAULT_CHUNK_SIZE),
                        constituentSlots.size(),
                        Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED),
                false);
    }

    private final class TableSourceLookup<T> implements UnionColumnSource.ConstituentSourceLookup<T> {

        private final String columnName;

        private TableSourceLookup(@NotNull final String columnName) {
            this.columnName = columnName;
        }

        @Override
        public ColumnSource<T> slotToCurrSource(final int slot) {
            return sourceFromTable(constituentTables.get(constituentSlots.get(slot)));
        }

        @Override
        public ColumnSource<T> slotToPrevSource(final int slot) {
            return sourceFromTable(constituentTables.getPrev(constituentSlots.getPrev(slot)));
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
