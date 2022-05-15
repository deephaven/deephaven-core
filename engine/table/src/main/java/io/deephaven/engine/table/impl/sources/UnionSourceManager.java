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
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.sources.UnionRedirection.checkOverflow;
import static io.deephaven.engine.table.impl.sources.UnionRedirection.keySpaceFor;

public class UnionSourceManager {

    /**
     * Re-usable empty table update to simplify update processing for listeners with no recorded update.
     */
    private static final TableUpdate EMPTY_TABLE_UPDATE = new TableUpdateImpl(
            RowSetFactory.empty(), RowSetFactory.empty(), RowSetFactory.empty(),
            RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);

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
     * The ListenerRecorders our MergedListener depends on. The first entry is a basic recorder for constituent changes
     * from the parent partitioned table. Subsequent entries are for individual parent tables that occupy our slots.
     * Correctness for shared use with the MergedUnionListener is delicate. MergedListener (the super class) only
     * iterates the data structure during construction and merged notification delivery, with one exception:
     * {@link MergedUnionListener#canExecute(long)}, which is mutually-synchronized with all modification operations.
     */
    private final IntrusiveDoublyLinkedQueue<LinkedListenerRecorder> listenerRecorders;
    private final MergedListener mergedListener;
    private final LinkedListenerRecorder constituentChangesListener;
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
            listenerRecorders = new IntrusiveDoublyLinkedQueue<>(
                    IntrusiveDoublyLinkedNode.Adapter.<LinkedListenerRecorder>getInstance());
            mergedListener = new MergedUnionListener(listenerRecorders, resultTable);
            resultTable.addParentReference(mergedListener);

            constituentChangesListener = new LinkedListenerRecorder(
                    "PartitionedTable.merge() Partitions Listener", coalescedPartitions, mergedListener);
            listenerRecorders.offer(constituentChangesListener);

            updateCommitter = new UpdateCommitter<>(this, usm -> usm.unionRedirection.copyCurrToPrev());
        } else {
            listenerRecorders = null;
            mergedListener = null;
            constituentChangesListener = null;
            updateCommitter = null;
        }

        currConstituents().forEach((final Table constituent) -> {
            final long shiftAmount = unionRedirection.appendInitialTable(constituent.getRowSet().lastRowKey());
            resultRows.insertWithShift(shiftAmount, constituent.getRowSet());
            if (constituent.isRefreshing()) {
                assert refreshing;
                listenerRecorders.offer(new UnionListenerRecorder(constituent));
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

    private static class LinkedListenerRecorder extends ListenerRecorder
            implements IntrusiveDoublyLinkedNode<LinkedListenerRecorder> {

        private LinkedListenerRecorder next;
        private LinkedListenerRecorder prev;

        private LinkedListenerRecorder(
                @NotNull final String description,
                @NotNull final Table parent,
                @Nullable final Object dependent) {
            super(description, parent, dependent);
        }

        @NotNull
        @Override
        public final LinkedListenerRecorder getNext() {
            return next;
        }

        @Override
        public final void setNext(@NotNull final LinkedListenerRecorder other) {
            next = other;
        }

        @NotNull
        @Override
        public final LinkedListenerRecorder getPrev() {
            return prev;
        }

        @Override
        public final void setPrev(@NotNull final LinkedListenerRecorder other) {
            prev = other;
        }
    }

    private final class UnionListenerRecorder extends LinkedListenerRecorder {

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
                @NotNull final Iterable<? extends ListenerRecorder> listenerRecorders,
                @NotNull final QueryTable resultTable) {
            super(listenerRecorders, List.of(), "PartitionedTable.merge()", resultTable);
        }

        @Override
        protected void process() {
            final long currentStep = LogicalClock.DEFAULT.currentStep();
            final TableUpdate constituentChanges = getAndCheckConstituentChanges();
            final TableUpdate downstream;
            try (final ChangeProcessingContext context = new ChangeProcessingContext(currentStep, constituentChanges)) {
                downstream = context.processChanges();
            }
            {
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

    private TableUpdate getAndCheckConstituentChanges() {
        final TableUpdate constituentChanges = constituentChangesListener.getUpdate();
        if (!constituentChangesPermitted && constituentChanges != null && !constituentChanges.empty()) {
            throw new IllegalStateException(
                    "Constituent changes not permitted, but received update " + constituentChanges);
        }
        return constituentChanges == null ? EMPTY_TABLE_UPDATE : constituentChanges;
    }

    /**
     * Context for processing constituent changes
     */
    private final class ChangeProcessingContext implements SafeCloseable {

        /** Clock step for this update cycle */
        private final long currentStep;

        /** Upstream changes to the parent table of constituents */
        private final TableUpdate constituentChanges;

        // Arrays to update
        final long[] currFirstRowKeys;
        final long[] prevFirstRowKeys;

        // Downstream update accumulators
        private final WritableRowSet downstreamAdded;
        private final WritableRowSet downstreamRemoved;
        private final WritableRowSet downstreamModified;
        private final RowSetShiftData.Builder downstreamShiftBuilder;

        // Iterators
        private final RowSet.Iterator currentKeys;
        private final ObjectColumnIterator<Table> currentValues;
        private final RowSet.Iterator removedSlots;
        private final ObjectColumnIterator<Table> removedValues;
        private final RowSet.Iterator addedKeys;
        private final RowSet.Iterator modifiedKeys;
        private final ObjectColumnIterator<Table> modifiedPreviousValues;
        private final Iterator<LinkedListenerRecorder> listeners;

        // Most recently retrieved item from each iterator
        private int nextRemovedSlot;
        private Table nextRemovedValue;
        private long nextCurrentKey;
        private Table nextCurrentValue;
        private long nextAddedKey;
        private long nextModifiedKey;
        private Table nextModifiedPreviousValue;
        private UnionListenerRecorder nextListener;

        // Slot indexes
        private int nextCurrentSlot;
        private int nextPreviousSlot;

        // Other state
        boolean truncatedResult;

        private ChangeProcessingContext(final long currentStep, @NotNull final TableUpdate constituentChanges) {
            this.currentStep = currentStep;
            this.constituentChanges = constituentChanges;

            currFirstRowKeys = unionRedirection.getCurrFirstRowKeysForUpdate();
            prevFirstRowKeys = unionRedirection.getPrevFirstRowKeysForUpdate();

            modifiedColumnSet.clear();
            downstreamAdded = RowSetFactory.empty();
            downstreamRemoved = RowSetFactory.empty();
            downstreamModified = RowSetFactory.empty();
            downstreamShiftBuilder = new RowSetShiftData.Builder();

            currentKeys = constituentRows.iterator();
            currentValues = currConstituentIter(constituentRows);
            // @formatter:off
            try (final RowSet previousRows = constituentRows.copyPrev();
                 final RowSet removedKeysInverted = previousRows.invert(constituentChanges.removed())) {
                // @formatter:on
                removedSlots = removedKeysInverted.iterator();
            }
            removedValues = prevConstituentIter(constituentChanges.removed());
            // noinspection resource
            addedKeys = constituentChanges.added().iterator();
            // noinspection resource
            modifiedKeys = constituentChanges.modified().iterator();
            modifiedPreviousValues = prevConstituentIter(constituentChanges.getModifiedPreShift());
            listeners = listenerRecorders.iterator();
            Assert.eq(listeners.next(), "first listener", constituentChangesListener, "constituentChangesListener");
        }

        private void advanceRemoved() {
            nextRemovedSlot = tryAdvanceSlot(removedSlots);
            nextRemovedValue = tryAdvanceTable(removedValues);
        }

        private void advanceCurrent() {
            nextCurrentKey = tryAdvanceKey(currentKeys);
            nextCurrentValue = tryAdvanceTable(currentValues);
        }

        private void advanceAdded() {
            nextAddedKey = tryAdvanceKey(addedKeys);
        }

        private void advanceModified() {
            nextModifiedKey = tryAdvanceKey(modifiedKeys);
            nextModifiedPreviousValue = tryAdvanceTable(modifiedPreviousValues);
        }

        private void advanceListener() {
            nextListener = tryAdvanceListener(listeners);
        }

        @Override
        public void close() {
            // @formatter:off
            //noinspection EmptyTryBlock
            try (final SafeCloseable ignored0 = currentKeys;
                 final SafeCloseable ignored1 = currentValues;
                 final SafeCloseable ignored2 = removedSlots;
                 final SafeCloseable ignored3 = removedValues;
                 final SafeCloseable ignored4 = addedKeys;
                 final SafeCloseable ignored5 = modifiedKeys;
                 final SafeCloseable ignored6 = modifiedPreviousValues;
            ) {}
            // @formatter:on
        }

        private TableUpdate processChanges(final long currentStep) {
            final int currConstituentCount = constituentRows.intSize();
            final int prevConstituentCount = constituentRows.intSizePrev();
            unionRedirection.updateCurrSize(currConstituentCount);

            advanceRemoved();
            advanceCurrent();
            advanceAdded();
            advanceModified();
            advanceListener();

            while (nextCurrentSlot < currConstituentCount && nextPreviousSlot < prevConstituentCount) {
                // Removed constituent processing
                if (nextPreviousSlot == nextRemovedSlot) {
                    assert nextRemovedValue != null;
                    processRemove(nextRemovedValue);
                    advanceRemoved();
                    ++nextPreviousSlot;
                }
                // Added constituent processing
                else if (nextCurrentKey == nextAddedKey) {
                    assert nextCurrentValue != null;
                    processAdd(nextCurrentValue);
                    advanceCurrent();
                    advanceAdded();
                    ++nextCurrentSlot;
                }
                // Modified constituent processing
                else if (nextCurrentKey == nextModifiedKey) {
                    assert nextModifiedPreviousValue != null;
                    // "Real" modification processing
                    if (nextCurrentValue != nextModifiedPreviousValue) {
                        processRemove(nextModifiedPreviousValue);
                        processAdd(nextCurrentValue);
                    } else {
                        processExisting(nextCurrentValue);
                    }
                    advanceCurrent();
                    advanceModified();
                    ++nextCurrentSlot;
                    ++nextPreviousSlot;
                }
                // Existing constituent processing
                else {
                    processExisting(nextCurrentValue);
                    advanceCurrent();
                    ++nextCurrentSlot;
                    ++nextPreviousSlot;
                }
            }

            // TODO-RWC: resultRowSet update (adds inserted?)

            if (truncatedResult) {
                updateCommitter.maybeActivate();
            }

            return new TableUpdateImpl(
                    downstreamAdded,
                    downstreamRemoved,
                    downstreamModified,
                    downstreamShiftBuilder.build(),
                    modifiedColumnSet);
        }

        private void processRemove(@NotNull final Table removedConstituent) {
            if (removedConstituent.isRefreshing()) {
                assert nextListener != null;
                Assert.eq(nextListener.getParent(), "listener parent", removedConstituent, "removed constituent");
                synchronized (listenerRecorders) {
                    listeners.remove();
                }
                removedConstituent.removeUpdateListener(nextListener);
                mergedListener.unmanage(nextListener);
                advanceListener();
            }
            final long firstRemovedKey = prevFirstRowKeys[nextPreviousSlot];
            maybeTruncateFrom(firstRemovedKey);
            try (final RowSet constituentPrevKeys = removedConstituent.getRowSet().copyPrev()) {
                downstreamRemoved.insertWithShift(firstRemovedKey, constituentPrevKeys);
            }
        }

        private void processAdd(@NotNull final Table addedConstituent) {
            if (addedConstituent.isRefreshing()) {
                final UnionListenerRecorder addedListener = new UnionListenerRecorder(addedConstituent);
                synchronized (listenerRecorders) {
                    listenerRecorders.insertBefore(addedListener, nextListener);
                }
            }
            final long firstAddedKey = currFirstRowKeys[nextCurrentSlot];
            currFirstRowKeys[nextCurrentSlot + 1] = checkOverflow(
                    firstAddedKey + keySpaceFor(addedConstituent.getRowSet().lastRowKey()));
            maybeTruncateFrom(firstAddedKey);
            downstreamAdded.insertWithShift(firstAddedKey, addedConstituent.getRowSet());
            resultRows.insertWithShift(firstAddedKey, addedConstituent.getRowSet());
        }

        private void processExisting(@NotNull final Table constituent) {
            final long currFirstRowKey = currFirstRowKeys[nextCurrentSlot];
            final long prevFirstRowKey = prevFirstRowKeys[nextPreviousSlot];
            final long nextSlotPrevFirstRowKey = prevFirstRowKeys[nextPreviousSlot + 1];
            final long shiftAmount = currFirstRowKey - prevFirstRowKey;

            final TableUpdate changes;
            final ModifiedColumnSet.Transformer mcsTransformer;
            if (constituent.isRefreshing()) {
                assert nextListener != null;
                Assert.eq(nextListener.getParent(), "listener parent", constituent, "existing constituent");
                changes = nextListener.getUpdate();
                mcsTransformer = nextListener.modifiedColumnsTransformer;
                advanceListener();
            } else {
                changes = null;
                mcsTransformer = null;
            }

            if (changes == null || changes.empty()) {
                if (truncatedResult) { // Some slot earlier than this was removed, added, or had to grow
                    currFirstRowKeys[nextCurrentSlot + 1] = checkOverflow(nextSlotPrevFirstRowKey + shiftAmount);
                    resultRows.insertWithShift(currFirstRowKey, constituent.getRowSet());
                }
                if (shiftAmount != 0 && constituent.getRowSet().isNonempty()) {
                    downstreamShiftBuilder.shiftRange(prevFirstRowKey, nextSlotPrevFirstRowKey - 1, shiftAmount);
                }
                return;
            }

            final long neededAllocation = keySpaceFor(constituent.getRowSet().lastRowKey());
            final long previousAllocation = nextSlotPrevFirstRowKey - prevFirstRowKey;
            if (neededAllocation > previousAllocation) {
                maybeTruncateFrom(prevFirstRowKey);
                currFirstRowKeys[nextCurrentSlot + 1] = checkOverflow(currFirstRowKey + neededAllocation);
            }

            downstreamAdded.insertWithShift(currFirstRowKey, changes.added());
            downstreamRemoved.insertWithShift(prevFirstRowKey, changes.removed());
            downstreamModified.insertWithShift(currFirstRowKey, changes.modified());
            mcsTransformer.transform(changes.modifiedColumnSet(), modifiedColumnSet);

            // TODO-RWC: Resume from here with shift processing

            if (truncatedResult) {
                resultRows.insertWithShift(currFirstRowKey, constituent.getRowSet());
            } else {
                try (final RowSet shiftedRemoved = changes.removed().shift(prevFirstRowKey)) {
                    resultRows.remove(shiftedRemoved);
                }
                // Adds will be inserted at the end from downstreamAdded
            }
        }

        private void maybeTruncateFrom(final long firstShiftedKey) {
            if (!truncatedResult) {
                resultRows.removeRange(firstShiftedKey, Long.MAX_VALUE);
                truncatedResult = true;
            }
        }
    }

    private static long tryAdvanceKey(@NotNull final RowSet.Iterator keys) {
        return keys.hasNext() ? keys.nextLong() : NULL_ROW_KEY;
    }

    private static int tryAdvanceSlot(@NotNull final RowSet.Iterator slots) {
        return Math.toIntExact(tryAdvanceKey(slots));
    }

    private static Table tryAdvanceTable(@NotNull final ObjectColumnIterator<Table> tables) {
        return tables.hasNext() ? tables.next() : null;
    }

    private static UnionListenerRecorder tryAdvanceListener(@NotNull final Iterator<LinkedListenerRecorder> listeners) {
        return listeners.hasNext() ? (UnionListenerRecorder) listeners.next() : null;
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
