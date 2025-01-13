//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.partitioned.TableTransformationColumn;
import io.deephaven.engine.table.iterators.ChunkedObjectColumnIterator;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.*;
import io.deephaven.util.MultiException;
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
     * The ListenerRecorders our MergedListener depends on. The first entry is a recorder for constituent changes from
     * the parent partitioned table. Subsequent entries are for individual parent constituent tables that occupy our
     * slots. Correctness for shared use with the MergedUnionListener is delicate. MergedListener (the super class) only
     * iterates the data structure during construction and merged notification delivery, with one exception:
     * {@link MergedUnionListener#canExecute(long)}, which is mutually-synchronized with all modification operations.
     */
    private final IntrusiveDoublyLinkedQueue<LinkedListenerRecorder> listenerRecorders;
    private final MergedListener mergedListener;
    private final ConstituentChangesListenerRecorder constituentChangesListener;
    private final UpdateCommitter<UnionSourceManager> updateCommitter;
    private final ExecutionContext executionContext;

    public UnionSourceManager(@NotNull final PartitionedTable partitionedTable) {
        constituentChangesPermitted = partitionedTable.constituentChangesPermitted();
        columnNames = partitionedTable.constituentDefinition().getColumnNamesArray();

        final Table coalescedPartitions = partitionedTable.table().coalesce().select(List.of(
                new TableTransformationColumn(
                        partitionedTable.constituentColumnName(),
                        null,
                        Table::coalesce)));
        constituentRows = coalescedPartitions.getRowSet();
        constituentTables = coalescedPartitions.getColumnSource(partitionedTable.constituentColumnName());

        final boolean refreshing = coalescedPartitions.isRefreshing();
        final int initialNumSlots = constituentRows.intSize();

        // noinspection resource
        resultRows = RowSetFactory.empty().toTracking();
        unionRedirection = new UnionRedirection(initialNumSlots, refreshing);
        resultColumnSources = partitionedTable.constituentDefinition().getColumnStream()
                .map(cd -> new UnionColumnSource<>(cd.getDataType(), cd.getComponentType(), this, unionRedirection,
                        new TableSourceLookup<>(cd.getName())))
                .toArray(UnionColumnSource[]::new);
        resultTable = new QueryTable(resultRows, getColumnSources());
        modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();

        if (refreshing) {
            listenerRecorders = new IntrusiveDoublyLinkedQueue<>(
                    IntrusiveDoublyLinkedNode.Adapter.<LinkedListenerRecorder>getInstance());
            mergedListener = new MergedUnionListener(listenerRecorders, resultTable);
            resultTable.addParentReference(mergedListener);

            constituentChangesListener = new ConstituentChangesListenerRecorder(coalescedPartitions);
            coalescedPartitions.addUpdateListener(constituentChangesListener);
            listenerRecorders.offer(constituentChangesListener);

            updateCommitter = new UpdateCommitter<>(this, partitionedTable.table().getUpdateGraph(),
                    usm -> usm.unionRedirection.copyCurrToPrev());

            executionContext = ExecutionContext.newBuilder()
                    .markSystemic()
                    .build();
        } else {
            listenerRecorders = null;
            mergedListener = null;
            constituentChangesListener = null;
            updateCommitter = null;
            executionContext = null;
        }

        try (final Stream<Table> initialConstituents = currConstituents()) {
            initialConstituents.forEach((final Table constituent) -> {
                final long shiftAmount = unionRedirection.appendInitialTable(constituent.getRowSet().lastRowKey());
                resultRows.insertWithShift(shiftAmount, constituent.getRowSet());
                if (constituent.isRefreshing()) {
                    assert refreshing;
                    final ConstituentListenerRecorder constituentListener =
                            new ConstituentListenerRecorder(constituent);
                    constituent.addUpdateListener(constituentListener);
                    listenerRecorders.offer(constituentListener);
                }
            });
        }
        unionRedirection.initializePrev();
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
        try (final Stream<Table> currConstituents = currConstituents()) {
            return currConstituents.collect(Collectors.toList());
        }
    }

    public Map<String, UnionColumnSource<?>> getColumnSources() {
        final int numColumns = columnNames.length;
        final Map<String, UnionColumnSource<?>> columnSourcesMap = new LinkedHashMap<>(numColumns);
        for (int ci = 0; ci < numColumns; ci++) {
            columnSourcesMap.put(columnNames[ci], resultColumnSources[ci]);
        }
        return columnSourcesMap;
    }

    @NotNull
    public QueryTable getResult() {
        return resultTable;
    }

    private static class LinkedListenerRecorder extends ListenerRecorder
            implements IntrusiveDoublyLinkedNode<LinkedListenerRecorder> {

        private LinkedListenerRecorder next = this;
        private LinkedListenerRecorder prev = this;

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

    private final class ConstituentChangesListenerRecorder extends LinkedListenerRecorder {

        ConstituentChangesListenerRecorder(@NotNull final Table partitions) {
            super("PartitionedTable.merge() Partitions", partitions, mergedListener);
            setMergedListener(mergedListener);
        }
    }

    private final class ConstituentListenerRecorder extends LinkedListenerRecorder {

        private final ModifiedColumnSet.Transformer modifiedColumnsTransformer;
        private Throwable error;

        ConstituentListenerRecorder(@NotNull final Table constituent) {
            super("PartitionedTable.merge() Constituent", constituent, mergedListener);
            modifiedColumnsTransformer =
                    ((QueryTable) constituent).newModifiedColumnSetTransformer(resultTable, columnNames);
            setMergedListener(mergedListener);
        }

        @Override
        protected void onFailureInternal(@NotNull final Throwable originalException,
                @Nullable final Entry sourceEntry) {
            // We will just record the error here for now. If the table was removed, then we don't actually care about
            // it but if the error is real, and the table is not removed, it will be propagated by processExisting()
            this.setNotificationStep(getUpdateGraph().clock().currentStep());
            this.error = originalException;
            mergedListener.notifyChanges();
        }

        @Override
        public boolean recordedVariablesAreValid() {
            return error == null && super.recordedVariablesAreValid();
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
            final TableUpdate constituentChanges = getAndCheckConstituentChanges();
            final TableUpdate downstream;
            try (final SafeCloseable ignored = executionContext.open();
                    final ChangeProcessingContext context = new ChangeProcessingContext(constituentChanges)) {
                downstream = context.processChanges();
            } catch (Throwable ex) {
                propagateError(false, ex, entry);
                return;
            }
            result.notifyListeners(downstream);
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

        // Arrays to update
        private long[] currFirstRowKeys;
        private long[] prevFirstRowKeys;

        // Most recently retrieved item from each iterator
        private int nextRemovedSlot;
        private Table nextRemovedValue;
        private long nextCurrentKey;
        private Table nextCurrentValue;
        private long nextAddedKey;
        private long nextModifiedKey;
        private Table nextModifiedPreviousValue;
        private ConstituentListenerRecorder nextListener;

        // Slot indexes
        private int nextCurrentSlot;
        private int nextPreviousSlot;

        // Other state
        /**
         * Whether some constituent has already been removed, been added, or had to grow, causing us to truncate
         * {@link #resultRows}. The truncating constituent and following will need to insert their entire shifted row
         * set, and must update the next slot in {@link #currFirstRowKeys}.
         */
        private boolean slotAllocationChanged;
        /**
         * The first key after which we began inserting shifted constituent row sets instead of trying for piecemeal
         * updates.
         */
        private long firstTruncatedResultKey;
        /**
         * Removed constituent listeners to bulk-unmanage.
         */
        private List<LivenessReferent> toUnmanage;

        private ChangeProcessingContext(@NotNull final TableUpdate constituentChanges) {
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
                 final SafeCloseable ignored7 = toUnmanage == null
                         ? null
                         : () -> mergedListener.unmanage(toUnmanage.stream())) {
            }
            // @formatter:on
        }

        private TableUpdate processChanges() throws Throwable {
            final int currConstituentCount = constituentRows.intSize();
            final int prevConstituentCount = constituentRows.intSizePrev();
            unionRedirection.updateCurrSize(currConstituentCount);
            currFirstRowKeys = unionRedirection.getCurrFirstRowKeysForUpdate();
            prevFirstRowKeys = unionRedirection.getPrevFirstRowKeysForUpdate();

            advanceRemoved();
            advanceCurrent();
            advanceAdded();
            advanceModified();
            advanceListener();

            List<ConstituentTableException> constituentExceptions = null;

            while (nextCurrentSlot < currConstituentCount || nextPreviousSlot < prevConstituentCount) {
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
                        try {
                            processExisting(nextCurrentValue);
                        } catch (ConstituentTableException ex) {
                            constituentExceptions = collectConstituentException(constituentExceptions, ex);
                        }
                    }
                    advanceCurrent();
                    advanceModified();
                    ++nextCurrentSlot;
                    ++nextPreviousSlot;
                }
                // Existing constituent processing
                else {
                    try {
                        processExisting(nextCurrentValue);
                    } catch (ConstituentTableException ex) {
                        constituentExceptions = collectConstituentException(constituentExceptions, ex);
                    }
                    advanceCurrent();
                    ++nextCurrentSlot;
                    ++nextPreviousSlot;
                }
            }

            if (constituentExceptions != null) {
                throw MultiException.maybeWrapInMultiException("Constituent tables reported failures",
                        constituentExceptions);
            }

            Assert.eq(nextCurrentKey, "nextCurrentKey", NULL_ROW_KEY, "NULL_ROW_KEY");
            Assert.eqNull(nextCurrentValue, "nextCurrentValue");
            Assert.eq(nextRemovedSlot, "nextRemovedSlot", NULL_ROW_KEY, "NULL_ROW_KEY");
            Assert.eqNull(nextRemovedValue, "nextRemovedValue");
            Assert.eq(nextAddedKey, "nextAddedKey", NULL_ROW_KEY, "NULL_ROW_KEY");
            Assert.eq(nextModifiedKey, "nextModifiedKey", NULL_ROW_KEY, "NULL_ROW_KEY");
            Assert.eqNull(nextModifiedPreviousValue, "nextModifiedPreviousValue");
            Assert.eqNull(nextListener, "nextListener");

            try (final RowSet addedBeforeTruncate = slotAllocationChanged
                    ? downstreamAdded.subSetByKeyRange(0, firstTruncatedResultKey - 1)
                    : null) {
                final RowSet addedToInsert = slotAllocationChanged ? addedBeforeTruncate : downstreamAdded;
                resultRows.insert(addedToInsert);
            }

            return new TableUpdateImpl(
                    downstreamAdded,
                    downstreamRemoved,
                    downstreamModified,
                    downstreamShiftBuilder.build(),
                    modifiedColumnSet);
        }

        private List<ConstituentTableException> collectConstituentException(
                @Nullable List<ConstituentTableException> exceptions,
                @NotNull final ConstituentTableException exception) {
            if (exceptions == null) {
                exceptions = new ArrayList<>();
            }

            exceptions.add(exception);
            return exceptions;
        }

        private void processRemove(@NotNull final Table removedConstituent) {
            if (removedConstituent.isRefreshing()) {
                assert nextListener != null;
                Assert.eq(nextListener.getParent(), "listener parent", removedConstituent, "removed constituent");
                synchronized (listenerRecorders) {
                    listeners.remove();
                }
                removedConstituent.removeUpdateListener(nextListener);
                if (toUnmanage == null) {
                    toUnmanage = new ArrayList<>();
                }
                toUnmanage.add(nextListener);
                advanceListener();
            }
            final long firstRemovedKey = prevFirstRowKeys[nextPreviousSlot];
            // This will be a no-op unless firstRemovedKey == currFirstRowKeys[nextCurrentSlot], because any adjustment
            // to our slot allocations (remove, add, grow) will have already been reported.
            onSlotAllocationChange(firstRemovedKey);
            try (final RowSet constituentPrevKeys = removedConstituent.getRowSet().copyPrev()) {
                downstreamRemoved.insertWithShift(firstRemovedKey, constituentPrevKeys);
            }
        }

        private void processAdd(@NotNull final Table addedConstituent) {
            if (addedConstituent.isRefreshing()) {
                final ConstituentListenerRecorder addedListener = new ConstituentListenerRecorder(addedConstituent);
                addedConstituent.addUpdateListener(addedListener);
                synchronized (listenerRecorders) {
                    listenerRecorders.insertBefore(addedListener, nextListener);
                }
            }
            final long firstAddedKey = currFirstRowKeys[nextCurrentSlot];
            onSlotAllocationChange(firstAddedKey);
            currFirstRowKeys[nextCurrentSlot + 1] = checkOverflow(
                    firstAddedKey + keySpaceFor(addedConstituent.getRowSet().lastRowKey()));
            resultRows.insertWithShift(firstAddedKey, addedConstituent.getRowSet());
            downstreamAdded.insertWithShift(firstAddedKey, addedConstituent.getRowSet());
        }

        private void processExisting(@NotNull final Table constituent) {
            final long prevFirstRowKey = prevFirstRowKeys[nextPreviousSlot];
            final long nextSlotPrevFirstRowKey = prevFirstRowKeys[nextPreviousSlot + 1];
            final long prevLastRowKey = nextSlotPrevFirstRowKey - 1;

            final long currFirstRowKey = currFirstRowKeys[nextCurrentSlot];
            final long shiftDelta = currFirstRowKey - prevFirstRowKey;

            final TableUpdate changes;
            final ModifiedColumnSet.Transformer mcsTransformer;
            if (constituent.isRefreshing()) {
                assert nextListener != null;
                Assert.eq(nextListener.getParent(), "listener parent", constituent, "existing constituent");

                // Make sure we propagate any actual error on to the listeners, and advance the listener so we can
                // continue to process the rest of the tables
                if (nextListener.error != null) {
                    final String referentDescription = nextListener.getParent().getDescription();
                    advanceListener();
                    throw new ConstituentTableException(referentDescription, nextListener.error);
                }

                changes = nextListener.getUpdate();
                mcsTransformer = nextListener.modifiedColumnsTransformer;
                advanceListener();
            } else {
                changes = null;
                mcsTransformer = null;
            }

            if (changes == null || changes.empty()) {
                // Constituent is either static or did not change this cycle
                if (slotAllocationChanged) {
                    currFirstRowKeys[nextCurrentSlot + 1] = checkOverflow(nextSlotPrevFirstRowKey + shiftDelta);
                    // Don't bother shifting or re-inserting if the constituent is empty
                    if (constituent.size() > 0) {
                        resultRows.insertWithShift(currFirstRowKey, constituent.getRowSet());
                        downstreamShiftBuilder.shiftRange(prevFirstRowKey, prevLastRowKey, shiftDelta);
                    }
                }
                return;
            }

            final long neededAllocation = keySpaceFor(constituent.getRowSet().lastRowKey());
            final long prevAllocation = nextSlotPrevFirstRowKey - prevFirstRowKey;
            final long nextSlotCurrFirstRowKey;
            if (neededAllocation > prevAllocation) {
                onSlotAllocationChange(currFirstRowKey);
                currFirstRowKeys[nextCurrentSlot + 1] = nextSlotCurrFirstRowKey =
                        checkOverflow(currFirstRowKey + neededAllocation);
            } else if (slotAllocationChanged) {
                // We have the option here to shrink this constituent's key space allocation to just the needed amount.
                // On the one hand, that would allow us to reclaim some key space to use elsewhere. On the other hand,
                // that might make subsequent churn on later cycles more likely, if the constituent grows back to a size
                // commensurate with its current over-large key space allocation. Taking the churn-averse approach for
                // now.
                currFirstRowKeys[nextCurrentSlot + 1] = nextSlotCurrFirstRowKey =
                        checkOverflow(currFirstRowKey + prevAllocation);
            } else {
                // No adjustments have been to allocation, so we can use the previous value.
                nextSlotCurrFirstRowKey = nextSlotPrevFirstRowKey;
            }

            // Ignore shifts if the constituent was empty or became empty
            final boolean needToProcessShifts = changes.shifted().nonempty()
                    && constituent.getRowSet().sizePrev() != changes.removed().size();

            if (slotAllocationChanged) {
                resultRows.insertWithShift(currFirstRowKey, constituent.getRowSet());
            } else if (!needToProcessShifts) {
                // Skip this if we will remove the entire range during shift processing
                // noinspection resource
                try (final RowSet shiftedRemoved = changes.removed().shift(prevFirstRowKey)) {
                    resultRows.remove(shiftedRemoved);
                }
                // Adds will be inserted at the end of processChanges from downstreamAdded
            }

            downstreamAdded.insertWithShift(currFirstRowKey, changes.added());
            downstreamRemoved.insertWithShift(prevFirstRowKey, changes.removed());
            downstreamModified.insertWithShift(currFirstRowKey, changes.modified());
            mcsTransformer.transform(changes.modifiedColumnSet(), modifiedColumnSet);

            if (needToProcessShifts) {
                final long currAllocation = nextSlotCurrFirstRowKey - currFirstRowKey;
                downstreamShiftBuilder.appendShiftData(
                        changes.shifted(), prevFirstRowKey, prevAllocation, currFirstRowKey, currAllocation);
                if (!slotAllocationChanged) {
                    resultRows.removeRange(prevFirstRowKey, prevLastRowKey);
                    resultRows.insertWithShift(currFirstRowKey, constituent.getRowSet());
                }
            } else if (shiftDelta != 0 && constituent.getRowSet().sizePrev() != changes.removed().size()) {
                // Note that changes.removed() must be a subset of the constituent's previous row set.
                // If constituent is removing all of its previous rows, then we do not need to propagate a shift.
                Assert.assertion(slotAllocationChanged, "slotAllocationChanged");
                downstreamShiftBuilder.shiftRange(prevFirstRowKey, prevLastRowKey, shiftDelta);
            }
        }

        private void onSlotAllocationChange(final long firstShiftedKey) {
            if (!slotAllocationChanged) {
                updateCommitter.maybeActivate();
                resultRows.removeRange(firstShiftedKey, Long.MAX_VALUE);
                slotAllocationChanged = true;
                firstTruncatedResultKey = firstShiftedKey;
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

    private static ConstituentListenerRecorder tryAdvanceListener(
            @NotNull final Iterator<LinkedListenerRecorder> listeners) {
        return listeners.hasNext() ? (ConstituentListenerRecorder) listeners.next() : null;
    }

    /**
     * Get a stream over all current constituent tables. This is for internal engine use only.
     *
     * @return The stream, which must be closed
     */
    private Stream<Table> currConstituents() {
        final ObjectColumnIterator<Table> currConstituents = currConstituentIter(constituentRows);
        return StreamSupport.stream(Spliterators.spliterator(
                currConstituents,
                constituentRows.size(),
                Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED),
                false)
                .onClose(currConstituents::close);
    }

    /**
     * Make an {@link ObjectColumnIterator} over the current constituent tables designated by {@code rows}.
     *
     * @param rows The row keys designating the constituents to iterate
     * @return The iterator
     */
    private ObjectColumnIterator<Table> currConstituentIter(@NotNull final RowSequence rows) {
        return new ChunkedObjectColumnIterator<>(constituentTables, rows);
    }

    /**
     * Make an {@link ObjectColumnIterator} over the previous constituent tables designated by {@code rows}.
     *
     * @param rows The row keys designating the constituents to iterate
     * @return The iterator
     */
    private ObjectColumnIterator<Table> prevConstituentIter(@NotNull final RowSequence rows) {
        return new ChunkedObjectColumnIterator<>(constituentTables.getPrevSource(), rows);
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
