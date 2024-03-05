//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.data;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.web.client.api.barrage.WebBarrageMessage;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.PrimitiveIterator;
import java.util.TreeMap;

public abstract class WebBarrageSubscription {
    public interface WebDataSink {
        void fillChunk(Chunk<?> data, PrimitiveIterator.OfLong destIterator);

        default void ensureCapacity(long size) {}
    }

    protected final ClientTableState state;
    protected final RangeSet currentRowSet = RangeSet.empty();

    protected long capacity = 0;
    protected final WebDataSink[] destSources;

    protected RangeSet serverViewport;
    protected BitSet serverColumns;
    protected boolean serverReverseViewport;

    public WebBarrageSubscription(ClientTableState state) {
        this.state = state;
        destSources = new WebDataSink[state.getTableDef().getColumns().length];
    }

    protected abstract void applyUpdates(WebBarrageMessage message);

    protected void updateServerViewport(RangeSet viewport, BitSet columns, boolean reverseViewport) {
        serverViewport = viewport;
        serverColumns = columns == null || columns.cardinality() == numColumns() ? null : columns;
        serverReverseViewport = reverseViewport;

        // TODO viewport change event?
    }

    private int numColumns() {
        return getDefinition().getColumns().length;
    }

    private InitialTableDefinition getDefinition() {
        return state.getTableDef();
    }

    public RangeSet getCurrentRowSet() {
        return currentRowSet;
    }

    protected boolean isSubscribedColumn(int ii) {
        return serverColumns == null || serverColumns.get(ii);
    }

    public static class BlinkImpl extends WebBarrageSubscription {
        enum Mode {
            BLINK, APPEND
        }

        private final Mode mode;

        public BlinkImpl(ClientTableState state) {
            super(state);
            mode = Mode.APPEND;
        }

        @Override
        protected void applyUpdates(WebBarrageMessage message) {
            if (message.isSnapshot) {
                updateServerViewport(message.snapshotRowSet, message.snapshotColumns, message.snapshotRowSetIsReversed);
            }

            assert message.shifted.length == 0;
            for (int i = 0; i < message.modColumnData.length; i++) {
                assert message.modColumnData[i].rowsModified.isEmpty();
            }

            if (message.rowsIncluded.isEmpty()) {
                return;
            }

            long addedRows = message.rowsAdded.size();
            RangeSet destinationRowSet;
            if (mode == Mode.APPEND) {
                destinationRowSet = RangeSet.ofRange(capacity, capacity + addedRows - 1);
                capacity += addedRows;
            } else {
                destinationRowSet = RangeSet.ofRange(0, addedRows - 1);
                capacity = addedRows;
            }
            Arrays.stream(destSources).forEach(s -> s.ensureCapacity(capacity));
            for (int ii = 0; ii < message.addColumnData.length; ii++) {
                if (isSubscribedColumn(ii)) {
                    WebBarrageMessage.AddColumnData column = message.addColumnData[ii];
                    PrimitiveIterator.OfLong destIterator = destinationRowSet.indexIterator();
                    for (int j = 0; j < column.data.size(); j++) {
                        Chunk<Values> chunk = column.data.get(j);
                        destSources[ii].fillChunk(chunk, destIterator);
                    }
                    assert !destIterator.hasNext();
                }
            }
        }
    }

    public static class RedirectedImpl extends WebBarrageSubscription {
        private RangeSet freeset = new RangeSet();
        private final TreeMap<Long, Long> redirectedIndexes = new TreeMap<>();

        public RedirectedImpl(ClientTableState state) {
            super(state);
        }

        @Override
        protected void applyUpdates(WebBarrageMessage message) {
            if (message.isSnapshot) {
                updateServerViewport(message.snapshotRowSet, message.snapshotColumns, message.snapshotRowSetIsReversed);
            }

            final boolean mightBeInitialSnapshot = getCurrentRowSet().isEmpty() && message.isSnapshot;

            RangeSet populatedRows =
                    serverViewport != null ? currentRowSet.subsetForPositions(serverViewport, serverReverseViewport)
                            : null;

            // Apply removes to our local rowset
            message.rowsRemoved.rangeIterator().forEachRemaining(currentRowSet::removeRange);
            if (serverViewport != null) {
                // limit the removed rows to what intersect the viewport
                serverViewport.rangeIterator().forEachRemaining(r -> message.rowsRemoved.removeRange(r));
            }
            // free rows that are no longer needed
            freeRows(message.rowsRemoved);

            // Apply shifts

            // Shift moved rows in the redir index
            boolean hasReverseShift = false;
            final ShiftedRange[] shiftedRanges = message.shifted;
            RangeSetBulkHelper currentRowsetShifter =
                    new RangeSetBulkHelper(currentRowSet, RangeSetBulkHelper.Operation.APPEND);
            RangeSetBulkHelper populatedRowsetShifter = populatedRows == null ? null
                    : new RangeSetBulkHelper(populatedRows, RangeSetBulkHelper.Operation.APPEND);
            for (int i = shiftedRanges.length - 1; i >= 0; --i) {
                final ShiftedRange shiftedRange = shiftedRanges[i];
                final long offset = shiftedRange.getDelta();
                if (offset < 0) {
                    hasReverseShift = true;
                    continue;
                }
                currentRowSet.removeRange(shiftedRange.getRange());
                if (populatedRows != null) {
                    populatedRows.removeRange(shiftedRange.getRange());
                }
                final NavigableSet<Long> toMove = redirectedIndexes.navigableKeySet()
                        .subSet(shiftedRange.getRange().getFirst(), true, shiftedRange.getRange().getLast(), true);
                // iterate backward and move them forward
                for (Long key : toMove.descendingSet()) {
                    long shiftedKey = key + offset;
                    Long oldValue = redirectedIndexes.put(shiftedKey, redirectedIndexes.remove(key));
                    assert oldValue == null : shiftedKey + " already has a value, " + oldValue;
                    currentRowsetShifter.append(shiftedKey);
                }
            }
            if (hasReverseShift) {
                for (int i = 0; i < shiftedRanges.length; ++i) {
                    final ShiftedRange shiftedRange = shiftedRanges[i];
                    final long offset = shiftedRange.getDelta();
                    if (offset > 0) {
                        continue;
                    }
                    currentRowSet.removeRange(shiftedRange.getRange());
                    if (populatedRows != null) {
                        populatedRows.removeRange(shiftedRange.getRange());
                    }
                    final NavigableSet<Long> toMove = redirectedIndexes.navigableKeySet()
                            .subSet(shiftedRange.getRange().getFirst(), true, shiftedRange.getRange().getLast(), true);
                    // iterate forward and move them backward
                    for (Long key : toMove) {
                        long shiftedKey = key + offset;
                        Long oldValue = redirectedIndexes.put(shiftedKey, redirectedIndexes.remove(key));
                        assert oldValue == null : shiftedKey + " already has a value, " + oldValue;
                        currentRowsetShifter.append(shiftedKey);
                    }
                }
            }
            currentRowsetShifter.flush();
            if (populatedRowsetShifter != null) {
                populatedRowsetShifter.flush();
            }

            message.rowsAdded.rangeIterator().forEachRemaining(currentRowSet::addRange);

            RangeSet totalMods = new RangeSet();
            for (int i = 0; i < message.modColumnData.length; i++) {
                WebBarrageMessage.ModColumnData column = message.modColumnData[i];
                column.rowsModified.rangeIterator().forEachRemaining(totalMods::addRange);
            }

            if (!message.rowsIncluded.isEmpty()) {
                // int addBatchSize = (int) Math.min(message.rowsIncluded.size(), 1 << 16);//reexamine this constant in
                // light of browsers being browsers

                if (mightBeInitialSnapshot) {
                    capacity = message.rowsIncluded.size();
                    Arrays.stream(destSources).forEach(s -> s.ensureCapacity(capacity));
                    freeset.addRange(new Range(0, capacity - 1));
                }

                RangeSet destinationRowSet = getFreeRows(message.rowsIncluded.size());
                // RangeSet destinationRowSet = new RangeSet();
                // message.rowsIncluded.indexIterator().forEachRemaining((long row) -> {
                // destinationRowSet.addRange(new Range(row, row));
                // });

                for (int ii = 0; ii < message.addColumnData.length; ii++) {
                    if (isSubscribedColumn(ii)) {
                        WebBarrageMessage.AddColumnData column = message.addColumnData[ii];
                        PrimitiveIterator.OfLong destIterator = destinationRowSet.indexIterator();
                        for (int j = 0; j < column.data.size(); j++) {
                            Chunk<Values> chunk = column.data.get(j);
                            destSources[ii].fillChunk(chunk, destIterator);
                        }
                        assert !destIterator.hasNext();
                    }
                }
            }

            for (int ii = 0; ii < message.modColumnData.length; ii++) {
                WebBarrageMessage.ModColumnData column = message.modColumnData[ii];
                if (column.rowsModified.isEmpty()) {
                    continue;
                }

                PrimitiveIterator.OfLong destIterator = column.rowsModified.indexIterator();
                for (int j = 0; j < column.data.size(); j++) {
                    Chunk<Values> chunk = column.data.get(j);
                    destSources[ii].fillChunk(chunk, destIterator);
                }
                assert !destIterator.hasNext();
            }
            if (serverViewport != null) {
                assert populatedRows != null;
                RangeSet newPopulated = currentRowSet.subsetForPositions(serverViewport, serverReverseViewport);
                newPopulated.rangeIterator().forEachRemaining(newPopulated::removeRange);
                freeRows(populatedRows);
            }
        }

        private RangeSet getFreeRows(long size) {
            if (size <= 0) {
                return RangeSet.empty();
            }
            boolean needsResizing = false;
            final RangeSet result;
            if (capacity == 0) {
                capacity = Long.highestOneBit(Math.max(size * 2, 8));
                freeset.addRange(new Range(size, capacity - 1));
                result = new RangeSet();
                result.addRange(new Range(0, size - 1));
                needsResizing = true;
            } else {
                result = new RangeSet();
                Iterator<Range> iterator = freeset.rangeIterator();
                int required = (int) Math.min(size, freeset.size());
                while (required > 0) {
                    assert iterator.hasNext();
                    Range next = iterator.next();
                    result.addRange(
                            next.size() < required ? next : new Range(next.getFirst(), next.getFirst() + required - 1));
                    required -= (int) next.size();
                }

                if (freeset.size() < size) {
                    // we need more, allocate extra, return some, grow the freeset for next time
                    long usedSlots = capacity - freeset.size();
                    long prevCapacity = capacity;

                    do {
                        capacity *= 2;
                    } while ((capacity - usedSlots) < size);

                    result.addRange(new Range(prevCapacity, size - 1));

                    freeset = new RangeSet();
                    freeset.addRange(new Range(size, capacity - 1));
                    needsResizing = true;
                }
            }

            if (needsResizing) {
                Arrays.stream(destSources).forEach(s -> s.ensureCapacity(capacity));
            }

            return result;
        }

        private void freeRows(RangeSet removed) {
            RangeSetBulkHelper reusableHelper = new RangeSetBulkHelper(freeset, RangeSetBulkHelper.Operation.APPEND);
            removed.indexIterator().forEachRemaining((long index) -> {
                long dest = redirectedIndexes.remove(index);
                reusableHelper.append(dest);
            });
            reusableHelper.flush();
        }
    }

    /**
     * Helper to avoid appending many times when modifying indexes. The append() method should be called for each key
     * _in order_ to ensure that addRange/removeRange isn't called excessively. When no more items will be added,
     * flush() must be called.
     */
    private static class RangeSetBulkHelper {
        enum Operation {
            APPEND, REMOVE
        }

        private final RangeSet rangeSet;
        private final Operation operation;

        private long currentFirst = -1;
        private long currentLast;

        public RangeSetBulkHelper(final RangeSet rangeSet, Operation operation) {
            this.rangeSet = rangeSet;
            this.operation = operation;
        }

        public void append(long key) {
            assert key >= 0;

            if (currentFirst == -1) {
                // first key to be added, move both first and last
                currentFirst = key;
                currentLast = key;
            } else if (key == currentLast + 1) {
                // key appends to our current range
                currentLast = key;
            } else if (key == currentFirst - 1) {
                // key appends to our current range
                currentFirst = key;
            } else {
                // existing range doesn't match the new item, finish the old range and start a new one
                if (operation == Operation.APPEND) {
                    rangeSet.addRange(new Range(currentFirst, currentLast));
                } else {
                    rangeSet.removeRange(new Range(currentFirst, currentLast));
                }
                currentFirst = key;
                currentLast = key;
            }
        }

        public void flush() {
            if (currentFirst != -1) {
                if (operation == Operation.APPEND) {
                    rangeSet.addRange(new Range(currentFirst, currentLast));
                } else {
                    rangeSet.removeRange(new Range(currentFirst, currentLast));
                }
                currentFirst = -1;
            }
        }
    }

}
