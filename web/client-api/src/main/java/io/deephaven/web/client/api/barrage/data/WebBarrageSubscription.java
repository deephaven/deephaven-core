//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.data;

import elemental2.core.JsArray;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.web.client.api.barrage.WebBarrageMessage;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.fu.JsData;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import jsinterop.base.Any;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.TreeMap;

/**
 * In contrast to the server implementation, the JS API holds the "table" as distinct from the "subscription", so that
 * developers are acutely aware of extra async costs in requesting data, and can clearly indicate how much data is
 * requested. This class represents a barrage subscription for the JS API, and exposes access to the data presently
 * available on the client.
 * <p>
 * This is a rough analog to {@link io.deephaven.extensions.barrage.table.BarrageTable} and its subtypes, but isn't
 * directly exposed to API consumers. Instead, the subscription types wrap this, and delegate their data storage and
 * snapshot/delta handling here.
 */
public abstract class WebBarrageSubscription {

    public static final boolean COLUMNS_AS_LIST = false;
    public static final int MAX_MESSAGE_SIZE = 10_000_000;
    public static final int BATCH_SIZE = 100_000;

    public static WebBarrageSubscription subscribe(ClientTableState cts, ViewportChangedHandler viewportChangedHandler,
            DataChangedHandler dataChangedHandler) {

        WebColumnData[] dataSinks = new WebColumnData[cts.columnTypes().length];
        for (int i = 0; i < dataSinks.length; i++) {
            JsArray<Any> arr = JsData.newArray(cts.columnTypes()[i].getCanonicalName());
            switch (cts.chunkTypes()[i]) {
                case Boolean:
                    throw new IllegalStateException("Boolean unsupported here");
                case Char:
                    dataSinks[i] = new WebCharColumnData();
                    break;
                case Byte:
                    dataSinks[i] = new WebByteColumnData();
                    break;
                case Short:
                    dataSinks[i] = new WebShortColumnData();
                    break;
                case Int:
                    dataSinks[i] = new WebIntColumnData();
                    break;
                case Long:
                    dataSinks[i] = new WebLongColumnData();
                    break;
                case Float:
                    dataSinks[i] = new WebFloatColumnData();
                    break;
                case Double:
                    dataSinks[i] = new WebDoubleColumnData();
                    break;
                case Object:
                    dataSinks[i] = new WebObjectColumnData();
                    break;
            }
        }

        if (cts.getTableDef().getAttributes().isBlinkTable()) {
            return new BlinkImpl(cts, viewportChangedHandler, dataChangedHandler, dataSinks);
        }
        return new RedirectedImpl(cts, viewportChangedHandler, dataChangedHandler, dataSinks);
    }

    public interface ViewportChangedHandler {
        void onServerViewportChanged(RangeSet serverViewport, BitSet serverColumns, boolean serverReverseViewport);
    }
    public interface DataChangedHandler {
        void onDataChanged(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods, ShiftedRange[] shifted,
                BitSet modifiedColumnSet);
    }

    protected final ClientTableState state;
    protected final ViewportChangedHandler viewportChangedHandler;
    protected final DataChangedHandler dataChangedHandler;
    protected final RangeSet currentRowSet = RangeSet.empty();

    protected long capacity = 0;
    protected WebColumnData[] destSources;

    protected RangeSet serverViewport;
    protected BitSet serverColumns;
    protected boolean serverReverseViewport;

    protected WebBarrageSubscription(ClientTableState state, ViewportChangedHandler viewportChangedHandler,
            DataChangedHandler dataChangedHandler, WebColumnData[] dataSinks) {
        this.state = state;
        destSources = dataSinks;
        this.viewportChangedHandler = viewportChangedHandler;
        this.dataChangedHandler = dataChangedHandler;
    }

    public abstract void applyUpdates(WebBarrageMessage message);

    protected void updateServerViewport(RangeSet viewport, BitSet columns, boolean reverseViewport) {
        serverViewport = viewport;
        serverColumns = columns == null || columns.cardinality() == numColumns() ? null : columns;
        serverReverseViewport = reverseViewport;
    }

    protected int numColumns() {
        return getDefinition().getColumns().length;
    }

    private InitialTableDefinition getDefinition() {
        return state.getTableDef();
    }

    public RangeSet getCurrentRowSet() {
        return currentRowSet;
    }

    public RangeSet getServerViewport() {
        return serverViewport;
    }

    public boolean isReversed() {
        return serverReverseViewport;
    }

    /**
     * Reads a value from the table subscription.
     *
     * @param key the row to read in key-space
     * @param col the index of the column to read
     * @return the value read from the table
     */
    public abstract Any getData(long key, int col);

    protected boolean isSubscribedColumn(int ii) {
        return serverColumns == null || serverColumns.get(ii);
    }

    public static class BlinkImpl extends WebBarrageSubscription {
        enum Mode {
            BLINK, APPEND
        }

        private final Mode mode;

        public BlinkImpl(ClientTableState state, ViewportChangedHandler viewportChangedHandler,
                DataChangedHandler dataChangedHandler, WebColumnData[] dataSinks) {
            super(state, viewportChangedHandler, dataChangedHandler, dataSinks);
            mode = Mode.BLINK;
        }

        @Override
        public void applyUpdates(WebBarrageMessage message) {
            if (message.isSnapshot) {
                updateServerViewport(message.snapshotRowSet, message.snapshotColumns, message.snapshotRowSetIsReversed);
                viewportChangedHandler.onServerViewportChanged(serverViewport, serverColumns, serverReverseViewport);
            }

            assert message.shifted.length == 0;
            for (int i = 0; i < message.modColumnData.length; i++) {
                assert message.modColumnData[i].rowsModified.isEmpty();
            }

            if (message.rowsIncluded.isEmpty()) {
                return;
            }

            long addedRows = message.rowsIncluded.size();
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

            message.rowsRemoved.rangeIterator().forEachRemaining(currentRowSet::removeRange);
            currentRowSet.addRangeSet(message.rowsAdded);
            state.setSize(message.rowsAdded.size());
            dataChangedHandler.onDataChanged(message.rowsAdded, message.rowsRemoved, RangeSet.empty(), message.shifted,
                    new BitSet(0));
        }

        @Override
        public Any getData(long key, int col) {
            if (!isSubscribedColumn(col)) {
                throw new NoSuchElementException("No column at index " + col);
            }
            return destSources[col].get(key);
        }
    }

    public static class RedirectedImpl extends WebBarrageSubscription {
        private RangeSet freeset = new RangeSet();
        private final TreeMap<Long, Long> redirectedIndexes = new TreeMap<>();

        public RedirectedImpl(ClientTableState state, ViewportChangedHandler viewportChangedHandler,
                DataChangedHandler dataChangedHandler, WebColumnData[] dataSinks) {
            super(state, viewportChangedHandler, dataChangedHandler, dataSinks);
        }

        @Override
        public void applyUpdates(WebBarrageMessage message) {
            RangeSet populatedRows = serverViewport != null
                    ? currentRowSet.subsetForPositions(serverViewport, serverReverseViewport)
                    : null;

            if (message.isSnapshot) {
                updateServerViewport(message.snapshotRowSet, message.snapshotColumns, message.snapshotRowSetIsReversed);
                viewportChangedHandler.onServerViewportChanged(serverViewport, serverColumns, serverReverseViewport);
            }

            final boolean mightBeInitialSnapshot = getCurrentRowSet().isEmpty() && message.isSnapshot;

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
            boolean hasReverseShift = COLUMNS_AS_LIST;
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

            currentRowSet.addRangeSet(message.rowsAdded);

            RangeSet totalMods = new RangeSet();
            for (int i = 0; i < message.modColumnData.length; i++) {
                WebBarrageMessage.ModColumnData column = message.modColumnData[i];
                totalMods.addRangeSet(column.rowsModified);
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
                // Add redirection mappings
                PrimitiveIterator.OfLong srcIter = message.rowsIncluded.indexIterator();
                PrimitiveIterator.OfLong destIter = destinationRowSet.indexIterator();
                while (srcIter.hasNext()) {
                    assert destIter.hasNext();
                    redirectedIndexes.put(srcIter.next(), destIter.next());
                }
                assert !destIter.hasNext();
            }

            BitSet modifiedColumnSet = new BitSet(numColumns());
            for (int ii = 0; ii < message.modColumnData.length; ii++) {
                WebBarrageMessage.ModColumnData column = message.modColumnData[ii];
                if (column.rowsModified.isEmpty()) {
                    continue;
                }

                modifiedColumnSet.set(ii);

                PrimitiveIterator.OfLong destIterator = column.rowsModified.indexIterator();
                for (int j = 0; j < column.data.size(); j++) {
                    Chunk<Values> chunk = column.data.get(j);
                    destSources[ii].fillChunk(chunk, destIterator);
                }
                assert !destIterator.hasNext();
            }
            if (serverViewport != null && populatedRows != null) {
                RangeSet newPopulated = currentRowSet.subsetForPositions(serverViewport, serverReverseViewport);
                newPopulated.rangeIterator().forEachRemaining(populatedRows::removeRange);
                freeRows(populatedRows);
            }

            state.setSize(currentRowSet.size());
            dataChangedHandler.onDataChanged(message.rowsAdded, message.rowsRemoved, totalMods, message.shifted,
                    modifiedColumnSet);
        }

        @Override
        public Any getData(long key, int col) {
            if (!isSubscribedColumn(col)) {
                throw new NoSuchElementException("No column at index " + col);
            }
            return this.destSources[col].get(redirectedIndexes.get(key));
        }

        private RangeSet getFreeRows(long size) {
            if (size <= 0) {
                return RangeSet.empty();
            }
            boolean needsResizing = COLUMNS_AS_LIST;
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
                int required = (int) size;
                while (required > 0 && iterator.hasNext()) {
                    Range next = iterator.next();
                    Range range =
                            next.size() < required ? next : new Range(next.getFirst(), next.getFirst() + required - 1);
                    result.addRange(range);
                    freeset.removeRange(range);
                    required -= (int) next.size();
                }

                if (required > 0) {
                    // we need more, allocate extra, return some, grow the freeset for next time
                    long usedSlots = capacity - freeset.size();
                    long prevCapacity = capacity;

                    do {
                        capacity *= 2;
                    } while ((capacity - usedSlots) < required);

                    result.addRange(new Range(prevCapacity, prevCapacity + required - 1));

                    freeset = new RangeSet();
                    if (capacity - prevCapacity > required) {
                        // extra was allocated for next time
                        freeset.addRange(new Range(prevCapacity + required, capacity - 1));
                    }
                    needsResizing = true;
                }
            }

            if (needsResizing) {
                Arrays.stream(destSources).forEach(s -> s.ensureCapacity(capacity));
            }

            assert result.size() == size;

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
     * <i>in order</i> to ensure that addRange/removeRange isn't called excessively. When no more items will be added,
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
