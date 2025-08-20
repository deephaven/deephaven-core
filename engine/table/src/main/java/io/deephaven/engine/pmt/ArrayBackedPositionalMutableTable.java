//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.pmt;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.util.UpdateCoalescer;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

public class ArrayBackedPositionalMutableTable extends QueryTable implements Runnable, PositionalMutableTable {
    /**
     * How many elements we read from a set2D operation at once. This happens to match the ArrayBackedColumnSource's
     * internal array size.
     */
    private static final int CHUNK_SIZE = 2048;

    /**
     * A queue of updates waiting to be processed by the UpdateGraph.
     */
    private final ConcurrentLinkedQueue<Update> updateQueue = new ConcurrentLinkedQueue<>();

    /**
     * A bundle of updates that we have removed from the updateQueue, but have not yet processed.
     */
    private final LinkedList<Update> bundledUpdates = new LinkedList<>();

    /**
     * The column sources that represent this table.
     */
    @SuppressWarnings("rawtypes")
    private final WritableColumnSource[] columnSources;

    /**
     * When we are inserting rows in the middle we want to null them out. For removing rows that contain objects, we
     * must null them out to avoid holding onto garbage.
     */
    private final ColumnSourceNuller[] nullers;
    private final ColumnSourceNuller[] objectNullers;

    /**
     * If we currently are in the midst of a transaction.
     */
    boolean inTransaction = false;

    /**
     * Create an ArrayBackedPositionalMutableTable based on the desired definition.
     *
     * <p>
     * The table will start out empty, and ready for use either in queries or by calling {@link PositionalMutableTable}
     * methods to update the data.
     * </p>
     *
     * @param definition the definition of this table
     */
    public ArrayBackedPositionalMutableTable(final @NotNull TableDefinition definition) {
        // noinspection resource
        super(
                RowSetFactory.empty().toTracking(),
                makeColumns(definition));
        columnSources = getColumnSourceMap().values().stream().map(cs -> (WritableColumnSource<?>) cs)
                .toArray(WritableColumnSource[]::new);
        nullers = new ColumnSourceNuller[columnSources.length];
        objectNullers = new ColumnSourceNuller[columnSources.length];
        for (int cc = 0; cc < columnSources.length; cc++) {
            final Class<?> type = columnSources[cc].getType();
            nullers[cc] = ColumnSourceNuller.makeNuller(type);
            if (!TypeUtils.isConvertibleToPrimitive(type)) {
                objectNullers[cc] = nullers[cc];
            }
            columnSources[cc].startTrackingPrevValues();
        }
        // our index is always 0...N, so we are flat. This can enable some engine operations to process this table
        // more efficiently
        setFlat();
        getUpdateGraph().addSource(this);
    }

    /**
     * Create the ArrayBackedColumnSources for this table from the definition.
     *
     * <p>
     * The ArrayBackedColumnSource uses a contiguous address space represented by segmented backing arrays (2K elements
     * allocated together). The arrays are segmented so that we can grow them without reallocation, and when recording
     * previous values (used for incremental computation) we can create reasonably sized temporary segments.
     * </p>
     *
     * @param definition the definition of our table
     * @return the map of column names to ColumnSources.
     */
    private static Map<String, ? extends ColumnSource<?>> makeColumns(final @NotNull TableDefinition definition) {
        final LinkedHashMap<String, ColumnSource<?>> result = new LinkedHashMap<>();
        definition.getColumns().forEach(cd -> {
            final Class<?> dataType = cd.getDataType();
            WritableColumnSource<?> memoryColumnSource = ArrayBackedColumnSource.getMemoryColumnSource(0, dataType);
            result.put(cd.getName(), memoryColumnSource);
        });
        return result;
    }

    /**
     * Apply the complete transactions to this table.
     * <p>
     * The {@code run()} method is called for each source (UpdatableTable) at the beginning of the UpdateGraph cycle.
     * Our table pulls updates off the updateQueue and processes them one complete transaction at a time. Each source
     * table is only permitted to send one notification per cycle, therefore we coalesce the updates from multiple
     * transactions into a single notification.
     */
    @Override
    public void run() {
        final MutableObject<UpdateCoalescer> coalescer = new MutableObject<>();

        Update update;
        while ((update = updateQueue.poll()) != null) {
            if (update instanceof StartTransaction) {
                if (inTransaction) {
                    throw new IllegalStateException(
                            "Attempt to start a transaction while a transaction is already in progress");
                }
                inTransaction = true;
                continue;
            }

            if (!inTransaction) {
                throw new IllegalStateException("Can not process " + update + " outside of transaction.");
            }

            if (update instanceof EndTransaction) {
                final EndTransaction endTransaction = (EndTransaction) update;
                try {
                    processUpdates(bundledUpdates, coalescer);
                    endTransaction.complete(null);
                } catch (Exception e) {
                    endTransaction.completeExceptionally(e);
                    throw e;
                } finally {
                    inTransaction = false;
                }
            } else {
                bundledUpdates.add(update);
            }
        }

        final UpdateCoalescer value = coalescer.getValue();
        if (value != null) {
            notifyListeners(value.coalesce());
        }
    }

    /**
     * Process a bundle of updates.
     *
     * @param bundledUpdates the updates to process
     * @param coalescer the coalescer for these updates
     */
    private void processUpdates(final @NotNull LinkedList<Update> bundledUpdates,
            final @NotNull MutableObject<UpdateCoalescer> coalescer) {
        // rather than coalescing a sequence of set cell operations using the coalescer we accumulate them into a random
        // index builder and a modificed column set. The coalescer operation is fairly expensive; whereas the
        // Index random builder is less expensive. Sequential builders are cheaper still, but we do not know what stream
        // of updates the users will choose to give us.
        RowSetBuilderRandom modifiedSetBuilder = null;
        ModifiedColumnSet modifiedColumnSet = null;

        try (final RowSet originalRowSet = getRowSet().copy()) {
            for (Update update : bundledUpdates) {
                // first we handle sets, because they have their own coalescing going on
                if (update instanceof SetCell) {
                    if (modifiedSetBuilder == null) {
                        modifiedSetBuilder = RowSetFactory.builderRandom();
                        modifiedColumnSet = getModifiedColumnSetForUpdates();
                        modifiedColumnSet.clear();
                    }
                    final SetCell setCell = (SetCell) update;
                    // noinspection unchecked
                    columnSources[setCell.column].set(setCell.rowPosition, setCell.value);
                    modifiedColumnSet.setColumnWithIndex(setCell.column);
                    modifiedSetBuilder.addKey(setCell.rowPosition);
                    continue;
                } else if (update instanceof Set2D) {
                    if (modifiedSetBuilder == null) {
                        modifiedSetBuilder = RowSetFactory.builderRandom();
                        modifiedColumnSet = getModifiedColumnSetForUpdates();
                        modifiedColumnSet.clear();
                    }
                    final Set2D setCell = (Set2D) update;
                    processSet2D(setCell);
                    modifiedColumnSet.setAllDirty();
                    modifiedSetBuilder.addRange(setCell.rowPosition, setCell.rowPosition + setCell.table.size() - 1);
                    continue;
                } else {
                    maybeApplyModifications(coalescer, modifiedSetBuilder, originalRowSet);
                    modifiedSetBuilder = null;
                    modifiedColumnSet = null;
                }

                // if we are not a set, then we create a new update for the add or delete
                final TableUpdateImpl newUpdate = new TableUpdateImpl();
                newUpdate.modifiedColumnSet = getModifiedColumnSetForUpdates();

                if (update instanceof AddUpdate) {
                    final AddUpdate addUpdate = (AddUpdate) update;

                    final long startSize = getRowSet().size();
                    final long newSize = startSize + addUpdate.count;
                    // we udpate the index at the end (because we are always flat)
                    // noinspection resource
                    getRowSet().writableCast().insertRange(startSize, newSize - 1);
                    // and we need to make sure the columns are big enough
                    for (final WritableColumnSource<?> columnSource : columnSources) {
                        columnSource.ensureCapacity(newSize);
                    }
                    // but we notify about the actual new rows
                    newUpdate.added = RowSetFactory.fromRange(addUpdate.row, addUpdate.row + addUpdate.count - 1);
                    // and have to shift data out of the way
                    if (startSize != addUpdate.row) {
                        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
                        builder.shiftRange(addUpdate.row, startSize - 1, addUpdate.count);
                        newUpdate.shifted = builder.build();

                        // we have to shift the individual column sources and null the hole
                        for (int cc = 0; cc < columnSources.length; ++cc) {
                            doShift(columnSources[cc], addUpdate.row, startSize - 1, addUpdate.count);
                            nullers[cc].nullColumnSource(columnSources[cc], addUpdate.row, addUpdate.count);
                        }
                    } else {
                        newUpdate.shifted = RowSetShiftData.EMPTY;
                    }
                    newUpdate.removed = RowSetFactory.empty();
                    newUpdate.modified = RowSetFactory.empty();
                    newUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                } else if (update instanceof DeleteUpdate) {
                    final DeleteUpdate deleteUpdate = (DeleteUpdate) update;

                    final long startSize = getRowSet().size();
                    // we remove from the end
                    // noinspection resource
                    getRowSet().writableCast().removeRange(startSize - deleteUpdate.count, startSize);

                    // but notify about the really removed rows
                    newUpdate.removed =
                            RowSetFactory.fromRange(deleteUpdate.row, deleteUpdate.row + deleteUpdate.count - 1);
                    // and a corresponding shift (if in the middle)
                    if (startSize - deleteUpdate.count != deleteUpdate.row) {
                        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
                        builder.shiftRange(deleteUpdate.row + deleteUpdate.count, startSize - 1, -deleteUpdate.count);
                        newUpdate.shifted = builder.build();

                        for (WritableColumnSource<?> columnSource : columnSources) {
                            doShift(columnSource, deleteUpdate.row + deleteUpdate.count, startSize - 1,
                                    -deleteUpdate.count);
                        }
                    } else {
                        newUpdate.shifted = RowSetShiftData.EMPTY;
                    }

                    for (int cc = 0; cc < columnSources.length; ++cc) {
                        // we only need to null out object column sources which may hold onto garbage
                        if (objectNullers[cc] != null) {
                            objectNullers[cc].nullColumnSource(columnSources[cc], startSize - deleteUpdate.count,
                                    deleteUpdate.count);
                        }
                    }

                    newUpdate.added = RowSetFactory.empty();
                    newUpdate.modified = RowSetFactory.empty();
                    newUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                } else {
                    throw new UnsupportedOperationException();
                }

                applyUpdate(coalescer, newUpdate, originalRowSet);
            }
            maybeApplyModifications(coalescer, modifiedSetBuilder, originalRowSet);
            bundledUpdates.clear();
        }
    }

    /**
     * The set2D call reads from a data table and inserts it into our column sources
     *
     * @param set2D a set2D command
     */
    private void processSet2D(final @NotNull Set2D set2D) {
        final List<ColumnDefinition<?>> columns = definition.getColumns();
        final ColumnSource<?>[] columnSourcesToRead = new ColumnSource[columns.size()];
        final ChunkSource.GetContext[] getContextArray = new ChunkSource.GetContext[columns.size()];
        final ChunkSink.FillFromContext[] fillFromContextArray = new ChunkSink.FillFromContext[columns.size()];

        final Table tableToRead = set2D.table;
        // we will allocate chunks and contexts that are suitable for either the chunk size or the total size of our
        // insertion to avoid allocating overly large chunks
        final int chunkSize = (int) Math.min(CHUNK_SIZE, tableToRead.size());

        long startPosition = set2D.rowPosition;

        // The RowSequence interface is how we traverse a table's Index. It has fewer operations, but importantly
        // has an iterator that lets us take views into an Index either by a fixed size at a time (used here to get
        // appropriately sized chunks) or based on keys (used elsewhere when a table's address space is divided into
        // multiple regions)
        //
        // when we create a context, it must be closed (e.g., to release memory back to a pool or to avoid leaking
        // file descriptors), the SafeCloseableArray is a convenient shortcut for closing all non-null items in an array
        // in our try with resources
        //
        // when creating contexts, we use a SharedContext that allows us to reuse things like redirections across
        // columns
        try (RowSequence.Iterator okit = tableToRead.getRowSet().getRowSequenceIterator();
                final SafeCloseableArray<ChunkSource.GetContext> ignored = new SafeCloseableArray<>(getContextArray);
                final SafeCloseableArray<ChunkSink.FillFromContext> ignored2 =
                        new SafeCloseableArray<>(fillFromContextArray);
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {
            for (int cc = 0; cc < columns.size(); cc++) {
                columnSourcesToRead[cc] = tableToRead.getColumnSource(columns.get(cc).getName());
                getContextArray[cc] = columnSourcesToRead[cc].makeGetContext(chunkSize, sharedContext);
                // noinspection resource
                fillFromContextArray[cc] = columnSources[cc].makeFillFromContext(chunkSize);
            }

            // as long as there is more data
            while (okit.hasMore()) {
                // reset our shared context, so that we don't have left over cached data
                sharedContext.reset();
                // get the keys to read from from the source iterator
                final RowSequence sourceKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);
                // write them into a range determined by our set2D start position and how many rows we've already
                // consumed
                final RowSequence destinationKeys =
                        RowSequenceFactory.forRange(startPosition, startPosition + sourceKeys.intSize() - 1);
                // now for each column, get the values from the set2D source, and then write them into our local array
                // backed column source
                for (int cc = 0; cc < columns.size(); ++cc) {
                    final Chunk<? extends Values> dataChunk =
                            columnSourcesToRead[cc].getChunk(getContextArray[cc], sourceKeys);
                    // noinspection unchecked
                    columnSources[cc].fillFromChunk(fillFromContextArray[cc], dataChunk, destinationKeys);
                }
            }
        }
    }

    /**
     * If there are outstanding modifications, then build the Index and apply them to the coalescer.
     *
     * @param coalescer a MutableObject containing the coalescer for our update
     * @param modifiedSetBuilder the Index builder of modified rows
     * @param originalRowSet the row set at the beginning of this operation
     */
    private void maybeApplyModifications(final @NotNull MutableObject<UpdateCoalescer> coalescer,
            final @Nullable RowSetBuilderRandom modifiedSetBuilder, final RowSet originalRowSet) {
        if (modifiedSetBuilder == null) {
            return;
        }
        final TableUpdate newUpdate = new TableUpdateImpl(
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                modifiedSetBuilder.build(),
                RowSetShiftData.EMPTY,
                getModifiedColumnSetForUpdates());
        applyUpdate(coalescer, newUpdate, originalRowSet);
    }

    /**
     * Apply an update to our coalescer; which is null on the first call and non-null on second calls.
     *
     * @param coalescer a MutableObject containing the coalescer for our update
     * @param newUpdate the new update to apply tot he coalescer
     */
    private void applyUpdate(final @NotNull MutableObject<UpdateCoalescer> coalescer,
            final @NotNull TableUpdate newUpdate, final @NotNull RowSet originalRowSet) {
        UpdateCoalescer coalescerValue = coalescer.getValue();
        if (coalescerValue == null) {
            coalescer.setValue(new UpdateCoalescer(originalRowSet, newUpdate));
        } else {
            coalescerValue.update(newUpdate);
        }
    }

    /**
     * When all reference counts to this table are dropped; then we should remove ourselves from the LTM source tables.
     */
    @Override
    protected void destroy() {
        super.destroy();
        getUpdateGraph().removeSource(this);
    }

    @Override
    public void addRows(final long rowPosition, final long count) {
        if (count <= 0) {
            throw new IllegalArgumentException();
        }
        updateQueue.add(new AddUpdate(rowPosition, count));
    }

    @Override
    public void deleteRows(final long rowPosition, final long count) {
        updateQueue.add(new DeleteUpdate(rowPosition, count));
    }

    @Override
    public void setCell(final long rowPosition, final int column, final @Nullable Object value) {
        updateQueue.add(new SetCell(rowPosition, column, value));
    }

    @Override
    public void set2D(final long rowPosition, final @NotNull Table newData) {
        final Table snapshottedTable = newData.snapshot();
        if (snapshottedTable.isEmpty()) {
            return;
        }
        // check compatibility
        snapshottedTable.getDefinition().checkCompatibility(definition);
        updateQueue.add(new Set2D(rowPosition, snapshottedTable));
    }

    @Override
    public void startTransaction() {
        updateQueue.add(new StartTransaction());
    }

    @Override
    public Future<Void> endTransaction() {
        final EndTransaction e = new EndTransaction();
        updateQueue.add(e);
        return e;
    }

    @SuppressWarnings("unused")
    public boolean isInTransaction() {
        return inTransaction;
    }

    private static <T> void doShift(final WritableColumnSource<T> writableSource, final long start, final long end,
            final long offset) {
        final Class<T> colType = writableSource.getType();
        final Shifter<T> shifter;
        if (colType == char.class) {
            shifter = (cs, src, dst) -> cs.set(dst, cs.getChar(src));
        } else if (colType == byte.class) {
            shifter = (cs, src, dst) -> cs.set(dst, cs.getByte(src));
        } else if (colType == short.class) {
            shifter = (cs, src, dst) -> cs.set(dst, cs.getShort(src));
        } else if (colType == int.class) {
            shifter = (cs, src, dst) -> cs.set(dst, cs.getInt(src));
        } else if (colType == long.class) {
            shifter = (cs, src, dst) -> cs.set(dst, cs.getLong(src));
        } else if (colType == double.class) {
            shifter = (cs, src, dst) -> cs.set(dst, cs.getDouble(src));
        } else if (colType == float.class) {
            shifter = (cs, src, dst) -> cs.set(dst, cs.getFloat(src));
        } else {
            shifter = (cs, src, dst) -> cs.set(dst, cs.get(src));
        }

        // TODO: we could do this with contexts/chunks/whatever, but would that even be faster when just shifting an
        // array?

        if (offset > 0) {
            for (long i = (int) end; i >= start; i--) {
                shifter.shift(writableSource, i, i + offset);
            }
        } else {
            for (int i = (int) start; i <= end; i++) {
                shifter.shift(writableSource, i, i + offset);
            }
        }
    }

    private interface Shifter<T> {
        void shift(WritableColumnSource<T> writableSource, long src, long dst);
    }

    /**
     * The Update interface is simply a marker for putting things on our queue, the remainder of the inner classes
     * represent each of the commands that can be sent to this table.
     */
    private interface Update {
    }

    private static class StartTransaction implements Update {
        @Override
        public String toString() {
            return "StartTransaction{}";
        }
    }

    private static class EndTransaction extends CompletableFuture<Void> implements Update {
        @Override
        public String toString() {
            return "EndTransaction{}";
        }
    }

    private static class AddUpdate implements Update {
        final long row;
        final long count;

        private AddUpdate(final long row, final long count) {
            this.row = row;
            this.count = count;
        }

        @Override
        public String toString() {
            return "AddUpdate{" +
                    "row=" + row +
                    ", count=" + count +
                    '}';
        }
    }

    private static class DeleteUpdate implements Update {
        final long row;
        final long count;

        private DeleteUpdate(final long row, final long count) {
            this.row = row;
            this.count = count;
        }

        @Override
        public String toString() {
            return "DeleteUpdate{" +
                    "row=" + row +
                    ", count=" + count +
                    '}';
        }
    }


    private static class SetCell implements Update {
        private final long rowPosition;
        private final int column;
        private final Object value;

        public SetCell(final long rowPosition, final int column, final @Nullable Object value) {
            this.rowPosition = rowPosition;
            this.column = column;
            this.value = value;
        }

        @Override
        public String toString() {
            return "SetCell{" +
                    "rowPosition=" + rowPosition +
                    ", column=" + column +
                    ", value=" + value +
                    '}';
        }
    }

    private static class Set2D implements Update {
        private final long rowPosition;
        private final Table table;

        public Set2D(final long rowPosition, final @NotNull Table table) {
            Assert.eqFalse(table.isRefreshing(), "table.isRefreshing()");
            this.rowPosition = rowPosition;
            this.table = table;
        }

        @Override
        public String toString() {
            return "Set2D{" +
                    "rowPosition=" + rowPosition +
                    ", table=" + TableTools.string(table, 10) +
                    '}';
        }
    }
}
