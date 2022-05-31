package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.exceptions.ArgumentException;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Consumer;

/**
 * An in-memory table that has keys for each row, which can be updated on the UGP.
 *
 * This is used to implement in-memory editable table columns from web plugins.
 */
public class KeyedArrayBackedMutableTable extends BaseArrayBackedMutableTable {
    static final String DEFAULT_DESCRIPTION = "In-Memory Input Table";

    private final List<String> keyColumnNames;
    private final Set<String> keyColumnSet;
    protected final ObjectArraySource<?>[] arrayValueSources;

    private final TObjectLongMap<Object> keyToRowMap =
            new TObjectLongHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, Long.MIN_VALUE);

    /**
     * Create an empty KeyedArrayBackedMutableTable.
     *
     * @param definition the definition of the table to create
     * @param keyColumnNames the name of the key columns
     *
     * @return an empty KeyedArrayBackedMutableTable with the given definition and key columns
     */
    public static KeyedArrayBackedMutableTable make(@NotNull TableDefinition definition,
            final String... keyColumnNames) {
        return make(new QueryTable(definition, RowSetFactory.empty().toTracking(),
                NullValueColumnSource.createColumnSourceMap(definition)), keyColumnNames);
    }

    /**
     * Create an empty KeyedArrayBackedMutableTable.
     *
     * @param definition the definition of the table to create
     * @param enumValues a map of column names to enumeration values
     * @param keyColumnNames the name of the key columns
     *
     * @return an empty KeyedArrayBackedMutableTable with the given definition and key columns
     */
    public static KeyedArrayBackedMutableTable make(@NotNull TableDefinition definition,
            final Map<String, Object[]> enumValues, final String... keyColumnNames) {
        return make(new QueryTable(definition, RowSetFactory.empty().toTracking(),
                NullValueColumnSource.createColumnSourceMap(definition)), enumValues, keyColumnNames);
    }

    /**
     * Create an empty KeyedArrayBackedMutableTable.
     *
     * The initialTable is processed in order, so if there are duplicate keys only the last row is reflected in the
     * output.
     *
     * @param initialTable the initial values to copy into the KeyedArrayBackedMutableTable
     * @param keyColumnNames the name of the key columns
     *
     * @return an empty KeyedArrayBackedMutableTable with the given definition and key columns
     */
    public static KeyedArrayBackedMutableTable make(final Table initialTable, final String... keyColumnNames) {
        return make(initialTable, Collections.emptyMap(), keyColumnNames);
    }

    /**
     * Create an empty KeyedArrayBackedMutableTable.
     *
     * The initialTable is processed in order, so if there are duplicate keys only the last row is reflected in the
     * output.
     *
     * @param initialTable the initial values to copy into the KeyedArrayBackedMutableTable
     * @param enumValues a map of column names to enumeration values
     * @param keyColumnNames the name of the key columns
     *
     * @return an empty KeyedArrayBackedMutableTable with the given definition and key columns
     */
    public static KeyedArrayBackedMutableTable make(final Table initialTable, final Map<String, Object[]> enumValues,
            final String... keyColumnNames) {
        final KeyedArrayBackedMutableTable result = new KeyedArrayBackedMutableTable(initialTable.getDefinition(),
                keyColumnNames, enumValues, new ProcessPendingUpdater());
        processInitial(initialTable, result);
        result.startTrackingPrev();
        return result;
    }

    private KeyedArrayBackedMutableTable(@NotNull TableDefinition definition, final String[] keyColumnNames,
            final Map<String, Object[]> enumValues, final ProcessPendingUpdater processPendingUpdater) {
        super(RowSetFactory.empty().toTracking(), makeColumnSourceMap(definition),
                enumValues, processPendingUpdater);
        final List<String> missingKeyColumns = new ArrayList<>(Arrays.asList(keyColumnNames));
        missingKeyColumns.removeAll(definition.getColumnNames());
        if (!missingKeyColumns.isEmpty()) {
            throw new ArgumentException("Missing key columns in definition: " + missingKeyColumns
                    + ", available columns: " + definition.getColumnNames());
        }

        this.keyColumnNames = Collections.unmodifiableList(new ArrayList<>(Arrays.asList(keyColumnNames)));
        this.keyColumnSet = new HashSet<>(Arrays.asList(keyColumnNames));
        this.arrayValueSources =
                definition.getColumnStream()
                        .map(ColumnDefinition::getName)
                        .filter(n -> !keyColumnSet.contains(n))
                        .map(this::getColumnSource)
                        .filter(cs -> cs instanceof ObjectArraySource)
                        .map(cs -> (ObjectArraySource<?>) cs)
                        .toArray(ObjectArraySource[]::new);
    }

    private void startTrackingPrev() {
        getColumnSourceMap().values().forEach(ColumnSource::startTrackingPrevValues);
    }

    @Override
    protected void processPendingTable(Table table, boolean allowEdits, RowSetChangeRecorder rowSetChangeRecorder,
            Consumer<String> errorNotifier) {
        final ChunkSource<Values> keySource = makeKeySource(table);
        final int chunkCapacity = table.intSize();

        final SharedContext sharedContext = SharedContext.makeSharedContext();

        long rowToInsert = nextRow;
        final StringBuilder errorBuilder = new StringBuilder();

        try (final RowSet addRowSet = table.getRowSet().copy();
                final WritableLongChunk<RowKeys> destinations =
                        WritableLongChunk.makeWritableChunk(chunkCapacity)) {
            try (final ChunkSource.GetContext getContext = keySource.makeGetContext(chunkCapacity, sharedContext);
                    final ChunkBoxer.BoxerKernel boxer = ChunkBoxer.getBoxer(keySource.getChunkType(), chunkCapacity)) {
                final Chunk<? extends Values> keys = keySource.getChunk(getContext, addRowSet);
                final ObjectChunk<?, ? extends Values> boxed = boxer.box(keys);
                for (int ii = 0; ii < boxed.size(); ++ii) {
                    final Object key = boxed.get(ii);
                    long rowNumber = keyToRowMap.putIfAbsent(key, rowToInsert);
                    if (rowNumber == keyToRowMap.getNoEntryValue()) {
                        rowNumber = rowToInsert++;
                        destinations.set(ii, rowNumber);
                    } else if (isDeletedRowNumber(rowNumber)) {
                        rowNumber = deletedRowNumberToRowNumber(rowNumber);
                        keyToRowMap.put(key, rowNumber);
                        rowSetChangeRecorder.addRowKey(rowNumber);
                        destinations.set(ii, rowNumber);
                    } else if (allowEdits) {
                        rowSetChangeRecorder.modifyRowKey(rowNumber);
                        destinations.set(ii, rowNumber);
                    } else {
                        // invalid edit
                        if (errorBuilder.length() > 0) {
                            errorBuilder.append(", ").append(key);
                        } else {
                            errorBuilder.append("Can not edit keys ").append(key);
                        }
                    }
                }
            }

            if (errorBuilder.length() > 0) {
                errorNotifier.accept(errorBuilder.toString());
                return;
            }

            for (long ii = nextRow; ii < rowToInsert; ++ii) {
                rowSetChangeRecorder.addRowKey(ii);
            }
            nextRow = rowToInsert;

            sharedContext.reset();

            getColumnSourceMap().forEach((name, cs) -> {
                final ArrayBackedColumnSource<?> arrayBackedColumnSource = (ArrayBackedColumnSource<?>) cs;
                arrayBackedColumnSource.ensureCapacity(nextRow);
                final ColumnSource<?> sourceColumnSource = table.getColumnSource(name);
                try (final ChunkSink.FillFromContext ffc =
                        arrayBackedColumnSource.makeFillFromContext(chunkCapacity);
                        final ChunkSource.GetContext getContext =
                                sourceColumnSource.makeGetContext(chunkCapacity, sharedContext)) {
                    final Chunk<? extends Values> valuesChunk =
                            sourceColumnSource.getChunk(getContext, addRowSet);
                    arrayBackedColumnSource.fillFromChunkUnordered(ffc, valuesChunk, destinations);
                }
            });
        }
    }

    @Override
    protected void processPendingDelete(Table table, RowSetChangeRecorder rowSetChangeRecorder) {
        final ChunkSource<Values> keySource = makeKeySource(table);
        final int chunkCapacity = table.intSize();

        final SharedContext sharedContext = SharedContext.makeSharedContext();

        try (final WritableLongChunk<RowKeys> destinations =
                WritableLongChunk.makeWritableChunk(chunkCapacity)) {
            try (final ChunkSource.GetContext getContext = keySource.makeGetContext(chunkCapacity, sharedContext);
                    final ChunkBoxer.BoxerKernel boxer = ChunkBoxer.getBoxer(keySource.getChunkType(), chunkCapacity);
                    final RowSet tableRowSet = table.getRowSet().copy();) {

                final Chunk<? extends Values> keys = keySource.getChunk(getContext, tableRowSet);
                final ObjectChunk<?, ? extends Values> boxed = boxer.box(keys);
                destinations.setSize(0);
                for (int ii = 0; ii < boxed.size(); ++ii) {
                    final Object key = boxed.get(ii);
                    long rowNumber = keyToRowMap.get(key);
                    if (rowNumber != keyToRowMap.getNoEntryValue() && !isDeletedRowNumber(rowNumber)) {
                        rowSetChangeRecorder.removeRowKey(rowNumber);
                        destinations.add(rowNumber);
                        keyToRowMap.put(key, rowNumberToDeletedRowNumber(rowNumber));
                    }
                }
            }

            // null out the values, so that we do not hold onto garbage forever, we keep the keys
            for (ObjectArraySource<?> objectArraySource : arrayValueSources) {
                try (final ChunkSink.FillFromContext ffc =
                        objectArraySource.makeFillFromContext(chunkCapacity);
                        final WritableObjectChunk<?, Values> nullChunk =
                                WritableObjectChunk.makeWritableChunk(chunkCapacity);) {
                    nullChunk.fillWithNullValue(0, chunkCapacity);
                    objectArraySource.fillFromChunkUnordered(ffc, nullChunk, destinations);
                }
            }
        }
    }

    private ChunkSource<Values> makeKeySource(Table table) {
        // noinspection unchecked
        return TupleSourceFactory.makeTupleSource(
                keyColumnNames.stream().map(table::getColumnSource).toArray(ColumnSource[]::new));
    }

    @Override
    protected String getDefaultDescription() {
        return DEFAULT_DESCRIPTION;
    }

    @Override
    protected List<String> getKeyNames() {
        return keyColumnNames;
    }

    /**
     * Convert row number to a deleted value for storage in the map
     *
     * @param rowNumber the undeleted row number
     *
     * @return the deleted row number
     */
    private static long rowNumberToDeletedRowNumber(long rowNumber) {
        return -(rowNumber + 1);
    }

    /**
     * Is the rowNumber a deleted row? Should not be called with noEntryValue.
     *
     * @param rowNumber the row number to check for deletion
     *
     * @return true if this represents a deleted row
     */
    private static boolean isDeletedRowNumber(long rowNumber) {
        return rowNumber < 0;
    }

    /**
     * Convert a deleted row number from the map into an actual row number
     *
     * @param deletedRowNumber the deleted row number
     *
     * @return the original row number
     */
    private static long deletedRowNumberToRowNumber(long deletedRowNumber) {
        return -(deletedRowNumber + 1);
    }
}
