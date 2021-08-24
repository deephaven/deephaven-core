package io.deephaven.db.v2.utils;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.config.InputTableStatusListener;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.NullValueColumnSource;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.chunk.*;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An in-memory table that allows you to add rows as if it were an InputTable, which can be updated
 * on the LTM.
 *
 * The table is not keyed, all rows are added to the end of the table. Deletions and edits are not
 * permitted.
 */
public class AppendOnlyArrayBackedMutableTable extends BaseArrayBackedMutableTable {
    static final String DEFAULT_DESCRIPTION = "Append Only In-Memory Input Table";

    /**
     * Create an empty AppendOnlyArrayBackedMutableTable with the given definition.
     *
     * @param definition the definition of the new table.
     *
     * @return an empty AppendOnlyArrayBackedMutableTable with the given definition
     */
    public static AppendOnlyArrayBackedMutableTable make(@NotNull TableDefinition definition) {
        return make(definition, Collections.emptyMap());
    }

    /**
     * Create an empty AppendOnlyArrayBackedMutableTable with the given definition.
     *
     * @param definition the definition of the new table.
     * @param enumValues a map of column names to enumeration values
     *
     * @return an empty AppendOnlyArrayBackedMutableTable with the given definition
     */
    public static AppendOnlyArrayBackedMutableTable make(@NotNull TableDefinition definition,
        final Map<String, Object[]> enumValues) {
        return make(new QueryTable(definition, Index.FACTORY.getEmptyIndex(),
            NullValueColumnSource.createColumnSourceMap(definition)), enumValues);
    }

    /**
     * Create an AppendOnlyArrayBackedMutableTable with the given initial data.
     *
     * @param initialTable the initial values to copy into the AppendOnlyArrayBackedMutableTable
     *
     * @return an empty AppendOnlyArrayBackedMutableTable with the given definition
     */
    public static AppendOnlyArrayBackedMutableTable make(final Table initialTable) {
        return make(initialTable, Collections.emptyMap());
    }

    /**
     * Create an AppendOnlyArrayBackedMutableTable with the given initial data.
     *
     * @param initialTable the initial values to copy into the AppendOnlyArrayBackedMutableTable
     * @param enumValues a map of column names to enumeration values
     *
     * @return an empty AppendOnlyArrayBackedMutableTable with the given definition
     */
    public static AppendOnlyArrayBackedMutableTable make(final Table initialTable,
        final Map<String, Object[]> enumValues) {
        final AppendOnlyArrayBackedMutableTable result = new AppendOnlyArrayBackedMutableTable(
            initialTable.getDefinition(), enumValues, new ProcessPendingUpdater());
        result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        result.setFlat();
        processInitial(initialTable, result);
        return result;
    }

    private AppendOnlyArrayBackedMutableTable(@NotNull TableDefinition definition,
        final Map<String, Object[]> enumValues, final ProcessPendingUpdater processPendingUpdater) {
        super(Index.FACTORY.getEmptyIndex(), makeColumnSourceMap(definition), enumValues,
            processPendingUpdater);
        inputTableDefinition.setKeys(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        inputTableDefinition.setValues(definition.getColumnNamesArray());
    }

    @Override
    protected void processPendingTable(Table table, boolean allowEdits,
        IndexChangeRecorder indexChangeRecorder, Consumer<String> errorNotifier) {
        final Index addIndex = table.getIndex();
        final long firstRow = nextRow;
        final long lastRow = firstRow + addIndex.intSize() - 1;
        final OrderedKeys destinations = OrderedKeys.forRange(firstRow, lastRow);
        destinations.forAllLongs(indexChangeRecorder::addIndex);
        nextRow = lastRow + 1;

        final SharedContext sharedContext = SharedContext.makeSharedContext();
        final int chunkCapacity = table.intSize();

        getColumnSourceMap().forEach((name, cs) -> {
            final ArrayBackedColumnSource<?> arrayBackedColumnSource =
                (ArrayBackedColumnSource<?>) cs;
            arrayBackedColumnSource.ensureCapacity(nextRow);
            final ColumnSource<?> sourceColumnSource = table.getColumnSource(name);
            try (
                final WritableChunkSink.FillFromContext ffc =
                    arrayBackedColumnSource.makeFillFromContext(chunkCapacity);
                final ChunkSource.GetContext getContext =
                    sourceColumnSource.makeGetContext(chunkCapacity, sharedContext)) {
                final Chunk<? extends Attributes.Values> valuesChunk =
                    sourceColumnSource.getChunk(getContext, addIndex);
                arrayBackedColumnSource.fillFromChunk(ffc, valuesChunk, destinations);
            }
        });
    }

    @Override
    protected void processPendingDelete(Table table, IndexChangeRecorder indexChangeRecorder) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getDefaultDescription() {
        return DEFAULT_DESCRIPTION;
    }

    @Override
    void validateDelete(final TableDefinition keyDefinition) {
        throw new UnsupportedOperationException();
    }

    @Override
    ArrayBackedMutableInputTable makeHandler() {
        return new AppendOnlyArrayBackedMutableInputTable();
    }

    private class AppendOnlyArrayBackedMutableInputTable extends ArrayBackedMutableInputTable {
        @Override
        public void delete(Table table, Index index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setRows(@NotNull Table defaultValues, int[] rowArray,
            Map<String, Object>[] valueArray, InputTableStatusListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addRows(Map<String, Object>[] valueArray, boolean allowEdits,
            InputTableStatusListener listener) {
            if (allowEdits) {
                throw new UnsupportedOperationException();
            }
            super.addRows(valueArray, allowEdits, listener);
        }
    }
}
