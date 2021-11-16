package io.deephaven.engine.v2.utils;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.impl.RowSequenceUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.config.InputTableStatusListener;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.v2.sources.NullValueColumnSource;
import io.deephaven.engine.table.WritableChunkSink;
import io.deephaven.engine.chunk.*;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An in-memory table that allows you to add rows as if it were an InputTable, which can be updated on the UGP.
 *
 * The table is not keyed, all rows are added to the end of the table. Deletions and edits are not permitted.
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
        return make(new QueryTable(definition, RowSetFactory.empty().toTracking(),
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
        super(RowSetFactory.empty().toTracking(), makeColumnSourceMap(definition),
                enumValues, processPendingUpdater);
    }

    @Override
    protected void processPendingTable(Table table, boolean allowEdits, IndexChangeRecorder indexChangeRecorder,
            Consumer<String> errorNotifier) {
        try (final RowSet addRowSet = table.getRowSet().clone()) {
            final long firstRow = nextRow;
            final long lastRow = firstRow + addRowSet.intSize() - 1;
            try (final RowSequence destinations = RowSequenceUtil.forRange(firstRow, lastRow)) {
                destinations.forAllRowKeys(indexChangeRecorder::addIndex);
                nextRow = lastRow + 1;

                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final int chunkCapacity = table.intSize();

                getColumnSourceMap().forEach((name, cs) -> {
                    final ArrayBackedColumnSource<?> arrayBackedColumnSource = (ArrayBackedColumnSource<?>) cs;
                    arrayBackedColumnSource.ensureCapacity(nextRow);
                    final ColumnSource<?> sourceColumnSource = table.getColumnSource(name);
                    try (final WritableChunkSink.FillFromContext ffc =
                            arrayBackedColumnSource.makeFillFromContext(chunkCapacity);
                            final ChunkSource.GetContext getContext =
                                    sourceColumnSource.makeGetContext(chunkCapacity, sharedContext)) {
                        final Chunk<? extends Attributes.Values> valuesChunk =
                                sourceColumnSource.getChunk(getContext, addRowSet);
                        arrayBackedColumnSource.fillFromChunk(ffc, valuesChunk, destinations);
                    }
                });
            }
        }
    }

    @Override
    protected void processPendingDelete(Table table, IndexChangeRecorder indexChangeRecorder) {
        throw new UnsupportedOperationException("Table doesn't support delete operation");
    }

    @Override
    protected String getDefaultDescription() {
        return DEFAULT_DESCRIPTION;
    }

    @Override
    protected List<String> getKeyNames() {
        return Collections.emptyList();
    }

    @Override
    ArrayBackedMutableInputTable makeHandler() {
        return new AppendOnlyArrayBackedMutableInputTable();
    }

    private class AppendOnlyArrayBackedMutableInputTable extends ArrayBackedMutableInputTable {
        @Override
        public void setRows(@NotNull Table defaultValues, int[] rowArray, Map<String, Object>[] valueArray,
                InputTableStatusListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validateDelete(Table tableToDelete) {
            throw new UnsupportedOperationException("Table doesn't support delete operation");
        }

        @Override
        public void addRows(Map<String, Object>[] valueArray, boolean allowEdits, InputTableStatusListener listener) {
            if (allowEdits) {
                throw new UnsupportedOperationException();
            }
            super.addRows(valueArray, allowEdits, listener);
        }
    }
}
