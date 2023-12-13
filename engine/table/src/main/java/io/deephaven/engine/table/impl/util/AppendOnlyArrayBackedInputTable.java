/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.chunk.*;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

/**
 * An in-memory table that allows you to add rows as if it were an InputTable, which can be updated on the UGP.
 * <p>
 * The table is not keyed, all rows are added to the end of the table. Deletions and edits are not permitted.
 */
public class AppendOnlyArrayBackedInputTable extends BaseArrayBackedInputTable {
    static final String DEFAULT_DESCRIPTION = "Append Only In-Memory Input Table";

    /**
     * Create an empty AppendOnlyArrayBackedMutableTable with the given definition.
     *
     * @param definition the definition of the new table.
     *
     * @return an empty AppendOnlyArrayBackedMutableTable with the given definition
     */
    public static AppendOnlyArrayBackedInputTable make(
            @NotNull TableDefinition definition) {
        // noinspection resource
        return make(new QueryTable(definition, RowSetFactory.empty().toTracking(),
                NullValueColumnSource.createColumnSourceMap(definition)));
    }

    /**
     * Create an AppendOnlyArrayBackedMutableTable with the given initial data.
     *
     * @param initialTable the initial values to copy into the AppendOnlyArrayBackedMutableTable
     *
     * @return an empty AppendOnlyArrayBackedMutableTable with the given definition
     */
    public static AppendOnlyArrayBackedInputTable make(final Table initialTable) {
        final AppendOnlyArrayBackedInputTable result =
                new AppendOnlyArrayBackedInputTable(
                        initialTable.getDefinition(), new ProcessPendingUpdater());
        result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        result.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        result.setFlat();
        processInitial(initialTable, result);
        return result;
    }

    private AppendOnlyArrayBackedInputTable(@NotNull TableDefinition definition,
            final ProcessPendingUpdater processPendingUpdater) {
        // noinspection resource
        super(RowSetFactory.empty().toTracking(), makeColumnSourceMap(definition),
                processPendingUpdater);
    }

    @Override
    protected void processPendingTable(Table table, RowSetChangeRecorder rowSetChangeRecorder) {
        try (final RowSet addRowSet = table.getRowSet().copy()) {
            final long firstRow = nextRow;
            final long lastRow = firstRow + addRowSet.intSize() - 1;
            try (final RowSequence destinations = RowSequenceFactory.forRange(firstRow, lastRow);
                    final SharedContext sharedContext = SharedContext.makeSharedContext()) {
                destinations.forAllRowKeys(rowSetChangeRecorder::addRowKey);
                nextRow = lastRow + 1;

                final int chunkCapacity = table.intSize();

                getColumnSourceMap().forEach((name, cs) -> {
                    final WritableColumnSource<?> writableColumnSource = (WritableColumnSource<?>) cs;
                    writableColumnSource.ensureCapacity(nextRow);
                    final ColumnSource<?> sourceColumnSource = table.getColumnSource(name, cs.getType());
                    try (final ChunkSink.FillFromContext ffc = writableColumnSource.makeFillFromContext(chunkCapacity);
                            final ChunkSource.GetContext getContext =
                                    sourceColumnSource.makeGetContext(chunkCapacity, sharedContext)) {
                        final Chunk<? extends Values> valuesChunk = sourceColumnSource.getChunk(getContext, addRowSet);
                        writableColumnSource.fillFromChunk(ffc, valuesChunk, destinations);
                    }
                });
            }
        }
    }

    @Override
    protected void processPendingDelete(Table table, RowSetChangeRecorder rowSetChangeRecorder) {
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
    ArrayBackedInputTableUpdater makeUpdater() {
        return new Updater();
    }

    private class Updater extends ArrayBackedInputTableUpdater {

        @Override
        public void validateDelete(Table tableToDelete) {
            throw new UnsupportedOperationException("Table doesn't support delete operation");
        }
    }
}
