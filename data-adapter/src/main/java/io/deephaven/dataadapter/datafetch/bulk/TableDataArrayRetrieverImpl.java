//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import gnu.trove.list.TLongList;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class TableDataArrayRetrieverImpl extends AbstractTableDataArrayRetrieverImpl {

    /**
     * The column sources whose data is retrieved.
     */
    @SuppressWarnings("rawtypes")
    private final ColumnSource[] columnSources;

    public TableDataArrayRetrieverImpl(final List<String> columnNames, final Table table) {
        super(columnNames, table.getDefinition());
        this.columnSources = columnNames.stream().map(table::getColumnSource).toArray(ColumnSource[]::new);
    }

    public void fillDataArrays(final boolean usePrev, final RowSet rowSet, final Object[] dataArrs,
            final @NotNull TLongList keyConsumer) {
        fillDataArrays(columnSources, usePrev, rowSet, dataArrs, keyConsumer);
    }
}
