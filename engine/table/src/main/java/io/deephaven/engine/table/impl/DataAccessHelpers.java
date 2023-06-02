package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

@Deprecated
public class DataAccessHelpers {

    // -----------------------------------------------------------------------------------------------------------------
    // DataColumns for fetching data by row position; generally much less efficient than ColumnSource
    // TODO(deephaven-core#3920): Delete DataColumn
    // -----------------------------------------------------------------------------------------------------------------

    public static DataColumn<?>[] getColumns(Table table) {
        return table.getDefinition().getColumnStream().map(c -> getColumn(table, c.getName()))
                .toArray(DataColumn[]::new);
    }

    public static <T> DataColumn<T> getColumn(Table table, final int columnIndex) {
        return getColumn(table, table.getDefinition().getColumns().get(columnIndex).getName());
    }

    public static <T> DataColumn<T> getColumn(Table table, @NotNull final String columnName) {
        return new IndexedDataColumn<>(columnName, table.coalesce());
    }
}
