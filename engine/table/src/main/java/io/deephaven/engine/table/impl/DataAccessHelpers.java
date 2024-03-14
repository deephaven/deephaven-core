//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

@Deprecated
public class DataAccessHelpers {

    // -----------------------------------------------------------------------------------------------------------------
    // DataColumns for fetching data by row position; generally much less efficient than ColumnSource
    // TODO(deephaven-core#3920): Delete DataColumn
    // -----------------------------------------------------------------------------------------------------------------

    public static DataColumn<?>[] getColumns(Table table) {
        final Table t = table.coalesce();
        return t.getDefinition()
                .getColumnStream()
                .map(c -> getColumn(t, c.getName()))
                .toArray(DataColumn[]::new);
    }

    public static <T> DataColumn<T> getColumn(Table table, final int columnIndex) {
        return getColumn(table, table.getDefinition().getColumns().get(columnIndex).getName());
    }

    public static <T> DataColumn<T> getColumn(Table table, @NotNull final String columnName) {
        return new IndexedDataColumn<>(columnName, table.coalesce());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Convenience data fetching; highly inefficient
    // -----------------------------------------------------------------------------------------------------------------

    public static Object[] getRecord(Table table, long rowNo, String... columnNames) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table t = table.coalesce();
            final long key = t.getRowSet().get(rowNo);
            return (columnNames.length > 0
                    ? Arrays.stream(columnNames).map(t::getColumnSource)
                    : t.getColumnSources().stream())
                    .map(columnSource -> columnSource.get(key))
                    .toArray(Object[]::new);
        }
    }
}
