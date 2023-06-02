package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

public class TableImpl {


    public static DataColumn[] getColumns(Table table) {
        return ((BaseTable)table).getColumns();
    }

    public static DataColumn getColumn(Table table, final int columnIndex) {
        return ((BaseTable)table).getColumn(columnIndex);
    }

    public static DataColumn getColumn(Table table, @NotNull final String columnName) {
        return ((BaseTable)table).getColumn(columnName);
    }
}
