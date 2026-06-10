//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.iterators.ColumnIterator;

public final class Utility {

    /**
     * Reads all of the data from {@code table} in an iterative manner on this thread. Reading proceeds based on the
     * column order specified in {@code table}'s {@link TableDefinition#getColumns() columns}.
     *
     * @param table the table
     * @see #forceRead(Table, String)
     */
    public static void forceRead(final Table table) {
        for (final ColumnDefinition<?> columnDef : table.getDefinition().getColumns()) {
            forceRead(table, columnDef.getName());
        }
    }

    /**
     * Reads all of the data for the given {@code table}'s {@code columnName} on this thread. Equivalent to
     *
     * <pre>{@code
     * try (final ColumnIterator<?> it = table.columnIterator(columnName)) {
     *     it.consumeAll();
     * }
     * }</pre>
     *
     * @param table the table
     * @param columnName the column name
     * @see Table#columnIterator(String)
     * @see ColumnIterator#consumeAll()
     */
    public static void forceRead(final Table table, final String columnName) {
        try (final ColumnIterator<?> it = table.columnIterator(columnName)) {
            it.consumeAll();
        }
    }

    private Utility() {}
}
