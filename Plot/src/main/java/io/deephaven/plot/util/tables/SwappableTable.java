/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util.tables;

import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Holds a handle on a table that may get swapped out for another table.
 */
public abstract class SwappableTable implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final TableMapHandle tableMapHandle;

    /**
     * Creates a SwappableTable instance with the {@code tableHandle}.
     *
     * @param tableMapHandle holds the table
     */
    public SwappableTable(@NotNull final TableMapHandle tableMapHandle) {
        this.tableMapHandle = tableMapHandle;
    }

    /**
     * Gets the {@link TableHandle} for this SwappableTable.
     *
     * @return this SwappableTable's {@link TableHandle}
     */
    public TableMapHandle getTableMapHandle() {
        return tableMapHandle;
    }

    /**
     * Adds a column to the underlying table structures.
     *
     * @param column column
     */
    public abstract void addColumn(final String column);

    /**
     * Gets the signature (columns and types) of the table, \even if the data is transformed.
     *
     * @return table with the columns and types of the final table
     */
    public TableDefinition getTableDefinition() {
        return getTableMapHandle().getTableDefinition();
    }
}
