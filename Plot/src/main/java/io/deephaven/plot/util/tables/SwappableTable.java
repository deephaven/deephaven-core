/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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

    protected final PartitionedTableHandle partitionedTableHandle;

    /**
     * Creates a SwappableTable instance with the {@code tableHandle}.
     *
     * @param partitionedTableHandle holds the table
     */
    public SwappableTable(@NotNull final PartitionedTableHandle partitionedTableHandle) {
        this.partitionedTableHandle = partitionedTableHandle;
    }

    /**
     * Gets the {@link PartitionedTableHandle} for this SwappableTable.
     *
     * @return this SwappableTable's {@link PartitionedTableHandle}
     */
    public PartitionedTableHandle getPartitionedTableHandle() {
        return partitionedTableHandle;
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
        return getPartitionedTableHandle().getTableDefinition();
    }
}
