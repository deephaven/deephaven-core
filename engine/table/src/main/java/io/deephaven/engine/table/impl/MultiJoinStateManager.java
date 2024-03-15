//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.RowRedirection;

/**
 * This is a common interface for the static and incremental state manager so that our bucketed MultiJoinTable system is
 * capable of using them interchangeably to build the table.
 */
public interface MultiJoinStateManager {
    /**
     * Add the given table to this multiJoin result.
     *
     * @param table the table to add
     * @param sources the column sources that contain the keys
     * @param tableNumber the table number for which we are adding rows
     */
    void build(final Table table, ColumnSource<?>[] sources, int tableNumber);

    /**
     * Get the number of rows in the result table
     *
     * @return the number of rows in the result table
     */
    long getResultSize();

    /**
     * Get the hash table column sources for the result table. These are used as the key columns of our result.
     */
    ColumnSource<?>[] getKeyHashTableSources();

    /**
     * Get the result {@link RowRedirection row redirection} for a given table
     *
     * @param tableNumber the table to fetch
     * @return the row redirection for the table
     */
    RowRedirection getRowRedirectionForTable(int tableNumber);

    /**
     * Ensure that this state manager can handle {@code numTables} tables as constituents of the multiJoin.
     *
     * @param numTables the number of tables that participate
     */
    void ensureTableCapacity(int numTables);

    void setTargetLoadFactor(final double targetLoadFactor);

    void setMaximumLoadFactor(final double maximumLoadFactor);
}
