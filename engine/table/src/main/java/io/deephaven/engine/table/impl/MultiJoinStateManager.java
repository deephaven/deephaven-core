package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;

/**
 * This is a common interface for the static and incremental state manager so that our bucketed MultiJoin system is
 * capable of using them interchangeably to build the table.
 */
public interface MultiJoinStateManager {
    /**
     * Add the given table to this multijoin result.
     *
     * @param table the table to add
     * @param sources the column sources that contain the keys
     * @param tableNumber the table number that we are adding rows for
     */
    void build(final Table table, ColumnSource<?>[] sources, int tableNumber);

    /**
     * How many rows are in our result table?
     *
     * @return the number of rows in the result table
     */
    long getResultSize();

    /**
     * Get the hash table column sources for the result table.  These are used as the key columns of our result.
     */
    ColumnSource<?>[] getKeyHashTableSources();

    /**
     * Get the result RedirectionIndex for a given table
     *
     * @param tableNumber the table to fetch
     * @return the redirection index for the table
     */
    WritableRowRedirection getRowRedirectionForTable(int tableNumber);

    /**
     * Ensure that this state manager can handle tables different tables as constituents of the multiJoin.
     *
     * @param tables the number of tables that participate
     */
    void ensureTableCapacity(int tables);

    void setTargetLoadFactor(final double targetLoadFactor);

    void setMaximumLoadFactor(final double maximumLoadFactor);
}
