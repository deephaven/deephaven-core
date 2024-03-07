//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.filters;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.util.parametrized.TableSupplier;

/**
 * Method for creating {@link SelectableDataSet}s.
 */
public class Selectables {
    private Selectables() {}

    /**
     * Creates a {@link SelectableDataSetOneClick} with the specified columns.
     *
     * @param t table
     * @param byColumns selected columns
     * @return {@link SelectableDataSetOneClick} with the specified table and columns
     */
    public static SelectableDataSetOneClick oneClick(final Table t, final String... byColumns) {
        return oneClick(t, true, byColumns);
    }

    /**
     * Creates a {@link SelectableDataSetOneClick} with the specified columns.
     *
     * @param pTable PartitionedTable
     * @return {@link SelectableDataSetOneClick} with the specified partitioned table and columns
     */
    public static SelectableDataSetOneClick oneClick(final PartitionedTable pTable) {
        return oneClick(pTable, true);
    }

    /**
     * Creates a {@link SelectableDataSetOneClick} with the specified columns.
     *
     * @param t table
     * @param byColumns selected columns
     * @param requireAllFiltersToDisplay false to display data when not all oneclicks are selected; true to only display
     *        data when appropriate oneclicks are selected
     * @return {@link SelectableDataSetOneClick} with the specified table and columns
     */
    public static SelectableDataSetOneClick oneClick(
            final Table t,
            final boolean requireAllFiltersToDisplay,
            final String... byColumns) {
        if (byColumns == null || byColumns.length < 1) {
            throw new IllegalArgumentException("byColumns can not be empty");
        }
        Require.neqNull(t, "t");
        // This allows a supplier to be used for one clicks
        final Table table = TableSupplier.complete(t);
        return oneClick(t.partitionBy(byColumns), requireAllFiltersToDisplay);
    }

    /**
     * Creates a {@link SelectableDataSetOneClick} with the specified columns.
     *
     * @param pTable PartitionedTable
     * @param requireAllFiltersToDisplay false to display data when not all oneclicks are selected; true to only display
     *        data when appropriate oneclicks are selected
     * @return {@link SelectableDataSetOneClick} with the specified partitioned table and columns
     */
    public static SelectableDataSetOneClick oneClick(
            final PartitionedTable pTable,
            final boolean requireAllFiltersToDisplay) {
        if (pTable.keyColumnNames().isEmpty()) {
            throw new IllegalArgumentException("byColumns can not be empty");
        }
        Require.neqNull(pTable, "pTable");
        return new SelectableDataSetOneClick(pTable, requireAllFiltersToDisplay);
    }
}
