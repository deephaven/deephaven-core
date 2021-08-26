/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.filters;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.TableMap;
import io.deephaven.db.v2.TableSupplier;

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
     * @param tMap TableMap
     * @param byColumns selected columns
     * @return {@link SelectableDataSetOneClick} with the specified table map and columns
     */
    public static SelectableDataSetOneClick oneClick(final TableMap tMap, final Table t, final String... byColumns) {
        return oneClick(tMap, t.getDefinition(), byColumns);
    }

    /**
     * Creates a {@link SelectableDataSetOneClick} with the specified columns.
     *
     * @param tMap TableMap
     * @param byColumns selected columns
     * @return {@link SelectableDataSetOneClick} with the specified table map and columns
     */
    public static SelectableDataSetOneClick oneClick(final TableMap tMap, final TableDefinition tableDefinition,
            final String... byColumns) {
        return oneClick(tMap, tableDefinition, true, byColumns);
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
    public static SelectableDataSetOneClick oneClick(final Table t, final boolean requireAllFiltersToDisplay,
            final String... byColumns) {
        if (byColumns == null || byColumns.length < 1) {
            throw new IllegalArgumentException("byColumns can not be empty");
        }
        Require.neqNull(t, "t");
        // This allows a supplier to be used for one clicks
        final Table table = TableSupplier.complete(t);
        return oneClick(t.byExternal(byColumns), table, requireAllFiltersToDisplay, byColumns);
    }

    /**
     * Creates a {@link SelectableDataSetOneClick} with the specified columns.
     *
     * @param tMap TableMap
     * @param byColumns selected columns
     * @param requireAllFiltersToDisplay false to display data when not all oneclicks are selected; true to only display
     *        data when appropriate oneclicks are selected
     * @return {@link SelectableDataSetOneClick} with the specified table map and columns
     */
    public static SelectableDataSetOneClick oneClick(final TableMap tMap, final Table t,
            final boolean requireAllFiltersToDisplay, final String... byColumns) {
        return oneClick(tMap, t.getDefinition(), requireAllFiltersToDisplay, byColumns);
    }

    /**
     * Creates a {@link SelectableDataSetOneClick} with the specified columns.
     *
     * @param tMap TableMap
     * @param byColumns selected columns
     * @param requireAllFiltersToDisplay false to display data when not all oneclicks are selected; true to only display
     *        data when appropriate oneclicks are selected
     * @return {@link SelectableDataSetOneClick} with the specified table map and columns
     */
    public static SelectableDataSetOneClick oneClick(final TableMap tMap, final TableDefinition tableDefinition,
            final boolean requireAllFiltersToDisplay, final String... byColumns) {
        if (byColumns == null || byColumns.length < 1) {
            throw new IllegalArgumentException("byColumns can not be empty");
        }
        Require.neqNull(tMap, "tMap");
        Require.neqNull(tableDefinition, "tableDefinition");
        return new SelectableDataSetOneClick(tMap, tableDefinition, byColumns, requireAllFiltersToDisplay);
    }
}
