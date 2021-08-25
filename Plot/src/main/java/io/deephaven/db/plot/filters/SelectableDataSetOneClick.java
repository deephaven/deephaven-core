/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.filters;

import io.deephaven.base.verify.Require;
import io.deephaven.db.plot.BaseFigureImpl;
import io.deephaven.db.plot.ChartImpl;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.plot.util.tables.SwappableTableOneClickMap;
import io.deephaven.db.plot.util.tables.TableMapBackedTableMapHandle;
import io.deephaven.db.plot.util.tables.TableMapHandle;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.TableMap;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A OneClick filtered table.
 *
 * If requireAllFiltersToDisplay is true, data is only displayed once the user has OneClick filtered
 * all byColumns. If requireAllFiltersToDisplay is false, data is displayed when not all oneclicks
 * are selected
 */
public class SelectableDataSetOneClick implements SelectableDataSet<String, Set<Object>> {
    private final TableMap tableMap;
    private final TableDefinition tableMapTableDefinition;
    private final String[] byColumns;
    private final boolean requireAllFiltersToDisplay;
    private final Map<Object, WeakReference<SelectableDataSet<String, Set<Object>>>> transformationCache =
        new HashMap<>();

    /**
     * Creates a SelectableDataSetOneClick instance.
     *
     * Listens for OneClick events for the specified {@code byColumns} and calculates the
     * {@link SwappableTable} by filtering the {@code table}. The {@link SwappableTable} will be
     * null until all {@code byColumns} have a corresponding OneClick filter.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code tableMap}, {@code table} and
     *         {@code byColumns} must not be null
     * @param tableMap table map
     * @param tableMapTableDefinition table definition
     * @param byColumns selected columns
     */
    public SelectableDataSetOneClick(TableMap tableMap, TableDefinition tableMapTableDefinition,
        String[] byColumns) {
        this(tableMap, tableMapTableDefinition, byColumns, true);
    }

    /**
     * Creates a SelectableDataSetOneClick instance.
     *
     * Listens for OneClick events for the specified {@code byColumns} and calculates the
     * {@link SwappableTable} by filtering the {@code table}. If {@code requireAllFiltersToDisplay}
     * is true, the {@link SwappableTable} will be null until all {@code byColumns} have a
     * corresponding OneClick filter. If {@code requireAllFiltersToDisplay} is false, the
     * {@link SwappableTable} will be calculated by filtering the {@code table} with the OneClicks
     * used.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code tableMap}, {@code table} and
     *         {@code byColumns} must not be null
     * @param tableMap table map
     * @param tableMapTableDefinition TableDefinition of the underlying table
     * @param byColumns selected columns
     * @param requireAllFiltersToDisplay false to display data when not all oneclicks are selected;
     *        true to only display data when appropriate oneclicks are selected
     */
    public SelectableDataSetOneClick(TableMap tableMap, TableDefinition tableMapTableDefinition,
        String[] byColumns, final boolean requireAllFiltersToDisplay) {
        Require.neqNull(tableMap, "tableMap");
        Require.neqNull(tableMapTableDefinition, "tableMapTableDefinition");
        Require.neqNull(byColumns, "byColumns");
        this.tableMap = tableMap;
        this.tableMapTableDefinition = tableMapTableDefinition;
        this.byColumns = byColumns;
        this.requireAllFiltersToDisplay = requireAllFiltersToDisplay;
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableMapTableDefinition;
    }

    public String[] getByColumns() {
        return byColumns;
    }

    @Override
    public SwappableTable getSwappableTable(final Comparable seriesName,
        final ChartImpl chart,
        final Function<Table, Table> tableTransform,
        final String... cols) {
        ArgumentValidations.assertNotNull(chart, "chart", chart.getPlotInfo());
        ArgumentValidations.assertNotNull(cols, "cols", chart.getPlotInfo());

        final BaseFigureImpl figure = chart.figure();

        final TableDefinition updatedTableDef =
            transformTableDefinition(tableMapTableDefinition, tableTransform);

        final List<String> allCols = new ArrayList<>(Arrays.asList(cols));
        allCols.addAll(Arrays.asList(byColumns));

        final List<String> missingColumns = allCols.stream()
            .filter(col -> updatedTableDef.getColumn(col) == null)
            .collect(Collectors.toList());

        if (!missingColumns.isEmpty()) {
            throw new IllegalStateException("The columns [" + String.join(", ", missingColumns)
                + "] do not exist in the resulting table. Available columns are [" +
                updatedTableDef.getColumnNamesAsString() + "]");
        }

        final List<String> viewColumns;
        // If these do not match Then we'll have to use a different set of view columns.
        if (!updatedTableDef.equals(tableMapTableDefinition)) {
            viewColumns = allCols.stream()
                .filter(col -> tableMapTableDefinition.getColumn(col) != null)
                .collect(Collectors.toList());
        } else {
            viewColumns = null;
        }

        final TableMapHandle tableMapHandle = new TableMapBackedTableMapHandle(tableMap,
            updatedTableDef, byColumns, chart.getPlotInfo(), allCols, viewColumns);
        tableMapHandle.setTableMap(tableMap);
        tableMapHandle.setKeyColumnsOrdered(byColumns);
        tableMapHandle.setOneClickMap(true);

        return new SwappableTableOneClickMap(seriesName, figure.getUpdateInterval(), tableMapHandle,
            tableTransform, requireAllFiltersToDisplay, byColumns);
    }

    private TableDefinition transformTableDefinition(TableDefinition toTransform,
        Function<Table, Table> transformation) {
        return transformation != null
            ? transformation.apply(TableTools.newTable(toTransform)).getDefinition()
            : toTransform;
    }

    @Override
    public SelectableDataSet<String, Set<Object>> transform(@NotNull Object memoKey,
        @NotNull Function<Table, Table> transformation) {
        SelectableDataSet<String, Set<Object>> value = null;
        final WeakReference<SelectableDataSet<String, Set<Object>>> reference =
            transformationCache.get(memoKey);
        if (reference != null) {
            value = reference.get();
        }

        if (value == null) {
            value = new SelectableDataSetOneClick(tableMap.transformTables(transformation),
                transformTableDefinition(tableMapTableDefinition, transformation),
                byColumns,
                requireAllFiltersToDisplay);

            transformationCache.put(memoKey, new WeakReference<>(value));
        }
        return value;
    }
}
