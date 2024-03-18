//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.filters;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.ChartImpl;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.PartitionedTableHandle;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.SwappableTableOneClickPartitioned;
import io.deephaven.plot.util.tables.PartitionedTableBackedPartitionedTableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A OneClick filtered table.
 *
 * If requireAllFiltersToDisplay is true, data is only displayed once the user has OneClick filtered all byColumns. If
 * requireAllFiltersToDisplay is false, data is displayed when not all oneclicks are selected
 */
public class SelectableDataSetOneClick implements SelectableDataSet<String, Set<Object>> {
    private final PartitionedTable partitionedTable;
    private final boolean requireAllFiltersToDisplay;
    private final Map<Object, WeakReference<SelectableDataSet<String, Set<Object>>>> transformationCache =
            new HashMap<>();

    /**
     * Creates a SelectableDataSetOneClick instance.
     * <p>
     * Listens for OneClick events for the specified {@code byColumns} and calculates the {@link SwappableTable} by
     * filtering the {@code table}. The {@link SwappableTable} will be null until all {@code byColumns} have a
     * corresponding OneClick filter.
     *
     * @param partitionedTable partitioned table
     * @throws io.deephaven.base.verify.RequirementFailure {@code partitionedTable}, {@code table} and {@code byColumns}
     *         must not be null
     */
    public SelectableDataSetOneClick(PartitionedTable partitionedTable) {
        this(partitionedTable, true);
    }

    /**
     * Creates a SelectableDataSetOneClick instance.
     * <p>
     * Listens for OneClick events for the specified {@code byColumns} and calculates the {@link SwappableTable} by
     * filtering the {@code table}. If {@code requireAllFiltersToDisplay} is true, the {@link SwappableTable} will be
     * null until all {@code byColumns} have a corresponding OneClick filter. If {@code requireAllFiltersToDisplay} is
     * false, the {@link SwappableTable} will be calculated by filtering the {@code table} with the OneClicks used.
     *
     * @param partitionedTable PartitionedTable
     * @param requireAllFiltersToDisplay false to display data when not all oneclicks are selected; true to only display
     *        data when appropriate oneclicks are selected
     * @throws io.deephaven.base.verify.RequirementFailure {@code partitionedTable}, {@code table} and {@code byColumns}
     *         must not be null
     */
    public SelectableDataSetOneClick(final PartitionedTable partitionedTable,
            final boolean requireAllFiltersToDisplay) {
        Require.neqNull(partitionedTable, "partitionedTable");
        this.partitionedTable = partitionedTable;
        this.requireAllFiltersToDisplay = requireAllFiltersToDisplay;
    }

    @Override
    public TableDefinition getTableDefinition() {
        return partitionedTable.constituentDefinition();
    }

    public String[] getByColumns() {
        return partitionedTable.keyColumnNames().toArray(new String[0]);
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
                transformTableDefinition(partitionedTable.constituentDefinition(), tableTransform);

        final List<String> allCols = new ArrayList<>(Arrays.asList(cols));
        allCols.addAll(partitionedTable.keyColumnNames());

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
        if (!updatedTableDef.equals(partitionedTable.constituentDefinition())) {
            viewColumns = allCols.stream()
                    .filter(col -> partitionedTable.constituentDefinition().getColumn(col) != null)
                    .collect(Collectors.toList());
        } else {
            viewColumns = null;
        }

        final String[] byColumns = partitionedTable.keyColumnNames().toArray(new String[0]);
        final PartitionedTableHandle partitionedTableHandle = new PartitionedTableBackedPartitionedTableHandle(
                partitionedTable, updatedTableDef, byColumns, chart.getPlotInfo(), allCols, viewColumns);
        partitionedTableHandle.setKeyColumnsOrdered(byColumns);
        partitionedTableHandle.setOneClickMap(true);

        return new SwappableTableOneClickPartitioned(seriesName, figure.getUpdateInterval(), partitionedTableHandle,
                tableTransform, requireAllFiltersToDisplay, byColumns);
    }

    private TableDefinition transformTableDefinition(TableDefinition toTransform,
            Function<Table, Table> transformation) {
        return transformation != null ? transformation.apply(TableTools.newTable(toTransform)).getDefinition()
                : toTransform;
    }

    @Override
    public SelectableDataSet<String, Set<Object>> transform(@NotNull Object memoKey,
            @NotNull Function<Table, Table> transformation) {
        SelectableDataSet<String, Set<Object>> value = null;
        final WeakReference<SelectableDataSet<String, Set<Object>>> reference = transformationCache.get(memoKey);
        if (reference != null) {
            value = reference.get();
        }

        if (value == null) {
            value = new SelectableDataSetOneClick(partitionedTable.transform(transformation::apply),
                    requireAllFiltersToDisplay);

            transformationCache.put(memoKey, new WeakReference<>(value));
        }
        return value;
    }
}
