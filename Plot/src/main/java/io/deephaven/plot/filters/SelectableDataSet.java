/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.filters;

import io.deephaven.plot.ChartImpl;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.Function;

/**
 * A {@link Table} with a view on a selectable subset.
 */
public interface SelectableDataSet<KEY_TYPE, VALUE_TYPE> {

    /**
     * @return underlying table definition.
     */
    TableDefinition getTableDefinition();

    /**
     * Gets the view of the {@link Table} with the selected subset.
     *
     * @param chart chart
     * @param tableTransform tableTransform applied to the tables in tableMaps. The purpose of this transform is to
     *        track the table definitions for tables inside tableMap
     * @param cols selected columns
     * @return table view on selected subset
     */
    SwappableTable getSwappableTable(final Comparable seriesName, final ChartImpl chart,
            Function<Table, Table> tableTransform, final String... cols);

    /**
     * Gets the view of the {@link Table} with the selected subset. The table transform is the identity function.
     *
     * @param chart chart
     * @param cols selected columns
     * @return table view on selected subset
     */
    default SwappableTable getSwappableTable(final Comparable seriesName, final ChartImpl chart, final String... cols) {
        return getSwappableTable(seriesName, chart, null, cols);
    }

    /**
     * Gets a version of the SelectableDataSet with a lastBy applied to the tables.
     *
     * @param groupByColumns The grouping columns for the lastBy
     * @return a new SelectableDataSet with lastBy applied
     *
     * @deprecated This method will be removed in a future release, use {@link #transform(Object, Function)} instead.
     */
    @Deprecated()
    default SelectableDataSet<KEY_TYPE, VALUE_TYPE> getLastBy(final Collection<String> groupByColumns) {
        return transform(groupByColumns, t -> t.lastBy(groupByColumns));
    }

    /**
     * Produces a derivative {@link SelectableDataSet} with the specified transformation applied to all internal tables.
     *
     * @param memoKey An Object that uniquely identifies the actions taken by the transformation so it can be cached.
     *
     * @return a new {@link SelectableDataSet} with the transformation applied
     */
    SelectableDataSet<KEY_TYPE, VALUE_TYPE> transform(@NotNull Object memoKey,
            @NotNull Function<Table, Table> transformation);
}
