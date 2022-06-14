/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.util.tables;

import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.engine.table.Table;

import java.util.*;
import java.util.function.Function;

/**
 * Holds a handle on a one click table that may get swapped out for another table. A PartitionedTable is used to compute
 * the OneClick.
 */
public class SwappableTableOneClickPartitioned extends SwappableTableOneClickAbstract {
    private static final long serialVersionUID = 1L;

    private final Function<Table, Table> transform;

    public SwappableTableOneClickPartitioned(
            final Comparable seriesName,
            final long updateInterval,
            final PartitionedTableHandle partitionedTableHandle,
            final Function<Table, Table> transform,
            final String... byColumns) {
        this(seriesName, updateInterval, partitionedTableHandle, transform, true, byColumns);
    }

    public SwappableTableOneClickPartitioned(
            final Comparable seriesName,
            final long updateInterval,
            final PartitionedTableHandle partitionedTableHandle,
            final Function<Table, Table> transform,
            final boolean requireAllFiltersToDisplay,
            final String... byColumns) {
        super(seriesName, updateInterval, partitionedTableHandle, requireAllFiltersToDisplay, byColumns);
        ArgumentValidations.assertColumnsInTable(partitionedTableHandle.getTableDefinition(), null, byColumns);
        this.transform = transform;
    }

    public Function<Table, Table> getTransform() {
        return transform;
    }

    @Override
    public List<String> getByColumns() {
        return Collections.unmodifiableList(Arrays.asList(byColumns));
    }
}
