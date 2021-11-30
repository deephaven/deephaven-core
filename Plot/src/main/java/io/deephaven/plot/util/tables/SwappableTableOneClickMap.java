/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util.tables;

import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.engine.table.Table;

import java.util.*;
import java.util.function.Function;

/**
 * Holds a handle on a one click table that may get swapped out for another table. A TableMap is used to compute the
 * OneClick.
 */
public class SwappableTableOneClickMap extends SwappableTableOneClickAbstract {
    private static final long serialVersionUID = 1L;

    private final Function<Table, Table> transform;

    public SwappableTableOneClickMap(final Comparable seriesName, final long updateInterval,
            final TableMapHandle tableMapHandle, final Function<Table, Table> transform, final String... byColumns) {
        this(seriesName, updateInterval, tableMapHandle, transform, true, byColumns);
    }

    public SwappableTableOneClickMap(final Comparable seriesName, final long updateInterval,
            final TableMapHandle tableMapHandle, final Function<Table, Table> transform,
            final boolean requireAllFiltersToDisplay, final String... byColumns) {
        super(seriesName, updateInterval, tableMapHandle, requireAllFiltersToDisplay, byColumns);
        ArgumentValidations.assertColumnsInTable(tableMapHandle.getTableDefinition(), null, byColumns);
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
