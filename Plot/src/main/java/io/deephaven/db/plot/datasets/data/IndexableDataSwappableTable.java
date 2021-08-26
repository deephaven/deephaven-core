/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;

/**
 * {@link IndexableData} dataset whose data is a column in a {@link SwappableTable} and whose indices are the row
 * numbers of the column.
 */
public class IndexableDataSwappableTable<T> extends LiveIndexableData<T> {
    private static final long serialVersionUID = -7007547039766134485L;
    private final SwappableTable swappableTable;
    private final String column;

    /**
     * Creates an IndexableDataSwappableTable instance. The data is a {@code column} in the {@code swappableTable}.
     * Indices are the row numbers of the column.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code swappableTable} and {@code column} must not be null
     * @throws IllegalArgumentException {@code column} must be a column in {@code swappableTable}
     * @param swappableTable swappable table
     * @param column column of {@code swappableTable} holding the data values
     * @param plotInfo plot information
     */
    public IndexableDataSwappableTable(final SwappableTable swappableTable, final String column,
            final PlotInfo plotInfo) {
        super(plotInfo);
        this.swappableTable = swappableTable;
        ArgumentValidations.assertNotNull(swappableTable, "swappableTable", getPlotInfo());
        ArgumentValidations.assertNotNull(column, "column", getPlotInfo());
        ArgumentValidations.assertColumnsInTable(swappableTable.getTableDefinition(), plotInfo, column);
        this.column = column;
    }

    @Override
    public synchronized int size() {
        return 0;
    }

    @Override
    public synchronized T get(int index) {
        return null;
    }

    /**
     * Converts between the value in the column and the return value of the provider.
     *
     * @param v value in the column
     * @return value in the column converted to return type of the provider
     */
    public T convert(Object v) {
        return (T) v;
    }
}
