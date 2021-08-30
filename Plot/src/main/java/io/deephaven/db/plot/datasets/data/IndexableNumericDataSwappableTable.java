/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;

/**
 * {@link IndexableData} dataset whose data is a column in a {@link SwappableTable} and whose
 * indices are the row numbers of the column.
 *
 * The column must be numeric.
 */
public class IndexableNumericDataSwappableTable extends LiveIndexableNumericData {
    private static final long serialVersionUID = -5440770798950942675L;
    private final SwappableTable swappableTable;
    private final String column;

    /**
     * Creates an IndexableNumericDataSwappableTable instance. The numeric data is a {@code column}
     * in the {@code swappableTable}. Indices are the row numbers of the column.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code swappableTable} and {@code column}
     *         must not be null
     * @throws IllegalArgumentException {@code column} must be a column in {@code swappableTable}
     * @throws RuntimeException {@code column} must be numeric
     * @param swappableTable swappable table
     * @param column column of {@code swappableTable} holding the data values
     * @param plotInfo plot information
     */
    public IndexableNumericDataSwappableTable(final SwappableTable swappableTable,
        final String column, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(swappableTable, "swappableTable", getPlotInfo());
        ArgumentValidations.assertNotNull(column, "column", getPlotInfo());
        ArgumentValidations.assertColumnsInTable(swappableTable.getTableDefinition(), plotInfo,
            column);
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), column,
            "Attempting to create a dataseries with a non-numeric column: column=" + column,
            plotInfo);
        this.swappableTable = swappableTable;
        this.column = column;
    }

    @Override
    public synchronized int size() {
        return 0;
    }

    @Override
    public synchronized double get(int i) {
        return Double.NaN;
    }

    public SwappableTable getSwappableTable() {
        return swappableTable;
    }

    public String getColumn() {
        return column;
    }
}
