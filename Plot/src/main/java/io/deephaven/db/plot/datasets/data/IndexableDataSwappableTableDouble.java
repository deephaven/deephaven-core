/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.plot.util.tables.SwappableTable;

/**
 * {@link IndexableData} dataset whose data is a column in a {@link SwappableTable} and whose
 * indices are the row numbers of the column.
 *
 * Data values are converted to doubles.
 */
public class IndexableDataSwappableTableDouble extends IndexableDataSwappableTable<Double> {

    private static final long serialVersionUID = 2719767692871468219L;

    /**
     * Creates an IndexableDataSwappableTable instance. The data is a {@code column} in the
     * {@code swappableTable}. Indices are the row numbers of the column.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code swappableTable} and {@code column}
     *         must not be null
     * @throws IllegalArgumentException {@code column} must be a column in {@code swappableTable}
     * @throws RuntimeException data in {@code column} must be numeric
     * @param swappableTable swappable table
     * @param column column of {@code swappableTable} holding the data values
     * @param plotInfo plot information
     */
    public IndexableDataSwappableTableDouble(SwappableTable swappableTable, String column,
        final PlotInfo plotInfo) {
        super(swappableTable, column, plotInfo);
        ArgumentValidations.assertIsNumeric(swappableTable.getTableDefinition(), column,
            "Non-numeric column cannot be converted to a double.  column=" + column, plotInfo);
    }

    @Override
    public Double convert(Object v) {
        return v == null ? null : PlotUtils.numberToDouble((Number) v);
    }
}
