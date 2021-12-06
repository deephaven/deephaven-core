/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.ColumnHandlerFactory;
import io.deephaven.plot.util.tables.TableHandle;

/**
 * {@link IndexableData} dataset whose data is a column in a table and whose indices are the row numbers of the column.
 *
 * The column must be numeric.
 */
public class IndexableNumericDataTable extends LiveIndexableNumericData {
    private static final long serialVersionUID = -6514312046355753066L;
    private final ColumnHandlerFactory.ColumnHandler columnHandler;

    /**
     * Creates an IndexableNumericDataTable instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code columnHandler} must not be null
     * @throws UnsupportedOperationException {@code columnHandler} must be numeric
     * @param columnHandler data
     * @param plotInfo plot information
     */
    public IndexableNumericDataTable(final ColumnHandlerFactory.ColumnHandler columnHandler, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(columnHandler, "columnHandler", getPlotInfo());

        this.columnHandler = columnHandler;

        if (!this.columnHandler.typeClassification().isNumeric()) {
            throw new PlotUnsupportedOperationException(
                    "Attempting to create a dataseries with a non-numeric column: column="
                            + columnHandler.getColumnName(),
                    this);
        }

    }

    /**
     * Creates an IndexableNumericDataTable instance. The numeric data is a {@code column} in the table held by
     * {@code tableHandle}. Indices are the row numbers of the column.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code tableHandle} and {@code column} must not be null
     * @throws IllegalArgumentException {@code column} must be a column in {@code tableHandle}
     * @throws UnsupportedOperationException {@code column} must be numeric
     * @param tableHandle holds the table
     * @param column column of underlying table holding the data values
     */
    public IndexableNumericDataTable(final TableHandle tableHandle, final String column, final PlotInfo plotInfo) {
        this(ColumnHandlerFactory.newNumericHandler(tableHandle, column, plotInfo), plotInfo);
    }

    @Override
    public int size() {
        return columnHandler.size();
    }

    @Override
    public double get(final int i) {
        return columnHandler.getDouble(i);
    }

    public ColumnHandlerFactory.ColumnHandler getColumnHandler() {
        return columnHandler;
    }

}
