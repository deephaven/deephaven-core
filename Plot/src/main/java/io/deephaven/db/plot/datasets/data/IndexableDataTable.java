/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.ColumnHandlerFactory;

/**
 * {@link IndexableData} dataset whose data is a column in a table and whose indices are the row
 * numbers of the column.
 */
public class IndexableDataTable<T> extends LiveIndexableData<T> {
    private static final long serialVersionUID = 8328713615740315451L;
    private final ColumnHandlerFactory.ColumnHandler columnHandler;

    /**
     * Creates an IndexableDataTable instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code columnHandler} must not be null
     * @param columnHandler data
     * @param plotInfo plot information
     */
    public IndexableDataTable(final ColumnHandlerFactory.ColumnHandler columnHandler,
        final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(columnHandler, "columnHandler", getPlotInfo());
        this.columnHandler = columnHandler;
    }

    @Override
    public int size() {
        return columnHandler.size();
    }

    @Override
    public T get(int index) {
        return convert(columnHandler.get(index));
    }

    /**
     * Converts between the value in the column and the return value of the provider.
     *
     * @param v raw value from the column
     * @return raw value converted into the desired data value
     */
    public T convert(Object v) {
        return (T) v;
    }

}
