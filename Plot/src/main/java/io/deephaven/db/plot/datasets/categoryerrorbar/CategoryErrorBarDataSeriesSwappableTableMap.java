/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.categoryerrorbar;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.TableSnapshotSeries;
import io.deephaven.db.plot.datasets.category.AbstractSwappableTableBasedCategoryDataSeries;
import io.deephaven.db.plot.datasets.category.CategorySwappableTableDataSeriesInternal;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.tables.Table;

import java.util.Collection;

/**
 * A dataset for {@link SwappableTable} based categorical data.
 */
public class CategoryErrorBarDataSeriesSwappableTableMap extends AbstractSwappableTableBasedCategoryDataSeries
        implements CategoryErrorBarDataSeriesInternal, CategorySwappableTableDataSeriesInternal, TableSnapshotSeries {
    private static final long serialVersionUID = 2L;

    private transient Table localTable;
    private final SwappableTable swappableTable;

    private final String categoryCol;
    private final String valueCol;
    private final String errorBarLowCol;
    private final String errorBarHighCol;

    private transient CategoryErrorBarDataSeriesKernel kernel;

    /**
     * Creates a CategoryErrorBarDataSeriesSwappableTableMap instance.
     *
     * @param axes axes on which this data series will be plotted
     * @param id data series id
     * @param name series name
     * @param swappableTable table data. Table may be swapped out
     * @param categories discrete data column in {@code swappableTable}
     * @param valueCol continuous data column in {@code swappableTable}
     * @param <T> type of the categorical data
     * @throws io.deephaven.db.plot.errors.PlotIllegalArgumentException {@code chart}, {@code swappableTable},
     *         {@code categories}, and {@code values} may not be null.
     * @throws RuntimeException {@code categories} column must be {@link Comparable} {@code values} column must be
     *         numeric
     */
    public <T extends Comparable> CategoryErrorBarDataSeriesSwappableTableMap(final AxesImpl axes,
            final int id,
            final Comparable name,
            final SwappableTable swappableTable,
            final String categories,
            final String valueCol,
            final String errorBarLowCol,
            final String errorBarHighCol) {
        super(axes, id, name);
        ArgumentValidations.assertNotNull(axes, "axes", getPlotInfo());
        ArgumentValidations.assertNotNull(swappableTable, "swappableTable", getPlotInfo());
        ArgumentValidations.assertInstance(swappableTable.getTableDefinition(), categories, Comparable.class,
                "Invalid data type in category column: column=" + categories, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), valueCol,
                "Invalid data type in data column: column=" + valueCol, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), errorBarLowCol,
                "Invalid data type in data column: column=" + errorBarLowCol, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), errorBarHighCol,
                "Invalid data type in data column: column=" + errorBarHighCol, getPlotInfo());

        this.swappableTable = swappableTable;
        this.categoryCol = categories;
        this.valueCol = valueCol;
        this.errorBarLowCol = errorBarLowCol;
        this.errorBarHighCol = errorBarHighCol;

        this.kernel = new CategoryErrorBarDataSeriesKernel(categories, valueCol, errorBarLowCol, errorBarHighCol,
                getPlotInfo());
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private CategoryErrorBarDataSeriesSwappableTableMap(final CategoryErrorBarDataSeriesSwappableTableMap series,
            final AxesImpl axes) {
        super(series, axes);

        this.swappableTable = series.swappableTable;
        this.categoryCol = series.categoryCol;
        this.valueCol = series.valueCol;
        this.errorBarLowCol = series.errorBarLowCol;
        this.errorBarHighCol = series.errorBarHighCol;
        this.kernel = new CategoryErrorBarDataSeriesKernel(categoryCol, valueCol, errorBarLowCol, errorBarHighCol,
                getPlotInfo());
    }

    @Override
    public CategoryErrorBarDataSeriesSwappableTableMap copy(AxesImpl axes) {
        return new CategoryErrorBarDataSeriesSwappableTableMap(this, axes);
    }

    @Override
    public int size() {
        return kernel.size();
    }

    @Override
    public Collection<Comparable> categories() {
        return kernel.categories();
    }

    @Override
    public Number getValue(final Comparable category) {
        return kernel.getValue(category);
    }

    @Override
    public long getCategoryLocation(final Comparable category) {
        return kernel.getCategoryKey(category);
    }

    @Override
    public Number getStartY(final Comparable category) {
        return kernel.getStartY(category);
    }

    @Override
    public Number getEndY(final Comparable category) {
        return kernel.getEndY(category);
    }

    @Override
    protected SwappableTable getSwappableTable() {
        return swappableTable;
    }

    @Override
    protected String getCategoryCol() {
        return categoryCol;
    }

    @Override
    protected String getNumericCol() {
        return valueCol;
    }

}
