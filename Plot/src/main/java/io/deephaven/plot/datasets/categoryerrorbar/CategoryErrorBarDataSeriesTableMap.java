/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.categoryerrorbar;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.SeriesInternal;
import io.deephaven.plot.datasets.category.AbstractTableBasedCategoryDataSeries;
import io.deephaven.plot.datasets.category.CategoryTableDataSeriesInternal;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;

import java.util.Collection;

public class CategoryErrorBarDataSeriesTableMap extends AbstractTableBasedCategoryDataSeries
        implements CategoryErrorBarDataSeriesInternal, CategoryTableDataSeriesInternal, SeriesInternal {
    private static final long serialVersionUID = 2L;

    private final TableHandle tableHandle;
    private transient boolean isInit = false;

    private final String categoryCol;
    private final String valueCol;
    private final String errorBarLowCol;
    private final String errorBarHighCol;

    private transient CategoryErrorBarDataSeriesKernel kernel;

    /**
     * Creates a new CategoryErrorBarDataSeriesTableMap instance.
     *
     * @param axes axes on which this data series will be plotted
     * @param id data series id
     * @param name series name
     * @param tableHandle table data
     * @param categoryCol discrete data column in {@code tableHandle}
     * @param valueCol continuous data column in {@code tableHandle}
     * @param errorBarLowCol column in {@code tableHandle} that holds the low whisker value in the y direction
     * @param errorBarHighCol column in {@code tableHandle} that holds the high whisker value in the y direction
     * @throws io.deephaven.base.verify.RequirementFailure {@code axes}, {@code tableHandle}, {@code categoryCol},
     *         {@code values} {@code yLow}, and {@code yHigh} may not be null.
     * @throws RuntimeException {@code categoryCol} column must be {@link Comparable} {@code values} column must be
     *         numeric {@code yLow} column must be numeric {@code yHigh} column must be numeric
     */
    public CategoryErrorBarDataSeriesTableMap(final AxesImpl axes,
            final int id,
            final Comparable name,
            final TableHandle tableHandle,
            final String categoryCol,
            final String valueCol,
            final String errorBarLowCol,
            final String errorBarHighCol) {
        super(axes, id, name);
        ArgumentValidations.assertNotNull(axes, "axes", getPlotInfo());
        ArgumentValidations.assertNotNull(tableHandle, "table", getPlotInfo());
        ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(tableHandle.getFinalTableDefinition(),
                categoryCol, "Invalid data type in category column: column=" + categoryCol, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(tableHandle.getFinalTableDefinition(), valueCol,
                "Invalid data type in data column: column=" + valueCol, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(tableHandle.getFinalTableDefinition(), errorBarLowCol,
                "Invalid data type in data column: column=" + errorBarLowCol, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(tableHandle.getFinalTableDefinition(), errorBarHighCol,
                "Invalid data type in data column: column=" + errorBarHighCol, getPlotInfo());

        this.tableHandle = tableHandle;
        this.categoryCol = categoryCol;
        this.valueCol = valueCol;
        this.errorBarLowCol = errorBarLowCol;
        this.errorBarHighCol = errorBarHighCol;
        this.kernel = new CategoryErrorBarDataSeriesKernel(categoryCol, valueCol, errorBarLowCol, errorBarHighCol,
                getPlotInfo());
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private CategoryErrorBarDataSeriesTableMap(final CategoryErrorBarDataSeriesTableMap series, final AxesImpl axes) {
        super(series, axes);

        this.tableHandle = series.tableHandle;
        this.categoryCol = series.categoryCol;
        this.valueCol = series.valueCol;
        this.errorBarLowCol = series.errorBarLowCol;
        this.errorBarHighCol = series.errorBarHighCol;
        this.kernel = new CategoryErrorBarDataSeriesKernel(categoryCol, valueCol, errorBarLowCol, errorBarHighCol,
                getPlotInfo());
    }

    @Override
    public CategoryErrorBarDataSeriesTableMap copy(AxesImpl axes) {
        return new CategoryErrorBarDataSeriesTableMap(this, axes);
    }

    ////////////////////////// internal //////////////////////////

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
    public long getCategoryLocation(Comparable category) {
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
    protected Table getTable() {
        return tableHandle.getTable();
    }

    @Override
    protected String getCategoryCol() {
        return categoryCol;
    }

    @Override
    protected String getValueCol() {
        return valueCol;
    }

}
