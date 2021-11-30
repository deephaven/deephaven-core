/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.category;

import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.TableSnapshotSeries;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;

import java.util.Collection;

/**
 * A dataset for table-based categorical data.
 */
public class CategoryDataSeriesTableMap extends AbstractTableBasedCategoryDataSeries
        implements CategoryTableDataSeriesInternal, TableSnapshotSeries {
    private static final long serialVersionUID = 2L;

    private final TableHandle tableHandle;
    private transient boolean isInit = false;

    private final String categoryCol;
    private final String valueCol;

    private transient CategoryDataSeriesKernel kernel;

    /**
     * Creates a new CategoryDataSeriesTableMap instance.
     *
     * @param axes {@link AxesImpl} on which this dataset is being plotted
     * @param id data series id
     * @param name series name
     * @param tableHandle holds the underlying table
     * @param categories column in the underlying table containing the categorical data
     * @param values column in the underlying table containing the numerical data
     * @param <T> type of the categorical data
     * @throws RequirementFailure {@code chart}, {@code tableHandle}, {@code categories}, and {@code values} must not be
     *         null
     * @throws RuntimeException {@code categories} column must be either time, char/{@link Character},
     *         {@link Comparable}, or numeric {@code values} column must be numeric
     */
    public <T extends Comparable> CategoryDataSeriesTableMap(final AxesImpl axes,
            final int id,
            final Comparable<?> name,
            final TableHandle tableHandle,
            final String categories,
            final String values) {
        super(axes, id, name);
        ArgumentValidations.assertNotNull(axes, "axes", getPlotInfo());
        ArgumentValidations.assertNotNull(tableHandle, "table", getPlotInfo());
        ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(tableHandle.getFinalTableDefinition(),
                categories, "Invalid data type in category column: column=" + categories, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(tableHandle.getFinalTableDefinition(), values,
                "Invalid data type in data column: column=" + values, getPlotInfo());

        this.tableHandle = tableHandle;
        this.categoryCol = categories;
        this.valueCol = values;
        this.kernel = new CategoryDataSeriesKernel(categoryCol, values, getPlotInfo());
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private CategoryDataSeriesTableMap(final CategoryDataSeriesTableMap series, final AxesImpl axes) {
        super(series, axes);

        this.tableHandle = series.tableHandle;
        this.categoryCol = series.categoryCol;
        this.valueCol = series.valueCol;
        this.kernel = new CategoryDataSeriesKernel(categoryCol, valueCol, getPlotInfo());
    }

    @Override
    public CategoryDataSeriesTableMap copy(AxesImpl axes) {
        return new CategoryDataSeriesTableMap(this, axes);
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

    public TableHandle getTableHandle() {
        return tableHandle;
    }

    @Override
    public String getCategoryCol() {
        return categoryCol;
    }

    @Override
    public String getValueCol() {
        return valueCol;
    }

    @Override
    protected Table getTable() {
        return tableHandle.getTable();
    }

}
