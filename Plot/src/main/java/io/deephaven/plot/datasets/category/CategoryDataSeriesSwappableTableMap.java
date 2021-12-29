/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.category;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.TableSnapshotSeries;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.engine.table.Table;

import java.util.Collection;

/**
 * A dataset for {@link SwappableTable} based categorical data.
 */
public class CategoryDataSeriesSwappableTableMap extends AbstractSwappableTableBasedCategoryDataSeries
        implements CategorySwappableTableDataSeriesInternal, TableSnapshotSeries {
    private static final long serialVersionUID = 2L;

    private transient Table localTable;
    private final SwappableTable swappableTable;

    private final String categoryCol;
    private final String valueCol;

    private transient CategoryDataSeriesKernel kernel;

    /**
     * Creates a CategoryDataSeriesSwappableTableMap instance.
     *
     * @param axes axes on which this data series will be plotted
     * @param id data series id
     * @param name series name
     * @param swappableTable table data. Table may be swapped out
     * @param categories discrete data column in {@code swappableTable}
     * @param values continuous data column in {@code swappableTable}
     * @param <T> type of the categorical data
     * @throws io.deephaven.plot.errors.PlotIllegalArgumentException {@code chart}, {@code swappableTable},
     *         {@code categories}, and {@code values} may not be null.
     * @throws RuntimeException {@code categories} column must be {@link Comparable} {@code values} column must be
     *         numeric
     */
    public <T extends Comparable> CategoryDataSeriesSwappableTableMap(final AxesImpl axes,
            final int id,
            final Comparable<?> name,
            final SwappableTable swappableTable,
            final String categories,
            final String values) {
        super(axes, id, name);
        ArgumentValidations.assertNotNull(axes, "axes", getPlotInfo());
        ArgumentValidations.assertNotNull(swappableTable, "swappableTable", getPlotInfo());
        ArgumentValidations.assertInstance(swappableTable.getTableDefinition(), categories, Comparable.class,
                "Invalid data type in category column: column=" + categories, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), values,
                "Invalid data type in data column: column=" + values, getPlotInfo());

        this.swappableTable = swappableTable;
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
    private CategoryDataSeriesSwappableTableMap(final CategoryDataSeriesSwappableTableMap series, final AxesImpl axes) {
        super(series, axes);

        this.swappableTable = series.swappableTable;
        this.categoryCol = series.categoryCol;
        this.valueCol = series.valueCol;
        this.kernel = new CategoryDataSeriesKernel(categoryCol, valueCol, getPlotInfo());
    }

    @Override
    public CategoryDataSeriesSwappableTableMap copy(AxesImpl axes) {
        return new CategoryDataSeriesSwappableTableMap(this, axes);
    }

    ////////////////////////// external //////////////////////////


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
    public String getCategoryCol() {
        return categoryCol;
    }

    @Override
    public String getNumericCol() {
        return valueCol;
    }

    @Override
    public SwappableTable getSwappableTable() {
        return swappableTable;
    }

}
