/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.multiseries;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.ChartImpl;
import io.deephaven.plot.SeriesInternal;
import io.deephaven.plot.datasets.DataSeries;
import io.deephaven.plot.datasets.DataSeriesInternal;
import io.deephaven.plot.datasets.DynamicSeriesNamer;
import io.deephaven.plot.errors.PlotExceptionCause;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.TableMap;

import java.util.Map;

/**
 * A parent data series that spawns a {@link DataSeries} for each unique key in the parent series.
 */
public interface MultiSeriesInternal<T extends DataSeriesInternal>
        extends MultiSeries, SeriesInternal, PlotExceptionCause {

    @Override
    MultiSeriesInternal<T> copy(final AxesImpl axes);

    /**
     * Gets the name of this data set.
     *
     * @return name of this data set
     */
    Comparable name();

    /**
     * Gets the chart on which this data will be plotted.
     *
     * @return chart on which this data will be plotted
     */
    ChartImpl chart();

    /**
     * Gets the axes on which this data will be plotted.
     *
     * @return axes on which this data will be plotted
     */
    AxesImpl axes();

    /**
     * Gets the id for the multi series.
     */
    int id();


    /**
     * Gets the by columns for the multi-series.
     *
     * @return by columns for the multi-series.
     */
    String[] getByColumns();

    /**
     * Gets the number of series in this multi-series.
     *
     * @return number of series in this multi-series
     */
    int getSeriesCount();

    /**
     * Gets the specified series from this multi-series.
     *
     * @param series series index
     * @return specified series
     */
    T get(final int series);

    /**
     * Creates a new series for this multi-series.
     *
     * @param seriesName name for the series
     * @param t client side source table
     * @return new series for this multi-series
     */
    T createSeries(final String seriesName, final BaseTable t);

    /**
     * Creates a new series for this multi-series.
     *
     * @param seriesName name for the series
     * @param t client side source table
     * @param dynamicSeriesNamer creates the name for the newly generated series. Ensures unique names.
     * @return new series for this multi-series
     */
    T createSeries(final String seriesName, final BaseTable t, final DynamicSeriesNamer dynamicSeriesNamer);

    /**
     * Gets a utility to make certain that all dynamic series have unique names.
     *
     * @return utility to make certain that all dynamic series have unique names.
     */
    DynamicSeriesNamer getDynamicSeriesNamer();

    void setDynamicSeriesNamer(DynamicSeriesNamer seriesNamer);

    /**
     * Assigns series modifiers, e.g. point color, to the given {@code series}
     * 
     * @param series series to initialize
     */
    void initializeSeries(T series);

    /**
     * @return the underlying {@link TableMap}
     */
    TableMap getTableMap();

    /**
     * Add the given series to this MultiSeries
     *
     * @param series series
     * @param key key used to determine the name of the {@code series}
     */
    void addSeries(T series, Object key);

    @Override
    default PlotInfo getPlotInfo() {
        return new PlotInfo(chart().figure(), chart(), this);
    }

    /**
     * Calls a .update() on the underlying table with the given formula: underlyingTable.update(columnName = update)
     *
     * @param columnName the resulting column
     * @param update the formula inside
     * @param classesToImport classes to import into the query scope
     * @param params parameters to add to the query scope
     * @param columnTypesPreserved set to true if the update clause is 'add only' with respect to columns. This allows
     *        the copying of ACLs
     *
     */
    void applyTransform(final String columnName, final String update, final Class[] classesToImport,
            final Map<String, Object> params, boolean columnTypesPreserved);

    /**
     * @return the x-axis data column
     */
    String getX();

    /**
     * @return the y-axis data column
     */
    String getY();

}
