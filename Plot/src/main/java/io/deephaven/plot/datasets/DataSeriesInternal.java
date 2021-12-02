/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets;

import io.deephaven.plot.*;
import io.deephaven.plot.errors.PlotExceptionCause;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.gui.color.Paint;

/**
 * A generic data series.
 */
public interface DataSeriesInternal extends DataSeries, SeriesInternal, PlotExceptionCause {

    ////////////////////////// internal //////////////////////////

    @Override
    DataSeriesInternal copy(final AxesImpl axes);

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
     * Gets the id for the data series.
     */
    int id();

    /**
     * Gets the name of this data series.
     *
     * @return name of this data series
     */
    Comparable name();

    /**
     * Gets the size of this data set.
     *
     * @return size of this data set
     */
    int size();

    /**
     * Gets the line color.
     *
     * @return line color
     */
    Paint getLineColor();

    /**
     * Gets the series color.
     *
     * @return series color
     */
    Paint getSeriesColor();

    /**
     * Gets the color of the error bars.
     *
     * @return error bar color
     */
    Paint getErrorBarColor();

    /**
     * Gets the line style.
     *
     * @return line style
     */
    LineStyle getLineStyle();

    /**
     * Gets whether points are visible.
     *
     * @return whether points are visible
     */
    Boolean getPointsVisible();

    /**
     * Gets whether lines are visible.
     *
     * @return whether lines are visible
     */
    Boolean getLinesVisible();

    /**
     * Gets whether the bar gradient is visible.
     *
     * @return whether the bar gradient is visible
     */
    boolean getGradientVisible();

    /**
     * Gets the point label format.
     *
     * @return point label format
     */
    String getPointLabelFormat();

    /**
     * Gets the tooltip format for x-values.
     *
     * @return x-value tooltip format
     */
    String getXToolTipPattern();

    /**
     * Gets the tooltip format for y-values.
     *
     * @return y-value tooltip format
     */
    String getYToolTipPattern();

    @Override
    default PlotInfo getPlotInfo() {
        return new PlotInfo(chart().figure(), chart(), this);
    }
}
