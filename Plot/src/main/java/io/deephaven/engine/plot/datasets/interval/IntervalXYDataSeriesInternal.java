/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.plot.datasets.interval;

import io.deephaven.engine.plot.AxesImpl;
import io.deephaven.engine.plot.datasets.xy.XYDataSeriesInternal;

/**
 * {@link XYDataSeriesInternal} suitable for bar charts.
 */
public interface IntervalXYDataSeriesInternal extends IntervalXYDataSeries, XYDataSeriesInternal {

    @Override
    IntervalXYDataSeriesInternal copy(final AxesImpl axes);

    /**
     * Gets the left-most point of the bar.
     *
     * @param item rowSet
     * @return left-most point of the bar
     */
    double getStartX(final int item);

    /**
     * Gets the right-most point of the bar.
     *
     * @param item rowSet
     * @return right-most point of the bar
     */
    double getEndX(final int item);

    /**
     * Gets the bottom-most point of the bar.
     *
     * @param item rowSet
     * @return bottom-most point of the bar
     */
    double getStartY(final int item);

    /**
     * Gets the top-most point of the bar.
     *
     * @param item rowSet
     * @return top-most point of the bar
     */
    double getEndY(final int item);

}
