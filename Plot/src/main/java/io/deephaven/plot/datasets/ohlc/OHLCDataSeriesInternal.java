/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.ohlc;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.xy.XYDataSeriesInternal;

/**
 * {@link XYDataSeriesInternal} for open-high-low-close charts.
 */
public interface OHLCDataSeriesInternal extends OHLCDataSeries, XYDataSeriesInternal {

    @Override
    OHLCDataSeriesInternal copy(final AxesImpl axes);

    /**
     * Gets the open value at rowSet {@code i}.
     *
     * @param i rowSet
     * @return open value at given rowSet
     */
    double getOpen(int i);

    /**
     * Gets the high value at rowSet {@code i}.
     *
     * @param i rowSet
     * @return high value at given rowSet
     */
    double getHigh(int i);

    /**
     * Gets the low value at rowSet {@code i}.
     *
     * @param i rowSet
     * @return low value at given rowSet
     */
    double getLow(int i);

    /**
     * Gets the close value at rowSet {@code i}.
     *
     * @param i rowSet
     * @return close value at given rowSet
     */
    double getClose(int i);

}
