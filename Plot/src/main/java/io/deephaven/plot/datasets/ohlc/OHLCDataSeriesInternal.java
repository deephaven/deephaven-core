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
     * Gets the open value at index {@code i}.
     *
     * @param i index
     * @return open value at given index
     */
    double getOpen(int i);

    /**
     * Gets the high value at index {@code i}.
     *
     * @param i index
     * @return high value at given index
     */
    double getHigh(int i);

    /**
     * Gets the low value at index {@code i}.
     *
     * @param i index
     * @return low value at given index
     */
    double getLow(int i);

    /**
     * Gets the close value at index {@code i}.
     *
     * @param i index
     * @return close value at given index
     */
    double getClose(int i);

}
