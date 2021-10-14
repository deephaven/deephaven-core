/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.plot.datasets.xyerrorbar;

import io.deephaven.engine.plot.AxesImpl;
import io.deephaven.engine.plot.datasets.xy.XYDataSeriesInternal;

/**
 * An {@link XYDataSeriesInternal} with error bars.
 */
public interface XYErrorBarDataSeriesInternal extends XYErrorBarDataSeries, XYDataSeriesInternal {
    @Override
    XYErrorBarDataSeriesInternal copy(final AxesImpl axes);
}
