/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.ohlc;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.datasets.xy.AbstractXYDataSeries;
import io.deephaven.db.plot.util.ArgumentValidations;

/**
 * An implementation of {@link OHLCDataSeriesInternal}. This Doesn't allow for multiple series.
 */
public class OHLCDataSeriesArray extends AbstractXYDataSeries implements OHLCDataSeriesInternal {

    private static final long serialVersionUID = -8864867700289144828L;
    private final IndexableNumericData time;
    private final IndexableNumericData open;
    private final IndexableNumericData high;
    private final IndexableNumericData low;
    private final IndexableNumericData close;

    /**
     * Creates an OHLCDataSeriesArray instance. This dataset is suited for open-high-low-close charts.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code time}, {@code open}, {@code high}, {@code low},
     *         {@code close} must not be null
     * @param axes axes on which the dataset will be plotted
     * @param id data series id
     * @param name series name
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     */
    public OHLCDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
            final IndexableNumericData time, final IndexableNumericData open, final IndexableNumericData high,
            final IndexableNumericData low, final IndexableNumericData close) {
        this(axes, id, name, time, open, high, low, close, null);
    }

    public OHLCDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
            final IndexableNumericData time, final IndexableNumericData open, final IndexableNumericData high,
            final IndexableNumericData low, final IndexableNumericData close, final AbstractXYDataSeries series) {
        super(axes, id, name, series);
        ArgumentValidations.assertNotNull(time, "Time", getPlotInfo());
        ArgumentValidations.assertNotNull(open, "Open", getPlotInfo());
        ArgumentValidations.assertNotNull(high, "High", getPlotInfo());
        ArgumentValidations.assertNotNull(low, "Low", getPlotInfo());
        ArgumentValidations.assertNotNull(close, "Close", getPlotInfo());

        this.time = time;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    protected OHLCDataSeriesArray(final OHLCDataSeriesArray series, final AxesImpl axes) {
        super(series, axes);
        this.time = series.time;
        this.open = series.open;
        this.high = series.high;
        this.low = series.low;
        this.close = series.close;
    }

    @Override
    public OHLCDataSeriesArray copy(AxesImpl axes) {
        return new OHLCDataSeriesArray(this, axes);
    }


    @Override
    public double getOpen(int item) {
        return open.get(item);
    }

    @Override
    public double getHigh(int item) {
        return high.get(item);
    }

    @Override
    public double getLow(int item) {
        return low.get(item);
    }

    @Override
    public double getClose(int item) {
        return close.get(item);
    }

    @Override
    public int size() {
        return time.size();
    }

    @Override
    public double getX(int item) {
        return time.get(item);
    }

    @Override
    public double getY(int item) {
        return close.get(item);
    }

    public IndexableNumericData getTime() {
        return time;
    }

    public IndexableNumericData getOpen() {
        return open;
    }

    public IndexableNumericData getHigh() {
        return high;
    }

    public IndexableNumericData getLow() {
        return low;
    }

    public IndexableNumericData getClose() {
        return close;
    }
}
