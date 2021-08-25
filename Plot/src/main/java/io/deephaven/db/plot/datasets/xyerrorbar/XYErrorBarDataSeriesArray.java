/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xyerrorbar;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.datasets.xy.AbstractXYDataSeries;
import io.deephaven.db.plot.datasets.xy.XYDataSeriesArray;
import io.deephaven.db.plot.util.ArgumentValidations;

/**
 * Dataset appropriate for an {@link XYErrorBarDataSeriesInternal} composed of indexable data.
 */
public class XYErrorBarDataSeriesArray extends XYDataSeriesArray implements XYErrorBarDataSeriesInternal {

    private IndexableNumericData x;
    private IndexableNumericData xLow;
    private IndexableNumericData xHigh;
    private IndexableNumericData yLow;
    private IndexableNumericData y;
    private IndexableNumericData yHigh;

    private final boolean drawXError;
    private final boolean drawYError;

    public XYErrorBarDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
            final IndexableNumericData x, final IndexableNumericData xLow, final IndexableNumericData xHigh,
            final IndexableNumericData y, final IndexableNumericData yLow, final IndexableNumericData yHigh,
            final boolean drawXError, final boolean drawYError) {
        this(axes, id, name, x, xLow, xHigh, y, yLow, yHigh, drawXError, drawYError, null);
    }

    public XYErrorBarDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
            final IndexableNumericData x, final IndexableNumericData xLow, final IndexableNumericData xHigh,
            final IndexableNumericData y, final IndexableNumericData yLow, final IndexableNumericData yHigh,
            final boolean drawXError, final boolean drawYError, final AbstractXYDataSeries series) {
        super(axes, id, name, x, y, series);
        ArgumentValidations.assertNotNull(x, "x", getPlotInfo());
        this.x = x;
        if (drawXError) {
            ArgumentValidations.assertNotNull(xLow, "xLow", getPlotInfo());
            ArgumentValidations.assertNotNull(xHigh, "xHigh", getPlotInfo());
            this.xLow = xLow;
            this.xHigh = xHigh;
        } else {
            this.xLow = x;
            this.xHigh = x;
        }
        ArgumentValidations.assertNotNull(y, "y", getPlotInfo());
        this.y = y;
        if (drawYError) {
            ArgumentValidations.assertNotNull(yLow, "yLow", getPlotInfo());
            ArgumentValidations.assertNotNull(yHigh, "yHigh", getPlotInfo());
            this.yLow = yLow;
            this.yHigh = yHigh;
        } else {
            this.yLow = y;
            this.yHigh = y;
        }

        this.drawXError = drawXError;
        this.drawYError = drawYError;
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    protected XYErrorBarDataSeriesArray(final XYErrorBarDataSeriesArray series, final AxesImpl axes) {
        super(series, axes);
        this.x = series.x;
        this.xLow = series.xLow;
        this.xHigh = series.xHigh;
        this.y = series.y;
        this.yLow = series.yLow;
        this.yHigh = series.yHigh;
        this.drawXError = series.drawXError;
        this.drawYError = series.drawYError;
    }

    @Override
    public XYErrorBarDataSeriesArray copy(AxesImpl axes) {
        return new XYErrorBarDataSeriesArray(this, axes);
    }

    @Override
    public double getStartX(int i) {
        return xLow.get(i);
    }

    @Override
    public double getEndX(int i) {
        return xHigh.get(i);
    }

    @Override
    public double getStartY(int i) {
        return yLow.get(i);
    }

    @Override
    public double getEndY(int i) {
        return yHigh.get(i);
    }

    @Override
    public boolean drawXError() {
        return drawXError;
    }

    @Override
    public boolean drawYError() {
        return drawYError;
    }

    public IndexableNumericData getXLow() {
        return xLow;
    }

    public IndexableNumericData getXHigh() {
        return xHigh;
    }

    public IndexableNumericData getYLow() {
        return yLow;
    }

    public IndexableNumericData getYHigh() {
        return yHigh;
    }
}
