/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xy;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.util.ArgumentValidations;

/**
 * {@link XYDataSeriesInternal} backed by {@link IndexableNumericData}.
 */
public class XYDataSeriesArray extends AbstractXYDataSeries {

    private static final long serialVersionUID = 7686441715908956603L;
    private final IndexableNumericData x;
    private final IndexableNumericData y;

    /**
     * Creates a XYDataSeriesArray instance.
     *
     * @param axes axes on which this data series will be plotted
     * @param id data series id
     * @param name series name
     * @param x x-values
     * @param y y-values
     */
    public XYDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
        final IndexableNumericData x, final IndexableNumericData y) {
        this(axes, id, name, x, y, null);
    }

    public XYDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
        final IndexableNumericData x, final IndexableNumericData y,
        final AbstractXYDataSeries series) {
        super(axes, id, name, series);
        ArgumentValidations.assertNotNull(x, "x", getPlotInfo());
        ArgumentValidations.assertNotNull(y, "y", getPlotInfo());
        ArgumentValidations.assertSameSize(new IndexableNumericData[] {x, y},
            new String[] {"x", "y"}, getPlotInfo());

        this.x = x;
        this.y = y;
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    protected XYDataSeriesArray(final XYDataSeriesArray series, final AxesImpl axes) {
        super(series, axes);
        this.x = series.x;
        this.y = series.y;
    }

    @Override
    public XYDataSeriesArray copy(AxesImpl axes) {
        return new XYDataSeriesArray(this, axes);
    }


    ////////////////////////// internal //////////////////////////


    @Override
    public int size() {
        return x.size();
    }

    @Override
    public double getX(int i) {
        return x.get(i);
    }

    @Override
    public double getY(int i) {
        return y.get(i);
    }

    public IndexableNumericData getX() {
        return x;
    }

    public IndexableNumericData getY() {
        return y;
    }
}
