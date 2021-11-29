/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.xy;

import io.deephaven.io.logger.Logger;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.Chart;
import io.deephaven.plot.errors.PlotIllegalArgumentException;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.PlotUtils;
import io.deephaven.function.DoubleFpPrimitives;
import io.deephaven.internal.log.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.DoubleUnaryOperator;

/**
 * {@link XYDataSeriesInternal} based on a function.
 *
 * By default, this calculates at least 200 data points inside the plots existing range. The number of points can be
 * increased for a finer grained plot, or decreased if less resolution is needed. The points are recomputed as the
 * {@link Chart}'s x-range changes.
 */
public class XYDataSeriesFunctionImpl extends AbstractXYDataSeries implements XYDataSeriesFunctionInternal {

    private static final long serialVersionUID = -2830236235998986828L;
    private static final Logger log = LoggerFactory.getLogger(XYDataSeriesFunctionImpl.class);
    private static final int MAX_BUFFER_LOAD = 10;

    private final transient DoubleUnaryOperator function;
    private double xmin = Double.NaN;
    private double xmax = Double.NaN;
    private double ymin = Double.NaN;
    private double ymax = Double.NaN;
    private int npoints = 200;
    private double[][] currentData;
    private double[][] nextData;

    private SortedMap<Double, Double> buffer;
    private boolean nPointsSet;
    private boolean rangeSet;

    /**
     * Creates a XYDataSeriesFunction instance.
     *
     * @param axes axes on which the function will be plotted
     * @param id data series id
     * @param name series name
     * @param function function to plot
     */
    public XYDataSeriesFunctionImpl(final AxesImpl axes, final int id, final Comparable name,
            @SuppressWarnings("ConstantConditions") final DoubleUnaryOperator function) {
        super(axes, id, name, null);
        this.function = function;
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private XYDataSeriesFunctionImpl(final XYDataSeriesFunctionImpl series, final AxesImpl axes) {
        super(series, axes);

        this.function = series.function;


        this.xmin = series.xmin;
        this.xmax = series.xmax;
        this.npoints = series.npoints;
        this.rangeSet = series.rangeSet;
        this.nPointsSet = series.nPointsSet;

        this.recompute(true);
    }

    @Override
    public XYDataSeriesFunctionImpl copy(AxesImpl axes) {
        return new XYDataSeriesFunctionImpl(this, axes);
    }


    ////////////////////////// internal //////////////////////////


    private synchronized void recompute(final boolean recomputeAll) {

        if (buffer == null) {
            buffer = new TreeMap<>();
        }

        // if there get to be too many points, restart
        maybeClearBuffer();
        boolean changed = false;

        if (recomputeAll || buffer.isEmpty()) {
            recomputeRange(xmin, xmax, npoints);
            changed = true;
        }

        final double dxmax = (xmax - xmin) / (npoints - 1);

        if (!buffer.isEmpty()) {
            final double lowerBound = buffer.firstKey();
            final double upperBound = buffer.lastKey();

            if (xmin < lowerBound) {
                final int n = (int) Math.ceil((lowerBound - xmin) / dxmax) + 1;
                recomputeRange(xmin, lowerBound, n);
                changed = true;
            }

            if (upperBound < xmax) {
                final int n = (int) Math.ceil((xmax - upperBound) / dxmax) + 1;
                recomputeRange(upperBound, xmax, n);
                changed = true;
            }
        }

        if (buffer.keySet().stream().filter(x -> x >= xmin && x <= xmax).count() < npoints) {
            recomputeRange(xmin, xmax, npoints);
            changed = true;
        }

        if (changed) {
            nextData = new double[2][buffer.size()];
            int indx = 0;
            for (Map.Entry<Double, Double> e : buffer.entrySet()) {
                nextData[0][indx] = e.getKey();
                nextData[1][indx] = e.getValue();
                indx++;
            }
            currentData = nextData;
        }
    }

    private void recomputeRange(final double xmin, final double xmax, final int npoints) {
        final double dx = (xmax - xmin) / (npoints - 1);

        if (!DoubleFpPrimitives.isNormal(xmin) || !DoubleFpPrimitives.isNormal(xmax)
                || !DoubleFpPrimitives.isNormal(dx)) {
            log.info("XYDataSeriesFunction: abnormal range: xmin=" + xmin + " xmax=" + xmax + " dx=" + dx + " npoints="
                    + npoints);
            return;
        }

        for (int i = 0; i < npoints; i++) {
            final double x = xmin + i * dx;
            computeY(x);
        }
    }

    private void computeY(final double x) {
        if (DoubleFpPrimitives.isNormal(x)) {
            buffer.computeIfAbsent(x, val -> {
                double y = function.applyAsDouble(x);
                if (!DoubleFpPrimitives.isNormal(y)) {
                    log.info("XYDataSeriesFunction: abnormal y value: x=" + x + " y=" + y + " xmin=" + xmin + " xmax="
                            + xmax + " npoints=" + npoints);
                    y = Double.NaN;
                }
                ymin = PlotUtils.minIgnoreNaN(ymin, y);
                ymax = PlotUtils.maxIgnoreNaN(ymax, y);
                return y;
            });
        } else {
            log.info("XYDataSeriesFunction: abnormal x value: x=" + x + " xmin=" + xmin + " xmax=" + xmax + " npoints="
                    + npoints);
        }
    }

    private void maybeClearBuffer() {
        if (buffer.size() > MAX_BUFFER_LOAD * npoints) {
            buffer.clear();
        }
    }

    @Override
    public int size() {
        if (currentData == null) {
            return 0;
        } else {
            return currentData[0].length;
        }
    }

    @Override
    public double getX(int i) {
        if (i < 0 || i > size()) {
            throw new IndexOutOfBoundsException(
                    "Index out of bounds. index=" + i + " size=" + size());
        }

        return currentData[0][i];
    }

    @Override
    public double getY(int i) {
        if (i < 0 || i > size()) {
            throw new IndexOutOfBoundsException(
                    "Index out of bounds. index=" + i + " size=" + size());
        }

        return currentData[1][i];
    }


    ////////////////////////// modification //////////////////////////


    /**
     * Sets the data range for this series.
     *
     * @param xmin range minimum
     * @param xmax range maximum
     * @return this data series with the new range
     */
    @Override
    public XYDataSeriesFunctionImpl funcRange(final double xmin, final double xmax) {
        rangeSet = true;
        return funcRangeInternal(xmin, xmax, true);
    }

    /**
     * Sets the data range for this series.
     *
     * @throws IllegalArgumentException {@code xmin} must not be less than {@code xmax} {@code xmin} and {@code xmax}
     *         must be normal. See {@link DoubleFpPrimitives#isNormal} {@code npoints} must non-negative
     * @param xmin range minimum
     * @param xmax range maximum
     * @param npoints number of data points
     * @return this data series with the new range
     */
    @Override
    public XYDataSeriesFunctionImpl funcRange(final double xmin, final double xmax, final int npoints) {
        rangeSet = true;
        nPointsSet = true;
        return funcRangeInternal(xmin, xmax, npoints, true);
    }

    /**
     * Sets the number of data points in this dataset.
     *
     * @throws IllegalArgumentException {@code npoints} must be non-negative.
     * @param npoints number of points
     * @return this data series with the specified number of points.
     */
    @Override
    public XYDataSeriesFunctionImpl funcNPoints(final int npoints) {
        nPointsSet = true;
        return funcNPointsInternal(npoints, true);
    }

    /**
     * Invokes a funcRangeInternal if xmin or xmax has changed.
     */
    public void invokeRecompute(final double xmin, final double xmax, final String name, final int sessionId) {
        if (xmin == this.xmin && xmax == this.xmax) {
            return;
        }

        funcRangeInternal(xmin, xmax);
    }

    // Public for use outside this package. Not meant to be customer facing
    public XYDataSeriesFunctionImpl funcRangeInternal(double xmin, double xmax) {
        return funcRangeInternal(xmin, xmax, false);
    }

    @Override
    // Public for use outside this package. Not meant to be customer facing
    public XYDataSeriesFunctionImpl funcRangeInternal(double xmin, double xmax, int npoints) {
        return funcRangeInternal(xmin, xmax, npoints, false);
    }

    @Override
    // Public for use outside this package. Not meant to be customer facing
    public XYDataSeriesFunctionImpl funcNPointsInternal(int npoints) {
        return funcNPointsInternal(npoints, false);
    }

    /**
     * Change the range of the function.
     *
     * @param xmin lower bound for the x values to plot
     * @param xmax upper bound for the x values to plot
     * @param isUser whether the user is the one who called for the range to be changed. If a user called for the
     *        change, we don't want internal calls to change the plot out from under them.
     * @return this series
     */
    private XYDataSeriesFunctionImpl funcRangeInternal(double xmin, double xmax, boolean isUser) {
        return funcRangeInternal(xmin, xmax, this.npoints, isUser);
    }

    /**
     * Change the range of the function, and compute values with the specified number of points.
     *
     * @param xmin lower bound for the x values to plot
     * @param xmax upper bound for the x values to plot
     * @param npoints number of points to compute
     * @param isUser whether the user is the one who called for the range to be changed. If a user called for the
     *        change, we don't want internal calls to change the plot out from under them.
     * @return this series
     */
    private XYDataSeriesFunctionImpl funcRangeInternal(double xmin, double xmax, int npoints, boolean isUser) {
        if (!DoubleFpPrimitives.isNormal(xmin) || !DoubleFpPrimitives.isNormal(xmax)) {
            throw new PlotIllegalArgumentException("Abnormal range value.  xmin=" + xmin + " xmax=" + xmax, this);
        }

        if (xmin > xmax) {
            throw new PlotIllegalArgumentException("xmax < xmin: xmin=" + xmin + " xmax=" + xmax, this);
        }

        if (npoints < 0) {
            throw new PlotIllegalArgumentException("npoints < 0", this);
        }

        if (!rangeSet || isUser) {
            final boolean changeNumberOfPoints = !nPointsSet || isUser;
            final boolean recompute = this.npoints != npoints && changeNumberOfPoints;
            final boolean changed = this.xmin != xmin || this.xmax != xmax || (recompute);
            this.xmin = xmin;
            this.xmax = xmax;
            this.npoints = npoints;

            if (changed) {
                recompute(recompute);
            }
        }

        return this;
    }

    /**
     * Change the granularity of the plot by specifying how many points to compute.
     *
     * @param npoints number of points to compute
     * @param isUser whether the user is the one who called for the number of points to be changed. If a user called for
     *        the change, we don't want internal calls to change the plot out from under them.
     * @return this series
     */
    private XYDataSeriesFunctionImpl funcNPointsInternal(int npoints, boolean isUser) {
        if (npoints < 0) {
            throw new PlotIllegalArgumentException("npoints < 0", this);
        }

        if (!nPointsSet || isUser) {
            final boolean changed = this.npoints != npoints;
            this.npoints = npoints;

            if (changed) {
                recompute(true);
            }
        }

        return this;
    }
}
