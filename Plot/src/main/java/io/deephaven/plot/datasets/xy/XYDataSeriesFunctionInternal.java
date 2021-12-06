package io.deephaven.plot.datasets.xy;

import io.deephaven.function.DoubleFpPrimitives;

/**
 * {@link XYDataSeries} based on a function.
 *
 * Internal {@link XYDataSeriesFunction} methods for use inner use.
 */
public interface XYDataSeriesFunctionInternal extends XYDataSeriesFunction {

    /**
     * Sets the data range for this series if the user did not set funcRange.
     *
     * @param xmin range minimum
     * @param xmax range maximum
     * @return this data series with the new range
     */
    XYDataSeriesFunctionInternal funcRangeInternal(final double xmin, final double xmax);

    /**
     * Sets the data range and number of points for this series if the user did not set funcRange.
     *
     * @throws IllegalArgumentException {@code xmin} must not be less than {@code xmax} {@code xmin} and {@code xmax}
     *         must be normal. See {@link DoubleFpPrimitives#isNormal} {@code npoints} must non-negative
     * @param xmin range minimum
     * @param xmax range maximum
     * @param npoints number of data points
     * @return this data series with the new range
     */
    XYDataSeriesFunctionInternal funcRangeInternal(final double xmin, final double xmax, final int npoints);

    /**
     * Sets the number of data points in this dataset if the user did not set funcNPoints.
     *
     * @throws IllegalArgumentException {@code npoints} must be non-negative.
     * @param npoints number of points
     * @return this data series with the specified number of points.
     */
    XYDataSeriesFunctionInternal funcNPointsInternal(final int npoints);
}
