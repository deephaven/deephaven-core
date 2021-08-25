package io.deephaven.db.plot.datasets.xy;

import io.deephaven.db.plot.Chart;
import io.deephaven.libs.primitives.DoubleFpPrimitives;

/**
 * {@link XYDataSeries} based on a function.
 *
 * By default, this calculates at least 200 data points inside the plot's existing range. The number
 * of points can be increased for a finer grained plot, or decreased if less resolution is needed.
 * The points are recomputed as the {@link Chart}'s x-range changes.
 */
public interface XYDataSeriesFunction extends XYDataSeries {

    /**
     * Sets the data range for this series.
     *
     * @param xmin range minimum
     * @param xmax range maximum
     * @return this data series with the new range
     */
    XYDataSeriesFunction funcRange(final double xmin, final double xmax);

    /**
     * Sets the data range for this series.
     *
     * @throws IllegalArgumentException {@code xmin} must not be less than {@code xmax} {@code xmin}
     *         and {@code xmax} must be normal. See {@link DoubleFpPrimitives#isNormal}
     *         {@code npoints} must non-negative
     * @param xmin range minimum
     * @param xmax range maximum
     * @param npoints number of data points
     * @return this data series with the new range
     */
    XYDataSeriesFunction funcRange(final double xmin, final double xmax, final int npoints);

    /**
     * Sets the number of data points in this dataset.
     *
     * @throws IllegalArgumentException {@code npoints} must be non-negative.
     * @param npoints number of points
     * @return this data series with the specified number of points.
     */
    XYDataSeriesFunction funcNPoints(final int npoints);
}
