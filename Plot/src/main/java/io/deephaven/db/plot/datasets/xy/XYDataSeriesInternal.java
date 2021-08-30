/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xy;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.datasets.DataSeriesInternal;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.Shape;

/**
 * {@link DataSeriesInternal} with two numerical components, x and y. Data points are numbered and
 * are accessed with an index.
 */
public interface XYDataSeriesInternal extends XYDataSeries, DataSeriesInternal {

    ////////////////////////// internal //////////////////////////

    @Override
    XYDataSeriesInternal copy(final AxesImpl axes);

    /**
     * Gets the x value of the data point at index {@code i}.
     *
     * @param i index
     * @return x value of this data point at index {@code i}
     */
    double getX(int i);

    /**
     * Gets the y value of the data point at index {@code i}.
     *
     * @param i index
     * @return x value of this data point at index {@code i}
     */
    double getY(int i);

    /**
     * Gets the size of the data point at index {@code i}.
     *
     * @param i index
     * @return size of this data point at index {@code i}
     */
    Double getPointSize(int i);

    /**
     * Gets the color of the data point at index {@code i}.
     *
     * @param i index
     * @return color of this data point at index {@code i}
     */
    Paint getPointColor(int i);

    /**
     * Gets the label of the data point at index {@code i}.
     *
     * @param i index
     * @return label of this data point at index {@code i}
     */
    String getPointLabel(int i);

    /**
     * Gets the shape of the data point at index {@code i}.
     *
     * @param i index
     * @return shape of this data point at index {@code i}
     */
    Shape getPointShape(int i);

    default double getStartX(int i) {
        return getX(i);
    }

    default double getEndX(int i) {
        return getX(i);
    }

    default double getStartY(int i) {
        return getY(i);
    }

    default double getEndY(int i) {
        return getY(i);
    }

    default boolean drawXError() {
        return false;
    }

    default boolean drawYError() {
        return false;
    }
}
