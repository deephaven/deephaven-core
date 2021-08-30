/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.axistransformations;

/**
 * Function applied to transform data before plotting. For example, a log plot is a logarithmic
 * transform of an axis.
 *
 * Determines how dataset values are displayed in the plot by transforming dataset values into Axis
 * space and back.
 *
 * For example, if the forward transform is x -> x^0.5, a dataset value of 1 maps to 1 in Axis
 * space; a dataset value of 4 maps to 2. In the plot these values will be displayed close together.
 */
public interface AxisTransform {

    /**
     * Transforms a data point to Axis space.
     *
     * @param dataValue data point
     * @return corresponding value in Axis space
     */
    double transform(double dataValue);

    /**
     * Transforms a point in Axis space to dataset space.
     *
     * @param axisValue Axis space data point
     * @return corresponding value in dataset space
     */
    double inverseTransform(double axisValue);

    /**
     * Gets whether a data point is visible in the plot.
     *
     * @param dataValue data point
     * @return whether {@code dataValue} is visible in the plot
     */
    boolean isVisible(double dataValue);
}
