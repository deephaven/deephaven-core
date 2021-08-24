/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.axistransformations;

import io.deephaven.base.verify.Require;

import java.io.Serializable;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;

/**
 * Transformations from dataset space to axis space and back.
 *
 * Axis space refers to how data is displayed in the chart. For example, if the transform from
 * dataset space to axis space was defined as x^0.5, the dataset values 1, 4, and 9 would be plotted
 * as 1, 2, and 3,] creating a square root axis scale.
 */
public class AxisTransformLambda implements AxisTransform, Serializable {

    private static final long serialVersionUID = 303405833375630361L;
    private final DoubleUnaryOperator dta;
    private final DoubleUnaryOperator atd;
    private final DoublePredicate isVisible;

    /**
     * Creates an AxisTransformLambda instance.
     *
     * @param dataToAxis transform from dataset space to axis space
     * @param axisToData transform from axis space to dataset space
     * @param isVisible function that determines if a particular data point should be displayed
     */
    public AxisTransformLambda(final DoubleUnaryOperator dataToAxis,
        final DoubleUnaryOperator axisToData, final DoublePredicate isVisible) {
        Require.neqNull(dataToAxis, "dataToAxis");
        Require.neqNull(axisToData, "axisToData");
        Require.neqNull(isVisible, "isVisible");
        this.dta = dataToAxis;
        this.atd = axisToData;
        this.isVisible = isVisible;
    }

    /**
     * Creates an AxisTransformLambda instance where all dataset values are visible in the plot.
     *
     * @param dataToAxis transform from dataset space to axis space
     * @param axisToData transform from axis space to dataset space
     */
    public AxisTransformLambda(final DoubleUnaryOperator dataToAxis,
        final DoubleUnaryOperator axisToData) {
        this(dataToAxis, axisToData, (DoublePredicate & Serializable) v -> true);
    }

    /**
     * Creates an AxisTransformLambda instance where all dataset values are visible in the plot and
     * the dataset to axis space transform and its inverse are the identity function.
     */
    public AxisTransformLambda() {
        this((DoubleUnaryOperator & Serializable) v -> v,
            (DoubleUnaryOperator & Serializable) v -> v,
            (DoublePredicate & Serializable) v -> true);
    }

    @Override
    public double transform(final double dataValue) {
        return dta.applyAsDouble(dataValue);
    }

    @Override
    public double inverseTransform(final double axisValue) {
        return atd.applyAsDouble(axisValue);
    }

    @Override
    public boolean isVisible(final double dataValue) {
        return isVisible.test(dataValue);
    }

}
