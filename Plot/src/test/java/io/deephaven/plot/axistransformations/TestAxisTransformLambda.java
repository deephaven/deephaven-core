/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.axistransformations;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;

public class TestAxisTransformLambda extends BaseArrayTestCase {
    private final DoubleUnaryOperator dataToAxis = Math::exp;
    private final DoubleUnaryOperator axisToData = Math::log;
    private final DoublePredicate isVisible = d -> d > 1;
    private final AxisTransformLambda lambda = new AxisTransformLambda(dataToAxis, axisToData, isVisible);
    private final AxisTransformLambda lambda2 = new AxisTransformLambda(dataToAxis, axisToData);
    private final AxisTransformLambda lambda3 = new AxisTransformLambda();
    private final double delta = 0.00001;

    public void testTransform() {
        final double d1 = 3.5;
        final double d2 = 4.2;
        final double d3 = 5.0;
        final double d4 = 6.2;
        final double d5 = 7.0;

        assertEquals(lambda.transform(1.0), Math.E, delta);
        assertEquals(lambda.inverseTransform(Math.E), 1.0, delta);
        assertTrue(lambda.isVisible(1.1));
        assertFalse(lambda.isVisible(0.1));
        assertEquals(d1, lambda.inverseTransform(lambda.transform(d1)), delta);
        assertEquals(d2, lambda.inverseTransform(lambda.transform(d2)), delta);
        assertEquals(d3, lambda.inverseTransform(lambda.transform(d3)), delta);
        assertEquals(d4, lambda.inverseTransform(lambda.transform(d4)), delta);
        assertEquals(d5, lambda.inverseTransform(lambda.transform(d5)), delta);


        assertEquals(lambda2.transform(1.0), Math.E, delta);
        assertEquals(lambda2.inverseTransform(Math.E), 1.0, delta);
        assertTrue(lambda2.isVisible(1.1));
        assertTrue(lambda2.isVisible(0.1));
        assertEquals(d1, lambda2.inverseTransform(lambda2.transform(d1)), delta);
        assertEquals(d2, lambda2.inverseTransform(lambda2.transform(d2)), delta);
        assertEquals(d3, lambda2.inverseTransform(lambda2.transform(d3)), delta);
        assertEquals(d4, lambda2.inverseTransform(lambda2.transform(d4)), delta);
        assertEquals(d5, lambda2.inverseTransform(lambda2.transform(d5)), delta);


        assertEquals(lambda3.transform(1.0), 1.0);
        assertEquals(lambda3.inverseTransform(Math.E), Math.E, delta);
        assertTrue(lambda3.isVisible(1.1));
        assertTrue(lambda3.isVisible(0.1));
        assertEquals(d1, lambda3.inverseTransform(lambda3.transform(d1)), delta);
        assertEquals(d2, lambda3.inverseTransform(lambda3.transform(d2)), delta);
        assertEquals(d3, lambda3.inverseTransform(lambda3.transform(d3)), delta);
        assertEquals(d4, lambda3.inverseTransform(lambda3.transform(d4)), delta);
        assertEquals(d5, lambda3.inverseTransform(lambda3.transform(d5)), delta);
    }
}
