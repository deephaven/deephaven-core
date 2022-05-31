/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util.functions;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import groovy.lang.Closure;
import junit.framework.TestCase;

public class TestClosureDoubleUnaryOperator extends BaseArrayTestCase {


    public void testClousureDoubleUnaryOperator() {
        ClosureDoubleUnaryOperator op = new ClosureDoubleUnaryOperator<>(new Closure<Double>(null) {
            @Override
            public void setResolveStrategy(int resolveStrategy) {
                super.setResolveStrategy(resolveStrategy);
            }

            @Override
            public Double call(Object arguments) {
                double d = (double) arguments;
                return d < 0 ? 1.0 : null;
            }
        });

        assertEquals(1.0, op.applyAsDouble(-1.0));
        assertEquals(1.0, op.applyAsDouble(-2.0));
        assertEquals(Double.NaN, op.applyAsDouble(1.0));

        try {
            new ClosureDoubleUnaryOperator<Double>(null);
            TestCase.fail("Expected an exception");
        } catch (RequirementFailure e) {
            assertTrue(e.getMessage().contains("null"));
        }
    }
}
