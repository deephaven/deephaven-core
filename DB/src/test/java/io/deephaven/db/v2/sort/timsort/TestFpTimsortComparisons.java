package io.deephaven.db.v2.sort.timsort;

import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Test;

public class TestFpTimsortComparisons {
    @Test
    public void doubleGt() {
        TestCase.assertTrue(DoubleLongTimsortKernel.gt(Double.NaN, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.gt(Double.NaN, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortKernel.gt(0.0, -1.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.gt(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertFalse(DoubleLongTimsortKernel.gt(0.0, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortKernel.gt(-1.0, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.gt(QueryConstants.NULL_DOUBLE, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.gt(-0.0, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.gt(0.0, -0.0));
    }

    @Test
    public void doubleLt() {
        TestCase.assertFalse(DoubleLongTimsortKernel.lt(Double.NaN, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.lt(Double.NaN, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortKernel.lt(0.0, -1.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.lt(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertTrue(DoubleLongTimsortKernel.lt(0.0, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortKernel.lt(-1.0, 0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.lt(QueryConstants.NULL_DOUBLE, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.lt(-0.0, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.lt(0.0, -0.0));
    }

    @Test
    public void doubleGeq() {
        TestCase.assertTrue(DoubleLongTimsortKernel.geq(Double.NaN, 0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.geq(Double.NaN, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortKernel.geq(0.0, -1.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.geq(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertFalse(DoubleLongTimsortKernel.geq(0.0, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortKernel.geq(-1.0, 0.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.geq(QueryConstants.NULL_DOUBLE, 0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.geq(0.0, -0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.geq(-0.0, 0.0));
    }

    @Test
    public void doubleLeq() {
        TestCase.assertFalse(DoubleLongTimsortKernel.leq(Double.NaN, 0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.leq(Double.NaN, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortKernel.leq(0.0, -1.0));
        TestCase.assertFalse(DoubleLongTimsortKernel.leq(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertTrue(DoubleLongTimsortKernel.leq(0.0, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortKernel.leq(-1.0, 0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.leq(QueryConstants.NULL_DOUBLE, 0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.leq(-0.0, 0.0));
        TestCase.assertTrue(DoubleLongTimsortKernel.leq(0.0, -0.0));
    }

    @Test
    public void doubleDescGt() {
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.gt(Double.NaN, 0.0));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.gt(Double.NaN, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.gt(0.0, -1.0));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.gt(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.gt(0.0, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.gt(-1.0, 0.0));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.gt(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void doubleDescLt() {
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.lt(Double.NaN, 0.0));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.lt(Double.NaN, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.lt(0.0, -1.0));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.lt(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.lt(0.0, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.lt(-1.0, 0.0));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.lt(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void doubleDescGeq() {
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.geq(Double.NaN, 0.0));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.geq(Double.NaN, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.geq(0.0, -1.0));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.geq(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.geq(0.0, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.geq(-1.0, 0.0));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.geq(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void doubleDescLeq() {
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.leq(Double.NaN, 0.0));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.leq(Double.NaN, Double.NaN));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.leq(0.0, -1.0));
        TestCase.assertTrue(DoubleLongTimsortDescendingKernel.leq(0.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.leq(0.0, Double.NaN));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.leq(-1.0, 0.0));
        TestCase.assertFalse(DoubleLongTimsortDescendingKernel.leq(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void floatGt() {
        TestCase.assertTrue(FloatLongTimsortKernel.gt(Float.NaN, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.gt(Float.NaN, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortKernel.gt(0.0f, -1.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.gt(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertFalse(FloatLongTimsortKernel.gt(0.0f, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortKernel.gt(-1.0f, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.gt(QueryConstants.NULL_FLOAT, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.gt(-0.0f, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.gt(0.0f, -0.0f));
    }

    @Test
    public void floatLt() {
        TestCase.assertFalse(FloatLongTimsortKernel.lt(Float.NaN, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.lt(Float.NaN, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortKernel.lt(0.0f, -1.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.lt(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatLongTimsortKernel.lt(0.0f, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortKernel.lt(-1.0f, 0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.lt(QueryConstants.NULL_FLOAT, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.lt(-0.0f, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.lt(0.0f, -0.0f));
    }

    @Test
    public void floatGeq() {
        TestCase.assertTrue(FloatLongTimsortKernel.geq(Float.NaN, 0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.geq(Float.NaN, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortKernel.geq(0.0f, -1.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.geq(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertFalse(FloatLongTimsortKernel.geq(0.0f, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortKernel.geq(-1.0f, 0.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.geq(QueryConstants.NULL_FLOAT, 0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.geq(0.0f, -0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.geq(-0.0f, 0.0f));
    }

    @Test
    public void floatLeq() {
        TestCase.assertFalse(FloatLongTimsortKernel.leq(Float.NaN, 0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.leq(Float.NaN, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortKernel.leq(0.0f, -1.0f));
        TestCase.assertFalse(FloatLongTimsortKernel.leq(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatLongTimsortKernel.leq(0.0f, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortKernel.leq(-1.0f, 0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.leq(QueryConstants.NULL_FLOAT, 0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.leq(0.0f, -0.0f));
        TestCase.assertTrue(FloatLongTimsortKernel.leq(-0.0f, 0.0f));
    }

    @Test
    public void floatDescGt() {
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.gt(Float.NaN, 0.0f));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.gt(Float.NaN, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.gt(0.0f, -1.0f));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.gt(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.gt(0.0f, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.gt(-1.0f, 0.0f));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.gt(QueryConstants.NULL_FLOAT, 0.0f));
    }

    @Test
    public void floatDescLt() {
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.lt(Float.NaN, 0.0f));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.lt(Float.NaN, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.lt(0.0f, -1.0f));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.lt(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.lt(0.0f, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.lt(-1.0f, 0.0f));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.lt(QueryConstants.NULL_FLOAT, 0.0f));
    }

    @Test
    public void floatDescGeq() {
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.geq(Float.NaN, 0.0f));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.geq(Float.NaN, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.geq(0.0f, -1.0f));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.geq(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.geq(0.0f, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.geq(-1.0f, 0.0f));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.geq(QueryConstants.NULL_FLOAT, 0.0f));
    }

    @Test
    public void floatDescLeq() {
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.leq(Float.NaN, 0.0f));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.leq(Float.NaN, Float.NaN));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.leq(0.0f, -1.0f));
        TestCase.assertTrue(FloatLongTimsortDescendingKernel.leq(0.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.leq(0.0f, Float.NaN));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.leq(-1.0f, 0.0f));
        TestCase.assertFalse(FloatLongTimsortDescendingKernel.leq(QueryConstants.NULL_FLOAT, 0.0f));
    }
}

