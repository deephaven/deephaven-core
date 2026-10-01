//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.util.QueryConstants;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFpTimsortComparisons {
    @Test
    public void doubleGt() {
        assertTrue(DoubleLongTimsortKernel.gt(Double.NaN, 0.0));
        assertFalse(DoubleLongTimsortKernel.gt(Double.NaN, Double.NaN));
        assertTrue(DoubleLongTimsortKernel.gt(0.0, -1.0));
        assertTrue(DoubleLongTimsortKernel.gt(0.0, QueryConstants.NULL_DOUBLE));
        assertFalse(DoubleLongTimsortKernel.gt(0.0, Double.NaN));
        assertFalse(DoubleLongTimsortKernel.gt(-1.0, 0.0));
        assertFalse(DoubleLongTimsortKernel.gt(QueryConstants.NULL_DOUBLE, 0.0));
        assertFalse(DoubleLongTimsortKernel.gt(-0.0, 0.0));
        assertFalse(DoubleLongTimsortKernel.gt(0.0, -0.0));
    }

    @Test
    public void doubleLt() {
        assertFalse(DoubleLongTimsortKernel.lt(Double.NaN, 0.0));
        assertFalse(DoubleLongTimsortKernel.lt(Double.NaN, Double.NaN));
        assertFalse(DoubleLongTimsortKernel.lt(0.0, -1.0));
        assertFalse(DoubleLongTimsortKernel.lt(0.0, QueryConstants.NULL_DOUBLE));
        assertTrue(DoubleLongTimsortKernel.lt(0.0, Double.NaN));
        assertTrue(DoubleLongTimsortKernel.lt(-1.0, 0.0));
        assertTrue(DoubleLongTimsortKernel.lt(QueryConstants.NULL_DOUBLE, 0.0));
        assertFalse(DoubleLongTimsortKernel.lt(-0.0, 0.0));
        assertFalse(DoubleLongTimsortKernel.lt(0.0, -0.0));
    }

    @Test
    public void doubleGeq() {
        assertTrue(DoubleLongTimsortKernel.geq(Double.NaN, 0.0));
        assertTrue(DoubleLongTimsortKernel.geq(Double.NaN, Double.NaN));
        assertTrue(DoubleLongTimsortKernel.geq(0.0, -1.0));
        assertTrue(DoubleLongTimsortKernel.geq(0.0, QueryConstants.NULL_DOUBLE));
        assertFalse(DoubleLongTimsortKernel.geq(0.0, Double.NaN));
        assertFalse(DoubleLongTimsortKernel.geq(-1.0, 0.0));
        assertFalse(DoubleLongTimsortKernel.geq(QueryConstants.NULL_DOUBLE, 0.0));
        assertTrue(DoubleLongTimsortKernel.geq(0.0, -0.0));
        assertTrue(DoubleLongTimsortKernel.geq(-0.0, 0.0));
    }

    @Test
    public void doubleLeq() {
        assertFalse(DoubleLongTimsortKernel.leq(Double.NaN, 0.0));
        assertTrue(DoubleLongTimsortKernel.leq(Double.NaN, Double.NaN));
        assertFalse(DoubleLongTimsortKernel.leq(0.0, -1.0));
        assertFalse(DoubleLongTimsortKernel.leq(0.0, QueryConstants.NULL_DOUBLE));
        assertTrue(DoubleLongTimsortKernel.leq(0.0, Double.NaN));
        assertTrue(DoubleLongTimsortKernel.leq(-1.0, 0.0));
        assertTrue(DoubleLongTimsortKernel.leq(QueryConstants.NULL_DOUBLE, 0.0));
        assertTrue(DoubleLongTimsortKernel.leq(-0.0, 0.0));
        assertTrue(DoubleLongTimsortKernel.leq(0.0, -0.0));
    }

    @Test
    public void doubleDescGt() {
        assertFalse(DoubleLongTimsortDescendingKernel.gt(Double.NaN, 0.0));
        assertFalse(DoubleLongTimsortDescendingKernel.gt(Double.NaN, Double.NaN));
        assertFalse(DoubleLongTimsortDescendingKernel.gt(0.0, -1.0));
        assertFalse(DoubleLongTimsortDescendingKernel.gt(0.0, QueryConstants.NULL_DOUBLE));
        assertTrue(DoubleLongTimsortDescendingKernel.gt(0.0, Double.NaN));
        assertTrue(DoubleLongTimsortDescendingKernel.gt(-1.0, 0.0));
        assertTrue(DoubleLongTimsortDescendingKernel.gt(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void doubleDescLt() {
        assertTrue(DoubleLongTimsortDescendingKernel.lt(Double.NaN, 0.0));
        assertFalse(DoubleLongTimsortDescendingKernel.lt(Double.NaN, Double.NaN));
        assertTrue(DoubleLongTimsortDescendingKernel.lt(0.0, -1.0));
        assertTrue(DoubleLongTimsortDescendingKernel.lt(0.0, QueryConstants.NULL_DOUBLE));
        assertFalse(DoubleLongTimsortDescendingKernel.lt(0.0, Double.NaN));
        assertFalse(DoubleLongTimsortDescendingKernel.lt(-1.0, 0.0));
        assertFalse(DoubleLongTimsortDescendingKernel.lt(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void doubleDescGeq() {
        assertFalse(DoubleLongTimsortDescendingKernel.geq(Double.NaN, 0.0));
        assertTrue(DoubleLongTimsortDescendingKernel.geq(Double.NaN, Double.NaN));
        assertFalse(DoubleLongTimsortDescendingKernel.geq(0.0, -1.0));
        assertFalse(DoubleLongTimsortDescendingKernel.geq(0.0, QueryConstants.NULL_DOUBLE));
        assertTrue(DoubleLongTimsortDescendingKernel.geq(0.0, Double.NaN));
        assertTrue(DoubleLongTimsortDescendingKernel.geq(-1.0, 0.0));
        assertTrue(DoubleLongTimsortDescendingKernel.geq(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void doubleDescLeq() {
        assertTrue(DoubleLongTimsortDescendingKernel.leq(Double.NaN, 0.0));
        assertTrue(DoubleLongTimsortDescendingKernel.leq(Double.NaN, Double.NaN));
        assertTrue(DoubleLongTimsortDescendingKernel.leq(0.0, -1.0));
        assertTrue(DoubleLongTimsortDescendingKernel.leq(0.0, QueryConstants.NULL_DOUBLE));
        assertFalse(DoubleLongTimsortDescendingKernel.leq(0.0, Double.NaN));
        assertFalse(DoubleLongTimsortDescendingKernel.leq(-1.0, 0.0));
        assertFalse(DoubleLongTimsortDescendingKernel.leq(QueryConstants.NULL_DOUBLE, 0.0));
    }

    @Test
    public void floatGt() {
        assertTrue(FloatLongTimsortKernel.gt(Float.NaN, 0.0f));
        assertFalse(FloatLongTimsortKernel.gt(Float.NaN, Float.NaN));
        assertTrue(FloatLongTimsortKernel.gt(0.0f, -1.0f));
        assertTrue(FloatLongTimsortKernel.gt(0.0f, QueryConstants.NULL_FLOAT));
        assertFalse(FloatLongTimsortKernel.gt(0.0f, Float.NaN));
        assertFalse(FloatLongTimsortKernel.gt(-1.0f, 0.0f));
        assertFalse(FloatLongTimsortKernel.gt(QueryConstants.NULL_FLOAT, 0.0f));
        assertFalse(FloatLongTimsortKernel.gt(-0.0f, 0.0f));
        assertFalse(FloatLongTimsortKernel.gt(0.0f, -0.0f));
    }

    @Test
    public void floatLt() {
        assertFalse(FloatLongTimsortKernel.lt(Float.NaN, 0.0f));
        assertFalse(FloatLongTimsortKernel.lt(Float.NaN, Float.NaN));
        assertFalse(FloatLongTimsortKernel.lt(0.0f, -1.0f));
        assertFalse(FloatLongTimsortKernel.lt(0.0f, QueryConstants.NULL_FLOAT));
        assertTrue(FloatLongTimsortKernel.lt(0.0f, Float.NaN));
        assertTrue(FloatLongTimsortKernel.lt(-1.0f, 0.0f));
        assertTrue(FloatLongTimsortKernel.lt(QueryConstants.NULL_FLOAT, 0.0f));
        assertFalse(FloatLongTimsortKernel.lt(-0.0f, 0.0f));
        assertFalse(FloatLongTimsortKernel.lt(0.0f, -0.0f));
    }

    @Test
    public void floatGeq() {
        assertTrue(FloatLongTimsortKernel.geq(Float.NaN, 0.0f));
        assertTrue(FloatLongTimsortKernel.geq(Float.NaN, Float.NaN));
        assertTrue(FloatLongTimsortKernel.geq(0.0f, -1.0f));
        assertTrue(FloatLongTimsortKernel.geq(0.0f, QueryConstants.NULL_FLOAT));
        assertFalse(FloatLongTimsortKernel.geq(0.0f, Float.NaN));
        assertFalse(FloatLongTimsortKernel.geq(-1.0f, 0.0f));
        assertFalse(FloatLongTimsortKernel.geq(QueryConstants.NULL_FLOAT, 0.0f));
        assertTrue(FloatLongTimsortKernel.geq(0.0f, -0.0f));
        assertTrue(FloatLongTimsortKernel.geq(-0.0f, 0.0f));
    }

    @Test
    public void floatLeq() {
        assertFalse(FloatLongTimsortKernel.leq(Float.NaN, 0.0f));
        assertTrue(FloatLongTimsortKernel.leq(Float.NaN, Float.NaN));
        assertFalse(FloatLongTimsortKernel.leq(0.0f, -1.0f));
        assertFalse(FloatLongTimsortKernel.leq(0.0f, QueryConstants.NULL_FLOAT));
        assertTrue(FloatLongTimsortKernel.leq(0.0f, Float.NaN));
        assertTrue(FloatLongTimsortKernel.leq(-1.0f, 0.0f));
        assertTrue(FloatLongTimsortKernel.leq(QueryConstants.NULL_FLOAT, 0.0f));
        assertTrue(FloatLongTimsortKernel.leq(0.0f, -0.0f));
        assertTrue(FloatLongTimsortKernel.leq(-0.0f, 0.0f));
    }

    @Test
    public void floatDescGt() {
        assertFalse(FloatLongTimsortDescendingKernel.gt(Float.NaN, 0.0f));
        assertFalse(FloatLongTimsortDescendingKernel.gt(Float.NaN, Float.NaN));
        assertFalse(FloatLongTimsortDescendingKernel.gt(0.0f, -1.0f));
        assertFalse(FloatLongTimsortDescendingKernel.gt(0.0f, QueryConstants.NULL_FLOAT));
        assertTrue(FloatLongTimsortDescendingKernel.gt(0.0f, Float.NaN));
        assertTrue(FloatLongTimsortDescendingKernel.gt(-1.0f, 0.0f));
        assertTrue(FloatLongTimsortDescendingKernel.gt(QueryConstants.NULL_FLOAT, 0.0f));
    }

    @Test
    public void floatDescLt() {
        assertTrue(FloatLongTimsortDescendingKernel.lt(Float.NaN, 0.0f));
        assertFalse(FloatLongTimsortDescendingKernel.lt(Float.NaN, Float.NaN));
        assertTrue(FloatLongTimsortDescendingKernel.lt(0.0f, -1.0f));
        assertTrue(FloatLongTimsortDescendingKernel.lt(0.0f, QueryConstants.NULL_FLOAT));
        assertFalse(FloatLongTimsortDescendingKernel.lt(0.0f, Float.NaN));
        assertFalse(FloatLongTimsortDescendingKernel.lt(-1.0f, 0.0f));
        assertFalse(FloatLongTimsortDescendingKernel.lt(QueryConstants.NULL_FLOAT, 0.0f));
    }

    @Test
    public void floatDescGeq() {
        assertFalse(FloatLongTimsortDescendingKernel.geq(Float.NaN, 0.0f));
        assertTrue(FloatLongTimsortDescendingKernel.geq(Float.NaN, Float.NaN));
        assertFalse(FloatLongTimsortDescendingKernel.geq(0.0f, -1.0f));
        assertFalse(FloatLongTimsortDescendingKernel.geq(0.0f, QueryConstants.NULL_FLOAT));
        assertTrue(FloatLongTimsortDescendingKernel.geq(0.0f, Float.NaN));
        assertTrue(FloatLongTimsortDescendingKernel.geq(-1.0f, 0.0f));
        assertTrue(FloatLongTimsortDescendingKernel.geq(QueryConstants.NULL_FLOAT, 0.0f));
    }

    @Test
    public void floatDescLeq() {
        assertTrue(FloatLongTimsortDescendingKernel.leq(Float.NaN, 0.0f));
        assertTrue(FloatLongTimsortDescendingKernel.leq(Float.NaN, Float.NaN));
        assertTrue(FloatLongTimsortDescendingKernel.leq(0.0f, -1.0f));
        assertTrue(FloatLongTimsortDescendingKernel.leq(0.0f, QueryConstants.NULL_FLOAT));
        assertFalse(FloatLongTimsortDescendingKernel.leq(0.0f, Float.NaN));
        assertFalse(FloatLongTimsortDescendingKernel.leq(-1.0f, 0.0f));
        assertFalse(FloatLongTimsortDescendingKernel.leq(QueryConstants.NULL_FLOAT, 0.0f));
    }
}

