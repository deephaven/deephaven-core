/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.libs.primitives.FloatNumericPrimitives.*;
import static io.deephaven.libs.primitives.FloatPrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestFloatNumericPrimitives extends BaseArrayTestCase {

    public void testSignum() {
        assertEquals((float) 1, signum((float) 5));
        assertEquals((float) 0, signum((float) 0));
        assertEquals((float) -1, signum((float) -5));
        assertEquals(NULL_FLOAT, signum(NULL_FLOAT));
    }

    public void testAvg() {
        assertEquals(50.0, avg(new float[]{40, 50, 60}));
        assertEquals(45.5, avg(new float[]{40, 51}));
        assertTrue(Double.isNaN(avg(new float[]{})));
        assertTrue(Double.isNaN(avg(new float[]{NULL_FLOAT})));
        assertEquals(10.0, avg(new float[]{5, NULL_FLOAT, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((float[])null));

        assertEquals(50.0, avg(new Float[]{(float)40, (float)50, (float)60}));
        assertEquals(45.5, avg(new Float[]{(float)40, (float)51}));
        assertTrue(Double.isNaN(avg(new Float[]{})));
        assertTrue(Double.isNaN(avg(new Float[]{NULL_FLOAT})));
        assertEquals(10.0, avg(new Float[]{(float)5, NULL_FLOAT, (float)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((Float[])null));

        assertEquals(50.0, avg(new DbFloatArrayDirect(new float[]{40, 50, 60})));
        assertEquals(45.5, avg(new DbFloatArrayDirect(new float[]{40, 51})));
        assertTrue(Double.isNaN(avg(new DbFloatArrayDirect())));
        assertTrue(Double.isNaN(avg(new DbFloatArrayDirect(NULL_FLOAT))));
        assertEquals(10.0, avg(new DbFloatArrayDirect(new float[]{5, NULL_FLOAT, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((DbFloatArrayDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, absAvg(new float[]{40, (float) 50, 60}));
        assertEquals(45.5, absAvg(new float[]{(float) 40, 51}));
        assertTrue(Double.isNaN(absAvg(new float[]{})));
        assertTrue(Double.isNaN(absAvg(new float[]{NULL_FLOAT})));
        assertEquals(10.0, absAvg(new float[]{(float) 5, NULL_FLOAT, (float) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((float[])null));

        assertEquals(50.0, absAvg(new Float[]{(float)40, (float) 50, (float)60}));
        assertEquals(45.5, absAvg(new Float[]{(float) 40, (float)51}));
        assertTrue(Double.isNaN(absAvg(new Float[]{})));
        assertTrue(Double.isNaN(absAvg(new Float[]{NULL_FLOAT})));
        assertEquals(10.0, absAvg(new Float[]{(float) 5, NULL_FLOAT, (float) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((Float[])null));

        assertEquals(50.0, absAvg(new DbFloatArrayDirect(new float[]{40, (float) 50, 60})));
        assertEquals(45.5, absAvg(new DbFloatArrayDirect(new float[]{(float) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new DbFloatArrayDirect())));
        assertTrue(Double.isNaN(absAvg(new DbFloatArrayDirect(NULL_FLOAT))));
        assertEquals(10.0, absAvg(new DbFloatArrayDirect((float) 5, NULL_FLOAT, (float) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((DbFloatArrayDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, countPos(new float[]{40, 50, 60, (float) 1, 0}));
        assertEquals(0, countPos(new float[]{}));
        assertEquals(0, countPos(new float[]{NULL_FLOAT}));
        assertEquals(3, countPos(new float[]{5, NULL_FLOAT, 15, (float) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, countPos((float[])null));

        assertEquals(4, countPos(new Float[]{(float)40, (float)50, (float)60, (float) 1, (float)0}));
        assertEquals(0, countPos(new Float[]{}));
        assertEquals(0, countPos(new Float[]{NULL_FLOAT}));
        assertEquals(3, countPos(new Float[]{(float)5, NULL_FLOAT, (float)15, (float) 1, (float)0}));
        assertEquals(QueryConstants.NULL_INT, countPos((float[])null));

        assertEquals(4, countPos(new DbFloatArrayDirect(new float[]{40, 50, 60, (float) 1, 0})));
        assertEquals(0, countPos(new DbFloatArrayDirect()));
        assertEquals(0, countPos(new DbFloatArrayDirect(NULL_FLOAT)));
        assertEquals(3, countPos(new DbFloatArrayDirect(new float[]{5, NULL_FLOAT, 15, (float) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((DbFloatArrayDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, countNeg(new float[]{40, (float) -50, 60, (float) -1, 0}));
        assertEquals(0, countNeg(new float[]{}));
        assertEquals(0, countNeg(new float[]{NULL_FLOAT}));
        assertEquals(1, countNeg(new float[]{5, NULL_FLOAT, 15, (float) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((float[])null));

        assertEquals(2, countNeg(new Float[]{(float)40, (float) -50, (float)60, (float) -1, (float)0}));
        assertEquals(0, countNeg(new Float[]{}));
        assertEquals(0, countNeg(new Float[]{NULL_FLOAT}));
        assertEquals(1, countNeg(new Float[]{(float)5, NULL_FLOAT, (float)15, (float) -1, (float)0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((Float[])null));

        assertEquals(2, countNeg(new DbFloatArrayDirect(new float[]{40, (float) -50, 60, (float) -1, 0})));
        assertEquals(0, countNeg(new DbFloatArrayDirect()));
        assertEquals(0, countNeg(new DbFloatArrayDirect(NULL_FLOAT)));
        assertEquals(1, countNeg(new DbFloatArrayDirect(new float[]{5, NULL_FLOAT, 15, (float) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((DbFloatArrayDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, countZero(new float[]{0, 40, 50, 60, (float) -1, 0}));
        assertEquals(0, countZero(new float[]{}));
        assertEquals(0, countZero(new float[]{NULL_FLOAT}));
        assertEquals(2, countZero(new float[]{0, 5, NULL_FLOAT, 0, (float) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((float[])null));

        assertEquals(2, countZero(new Float[]{(float)0, (float)40, (float)50, (float)60, (float) -1, (float)0}));
        assertEquals(0, countZero(new Float[]{}));
        assertEquals(0, countZero(new Float[]{NULL_FLOAT}));
        assertEquals(2, countZero(new Float[]{(float)0, (float)5, NULL_FLOAT, (float)0, (float) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((Float[])null));

        assertEquals(2, countZero(new DbFloatArrayDirect(new float[]{0, 40, 50, 60, (float) -1, 0})));
        assertEquals(0, countZero(new DbFloatArrayDirect()));
        assertEquals(0, countZero(new DbFloatArrayDirect(NULL_FLOAT)));
        assertEquals(2, countZero(new DbFloatArrayDirect(new float[]{0, 5, NULL_FLOAT, 0, (float) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((DbFloatArrayDirect)null));
    }

    public void testMax() {
        assertEquals((float) 60, max(new DbFloatArrayDirect(new float[]{0, 40, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals((float) 60, max(new DbFloatArrayDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1)));
        assertEquals(NULL_FLOAT, max(new DbFloatArrayDirect()));
        assertEquals(NULL_FLOAT, max(new DbFloatArrayDirect(NULL_FLOAT)));
        assertEquals(NULL_FLOAT, max((DbFloatArray) null));

        assertEquals((float) 60, max((float) 0, (float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1, (float) 0));
        assertEquals((float) 60, max((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1));
        assertEquals(NULL_FLOAT, max());
        assertEquals(NULL_FLOAT, max(NULL_FLOAT));
        assertEquals(NULL_FLOAT, max((float[]) null));
        assertEquals(NULL_FLOAT, max((Float[]) null));
    }

    public void testMin() {
        assertEquals((float) 0, min(new DbFloatArrayDirect(new float[]{0, 40, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals((float) -1, min(new DbFloatArrayDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1)));
        assertEquals(NULL_FLOAT, min(new DbFloatArrayDirect()));
        assertEquals(NULL_FLOAT, min(new DbFloatArrayDirect(NULL_FLOAT)));
        assertEquals(NULL_FLOAT, min((DbFloatArray) null));

        assertEquals((float) 0, min((float) 0, (float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1, (float) 0));
        assertEquals((float) -1, min((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1));
        assertEquals(NULL_FLOAT, min());
        assertEquals(NULL_FLOAT, min(NULL_FLOAT));
        assertEquals(NULL_FLOAT, min((float[]) null));
        assertEquals(NULL_FLOAT, min((Float[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, firstIndexOf(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}, (float)40));
        assertEquals(4, firstIndexOf(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}, (float)60));
        assertEquals(NULL_INT, firstIndexOf(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}, (float)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((float[])null, (float)40));

        assertEquals(1, firstIndexOf(new DbFloatArrayDirect(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}), (float)40));
        assertEquals(4, firstIndexOf(new DbFloatArrayDirect(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}), (float)60));
        assertEquals(NULL_INT, firstIndexOf(new DbFloatArrayDirect(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}), (float)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((DbFloatArray) null, (float)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, indexOfMax(new float[]{0, 40, NULL_FLOAT, 50, 60, (float) 1, 0}));
        assertEquals(3, indexOfMax(new float[]{(float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1}));
        assertEquals(-1, indexOfMax(new float[]{}));
        assertEquals(-1, indexOfMax(new float[]{NULL_FLOAT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((float[])null));

        assertEquals(4, indexOfMax(new Float[]{(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) 1, (float)0}));
        assertEquals(3, indexOfMax(new Float[]{(float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1}));
        assertEquals(-1, indexOfMax(new Float[]{}));
        assertEquals(-1, indexOfMax(new Float[]{NULL_FLOAT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((Float[])null));

        assertEquals(4, indexOfMax(new DbFloatArrayDirect(new float[]{0, 40, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals(3, indexOfMax(new DbFloatArrayDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1)));
        assertEquals(-1, indexOfMax(new DbFloatArrayDirect()));
        assertEquals(-1, indexOfMax(new DbFloatArrayDirect(NULL_FLOAT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((DbFloatArrayDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, indexOfMin(new float[]{40, 0, NULL_FLOAT, 50, 60, (float) 1, 0}));
        assertEquals(4, indexOfMin(new float[]{(float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1}));
        assertEquals(-1, indexOfMin(new float[]{}));
        assertEquals(-1, indexOfMin(new float[]{NULL_FLOAT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((float[])null));

        assertEquals(1, indexOfMin(new Float[]{(float)40, (float)0, NULL_FLOAT, (float)50, (float)60, (float) 1, (float)0}));
        assertEquals(4, indexOfMin(new Float[]{(float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1}));
        assertEquals(-1, indexOfMin(new Float[]{}));
        assertEquals(-1, indexOfMin(new Float[]{NULL_FLOAT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((Float[])null));

        assertEquals(1, indexOfMin(new DbFloatArrayDirect(new float[]{40, 0, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals(4, indexOfMin(new DbFloatArrayDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1)));
        assertEquals(-1, indexOfMin(new DbFloatArrayDirect()));
        assertEquals(-1, indexOfMin(new DbFloatArrayDirect(NULL_FLOAT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((DbFloatArrayDirect)null));
    }


    public void testVar() {
        float[] v = {0, 40, NULL_FLOAT, 50, 60, (float) -1, 0};
        Float[] V = {(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) -1, (float)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, var((float[])null));

        assertEquals(var, var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, var((Float[])null));

        assertEquals(var, var(new DbFloatArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((DbFloatArrayDirect)null));
    }

    public void testStd() {
        float[] v = {0, 40, NULL_FLOAT, 50, 60, (float) -1, 0};
        Float[] V = {(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) -1, (float)0};

        assertEquals(Math.sqrt(var(new DbFloatArrayDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((float[])null));

        assertEquals(Math.sqrt(var(new DbFloatArrayDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Float[])null));

        assertEquals(Math.sqrt(var(new DbFloatArrayDirect(v))), std(new DbFloatArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((DbFloatArrayDirect)null));
    }

    public void testSte() {
        float[] v = {0, 40, NULL_FLOAT, 50, 60, (float) -1, 0};
        Float[] V = {(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) -1, (float)0};

        assertEquals(std(new DbFloatArrayDirect(v)) / Math.sqrt(FloatPrimitives.count(new DbFloatArrayDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((float[])null));

        assertEquals(std(new DbFloatArrayDirect(v)) / Math.sqrt(FloatPrimitives.count(new DbFloatArrayDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Float[])null));

        assertEquals(std(new DbFloatArrayDirect(v)) / Math.sqrt(FloatPrimitives.count(new DbFloatArrayDirect(v))), ste(new DbFloatArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((DbFloatArrayDirect)null));
    }

    public void testTstat() {
        float[] v = {0, 40, NULL_FLOAT, 50, 60, (float) -1, 0};
        Float[] V = {(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) -1, (float)0};

        assertEquals(avg(new DbFloatArrayDirect(v)) / ste(new DbFloatArrayDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((float[])null));

        assertEquals(avg(new DbFloatArrayDirect(v)) / ste(new DbFloatArrayDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Float[])null));

        assertEquals(avg(new DbFloatArrayDirect(v)) / ste(new DbFloatArrayDirect(v)), tstat(new DbFloatArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((DbFloatArrayDirect)null));
    }

    public void testCov() {
        float[] a = {10, 40, NULL_FLOAT, 50, NULL_FLOAT, (float) -1, 0, (float) -7};
        float[] b = {0, (float) -40, NULL_FLOAT, NULL_FLOAT, 6, (float) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (float[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((float[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((float[])null, (float[]) null));

        assertEquals(cov, cov(a, new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (DbFloatArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((float[])null, new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((float[])null, (DbFloatArrayDirect)null));

        assertEquals(cov, cov(new DbFloatArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbFloatArrayDirect(a), (float[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbFloatArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbFloatArrayDirect)null, (float[])null));

        assertEquals(cov, cov(new DbFloatArrayDirect(a), new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbFloatArrayDirect(a), (DbFloatArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbFloatArrayDirect)null, new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbFloatArrayDirect)null, (DbFloatArrayDirect)null));
    }

    public void testCor() {
        float[] a = {10, 40, NULL_FLOAT, 50, NULL_FLOAT, (float) -1, 0, (float) -7};
        float[] b = {0, (float) -40, NULL_FLOAT, NULL_FLOAT, 6, (float) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumsqA = a[0] * a[0] + a[1] * a[1] + a[5] * a[5] + a[6] * a[6] + a[7] * a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumsqB = b[0] * b[0] + b[1] * b[1] + b[5] * b[5] + b[6] * b[6] + b[7] * b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;
        double varA = sumsqA / count - sumA * sumA / count / count;
        double varB = sumsqB / count - sumB * sumB / count / count;
        double cor = cov / Math.sqrt(varA * varB);

        assertEquals(cor, cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (float[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((float[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((float[])null, (float[])null));

        assertEquals(cor, cor(a, new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (DbFloatArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((float[])null, new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((float[])null, (DbFloatArrayDirect)null));

        assertEquals(cor, cor(new DbFloatArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbFloatArrayDirect(a), (float[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbFloatArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbFloatArrayDirect)null, (float[])null));

        assertEquals(cor, cor(new DbFloatArrayDirect(a), new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbFloatArrayDirect(a), (DbFloatArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbFloatArrayDirect)null, new DbFloatArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbFloatArrayDirect)null, (DbFloatArrayDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new DbFloatArrayDirect(new float[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbFloatArrayDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbFloatArrayDirect(NULL_FLOAT))) == 0.0);
        assertTrue(Math.abs(20 - sum(new DbFloatArrayDirect(new float[]{5, NULL_FLOAT, 15}))) == 0.0);
        assertEquals(NULL_FLOAT, sum((DbFloatArray) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new float[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new float[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new float[]{NULL_FLOAT})) == 0.0);
        assertTrue(Math.abs(20 - sum(new float[]{5, NULL_FLOAT, 15})) == 0.0);
        assertEquals(NULL_FLOAT, sum((float[]) null));
    }

    public void testSumDbArray() {
        assertEquals(new float[]{4, 15}, sum(new DbArrayDirect<>(new float[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new float[]{4, NULL_FLOAT}, sum(new DbArrayDirect<>(new float[][]{{5, NULL_FLOAT}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((DbArray<float[]>) null));

        try {
            sum(new DbArrayDirect<>(new float[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new float[]{4, 15}, sum(new float[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new float[]{4, NULL_FLOAT}, sum(new float[][]{{5, NULL_FLOAT}, {-3, 5}, {2, 6}}));
        assertEquals(null, sum((float[][]) null));

        try {
            sum(new float[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - product(new float[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_FLOAT, product(new float[]{}));
        assertEquals(NULL_FLOAT, product(new float[]{NULL_FLOAT}));
        assertTrue(Math.abs(75 - product(new float[]{5, NULL_FLOAT, 15})) == 0.0);
        assertEquals(NULL_FLOAT, product((float[]) null));

        assertTrue(Math.abs(120 - product(new DbFloatArrayDirect(new float[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_FLOAT, product(new DbFloatArrayDirect()));
        assertEquals(NULL_FLOAT, product(new DbFloatArrayDirect(NULL_FLOAT)));
        assertTrue(Math.abs(75 - product(new DbFloatArrayDirect(new float[]{5, NULL_FLOAT, 15}))) == 0.0);
        assertEquals(NULL_FLOAT, product((DbFloatArray) null));
    }

//    public void testProdDbArray() {
//        assertEquals(new float[]{-30, 120}, product(new DbArrayDirect<>(new float[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new float[]{-30, NULL_FLOAT}, product(new DbArrayDirect<>(new float[][]{{5, NULL_FLOAT}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((DbArray<float[]>) null));
//
//        try {
//            product(new DbArrayDirect<>(new float[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void testProdArray() {
//        assertEquals(new float[]{-30, 120}, product(new float[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new float[]{-30, NULL_FLOAT}, product(new float[][]{{5, NULL_FLOAT}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((float[][]) null));
//
//        try {
//            product(new float[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void testCumSumArray() {
        assertEquals(new float[]{1, 3, 6, 10, 15}, cumsum(new float[]{1, 2, 3, 4, 5}));
        assertEquals(new float[]{1, 3, 6, 6, 11}, cumsum(new float[]{1, 2, 3, NULL_FLOAT, 5}));
        assertEquals(new float[]{NULL_FLOAT, 2, 5, 9, 14}, cumsum(new float[]{NULL_FLOAT, 2, 3, 4, 5}));
        assertEquals(new float[0], cumsum());
        assertEquals(null, cumsum((float[]) null));

        assertEquals(new float[]{1, 3, 6, 10, 15}, cumsum(new DbFloatArrayDirect(new float[]{1, 2, 3, 4, 5})));
        assertEquals(new float[]{1, 3, 6, 6, 11}, cumsum(new DbFloatArrayDirect(new float[]{1, 2, 3, NULL_FLOAT, 5})));
        assertEquals(new float[]{NULL_FLOAT, 2, 5, 9, 14}, cumsum(new DbFloatArrayDirect(new float[]{NULL_FLOAT, 2, 3, 4, 5})));
        assertEquals(new float[0], cumsum(new DbFloatArrayDirect()));
        assertEquals(null, cumsum((DbFloatArray) null));
    }

    public void testCumProdArray() {
        assertEquals(new float[]{1, 2, 6, 24, 120}, cumprod(new float[]{1, 2, 3, 4, 5}));
        assertEquals(new float[]{1, 2, 6, 6, 30}, cumprod(new float[]{1, 2, 3, NULL_FLOAT, 5}));
        assertEquals(new float[]{NULL_FLOAT, 2, 6, 24, 120}, cumprod(new float[]{NULL_FLOAT, 2, 3, 4, 5}));
        assertEquals(new float[0], cumprod());
        assertEquals(null, cumprod((float[]) null));

        assertEquals(new float[]{1, 2, 6, 24, 120}, cumprod(new DbFloatArrayDirect(new float[]{1, 2, 3, 4, 5})));
        assertEquals(new float[]{1, 2, 6, 6, 30}, cumprod(new DbFloatArrayDirect(new float[]{1, 2, 3, NULL_FLOAT, 5})));
        assertEquals(new float[]{NULL_FLOAT, 2, 6, 24, 120}, cumprod(new DbFloatArrayDirect(new float[]{NULL_FLOAT, 2, 3, 4, 5})));
        assertEquals(new float[0], cumprod(new DbFloatArrayDirect()));
        assertEquals(null, cumprod((DbFloatArray) null));
    }

    public void testAbs() {
        float value = -5;
        assertEquals((float) Math.abs(value), abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_FLOAT, abs(NULL_FLOAT), 1e-10);
    }

    public void testAcos() {
        float value = -5;
        assertEquals(Math.acos(value), acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, acos(NULL_FLOAT), 1e-10);
    }

    public void testAsin() {
        float value = -5;
        assertEquals(Math.asin(value), asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, asin(NULL_FLOAT), 1e-10);
    }

    public void testAtan() {
        float value = -5;
        assertEquals(Math.atan(value), atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, atan(NULL_FLOAT), 1e-10);
    }

    public void testCeil() {
        float value = -5;
        assertEquals(Math.ceil(value), ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ceil(NULL_FLOAT), 1e-10);
    }

    public void testCos() {
        float value = -5;
        assertEquals(Math.cos(value), cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, cos(NULL_FLOAT), 1e-10);
    }

    public void testExp() {
        float value = -5;
        assertEquals(Math.exp(value), exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, exp(NULL_FLOAT), 1e-10);
    }

    public void testFloor() {
        float value = -5;
        assertEquals(Math.floor(value), floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, floor(NULL_FLOAT), 1e-10);
    }

    public void testLog() {
        float value = -5;
        assertEquals(Math.log(value), log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, log(NULL_FLOAT), 1e-10);
    }

    public void testPow() {
        float value0 = -5;
        float value1 = 2;
        assertEquals(Math.pow(value0, value1), pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(NULL_FLOAT, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(value0, NULL_FLOAT), 1e-10);
    }

    public void testRint() {
        float value = -5;
        assertEquals(Math.rint(value), rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, rint(NULL_FLOAT), 1e-10);
    }

    public void testRound() {
        float value = -5;
        assertEquals(Math.round(value), round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, round(NULL_FLOAT), 1e-10);
    }

    public void testSin() {
        float value = -5;
        assertEquals(Math.sin(value), sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sin(NULL_FLOAT), 1e-10);
    }

    public void testSqrt() {
        float value = -5;
        assertEquals(Math.sqrt(value), sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sqrt(NULL_FLOAT), 1e-10);
    }

    public void testTan() {
        float value = -5;
        assertEquals(Math.tan(value), tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, tan(NULL_FLOAT), 1e-10);
    }

    public void testLowerBin() {
        float value = (float) 114;

        assertEquals((float) 110, lowerBin(value, (float) 5));
        assertEquals((float) 110, lowerBin(value, (float) 10));
        assertEquals((float) 100, lowerBin(value, (float) 20));
        assertEquals(NULL_FLOAT, lowerBin(NULL_FLOAT, (float) 5));
        assertEquals(NULL_FLOAT, lowerBin(value, NULL_FLOAT));

        assertEquals(lowerBin(value, (float) 5), lowerBin(lowerBin(value, (float) 5), (float) 5));
    }

    public void testLowerBinWithOffset() {
        float value = (float) 114;
        float offset = (float) 3;

        assertEquals((float) 113, lowerBin(value, (float) 5, offset));
        assertEquals((float) 113, lowerBin(value, (float) 10, offset));
        assertEquals((float) 103, lowerBin(value, (float) 20, offset));
        assertEquals(NULL_FLOAT, lowerBin(NULL_FLOAT, (float) 5, offset));
        assertEquals(NULL_FLOAT, lowerBin(value, NULL_FLOAT, offset));

        assertEquals(lowerBin(value, (float) 5, offset), lowerBin(lowerBin(value, (float) 5, offset), (float) 5, offset));
    }

    public void testUpperBin() {
        float value = (float) 114;

        assertEquals((float) 115, upperBin(value, (float) 5));
        assertEquals((float) 120, upperBin(value, (float) 10));
        assertEquals((float) 120, upperBin(value, (float) 20));
        assertEquals(NULL_FLOAT, upperBin(NULL_FLOAT, (float) 5));
        assertEquals(NULL_FLOAT, upperBin(value, NULL_FLOAT));

        assertEquals(upperBin(value, (float) 5), upperBin(upperBin(value, (float) 5), (float) 5));
    }

    public void testUpperBinWithOffset() {
        float value = (float) 114;
        float offset = (float) 3;

        assertEquals((float) 118, upperBin(value, (float) 5, offset));
        assertEquals((float) 123, upperBin(value, (float) 10, offset));
        assertEquals((float) 123, upperBin(value, (float) 20, offset));
        assertEquals(NULL_FLOAT, upperBin(NULL_FLOAT, (float) 5, offset));
        assertEquals(NULL_FLOAT, upperBin(value, NULL_FLOAT, offset));

        assertEquals(upperBin(value, (float) 5, offset), upperBin(upperBin(value, (float) 5, offset), (float) 5, offset));
    }

    public void testClamp() {
        assertEquals((float) 3, clamp((float) 3, (float) -6, (float) 5));
        assertEquals((float) -6, clamp((float) -7, (float) -6, (float) 5));
        assertEquals((float) 5, clamp((float) 7, (float) -6, (float) 5));
        assertEquals(NULL_FLOAT, clamp(NULL_FLOAT, (float) -6, (float) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, binSearchIndex((float[]) null, (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new float[]{1,3,4}, (float)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new float[]{1,3,4}, (float)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new float[]{1,3,4}, (float)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new float[]{1,3,4}, (float)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new float[]{1,3,4}, (float)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new float[]{1,3,4}, (float)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((DbFloatArray) null, (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new DbFloatArrayDirect(new float[]{1,3,4}), (float)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbFloatArrayDirect(new float[]{1,3,4}), (float)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbFloatArrayDirect(new float[]{1,3,4}), (float)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new DbFloatArrayDirect(new float[]{1,3,4}), (float)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbFloatArrayDirect(new float[]{1,3,4}), (float)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbFloatArrayDirect(new float[]{1,3,4}), (float)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((DbFloatArray)null, (float) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((DbFloatArray)null, (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((DbFloatArray)null, (float) 0, BinSearch.BS_LOWEST));

        float[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(empty), (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(empty), (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(empty), (float) 0, BinSearch.BS_LOWEST));

        float[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbFloatArrayDirect(one), (float) 11, BinSearch.BS_LOWEST));


        float[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((DbFloatArray)null, (float) 0, null);

        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new DbFloatArrayDirect(v), (float) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, rawBinSearchIndex((float[]) null, (float) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((float[])null, (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((float[])null, (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(empty, (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(empty, (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(empty, (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(one, (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(one, (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(one, (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(one, (float) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(one, (float) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(one, (float) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(one, (float) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(one, (float) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(one, (float) 11, BinSearch.BS_LOWEST));


        rawBinSearchIndex((float[])null, (float) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (float) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (float) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (float) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (float) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (float) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (float) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (float) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (float) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (float) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (float) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (float) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (float) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (float) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (float) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (float) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (float) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (float) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (float) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (float) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (float) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (float) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (float) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (float) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (float) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (float) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (float) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final float[] floats = new float[]{1, -5, -2, -2, 96, 0, 12, NULL_FLOAT, NULL_FLOAT};
        final DbFloatArray sort = sort(new DbFloatArrayDirect(floats));
        final DbFloatArray expected = new DbFloatArrayDirect(new float[]{NULL_FLOAT, NULL_FLOAT, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        float[] sortedArray = sort(floats);
        assertEquals(new float[]{NULL_FLOAT, NULL_FLOAT, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((DbFloatArray)null));
        assertNull(sort((float[])null));
        assertNull(sort((Float[])null));
        assertEquals(new DbFloatArrayDirect(), sort(new DbFloatArrayDirect()));
        assertEquals(new float[]{}, sort(new float[]{}));
        assertEquals(new float[]{}, sort(new Float[]{}));
    }

    public void testSortDescending() {
        final float[] floats = new float[]{1, -5, -2, -2, 96, 0, 12, NULL_FLOAT, NULL_FLOAT};
        final DbFloatArray sort = sortDescending(new DbFloatArrayDirect(floats));
        final DbFloatArray expected = new DbFloatArrayDirect(new float[]{96, 12, 1, 0, -2, -2, -5, NULL_FLOAT, NULL_FLOAT});
        assertEquals(expected, sort);

        float[] sortedArray = sortDescending(floats);
        assertEquals(new float[]{96, 12, 1, 0, -2, -2, -5, NULL_FLOAT, NULL_FLOAT}, sortedArray);

        assertNull(sortDescending((DbFloatArray)null));
        assertNull(sortDescending((float[])null));
        assertNull(sortDescending((Float[])null));
        assertEquals(new DbFloatArrayDirect(), sortDescending(new DbFloatArrayDirect()));
        assertEquals(new float[]{}, sortDescending(new float[]{}));
        assertEquals(new float[]{}, sortDescending(new Float[]{}));
    }

    public void testSortsExceptions() {
        DbFloatArray dbFloatArray = null;
        DbFloatArray sort = sort(dbFloatArray);
        assertNull(sort);

        float[] floats = null;
        float[] sortArray = sort(floats);
        assertNull(sortArray);

        floats = new float[]{};
        sort = sort(new DbFloatArrayDirect(floats));
        assertEquals(new DbFloatArrayDirect(), sort);

        sortArray = sort(floats);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DbFloatArray dbFloatArray = null;
        DbFloatArray sort = sortDescending(dbFloatArray);
        assertNull(sort);

        float[] floats = null;
        float[] sortArray = sortDescending(floats);
        assertNull(sortArray);

        floats = new float[]{};
        sort = sortDescending(new DbFloatArrayDirect(floats));
        assertEquals(new DbFloatArrayDirect(), sort);

        sortArray = sortDescending(floats);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSequence(){
        assertEquals(new float[]{0,1,2,3,4,5}, FloatNumericPrimitives.sequence((float)0, (float)5, (float)1));
        assertEquals(new float[]{-5,-4,-3,-2,-1,0}, FloatNumericPrimitives.sequence((float)-5, (float)0, (float)1));

        assertEquals(new float[]{0,2,4}, FloatNumericPrimitives.sequence((float)0, (float)5, (float)2));
        assertEquals(new float[]{-5,-3,-1}, FloatNumericPrimitives.sequence((float)-5, (float)0, (float)2));

        assertEquals(new float[]{5,3,1}, FloatNumericPrimitives.sequence((float)5, (float)0, (float)-2));
        assertEquals(new float[]{0,-2,-4}, FloatNumericPrimitives.sequence((float)0, (float)-5, (float)-2));

        assertEquals(new float[]{}, FloatNumericPrimitives.sequence((float)0, (float)5, (float)0));
        assertEquals(new float[]{}, FloatNumericPrimitives.sequence((float)5, (float)0, (float)1));
    }

    public void testMedian() {
        assertEquals(3.0, median(new float[]{4,2,3}));
        assertEquals(3.5, median(new float[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((float[])null));

        assertEquals(3.0, median(new Float[]{(float)4,(float)2,(float)3}));
        assertEquals(3.5, median(new Float[]{(float)5,(float)4,(float)2,(float)3}));
        assertEquals(NULL_DOUBLE, median((Float[])null));

        assertEquals(3.0, median(new DbFloatArrayDirect(new float[]{4,2,3})));
        assertEquals(3.5, median(new DbFloatArrayDirect(new float[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((DbFloatArray) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(new float[]{4,2,3},0.00));
        assertEquals(3.0, percentile(new float[]{4,2,3},0.50));
        assertEquals(NULL_DOUBLE, percentile((float[])null, 0.25));

        assertEquals(2.0, percentile(new DbFloatArrayDirect(new float[]{4,2,3}),0.00));
        assertEquals(3.0, percentile(new DbFloatArrayDirect(new float[]{4,2,3}),0.50));
        assertEquals(NULL_DOUBLE, percentile((DbFloatArray) null, 0.25));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (short[])null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (int[])null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (long[])null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (double[])null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wvar(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (short[])null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (int[])null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (long[])null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (double[])null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wstd(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (short[])null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (int[])null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (long[])null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (double[])null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wste(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWtstat() {
        final double target = wavg(new float[]{1,2,3}, new float[]{4,5,6}) / wste(new float[]{1,2,3}, new float[]{4,5,6});

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (short[])null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (int[])null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (long[])null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (double[])null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wtstat(new DbFloatArrayDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((DbFloatArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbFloatArrayDirect(new float[]{1,2,3}), (DbFloatArray)null));
    }
}
