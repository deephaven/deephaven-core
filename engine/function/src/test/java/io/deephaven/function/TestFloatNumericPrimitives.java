/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.vector.*;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.function.FloatNumericPrimitives.*;
import static io.deephaven.function.FloatPrimitives.count;
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

        assertEquals(50.0, avg(new FloatVectorDirect(new float[]{40, 50, 60})));
        assertEquals(45.5, avg(new FloatVectorDirect(new float[]{40, 51})));
        assertTrue(Double.isNaN(avg(new FloatVectorDirect())));
        assertTrue(Double.isNaN(avg(new FloatVectorDirect(NULL_FLOAT))));
        assertEquals(10.0, avg(new FloatVectorDirect(new float[]{5, NULL_FLOAT, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((FloatVectorDirect)null));
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

        assertEquals(50.0, absAvg(new FloatVectorDirect(new float[]{40, (float) 50, 60})));
        assertEquals(45.5, absAvg(new FloatVectorDirect(new float[]{(float) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new FloatVectorDirect())));
        assertTrue(Double.isNaN(absAvg(new FloatVectorDirect(NULL_FLOAT))));
        assertEquals(10.0, absAvg(new FloatVectorDirect((float) 5, NULL_FLOAT, (float) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((FloatVectorDirect)null));
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

        assertEquals(4, countPos(new FloatVectorDirect(new float[]{40, 50, 60, (float) 1, 0})));
        assertEquals(0, countPos(new FloatVectorDirect()));
        assertEquals(0, countPos(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(3, countPos(new FloatVectorDirect(new float[]{5, NULL_FLOAT, 15, (float) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((FloatVectorDirect)null));
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

        assertEquals(2, countNeg(new FloatVectorDirect(new float[]{40, (float) -50, 60, (float) -1, 0})));
        assertEquals(0, countNeg(new FloatVectorDirect()));
        assertEquals(0, countNeg(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(1, countNeg(new FloatVectorDirect(new float[]{5, NULL_FLOAT, 15, (float) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((FloatVectorDirect)null));
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

        assertEquals(2, countZero(new FloatVectorDirect(new float[]{0, 40, 50, 60, (float) -1, 0})));
        assertEquals(0, countZero(new FloatVectorDirect()));
        assertEquals(0, countZero(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(2, countZero(new FloatVectorDirect(new float[]{0, 5, NULL_FLOAT, 0, (float) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((FloatVectorDirect)null));
    }

    public void testMax() {
        assertEquals((float) 60, max(new FloatVectorDirect(new float[]{0, 40, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals((float) 60, max(new FloatVectorDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1)));
        assertEquals(NULL_FLOAT, max(new FloatVectorDirect()));
        assertEquals(NULL_FLOAT, max(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(NULL_FLOAT, max((FloatVector) null));

        assertEquals((float) 60, max((float) 0, (float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1, (float) 0));
        assertEquals((float) 60, max((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1));
        assertEquals(NULL_FLOAT, max());
        assertEquals(NULL_FLOAT, max(NULL_FLOAT));
        assertEquals(NULL_FLOAT, max((float[]) null));
        assertEquals(NULL_FLOAT, max((Float[]) null));
    }

    public void testMin() {
        assertEquals((float) 0, min(new FloatVectorDirect(new float[]{0, 40, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals((float) -1, min(new FloatVectorDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1)));
        assertEquals(NULL_FLOAT, min(new FloatVectorDirect()));
        assertEquals(NULL_FLOAT, min(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(NULL_FLOAT, min((FloatVector) null));

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

        assertEquals(1, firstIndexOf(new FloatVectorDirect(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}), (float)40));
        assertEquals(4, firstIndexOf(new FloatVectorDirect(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}), (float)60));
        assertEquals(NULL_INT, firstIndexOf(new FloatVectorDirect(new float[]{0, 40, NULL_FLOAT, 40, 60, 40, 0}), (float)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((FloatVector) null, (float)40));
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

        assertEquals(4, indexOfMax(new FloatVectorDirect(new float[]{0, 40, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals(3, indexOfMax(new FloatVectorDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) 1)));
        assertEquals(-1, indexOfMax(new FloatVectorDirect()));
        assertEquals(-1, indexOfMax(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((FloatVectorDirect)null));
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

        assertEquals(1, indexOfMin(new FloatVectorDirect(new float[]{40, 0, NULL_FLOAT, 50, 60, (float) 1, 0})));
        assertEquals(4, indexOfMin(new FloatVectorDirect((float) 40, NULL_FLOAT, (float) 50, (float) 60, (float) -1)));
        assertEquals(-1, indexOfMin(new FloatVectorDirect()));
        assertEquals(-1, indexOfMin(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((FloatVectorDirect)null));
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

        assertEquals(var, var(new FloatVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((FloatVectorDirect)null));
    }

    public void testStd() {
        float[] v = {0, 40, NULL_FLOAT, 50, 60, (float) -1, 0};
        Float[] V = {(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) -1, (float)0};

        assertEquals(Math.sqrt(var(new FloatVectorDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((float[])null));

        assertEquals(Math.sqrt(var(new FloatVectorDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Float[])null));

        assertEquals(Math.sqrt(var(new FloatVectorDirect(v))), std(new FloatVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((FloatVectorDirect)null));
    }

    public void testSte() {
        float[] v = {0, 40, NULL_FLOAT, 50, 60, (float) -1, 0};
        Float[] V = {(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) -1, (float)0};

        assertEquals(std(new FloatVectorDirect(v)) / Math.sqrt(FloatPrimitives.count(new FloatVectorDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((float[])null));

        assertEquals(std(new FloatVectorDirect(v)) / Math.sqrt(FloatPrimitives.count(new FloatVectorDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Float[])null));

        assertEquals(std(new FloatVectorDirect(v)) / Math.sqrt(FloatPrimitives.count(new FloatVectorDirect(v))), ste(new FloatVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((FloatVectorDirect)null));
    }

    public void testTstat() {
        float[] v = {0, 40, NULL_FLOAT, 50, 60, (float) -1, 0};
        Float[] V = {(float)0, (float)40, NULL_FLOAT, (float)50, (float)60, (float) -1, (float)0};

        assertEquals(avg(new FloatVectorDirect(v)) / ste(new FloatVectorDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((float[])null));

        assertEquals(avg(new FloatVectorDirect(v)) / ste(new FloatVectorDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Float[])null));

        assertEquals(avg(new FloatVectorDirect(v)) / ste(new FloatVectorDirect(v)), tstat(new FloatVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((FloatVectorDirect)null));
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

        assertEquals(cov, cov(a, new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (FloatVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((float[])null, new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((float[])null, (FloatVectorDirect)null));

        assertEquals(cov, cov(new FloatVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new FloatVectorDirect(a), (float[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((FloatVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((FloatVectorDirect)null, (float[])null));

        assertEquals(cov, cov(new FloatVectorDirect(a), new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new FloatVectorDirect(a), (FloatVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((FloatVectorDirect)null, new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((FloatVectorDirect)null, (FloatVectorDirect)null));
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

        assertEquals(cor, cor(a, new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (FloatVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((float[])null, new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((float[])null, (FloatVectorDirect)null));

        assertEquals(cor, cor(new FloatVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new FloatVectorDirect(a), (float[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((FloatVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((FloatVectorDirect)null, (float[])null));

        assertEquals(cor, cor(new FloatVectorDirect(a), new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new FloatVectorDirect(a), (FloatVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((FloatVectorDirect)null, new FloatVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((FloatVectorDirect)null, (FloatVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new FloatVectorDirect(new float[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new FloatVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new FloatVectorDirect(NULL_FLOAT))) == 0.0);
        assertTrue(Math.abs(20 - sum(new FloatVectorDirect(new float[]{5, NULL_FLOAT, 15}))) == 0.0);
        assertEquals(NULL_FLOAT, sum((FloatVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new float[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new float[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new float[]{NULL_FLOAT})) == 0.0);
        assertTrue(Math.abs(20 - sum(new float[]{5, NULL_FLOAT, 15})) == 0.0);
        assertEquals(NULL_FLOAT, sum((float[]) null));
    }

    public void testSumVector() {
        assertEquals(new float[]{4, 15}, sum(new ObjectVectorDirect<>(new float[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new float[]{4, NULL_FLOAT}, sum(new ObjectVectorDirect<>(new float[][]{{5, NULL_FLOAT}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((ObjectVector<float[]>) null));

        try {
            sum(new ObjectVectorDirect<>(new float[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertTrue(Math.abs(120 - product(new FloatVectorDirect(new float[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_FLOAT, product(new FloatVectorDirect()));
        assertEquals(NULL_FLOAT, product(new FloatVectorDirect(NULL_FLOAT)));
        assertTrue(Math.abs(75 - product(new FloatVectorDirect(new float[]{5, NULL_FLOAT, 15}))) == 0.0);
        assertEquals(NULL_FLOAT, product((FloatVector) null));
    }

//    public void testProdVector() {
//        assertEquals(new float[]{-30, 120}, product(new ObjectVectorDirect<>(new float[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new float[]{-30, NULL_FLOAT}, product(new ObjectVectorDirect<>(new float[][]{{5, NULL_FLOAT}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((Vector<float[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new float[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertEquals(new float[]{1, 3, 6, 10, 15}, cumsum(new FloatVectorDirect(new float[]{1, 2, 3, 4, 5})));
        assertEquals(new float[]{1, 3, 6, 6, 11}, cumsum(new FloatVectorDirect(new float[]{1, 2, 3, NULL_FLOAT, 5})));
        assertEquals(new float[]{NULL_FLOAT, 2, 5, 9, 14}, cumsum(new FloatVectorDirect(new float[]{NULL_FLOAT, 2, 3, 4, 5})));
        assertEquals(new float[0], cumsum(new FloatVectorDirect()));
        assertEquals(null, cumsum((FloatVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new float[]{1, 2, 6, 24, 120}, cumprod(new float[]{1, 2, 3, 4, 5}));
        assertEquals(new float[]{1, 2, 6, 6, 30}, cumprod(new float[]{1, 2, 3, NULL_FLOAT, 5}));
        assertEquals(new float[]{NULL_FLOAT, 2, 6, 24, 120}, cumprod(new float[]{NULL_FLOAT, 2, 3, 4, 5}));
        assertEquals(new float[0], cumprod());
        assertEquals(null, cumprod((float[]) null));

        assertEquals(new float[]{1, 2, 6, 24, 120}, cumprod(new FloatVectorDirect(new float[]{1, 2, 3, 4, 5})));
        assertEquals(new float[]{1, 2, 6, 6, 30}, cumprod(new FloatVectorDirect(new float[]{1, 2, 3, NULL_FLOAT, 5})));
        assertEquals(new float[]{NULL_FLOAT, 2, 6, 24, 120}, cumprod(new FloatVectorDirect(new float[]{NULL_FLOAT, 2, 3, 4, 5})));
        assertEquals(new float[0], cumprod(new FloatVectorDirect()));
        assertEquals(null, cumprod((FloatVector) null));
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

        assertEquals(NULL_INT, binSearchIndex((FloatVector) null, (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new FloatVectorDirect(new float[]{1,3,4}), (float)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new FloatVectorDirect(new float[]{1,3,4}), (float)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new FloatVectorDirect(new float[]{1,3,4}), (float)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new FloatVectorDirect(new float[]{1,3,4}), (float)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new FloatVectorDirect(new float[]{1,3,4}), (float)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new FloatVectorDirect(new float[]{1,3,4}), (float)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((FloatVector)null, (float) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((FloatVector)null, (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((FloatVector)null, (float) 0, BinSearch.BS_LOWEST));

        float[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(empty), (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(empty), (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(empty), (float) 0, BinSearch.BS_LOWEST));

        float[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(one), (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(one), (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(one), (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new FloatVectorDirect(one), (float) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new FloatVectorDirect(one), (float) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new FloatVectorDirect(one), (float) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new FloatVectorDirect(one), (float) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new FloatVectorDirect(one), (float) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new FloatVectorDirect(one), (float) 11, BinSearch.BS_LOWEST));


        float[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((FloatVector)null, (float) 0, null);

        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(v), (float) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(v), (float) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new FloatVectorDirect(v), (float) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new FloatVectorDirect(v), (float) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new FloatVectorDirect(v), (float) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new FloatVectorDirect(v), (float) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new FloatVectorDirect(v), (float) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new FloatVectorDirect(v), (float) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new FloatVectorDirect(v), (float) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new FloatVectorDirect(v), (float) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new FloatVectorDirect(v), (float) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new FloatVectorDirect(v), (float) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new FloatVectorDirect(v), (float) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new FloatVectorDirect(v), (float) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new FloatVectorDirect(v), (float) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new FloatVectorDirect(v), (float) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new FloatVectorDirect(v), (float) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new FloatVectorDirect(v), (float) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new FloatVectorDirect(v), (float) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new FloatVectorDirect(v), (float) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new FloatVectorDirect(v), (float) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new FloatVectorDirect(v), (float) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new FloatVectorDirect(v), (float) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new FloatVectorDirect(v), (float) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new FloatVectorDirect(v), (float) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new FloatVectorDirect(v), (float) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new FloatVectorDirect(v), (float) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new FloatVectorDirect(v), (float) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new FloatVectorDirect(v), (float) 25, BinSearch.BS_LOWEST));

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
        final FloatVector sort = sort(new FloatVectorDirect(floats));
        final FloatVector expected = new FloatVectorDirect(new float[]{NULL_FLOAT, NULL_FLOAT, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        float[] sortedArray = sort(floats);
        assertEquals(new float[]{NULL_FLOAT, NULL_FLOAT, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((FloatVector)null));
        assertNull(sort((float[])null));
        assertNull(sort((Float[])null));
        assertEquals(new FloatVectorDirect(), sort(new FloatVectorDirect()));
        assertEquals(new float[]{}, sort(new float[]{}));
        assertEquals(new float[]{}, sort(new Float[]{}));
    }

    public void testSortDescending() {
        final float[] floats = new float[]{1, -5, -2, -2, 96, 0, 12, NULL_FLOAT, NULL_FLOAT};
        final FloatVector sort = sortDescending(new FloatVectorDirect(floats));
        final FloatVector expected = new FloatVectorDirect(new float[]{96, 12, 1, 0, -2, -2, -5, NULL_FLOAT, NULL_FLOAT});
        assertEquals(expected, sort);

        float[] sortedArray = sortDescending(floats);
        assertEquals(new float[]{96, 12, 1, 0, -2, -2, -5, NULL_FLOAT, NULL_FLOAT}, sortedArray);

        assertNull(sortDescending((FloatVector)null));
        assertNull(sortDescending((float[])null));
        assertNull(sortDescending((Float[])null));
        assertEquals(new FloatVectorDirect(), sortDescending(new FloatVectorDirect()));
        assertEquals(new float[]{}, sortDescending(new float[]{}));
        assertEquals(new float[]{}, sortDescending(new Float[]{}));
    }

    public void testSortsExceptions() {
        FloatVector floatVector = null;
        FloatVector sort = sort(floatVector);
        assertNull(sort);

        float[] floats = null;
        float[] sortArray = sort(floats);
        assertNull(sortArray);

        floats = new float[]{};
        sort = sort(new FloatVectorDirect(floats));
        assertEquals(new FloatVectorDirect(), sort);

        sortArray = sort(floats);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        FloatVector floatVector = null;
        FloatVector sort = sortDescending(floatVector);
        assertNull(sort);

        float[] floats = null;
        float[] sortArray = sortDescending(floats);
        assertNull(sortArray);

        floats = new float[]{};
        sort = sortDescending(new FloatVectorDirect(floats));
        assertEquals(new FloatVectorDirect(), sort);

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

        assertEquals(3.0, median(new FloatVectorDirect(new float[]{4,2,3})));
        assertEquals(3.5, median(new FloatVectorDirect(new float[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((FloatVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(new float[]{4,2,3},0.00));
        assertEquals(3.0, percentile(new float[]{4,2,3},0.50));
        assertEquals(NULL_DOUBLE, percentile((float[])null, 0.25));

        assertEquals(2.0, percentile(new FloatVectorDirect(new float[]{4,2,3}),0.00));
        assertEquals(3.0, percentile(new FloatVectorDirect(new float[]{4,2,3}),0.50));
        assertEquals(NULL_DOUBLE, percentile((FloatVector) null, 0.25));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (IntVector) null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (LongVector) null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wvar(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wvar(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (IntVector) null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (LongVector) null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wstd(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wstd(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (IntVector) null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (LongVector) null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wste(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wste(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (short[])null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (IntVector) null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (LongVector) null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wtstat(new float[]{1,2,3,NULL_FLOAT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((float[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new float[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (IntVector) null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (LongVector) null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wtstat(new FloatVectorDirect(new float[]{1,2,3,NULL_FLOAT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((FloatVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new FloatVectorDirect(new float[]{1,2,3}), (FloatVector)null));
    }
}
