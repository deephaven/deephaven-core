/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestFloatNumericPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.structures.vector.*;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.libs.primitives.DoubleNumericPrimitives.*;
import static io.deephaven.libs.primitives.DoublePrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestDoubleNumericPrimitives extends BaseArrayTestCase {

    public void testSignum() {
        assertEquals((double) 1, signum((double) 5));
        assertEquals((double) 0, signum((double) 0));
        assertEquals((double) -1, signum((double) -5));
        assertEquals(NULL_DOUBLE, signum(NULL_DOUBLE));
    }

    public void testAvg() {
        assertEquals(50.0, avg(new double[]{40, 50, 60}));
        assertEquals(45.5, avg(new double[]{40, 51}));
        assertTrue(Double.isNaN(avg(new double[]{})));
        assertTrue(Double.isNaN(avg(new double[]{NULL_DOUBLE})));
        assertEquals(10.0, avg(new double[]{5, NULL_DOUBLE, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((double[])null));

        assertEquals(50.0, avg(new Double[]{(double)40, (double)50, (double)60}));
        assertEquals(45.5, avg(new Double[]{(double)40, (double)51}));
        assertTrue(Double.isNaN(avg(new Double[]{})));
        assertTrue(Double.isNaN(avg(new Double[]{NULL_DOUBLE})));
        assertEquals(10.0, avg(new Double[]{(double)5, NULL_DOUBLE, (double)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((Double[])null));

        assertEquals(50.0, avg(new DbDoubleArrayDirect(new double[]{40, 50, 60})));
        assertEquals(45.5, avg(new DbDoubleArrayDirect(new double[]{40, 51})));
        assertTrue(Double.isNaN(avg(new DbDoubleArrayDirect())));
        assertTrue(Double.isNaN(avg(new DbDoubleArrayDirect(NULL_DOUBLE))));
        assertEquals(10.0, avg(new DbDoubleArrayDirect(new double[]{5, NULL_DOUBLE, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((DbDoubleArrayDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, absAvg(new double[]{40, (double) 50, 60}));
        assertEquals(45.5, absAvg(new double[]{(double) 40, 51}));
        assertTrue(Double.isNaN(absAvg(new double[]{})));
        assertTrue(Double.isNaN(absAvg(new double[]{NULL_DOUBLE})));
        assertEquals(10.0, absAvg(new double[]{(double) 5, NULL_DOUBLE, (double) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((double[])null));

        assertEquals(50.0, absAvg(new Double[]{(double)40, (double) 50, (double)60}));
        assertEquals(45.5, absAvg(new Double[]{(double) 40, (double)51}));
        assertTrue(Double.isNaN(absAvg(new Double[]{})));
        assertTrue(Double.isNaN(absAvg(new Double[]{NULL_DOUBLE})));
        assertEquals(10.0, absAvg(new Double[]{(double) 5, NULL_DOUBLE, (double) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((Double[])null));

        assertEquals(50.0, absAvg(new DbDoubleArrayDirect(new double[]{40, (double) 50, 60})));
        assertEquals(45.5, absAvg(new DbDoubleArrayDirect(new double[]{(double) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new DbDoubleArrayDirect())));
        assertTrue(Double.isNaN(absAvg(new DbDoubleArrayDirect(NULL_DOUBLE))));
        assertEquals(10.0, absAvg(new DbDoubleArrayDirect((double) 5, NULL_DOUBLE, (double) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((DbDoubleArrayDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, countPos(new double[]{40, 50, 60, (double) 1, 0}));
        assertEquals(0, countPos(new double[]{}));
        assertEquals(0, countPos(new double[]{NULL_DOUBLE}));
        assertEquals(3, countPos(new double[]{5, NULL_DOUBLE, 15, (double) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, countPos((double[])null));

        assertEquals(4, countPos(new Double[]{(double)40, (double)50, (double)60, (double) 1, (double)0}));
        assertEquals(0, countPos(new Double[]{}));
        assertEquals(0, countPos(new Double[]{NULL_DOUBLE}));
        assertEquals(3, countPos(new Double[]{(double)5, NULL_DOUBLE, (double)15, (double) 1, (double)0}));
        assertEquals(QueryConstants.NULL_INT, countPos((double[])null));

        assertEquals(4, countPos(new DbDoubleArrayDirect(new double[]{40, 50, 60, (double) 1, 0})));
        assertEquals(0, countPos(new DbDoubleArrayDirect()));
        assertEquals(0, countPos(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(3, countPos(new DbDoubleArrayDirect(new double[]{5, NULL_DOUBLE, 15, (double) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((DbDoubleArrayDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, countNeg(new double[]{40, (double) -50, 60, (double) -1, 0}));
        assertEquals(0, countNeg(new double[]{}));
        assertEquals(0, countNeg(new double[]{NULL_DOUBLE}));
        assertEquals(1, countNeg(new double[]{5, NULL_DOUBLE, 15, (double) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((double[])null));

        assertEquals(2, countNeg(new Double[]{(double)40, (double) -50, (double)60, (double) -1, (double)0}));
        assertEquals(0, countNeg(new Double[]{}));
        assertEquals(0, countNeg(new Double[]{NULL_DOUBLE}));
        assertEquals(1, countNeg(new Double[]{(double)5, NULL_DOUBLE, (double)15, (double) -1, (double)0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((Double[])null));

        assertEquals(2, countNeg(new DbDoubleArrayDirect(new double[]{40, (double) -50, 60, (double) -1, 0})));
        assertEquals(0, countNeg(new DbDoubleArrayDirect()));
        assertEquals(0, countNeg(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(1, countNeg(new DbDoubleArrayDirect(new double[]{5, NULL_DOUBLE, 15, (double) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((DbDoubleArrayDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, countZero(new double[]{0, 40, 50, 60, (double) -1, 0}));
        assertEquals(0, countZero(new double[]{}));
        assertEquals(0, countZero(new double[]{NULL_DOUBLE}));
        assertEquals(2, countZero(new double[]{0, 5, NULL_DOUBLE, 0, (double) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((double[])null));

        assertEquals(2, countZero(new Double[]{(double)0, (double)40, (double)50, (double)60, (double) -1, (double)0}));
        assertEquals(0, countZero(new Double[]{}));
        assertEquals(0, countZero(new Double[]{NULL_DOUBLE}));
        assertEquals(2, countZero(new Double[]{(double)0, (double)5, NULL_DOUBLE, (double)0, (double) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((Double[])null));

        assertEquals(2, countZero(new DbDoubleArrayDirect(new double[]{0, 40, 50, 60, (double) -1, 0})));
        assertEquals(0, countZero(new DbDoubleArrayDirect()));
        assertEquals(0, countZero(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(2, countZero(new DbDoubleArrayDirect(new double[]{0, 5, NULL_DOUBLE, 0, (double) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((DbDoubleArrayDirect)null));
    }

    public void testMax() {
        assertEquals((double) 60, max(new DbDoubleArrayDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals((double) 60, max(new DbDoubleArrayDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1)));
        assertEquals(NULL_DOUBLE, max(new DbDoubleArrayDirect()));
        assertEquals(NULL_DOUBLE, max(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(NULL_DOUBLE, max((DbDoubleArray) null));

        assertEquals((double) 60, max((double) 0, (double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1, (double) 0));
        assertEquals((double) 60, max((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1));
        assertEquals(NULL_DOUBLE, max());
        assertEquals(NULL_DOUBLE, max(NULL_DOUBLE));
        assertEquals(NULL_DOUBLE, max((double[]) null));
        assertEquals(NULL_DOUBLE, max((Double[]) null));
    }

    public void testMin() {
        assertEquals((double) 0, min(new DbDoubleArrayDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals((double) -1, min(new DbDoubleArrayDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1)));
        assertEquals(NULL_DOUBLE, min(new DbDoubleArrayDirect()));
        assertEquals(NULL_DOUBLE, min(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(NULL_DOUBLE, min((DbDoubleArray) null));

        assertEquals((double) 0, min((double) 0, (double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1, (double) 0));
        assertEquals((double) -1, min((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1));
        assertEquals(NULL_DOUBLE, min());
        assertEquals(NULL_DOUBLE, min(NULL_DOUBLE));
        assertEquals(NULL_DOUBLE, min((double[]) null));
        assertEquals(NULL_DOUBLE, min((Double[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, firstIndexOf(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}, (double)40));
        assertEquals(4, firstIndexOf(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}, (double)60));
        assertEquals(NULL_INT, firstIndexOf(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}, (double)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((double[])null, (double)40));

        assertEquals(1, firstIndexOf(new DbDoubleArrayDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)40));
        assertEquals(4, firstIndexOf(new DbDoubleArrayDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)60));
        assertEquals(NULL_INT, firstIndexOf(new DbDoubleArrayDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((DbDoubleArray) null, (double)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, indexOfMax(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0}));
        assertEquals(3, indexOfMax(new double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1}));
        assertEquals(-1, indexOfMax(new double[]{}));
        assertEquals(-1, indexOfMax(new double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((double[])null));

        assertEquals(4, indexOfMax(new Double[]{(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) 1, (double)0}));
        assertEquals(3, indexOfMax(new Double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1}));
        assertEquals(-1, indexOfMax(new Double[]{}));
        assertEquals(-1, indexOfMax(new Double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((Double[])null));

        assertEquals(4, indexOfMax(new DbDoubleArrayDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals(3, indexOfMax(new DbDoubleArrayDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1)));
        assertEquals(-1, indexOfMax(new DbDoubleArrayDirect()));
        assertEquals(-1, indexOfMax(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((DbDoubleArrayDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, indexOfMin(new double[]{40, 0, NULL_DOUBLE, 50, 60, (double) 1, 0}));
        assertEquals(4, indexOfMin(new double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1}));
        assertEquals(-1, indexOfMin(new double[]{}));
        assertEquals(-1, indexOfMin(new double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((double[])null));

        assertEquals(1, indexOfMin(new Double[]{(double)40, (double)0, NULL_DOUBLE, (double)50, (double)60, (double) 1, (double)0}));
        assertEquals(4, indexOfMin(new Double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1}));
        assertEquals(-1, indexOfMin(new Double[]{}));
        assertEquals(-1, indexOfMin(new Double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((Double[])null));

        assertEquals(1, indexOfMin(new DbDoubleArrayDirect(new double[]{40, 0, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals(4, indexOfMin(new DbDoubleArrayDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1)));
        assertEquals(-1, indexOfMin(new DbDoubleArrayDirect()));
        assertEquals(-1, indexOfMin(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((DbDoubleArrayDirect)null));
    }


    public void testVar() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, var((double[])null));

        assertEquals(var, var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, var((Double[])null));

        assertEquals(var, var(new DbDoubleArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((DbDoubleArrayDirect)null));
    }

    public void testStd() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(Math.sqrt(var(new DbDoubleArrayDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((double[])null));

        assertEquals(Math.sqrt(var(new DbDoubleArrayDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Double[])null));

        assertEquals(Math.sqrt(var(new DbDoubleArrayDirect(v))), std(new DbDoubleArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((DbDoubleArrayDirect)null));
    }

    public void testSte() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(std(new DbDoubleArrayDirect(v)) / Math.sqrt(DoublePrimitives.count(new DbDoubleArrayDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((double[])null));

        assertEquals(std(new DbDoubleArrayDirect(v)) / Math.sqrt(DoublePrimitives.count(new DbDoubleArrayDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Double[])null));

        assertEquals(std(new DbDoubleArrayDirect(v)) / Math.sqrt(DoublePrimitives.count(new DbDoubleArrayDirect(v))), ste(new DbDoubleArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((DbDoubleArrayDirect)null));
    }

    public void testTstat() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(avg(new DbDoubleArrayDirect(v)) / ste(new DbDoubleArrayDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((double[])null));

        assertEquals(avg(new DbDoubleArrayDirect(v)) / ste(new DbDoubleArrayDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Double[])null));

        assertEquals(avg(new DbDoubleArrayDirect(v)) / ste(new DbDoubleArrayDirect(v)), tstat(new DbDoubleArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((DbDoubleArrayDirect)null));
    }

    public void testCov() {
        double[] a = {10, 40, NULL_DOUBLE, 50, NULL_DOUBLE, (double) -1, 0, (double) -7};
        double[] b = {0, (double) -40, NULL_DOUBLE, NULL_DOUBLE, 6, (double) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((double[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((double[])null, (double[]) null));

        assertEquals(cov, cov(a, new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (DbDoubleArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((double[])null, new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((double[])null, (DbDoubleArrayDirect)null));

        assertEquals(cov, cov(new DbDoubleArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbDoubleArrayDirect(a), (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbDoubleArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbDoubleArrayDirect)null, (double[])null));

        assertEquals(cov, cov(new DbDoubleArrayDirect(a), new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbDoubleArrayDirect(a), (DbDoubleArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbDoubleArrayDirect)null, new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbDoubleArrayDirect)null, (DbDoubleArrayDirect)null));
    }

    public void testCor() {
        double[] a = {10, 40, NULL_DOUBLE, 50, NULL_DOUBLE, (double) -1, 0, (double) -7};
        double[] b = {0, (double) -40, NULL_DOUBLE, NULL_DOUBLE, 6, (double) -1, 11, 3};
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
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((double[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((double[])null, (double[])null));

        assertEquals(cor, cor(a, new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (DbDoubleArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((double[])null, new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((double[])null, (DbDoubleArrayDirect)null));

        assertEquals(cor, cor(new DbDoubleArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbDoubleArrayDirect(a), (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbDoubleArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbDoubleArrayDirect)null, (double[])null));

        assertEquals(cor, cor(new DbDoubleArrayDirect(a), new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbDoubleArrayDirect(a), (DbDoubleArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbDoubleArrayDirect)null, new DbDoubleArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbDoubleArrayDirect)null, (DbDoubleArrayDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new DbDoubleArrayDirect(new double[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbDoubleArrayDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbDoubleArrayDirect(NULL_DOUBLE))) == 0.0);
        assertTrue(Math.abs(20 - sum(new DbDoubleArrayDirect(new double[]{5, NULL_DOUBLE, 15}))) == 0.0);
        assertEquals(NULL_DOUBLE, sum((DbDoubleArray) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new double[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new double[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new double[]{NULL_DOUBLE})) == 0.0);
        assertTrue(Math.abs(20 - sum(new double[]{5, NULL_DOUBLE, 15})) == 0.0);
        assertEquals(NULL_DOUBLE, sum((double[]) null));
    }

    public void testSumDbArray() {
        assertEquals(new double[]{4, 15}, sum(new DbArrayDirect<>(new double[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new double[]{4, NULL_DOUBLE}, sum(new DbArrayDirect<>(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((DbArray<double[]>) null));

        try {
            sum(new DbArrayDirect<>(new double[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new double[]{4, 15}, sum(new double[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new double[]{4, NULL_DOUBLE}, sum(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}}));
        assertEquals(null, sum((double[][]) null));

        try {
            sum(new double[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - product(new double[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_DOUBLE, product(new double[]{}));
        assertEquals(NULL_DOUBLE, product(new double[]{NULL_DOUBLE}));
        assertTrue(Math.abs(75 - product(new double[]{5, NULL_DOUBLE, 15})) == 0.0);
        assertEquals(NULL_DOUBLE, product((double[]) null));

        assertTrue(Math.abs(120 - product(new DbDoubleArrayDirect(new double[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_DOUBLE, product(new DbDoubleArrayDirect()));
        assertEquals(NULL_DOUBLE, product(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertTrue(Math.abs(75 - product(new DbDoubleArrayDirect(new double[]{5, NULL_DOUBLE, 15}))) == 0.0);
        assertEquals(NULL_DOUBLE, product((DbDoubleArray) null));
    }

//    public void testProdDbArray() {
//        assertEquals(new double[]{-30, 120}, product(new DbArrayDirect<>(new double[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new double[]{-30, NULL_DOUBLE}, product(new DbArrayDirect<>(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((DbArray<double[]>) null));
//
//        try {
//            product(new DbArrayDirect<>(new double[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void testProdArray() {
//        assertEquals(new double[]{-30, 120}, product(new double[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new double[]{-30, NULL_DOUBLE}, product(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((double[][]) null));
//
//        try {
//            product(new double[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void testCumSumArray() {
        assertEquals(new double[]{1, 3, 6, 10, 15}, cumsum(new double[]{1, 2, 3, 4, 5}));
        assertEquals(new double[]{1, 3, 6, 6, 11}, cumsum(new double[]{1, 2, 3, NULL_DOUBLE, 5}));
        assertEquals(new double[]{NULL_DOUBLE, 2, 5, 9, 14}, cumsum(new double[]{NULL_DOUBLE, 2, 3, 4, 5}));
        assertEquals(new double[0], cumsum());
        assertEquals(null, cumsum((double[]) null));

        assertEquals(new double[]{1, 3, 6, 10, 15}, cumsum(new DbDoubleArrayDirect(new double[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 3, 6, 6, 11}, cumsum(new DbDoubleArrayDirect(new double[]{1, 2, 3, NULL_DOUBLE, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 5, 9, 14}, cumsum(new DbDoubleArrayDirect(new double[]{NULL_DOUBLE, 2, 3, 4, 5})));
        assertEquals(new double[0], cumsum(new DbDoubleArrayDirect()));
        assertEquals(null, cumsum((DbDoubleArray) null));
    }

    public void testCumProdArray() {
        assertEquals(new double[]{1, 2, 6, 24, 120}, cumprod(new double[]{1, 2, 3, 4, 5}));
        assertEquals(new double[]{1, 2, 6, 6, 30}, cumprod(new double[]{1, 2, 3, NULL_DOUBLE, 5}));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new double[]{NULL_DOUBLE, 2, 3, 4, 5}));
        assertEquals(new double[0], cumprod());
        assertEquals(null, cumprod((double[]) null));

        assertEquals(new double[]{1, 2, 6, 24, 120}, cumprod(new DbDoubleArrayDirect(new double[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 2, 6, 6, 30}, cumprod(new DbDoubleArrayDirect(new double[]{1, 2, 3, NULL_DOUBLE, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new DbDoubleArrayDirect(new double[]{NULL_DOUBLE, 2, 3, 4, 5})));
        assertEquals(new double[0], cumprod(new DbDoubleArrayDirect()));
        assertEquals(null, cumprod((DbDoubleArray) null));
    }

    public void testAbs() {
        double value = -5;
        assertEquals((double) Math.abs(value), abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, abs(NULL_DOUBLE), 1e-10);
    }

    public void testAcos() {
        double value = -5;
        assertEquals(Math.acos(value), acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, acos(NULL_DOUBLE), 1e-10);
    }

    public void testAsin() {
        double value = -5;
        assertEquals(Math.asin(value), asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, asin(NULL_DOUBLE), 1e-10);
    }

    public void testAtan() {
        double value = -5;
        assertEquals(Math.atan(value), atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, atan(NULL_DOUBLE), 1e-10);
    }

    public void testCeil() {
        double value = -5;
        assertEquals(Math.ceil(value), ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ceil(NULL_DOUBLE), 1e-10);
    }

    public void testCos() {
        double value = -5;
        assertEquals(Math.cos(value), cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, cos(NULL_DOUBLE), 1e-10);
    }

    public void testExp() {
        double value = -5;
        assertEquals(Math.exp(value), exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, exp(NULL_DOUBLE), 1e-10);
    }

    public void testFloor() {
        double value = -5;
        assertEquals(Math.floor(value), floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, floor(NULL_DOUBLE), 1e-10);
    }

    public void testLog() {
        double value = -5;
        assertEquals(Math.log(value), log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, log(NULL_DOUBLE), 1e-10);
    }

    public void testPow() {
        double value0 = -5;
        double value1 = 2;
        assertEquals(Math.pow(value0, value1), pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(NULL_DOUBLE, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(value0, NULL_DOUBLE), 1e-10);
    }

    public void testRint() {
        double value = -5;
        assertEquals(Math.rint(value), rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, rint(NULL_DOUBLE), 1e-10);
    }

    public void testRound() {
        double value = -5;
        assertEquals(Math.round(value), round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, round(NULL_DOUBLE), 1e-10);
    }

    public void testSin() {
        double value = -5;
        assertEquals(Math.sin(value), sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sin(NULL_DOUBLE), 1e-10);
    }

    public void testSqrt() {
        double value = -5;
        assertEquals(Math.sqrt(value), sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sqrt(NULL_DOUBLE), 1e-10);
    }

    public void testTan() {
        double value = -5;
        assertEquals(Math.tan(value), tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, tan(NULL_DOUBLE), 1e-10);
    }

    public void testLowerBin() {
        double value = (double) 114;

        assertEquals((double) 110, lowerBin(value, (double) 5));
        assertEquals((double) 110, lowerBin(value, (double) 10));
        assertEquals((double) 100, lowerBin(value, (double) 20));
        assertEquals(NULL_DOUBLE, lowerBin(NULL_DOUBLE, (double) 5));
        assertEquals(NULL_DOUBLE, lowerBin(value, NULL_DOUBLE));

        assertEquals(lowerBin(value, (double) 5), lowerBin(lowerBin(value, (double) 5), (double) 5));
    }

    public void testLowerBinWithOffset() {
        double value = (double) 114;
        double offset = (double) 3;

        assertEquals((double) 113, lowerBin(value, (double) 5, offset));
        assertEquals((double) 113, lowerBin(value, (double) 10, offset));
        assertEquals((double) 103, lowerBin(value, (double) 20, offset));
        assertEquals(NULL_DOUBLE, lowerBin(NULL_DOUBLE, (double) 5, offset));
        assertEquals(NULL_DOUBLE, lowerBin(value, NULL_DOUBLE, offset));

        assertEquals(lowerBin(value, (double) 5, offset), lowerBin(lowerBin(value, (double) 5, offset), (double) 5, offset));
    }

    public void testUpperBin() {
        double value = (double) 114;

        assertEquals((double) 115, upperBin(value, (double) 5));
        assertEquals((double) 120, upperBin(value, (double) 10));
        assertEquals((double) 120, upperBin(value, (double) 20));
        assertEquals(NULL_DOUBLE, upperBin(NULL_DOUBLE, (double) 5));
        assertEquals(NULL_DOUBLE, upperBin(value, NULL_DOUBLE));

        assertEquals(upperBin(value, (double) 5), upperBin(upperBin(value, (double) 5), (double) 5));
    }

    public void testUpperBinWithOffset() {
        double value = (double) 114;
        double offset = (double) 3;

        assertEquals((double) 118, upperBin(value, (double) 5, offset));
        assertEquals((double) 123, upperBin(value, (double) 10, offset));
        assertEquals((double) 123, upperBin(value, (double) 20, offset));
        assertEquals(NULL_DOUBLE, upperBin(NULL_DOUBLE, (double) 5, offset));
        assertEquals(NULL_DOUBLE, upperBin(value, NULL_DOUBLE, offset));

        assertEquals(upperBin(value, (double) 5, offset), upperBin(upperBin(value, (double) 5, offset), (double) 5, offset));
    }

    public void testClamp() {
        assertEquals((double) 3, clamp((double) 3, (double) -6, (double) 5));
        assertEquals((double) -6, clamp((double) -7, (double) -6, (double) 5));
        assertEquals((double) 5, clamp((double) 7, (double) -6, (double) 5));
        assertEquals(NULL_DOUBLE, clamp(NULL_DOUBLE, (double) -6, (double) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, binSearchIndex((double[]) null, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new double[]{1,3,4}, (double)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new double[]{1,3,4}, (double)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new double[]{1,3,4}, (double)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new double[]{1,3,4}, (double)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new double[]{1,3,4}, (double)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new double[]{1,3,4}, (double)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((DbDoubleArray) null, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new DbDoubleArrayDirect(new double[]{1,3,4}), (double)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbDoubleArrayDirect(new double[]{1,3,4}), (double)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbDoubleArrayDirect(new double[]{1,3,4}), (double)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new DbDoubleArrayDirect(new double[]{1,3,4}), (double)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbDoubleArrayDirect(new double[]{1,3,4}), (double)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbDoubleArrayDirect(new double[]{1,3,4}), (double)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((DbDoubleArray)null, (double) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((DbDoubleArray)null, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((DbDoubleArray)null, (double) 0, BinSearch.BS_LOWEST));

        double[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(empty), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(empty), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(empty), (double) 0, BinSearch.BS_LOWEST));

        double[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbDoubleArrayDirect(one), (double) 11, BinSearch.BS_LOWEST));


        double[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((DbDoubleArray)null, (double) 0, null);

        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new DbDoubleArrayDirect(v), (double) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, rawBinSearchIndex((double[]) null, (double) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((double[])null, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((double[])null, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(empty, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(empty, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(empty, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(one, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(one, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(one, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(one, (double) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(one, (double) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(one, (double) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(one, (double) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(one, (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(one, (double) 11, BinSearch.BS_LOWEST));


        rawBinSearchIndex((double[])null, (double) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (double) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (double) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (double) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (double) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (double) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (double) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (double) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (double) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (double) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (double) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (double) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (double) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (double) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (double) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (double) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (double) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (double) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (double) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (double) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (double) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (double) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (double) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (double) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (double) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (double) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final double[] doubles = new double[]{1, -5, -2, -2, 96, 0, 12, NULL_DOUBLE, NULL_DOUBLE};
        final DbDoubleArray sort = sort(new DbDoubleArrayDirect(doubles));
        final DbDoubleArray expected = new DbDoubleArrayDirect(new double[]{NULL_DOUBLE, NULL_DOUBLE, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        double[] sortedArray = sort(doubles);
        assertEquals(new double[]{NULL_DOUBLE, NULL_DOUBLE, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((DbDoubleArray)null));
        assertNull(sort((double[])null));
        assertNull(sort((Double[])null));
        assertEquals(new DbDoubleArrayDirect(), sort(new DbDoubleArrayDirect()));
        assertEquals(new double[]{}, sort(new double[]{}));
        assertEquals(new double[]{}, sort(new Double[]{}));
    }

    public void testSortDescending() {
        final double[] doubles = new double[]{1, -5, -2, -2, 96, 0, 12, NULL_DOUBLE, NULL_DOUBLE};
        final DbDoubleArray sort = sortDescending(new DbDoubleArrayDirect(doubles));
        final DbDoubleArray expected = new DbDoubleArrayDirect(new double[]{96, 12, 1, 0, -2, -2, -5, NULL_DOUBLE, NULL_DOUBLE});
        assertEquals(expected, sort);

        double[] sortedArray = sortDescending(doubles);
        assertEquals(new double[]{96, 12, 1, 0, -2, -2, -5, NULL_DOUBLE, NULL_DOUBLE}, sortedArray);

        assertNull(sortDescending((DbDoubleArray)null));
        assertNull(sortDescending((double[])null));
        assertNull(sortDescending((Double[])null));
        assertEquals(new DbDoubleArrayDirect(), sortDescending(new DbDoubleArrayDirect()));
        assertEquals(new double[]{}, sortDescending(new double[]{}));
        assertEquals(new double[]{}, sortDescending(new Double[]{}));
    }

    public void testSortsExceptions() {
        DbDoubleArray dbDoubleArray = null;
        DbDoubleArray sort = sort(dbDoubleArray);
        assertNull(sort);

        double[] doubles = null;
        double[] sortArray = sort(doubles);
        assertNull(sortArray);

        doubles = new double[]{};
        sort = sort(new DbDoubleArrayDirect(doubles));
        assertEquals(new DbDoubleArrayDirect(), sort);

        sortArray = sort(doubles);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DbDoubleArray dbDoubleArray = null;
        DbDoubleArray sort = sortDescending(dbDoubleArray);
        assertNull(sort);

        double[] doubles = null;
        double[] sortArray = sortDescending(doubles);
        assertNull(sortArray);

        doubles = new double[]{};
        sort = sortDescending(new DbDoubleArrayDirect(doubles));
        assertEquals(new DbDoubleArrayDirect(), sort);

        sortArray = sortDescending(doubles);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSequence(){
        assertEquals(new double[]{0,1,2,3,4,5}, DoubleNumericPrimitives.sequence((double)0, (double)5, (double)1));
        assertEquals(new double[]{-5,-4,-3,-2,-1,0}, DoubleNumericPrimitives.sequence((double)-5, (double)0, (double)1));

        assertEquals(new double[]{0,2,4}, DoubleNumericPrimitives.sequence((double)0, (double)5, (double)2));
        assertEquals(new double[]{-5,-3,-1}, DoubleNumericPrimitives.sequence((double)-5, (double)0, (double)2));

        assertEquals(new double[]{5,3,1}, DoubleNumericPrimitives.sequence((double)5, (double)0, (double)-2));
        assertEquals(new double[]{0,-2,-4}, DoubleNumericPrimitives.sequence((double)0, (double)-5, (double)-2));

        assertEquals(new double[]{}, DoubleNumericPrimitives.sequence((double)0, (double)5, (double)0));
        assertEquals(new double[]{}, DoubleNumericPrimitives.sequence((double)5, (double)0, (double)1));
    }

    public void testMedian() {
        assertEquals(3.0, median(new double[]{4,2,3}));
        assertEquals(3.5, median(new double[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((double[])null));

        assertEquals(3.0, median(new Double[]{(double)4,(double)2,(double)3}));
        assertEquals(3.5, median(new Double[]{(double)5,(double)4,(double)2,(double)3}));
        assertEquals(NULL_DOUBLE, median((Double[])null));

        assertEquals(3.0, median(new DbDoubleArrayDirect(new double[]{4,2,3})));
        assertEquals(3.5, median(new DbDoubleArrayDirect(new double[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((DbDoubleArray) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(new double[]{4,2,3},0.00));
        assertEquals(3.0, percentile(new double[]{4,2,3},0.50));
        assertEquals(NULL_DOUBLE, percentile((double[])null, 0.25));

        assertEquals(2.0, percentile(new DbDoubleArrayDirect(new double[]{4,2,3}),0.00));
        assertEquals(3.0, percentile(new DbDoubleArrayDirect(new double[]{4,2,3}),0.50));
        assertEquals(NULL_DOUBLE, percentile((DbDoubleArray) null, 0.25));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (short[])null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (int[])null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (long[])null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (double[])null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wvar(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (short[])null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (int[])null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (long[])null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (double[])null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wstd(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (short[])null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (int[])null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (long[])null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (double[])null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wste(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }

    public void testWtstat() {
        final double target = wavg(new double[]{1,2,3}, new double[]{4,5,6}) / wste(new double[]{1,2,3}, new double[]{4,5,6});

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (short[])null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (int[])null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (long[])null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (double[])null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (DbDoubleArray)null));

        /////

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DbDoubleArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbDoubleArrayDirect(new double[]{1,2,3}), (DbDoubleArray)null));
    }
}
