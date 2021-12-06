/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestFloatNumericPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.vector.*;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.function.DoubleNumericPrimitives.*;
import static io.deephaven.function.DoublePrimitives.count;
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

        assertEquals(50.0, avg(new DoubleVectorDirect(new double[]{40, 50, 60})));
        assertEquals(45.5, avg(new DoubleVectorDirect(new double[]{40, 51})));
        assertTrue(Double.isNaN(avg(new DoubleVectorDirect())));
        assertTrue(Double.isNaN(avg(new DoubleVectorDirect(NULL_DOUBLE))));
        assertEquals(10.0, avg(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((DoubleVectorDirect)null));
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

        assertEquals(50.0, absAvg(new DoubleVectorDirect(new double[]{40, (double) 50, 60})));
        assertEquals(45.5, absAvg(new DoubleVectorDirect(new double[]{(double) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new DoubleVectorDirect())));
        assertTrue(Double.isNaN(absAvg(new DoubleVectorDirect(NULL_DOUBLE))));
        assertEquals(10.0, absAvg(new DoubleVectorDirect((double) 5, NULL_DOUBLE, (double) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((DoubleVectorDirect)null));
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

        assertEquals(4, countPos(new DoubleVectorDirect(new double[]{40, 50, 60, (double) 1, 0})));
        assertEquals(0, countPos(new DoubleVectorDirect()));
        assertEquals(0, countPos(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(3, countPos(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15, (double) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((DoubleVectorDirect)null));
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

        assertEquals(2, countNeg(new DoubleVectorDirect(new double[]{40, (double) -50, 60, (double) -1, 0})));
        assertEquals(0, countNeg(new DoubleVectorDirect()));
        assertEquals(0, countNeg(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(1, countNeg(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15, (double) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((DoubleVectorDirect)null));
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

        assertEquals(2, countZero(new DoubleVectorDirect(new double[]{0, 40, 50, 60, (double) -1, 0})));
        assertEquals(0, countZero(new DoubleVectorDirect()));
        assertEquals(0, countZero(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(2, countZero(new DoubleVectorDirect(new double[]{0, 5, NULL_DOUBLE, 0, (double) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((DoubleVectorDirect)null));
    }

    public void testMax() {
        assertEquals((double) 60, max(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals((double) 60, max(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1)));
        assertEquals(NULL_DOUBLE, max(new DoubleVectorDirect()));
        assertEquals(NULL_DOUBLE, max(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(NULL_DOUBLE, max((DoubleVector) null));

        assertEquals((double) 60, max((double) 0, (double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1, (double) 0));
        assertEquals((double) 60, max((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1));
        assertEquals(NULL_DOUBLE, max());
        assertEquals(NULL_DOUBLE, max(NULL_DOUBLE));
        assertEquals(NULL_DOUBLE, max((double[]) null));
        assertEquals(NULL_DOUBLE, max((Double[]) null));
    }

    public void testMin() {
        assertEquals((double) 0, min(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals((double) -1, min(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1)));
        assertEquals(NULL_DOUBLE, min(new DoubleVectorDirect()));
        assertEquals(NULL_DOUBLE, min(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(NULL_DOUBLE, min((DoubleVector) null));

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

        assertEquals(1, firstIndexOf(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)40));
        assertEquals(4, firstIndexOf(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)60));
        assertEquals(NULL_INT, firstIndexOf(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((DoubleVector) null, (double)40));
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

        assertEquals(4, indexOfMax(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals(3, indexOfMax(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1)));
        assertEquals(-1, indexOfMax(new DoubleVectorDirect()));
        assertEquals(-1, indexOfMax(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((DoubleVectorDirect)null));
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

        assertEquals(1, indexOfMin(new DoubleVectorDirect(new double[]{40, 0, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals(4, indexOfMin(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1)));
        assertEquals(-1, indexOfMin(new DoubleVectorDirect()));
        assertEquals(-1, indexOfMin(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((DoubleVectorDirect)null));
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

        assertEquals(var, var(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((DoubleVectorDirect)null));
    }

    public void testStd() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(Math.sqrt(var(new DoubleVectorDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((double[])null));

        assertEquals(Math.sqrt(var(new DoubleVectorDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Double[])null));

        assertEquals(Math.sqrt(var(new DoubleVectorDirect(v))), std(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((DoubleVectorDirect)null));
    }

    public void testSte() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(std(new DoubleVectorDirect(v)) / Math.sqrt(DoublePrimitives.count(new DoubleVectorDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((double[])null));

        assertEquals(std(new DoubleVectorDirect(v)) / Math.sqrt(DoublePrimitives.count(new DoubleVectorDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Double[])null));

        assertEquals(std(new DoubleVectorDirect(v)) / Math.sqrt(DoublePrimitives.count(new DoubleVectorDirect(v))), ste(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((DoubleVectorDirect)null));
    }

    public void testTstat() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(avg(new DoubleVectorDirect(v)) / ste(new DoubleVectorDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((double[])null));

        assertEquals(avg(new DoubleVectorDirect(v)) / ste(new DoubleVectorDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Double[])null));

        assertEquals(avg(new DoubleVectorDirect(v)) / ste(new DoubleVectorDirect(v)), tstat(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((DoubleVectorDirect)null));
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

        assertEquals(cov, cov(a, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((double[])null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((double[])null, (DoubleVectorDirect)null));

        assertEquals(cov, cov(new DoubleVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DoubleVectorDirect(a), (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DoubleVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DoubleVectorDirect)null, (double[])null));

        assertEquals(cov, cov(new DoubleVectorDirect(a), new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DoubleVectorDirect(a), (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DoubleVectorDirect)null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DoubleVectorDirect)null, (DoubleVectorDirect)null));
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

        assertEquals(cor, cor(a, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((double[])null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((double[])null, (DoubleVectorDirect)null));

        assertEquals(cor, cor(new DoubleVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DoubleVectorDirect(a), (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DoubleVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DoubleVectorDirect)null, (double[])null));

        assertEquals(cor, cor(new DoubleVectorDirect(a), new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DoubleVectorDirect(a), (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DoubleVectorDirect)null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DoubleVectorDirect)null, (DoubleVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new DoubleVectorDirect(new double[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new DoubleVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new DoubleVectorDirect(NULL_DOUBLE))) == 0.0);
        assertTrue(Math.abs(20 - sum(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15}))) == 0.0);
        assertEquals(NULL_DOUBLE, sum((DoubleVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new double[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new double[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new double[]{NULL_DOUBLE})) == 0.0);
        assertTrue(Math.abs(20 - sum(new double[]{5, NULL_DOUBLE, 15})) == 0.0);
        assertEquals(NULL_DOUBLE, sum((double[]) null));
    }

    public void testSumVector() {
        assertEquals(new double[]{4, 15}, sum(new ObjectVectorDirect<>(new double[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new double[]{4, NULL_DOUBLE}, sum(new ObjectVectorDirect<>(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((ObjectVector<double[]>) null));

        try {
            sum(new ObjectVectorDirect<>(new double[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertTrue(Math.abs(120 - product(new DoubleVectorDirect(new double[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_DOUBLE, product(new DoubleVectorDirect()));
        assertEquals(NULL_DOUBLE, product(new DoubleVectorDirect(NULL_DOUBLE)));
        assertTrue(Math.abs(75 - product(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15}))) == 0.0);
        assertEquals(NULL_DOUBLE, product((DoubleVector) null));
    }

//    public void testProdVector() {
//        assertEquals(new double[]{-30, 120}, product(new ObjectVectorDirect<>(new double[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new double[]{-30, NULL_DOUBLE}, product(new ObjectVectorDirect<>(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((Vector<double[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new double[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertEquals(new double[]{1, 3, 6, 10, 15}, cumsum(new DoubleVectorDirect(new double[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 3, 6, 6, 11}, cumsum(new DoubleVectorDirect(new double[]{1, 2, 3, NULL_DOUBLE, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 5, 9, 14}, cumsum(new DoubleVectorDirect(new double[]{NULL_DOUBLE, 2, 3, 4, 5})));
        assertEquals(new double[0], cumsum(new DoubleVectorDirect()));
        assertEquals(null, cumsum((DoubleVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new double[]{1, 2, 6, 24, 120}, cumprod(new double[]{1, 2, 3, 4, 5}));
        assertEquals(new double[]{1, 2, 6, 6, 30}, cumprod(new double[]{1, 2, 3, NULL_DOUBLE, 5}));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new double[]{NULL_DOUBLE, 2, 3, 4, 5}));
        assertEquals(new double[0], cumprod());
        assertEquals(null, cumprod((double[]) null));

        assertEquals(new double[]{1, 2, 6, 24, 120}, cumprod(new DoubleVectorDirect(new double[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 2, 6, 6, 30}, cumprod(new DoubleVectorDirect(new double[]{1, 2, 3, NULL_DOUBLE, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new DoubleVectorDirect(new double[]{NULL_DOUBLE, 2, 3, 4, 5})));
        assertEquals(new double[0], cumprod(new DoubleVectorDirect()));
        assertEquals(null, cumprod((DoubleVector) null));
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

        assertEquals(NULL_INT, binSearchIndex((DoubleVector) null, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((DoubleVector)null, (double) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((DoubleVector)null, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((DoubleVector)null, (double) 0, BinSearch.BS_LOWEST));

        double[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(empty), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(empty), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(empty), (double) 0, BinSearch.BS_LOWEST));

        double[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DoubleVectorDirect(one), (double) 11, BinSearch.BS_LOWEST));


        double[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((DoubleVector)null, (double) 0, null);

        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new DoubleVectorDirect(v), (double) 25, BinSearch.BS_LOWEST));

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
        final DoubleVector sort = sort(new DoubleVectorDirect(doubles));
        final DoubleVector expected = new DoubleVectorDirect(new double[]{NULL_DOUBLE, NULL_DOUBLE, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        double[] sortedArray = sort(doubles);
        assertEquals(new double[]{NULL_DOUBLE, NULL_DOUBLE, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((DoubleVector)null));
        assertNull(sort((double[])null));
        assertNull(sort((Double[])null));
        assertEquals(new DoubleVectorDirect(), sort(new DoubleVectorDirect()));
        assertEquals(new double[]{}, sort(new double[]{}));
        assertEquals(new double[]{}, sort(new Double[]{}));
    }

    public void testSortDescending() {
        final double[] doubles = new double[]{1, -5, -2, -2, 96, 0, 12, NULL_DOUBLE, NULL_DOUBLE};
        final DoubleVector sort = sortDescending(new DoubleVectorDirect(doubles));
        final DoubleVector expected = new DoubleVectorDirect(new double[]{96, 12, 1, 0, -2, -2, -5, NULL_DOUBLE, NULL_DOUBLE});
        assertEquals(expected, sort);

        double[] sortedArray = sortDescending(doubles);
        assertEquals(new double[]{96, 12, 1, 0, -2, -2, -5, NULL_DOUBLE, NULL_DOUBLE}, sortedArray);

        assertNull(sortDescending((DoubleVector)null));
        assertNull(sortDescending((double[])null));
        assertNull(sortDescending((Double[])null));
        assertEquals(new DoubleVectorDirect(), sortDescending(new DoubleVectorDirect()));
        assertEquals(new double[]{}, sortDescending(new double[]{}));
        assertEquals(new double[]{}, sortDescending(new Double[]{}));
    }

    public void testSortsExceptions() {
        DoubleVector doubleVector = null;
        DoubleVector sort = sort(doubleVector);
        assertNull(sort);

        double[] doubles = null;
        double[] sortArray = sort(doubles);
        assertNull(sortArray);

        doubles = new double[]{};
        sort = sort(new DoubleVectorDirect(doubles));
        assertEquals(new DoubleVectorDirect(), sort);

        sortArray = sort(doubles);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DoubleVector doubleVector = null;
        DoubleVector sort = sortDescending(doubleVector);
        assertNull(sort);

        double[] doubles = null;
        double[] sortArray = sortDescending(doubles);
        assertNull(sortArray);

        doubles = new double[]{};
        sort = sortDescending(new DoubleVectorDirect(doubles));
        assertEquals(new DoubleVectorDirect(), sort);

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

        assertEquals(3.0, median(new DoubleVectorDirect(new double[]{4,2,3})));
        assertEquals(3.5, median(new DoubleVectorDirect(new double[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((DoubleVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(new double[]{4,2,3},0.00));
        assertEquals(3.0, percentile(new double[]{4,2,3},0.50));
        assertEquals(NULL_DOUBLE, percentile((double[])null, 0.25));

        assertEquals(2.0, percentile(new DoubleVectorDirect(new double[]{4,2,3}),0.00));
        assertEquals(3.0, percentile(new DoubleVectorDirect(new double[]{4,2,3}),0.50));
        assertEquals(NULL_DOUBLE, percentile((DoubleVector) null, 0.25));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
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

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
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

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
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

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
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

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }
}
