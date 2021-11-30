/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestShortNumericPrimitives and regenerate
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

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.function.IntegerPrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestIntegerNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((int) 1, IntegerNumericPrimitives.signum((int) 5));
        assertEquals((int) 0, IntegerNumericPrimitives.signum((int) 0));
        assertEquals((int) -1, IntegerNumericPrimitives.signum((int) -5));
        assertEquals(NULL_INT, IntegerNumericPrimitives.signum(NULL_INT));
    }

    public void testAvg() {
        assertEquals(50.0, IntegerNumericPrimitives.avg(new int[]{40, 50, 60}));
        assertEquals(45.5, IntegerNumericPrimitives.avg(new int[]{40, 51}));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.avg(new int[]{})));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.avg(new int[]{NULL_INT})));
        assertEquals(10.0, IntegerNumericPrimitives.avg(new int[]{5, NULL_INT, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.avg((int[])null));

        assertEquals(50.0, IntegerNumericPrimitives.avg(new Integer[]{(int)40, (int)50, (int)60}));
        assertEquals(45.5, IntegerNumericPrimitives.avg(new Integer[]{(int)40, (int)51}));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.avg(new Integer[]{})));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.avg(new Integer[]{NULL_INT})));
        assertEquals(10.0, IntegerNumericPrimitives.avg(new Integer[]{(int)5, NULL_INT, (int)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.avg((Integer[])null));

        assertEquals(50.0, IntegerNumericPrimitives.avg(new IntVectorDirect(new int[]{40, 50, 60})));
        assertEquals(45.5, IntegerNumericPrimitives.avg(new IntVectorDirect(new int[]{40, 51})));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.avg(new IntVectorDirect())));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.avg(new IntVectorDirect(NULL_INT))));
        assertEquals(10.0, IntegerNumericPrimitives.avg(new IntVectorDirect(new int[]{5, NULL_INT, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.avg((IntVectorDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, IntegerNumericPrimitives.absAvg(new int[]{40, (int) 50, 60}));
        assertEquals(45.5, IntegerNumericPrimitives.absAvg(new int[]{(int) 40, 51}));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.absAvg(new int[]{})));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.absAvg(new int[]{NULL_INT})));
        assertEquals(10.0, IntegerNumericPrimitives.absAvg(new int[]{(int) 5, NULL_INT, (int) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.absAvg((int[])null));

        assertEquals(50.0, IntegerNumericPrimitives.absAvg(new Integer[]{(int)40, (int) 50, (int)60}));
        assertEquals(45.5, IntegerNumericPrimitives.absAvg(new Integer[]{(int) 40, (int)51}));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.absAvg(new Integer[]{})));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.absAvg(new Integer[]{NULL_INT})));
        assertEquals(10.0, IntegerNumericPrimitives.absAvg(new Integer[]{(int) 5, NULL_INT, (int) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.absAvg((Integer[])null));

        assertEquals(50.0, IntegerNumericPrimitives.absAvg(new IntVectorDirect(new int[]{40, (int) 50, 60})));
        assertEquals(45.5, IntegerNumericPrimitives.absAvg(new IntVectorDirect(new int[]{(int) 40, 51})));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.absAvg(new IntVectorDirect())));
        assertTrue(Double.isNaN(IntegerNumericPrimitives.absAvg(new IntVectorDirect(NULL_INT))));
        assertEquals(10.0, IntegerNumericPrimitives.absAvg(new IntVectorDirect((int) 5, NULL_INT, (int) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.absAvg((IntVectorDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, IntegerNumericPrimitives.countPos(new int[]{40, 50, 60, (int) 1, 0}));
        assertEquals(0, IntegerNumericPrimitives.countPos(new int[]{}));
        assertEquals(0, IntegerNumericPrimitives.countPos(new int[]{NULL_INT}));
        assertEquals(3, IntegerNumericPrimitives.countPos(new int[]{5, NULL_INT, 15, (int) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countPos((int[])null));

        assertEquals(4, IntegerNumericPrimitives.countPos(new Integer[]{(int)40, (int)50, (int)60, (int) 1, (int)0}));
        assertEquals(0, IntegerNumericPrimitives.countPos(new Integer[]{}));
        assertEquals(0, IntegerNumericPrimitives.countPos(new Integer[]{NULL_INT}));
        assertEquals(3, IntegerNumericPrimitives.countPos(new Integer[]{(int)5, NULL_INT, (int)15, (int) 1, (int)0}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countPos((int[])null));

        assertEquals(4, IntegerNumericPrimitives.countPos(new IntVectorDirect(new int[]{40, 50, 60, (int) 1, 0})));
        assertEquals(0, IntegerNumericPrimitives.countPos(new IntVectorDirect()));
        assertEquals(0, IntegerNumericPrimitives.countPos(new IntVectorDirect(NULL_INT)));
        assertEquals(3, IntegerNumericPrimitives.countPos(new IntVectorDirect(new int[]{5, NULL_INT, 15, (int) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countPos((IntVectorDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, IntegerNumericPrimitives.countNeg(new int[]{40, (int) -50, 60, (int) -1, 0}));
        assertEquals(0, IntegerNumericPrimitives.countNeg(new int[]{}));
        assertEquals(0, IntegerNumericPrimitives.countNeg(new int[]{NULL_INT}));
        assertEquals(1, IntegerNumericPrimitives.countNeg(new int[]{5, NULL_INT, 15, (int) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countNeg((int[])null));

        assertEquals(2, IntegerNumericPrimitives.countNeg(new Integer[]{(int)40, (int) -50, (int)60, (int) -1, (int)0}));
        assertEquals(0, IntegerNumericPrimitives.countNeg(new Integer[]{}));
        assertEquals(0, IntegerNumericPrimitives.countNeg(new Integer[]{NULL_INT}));
        assertEquals(1, IntegerNumericPrimitives.countNeg(new Integer[]{(int)5, NULL_INT, (int)15, (int) -1, (int)0}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countNeg((Integer[])null));

        assertEquals(2, IntegerNumericPrimitives.countNeg(new IntVectorDirect(new int[]{40, (int) -50, 60, (int) -1, 0})));
        assertEquals(0, IntegerNumericPrimitives.countNeg(new IntVectorDirect()));
        assertEquals(0, IntegerNumericPrimitives.countNeg(new IntVectorDirect(NULL_INT)));
        assertEquals(1, IntegerNumericPrimitives.countNeg(new IntVectorDirect(new int[]{5, NULL_INT, 15, (int) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countNeg((IntVectorDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, IntegerNumericPrimitives.countZero(new int[]{0, 40, 50, 60, (int) -1, 0}));
        assertEquals(0, IntegerNumericPrimitives.countZero(new int[]{}));
        assertEquals(0, IntegerNumericPrimitives.countZero(new int[]{NULL_INT}));
        assertEquals(2, IntegerNumericPrimitives.countZero(new int[]{0, 5, NULL_INT, 0, (int) -15}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countZero((int[])null));

        assertEquals(2, IntegerNumericPrimitives.countZero(new Integer[]{(int)0, (int)40, (int)50, (int)60, (int) -1, (int)0}));
        assertEquals(0, IntegerNumericPrimitives.countZero(new Integer[]{}));
        assertEquals(0, IntegerNumericPrimitives.countZero(new Integer[]{NULL_INT}));
        assertEquals(2, IntegerNumericPrimitives.countZero(new Integer[]{(int)0, (int)5, NULL_INT, (int)0, (int) -15}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countZero((Integer[])null));

        assertEquals(2, IntegerNumericPrimitives.countZero(new IntVectorDirect(new int[]{0, 40, 50, 60, (int) -1, 0})));
        assertEquals(0, IntegerNumericPrimitives.countZero(new IntVectorDirect()));
        assertEquals(0, IntegerNumericPrimitives.countZero(new IntVectorDirect(NULL_INT)));
        assertEquals(2, IntegerNumericPrimitives.countZero(new IntVectorDirect(new int[]{0, 5, NULL_INT, 0, (int) -15})));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.countZero((IntVectorDirect)null));
    }

    public void testMax() {
        assertEquals((int) 60, IntegerNumericPrimitives.max(new IntVectorDirect(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals((int) 60, IntegerNumericPrimitives.max(new IntVectorDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) 1)));
        assertEquals(NULL_INT, IntegerNumericPrimitives.max(new IntVectorDirect()));
        assertEquals(NULL_INT, IntegerNumericPrimitives.max(new IntVectorDirect(NULL_INT)));
        assertEquals(NULL_INT, IntegerNumericPrimitives.max((IntVector) null));

        assertEquals((int) 60, IntegerNumericPrimitives.max((int) 0, (int) 40, NULL_INT, (int) 50, (int) 60, (int) 1, (int) 0));
        assertEquals((int) 60, IntegerNumericPrimitives.max((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1));
        assertEquals(NULL_INT, IntegerNumericPrimitives.max());
        assertEquals(NULL_INT, IntegerNumericPrimitives.max(NULL_INT));
        assertEquals(NULL_INT, IntegerNumericPrimitives.max((int[]) null));
        assertEquals(NULL_INT, IntegerNumericPrimitives.max((Integer[]) null));
    }

    public void testMin() {
        assertEquals((int) 0, IntegerNumericPrimitives.min(new IntVectorDirect(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals((int) -1, IntegerNumericPrimitives.min(new IntVectorDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1)));
        assertEquals(NULL_INT, IntegerNumericPrimitives.min(new IntVectorDirect()));
        assertEquals(NULL_INT, IntegerNumericPrimitives.min(new IntVectorDirect(NULL_INT)));
        assertEquals(NULL_INT, IntegerNumericPrimitives.min((IntVector) null));

        assertEquals((int) 0, IntegerNumericPrimitives.min((int) 0, (int) 40, NULL_INT, (int) 50, (int) 60, (int) 1, (int) 0));
        assertEquals((int) -1, IntegerNumericPrimitives.min((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1));
        assertEquals(NULL_INT, IntegerNumericPrimitives.min());
        assertEquals(NULL_INT, IntegerNumericPrimitives.min(NULL_INT));
        assertEquals(NULL_INT, IntegerNumericPrimitives.min((int[]) null));
        assertEquals(NULL_INT, IntegerNumericPrimitives.min((Integer[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, IntegerNumericPrimitives.firstIndexOf(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}, (int)40));
        assertEquals(4, IntegerNumericPrimitives.firstIndexOf(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}, (int)60));
        assertEquals(NULL_INT, IntegerNumericPrimitives.firstIndexOf(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}, (int)1));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.firstIndexOf((int[])null, (int)40));

        assertEquals(1, IntegerNumericPrimitives.firstIndexOf(new IntVectorDirect(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}), (int)40));
        assertEquals(4, IntegerNumericPrimitives.firstIndexOf(new IntVectorDirect(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}), (int)60));
        assertEquals(NULL_INT, IntegerNumericPrimitives.firstIndexOf(new IntVectorDirect(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}), (int)1));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.firstIndexOf((IntVector) null, (int)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, IntegerNumericPrimitives.indexOfMax(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0}));
        assertEquals(3, IntegerNumericPrimitives.indexOfMax(new int[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) 1}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMax(new int[]{}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMax(new int[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.indexOfMax((int[])null));

        assertEquals(4, IntegerNumericPrimitives.indexOfMax(new Integer[]{(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) 1, (int)0}));
        assertEquals(3, IntegerNumericPrimitives.indexOfMax(new Integer[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) 1}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMax(new Integer[]{}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMax(new Integer[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.indexOfMax((Integer[])null));

        assertEquals(4, IntegerNumericPrimitives.indexOfMax(new IntVectorDirect(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals(3, IntegerNumericPrimitives.indexOfMax(new IntVectorDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) 1)));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMax(new IntVectorDirect()));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMax(new IntVectorDirect(NULL_INT)));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.indexOfMax((IntVectorDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, IntegerNumericPrimitives.indexOfMin(new int[]{40, 0, NULL_INT, 50, 60, (int) 1, 0}));
        assertEquals(4, IntegerNumericPrimitives.indexOfMin(new int[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) -1}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMin(new int[]{}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMin(new int[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.indexOfMin((int[])null));

        assertEquals(1, IntegerNumericPrimitives.indexOfMin(new Integer[]{(int)40, (int)0, NULL_INT, (int)50, (int)60, (int) 1, (int)0}));
        assertEquals(4, IntegerNumericPrimitives.indexOfMin(new Integer[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) -1}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMin(new Integer[]{}));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMin(new Integer[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.indexOfMin((Integer[])null));

        assertEquals(1, IntegerNumericPrimitives.indexOfMin(new IntVectorDirect(new int[]{40, 0, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals(4, IntegerNumericPrimitives.indexOfMin(new IntVectorDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1)));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMin(new IntVectorDirect()));
        assertEquals(-1, IntegerNumericPrimitives.indexOfMin(new IntVectorDirect(NULL_INT)));
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.indexOfMin((IntVectorDirect)null));
    }


    public void testVar() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, IntegerNumericPrimitives.var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.var((int[])null));

        assertEquals(var, IntegerNumericPrimitives.var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.var((Integer[])null));

        assertEquals(var, IntegerNumericPrimitives.var(new IntVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.var((IntVectorDirect)null));
    }

    public void testStd() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};

        assertEquals(Math.sqrt(IntegerNumericPrimitives.var(new IntVectorDirect(v))), IntegerNumericPrimitives.std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.std((int[])null));

        assertEquals(Math.sqrt(IntegerNumericPrimitives.var(new IntVectorDirect(v))), IntegerNumericPrimitives.std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.std((Integer[])null));

        assertEquals(Math.sqrt(IntegerNumericPrimitives.var(new IntVectorDirect(v))), IntegerNumericPrimitives.std(new IntVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.std((IntVectorDirect)null));
    }

    public void testSte() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};

        assertEquals(IntegerNumericPrimitives.std(new IntVectorDirect(v)) / Math.sqrt(count(new IntVectorDirect(v))), IntegerNumericPrimitives.ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.ste((int[])null));

        assertEquals(IntegerNumericPrimitives.std(new IntVectorDirect(v)) / Math.sqrt(count(new IntVectorDirect(v))), IntegerNumericPrimitives.ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.ste((Integer[])null));

        assertEquals(IntegerNumericPrimitives.std(new IntVectorDirect(v)) / Math.sqrt(count(new IntVectorDirect(v))), IntegerNumericPrimitives.ste(new IntVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.ste((IntVectorDirect)null));
    }

    public void testTstat() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};

        assertEquals(IntegerNumericPrimitives.avg(new IntVectorDirect(v)) / IntegerNumericPrimitives.ste(new IntVectorDirect(v)), IntegerNumericPrimitives.tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.tstat((int[])null));

        assertEquals(IntegerNumericPrimitives.avg(new IntVectorDirect(v)) / IntegerNumericPrimitives.ste(new IntVectorDirect(v)), IntegerNumericPrimitives.tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.tstat((Integer[])null));

        assertEquals(IntegerNumericPrimitives.avg(new IntVectorDirect(v)) / IntegerNumericPrimitives.ste(new IntVectorDirect(v)), IntegerNumericPrimitives.tstat(new IntVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.tstat((IntVectorDirect)null));
    }

    public void testCov() {
        int[] a = {10, 40, NULL_INT, 50, NULL_INT, (int) -1, 0, (int) -7};
        int[] b = {0, (int) -40, NULL_INT, NULL_INT, 6, (int) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, IntegerNumericPrimitives.cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov(a, (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((int[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((int[])null, (int[]) null));

        assertEquals(cov, IntegerNumericPrimitives.cov(a, new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov(a, (IntVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((int[])null, new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((int[])null, (IntVectorDirect)null));

        assertEquals(cov, IntegerNumericPrimitives.cov(new IntVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov(new IntVectorDirect(a), (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((IntVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((IntVectorDirect)null, (int[])null));

        assertEquals(cov, IntegerNumericPrimitives.cov(new IntVectorDirect(a), new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov(new IntVectorDirect(a), (IntVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((IntVectorDirect)null, new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cov((IntVectorDirect)null, (IntVectorDirect)null));
    }

    public void testCor() {
        int[] a = {10, 40, NULL_INT, 50, NULL_INT, (int) -1, 0, (int) -7};
        int[] b = {0, (int) -40, NULL_INT, NULL_INT, 6, (int) -1, 11, 3};
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

        assertEquals(cor, IntegerNumericPrimitives.cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor(a, (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((int[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((int[])null, (int[])null));

        assertEquals(cor, IntegerNumericPrimitives.cor(a, new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor(a, (IntVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((int[])null, new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((int[])null, (IntVectorDirect)null));

        assertEquals(cor, IntegerNumericPrimitives.cor(new IntVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor(new IntVectorDirect(a), (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((IntVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((IntVectorDirect)null, (int[])null));

        assertEquals(cor, IntegerNumericPrimitives.cor(new IntVectorDirect(a), new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor(new IntVectorDirect(a), (IntVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((IntVectorDirect)null, new IntVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cor((IntVectorDirect)null, (IntVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - IntegerNumericPrimitives.sum(new IntVectorDirect(new int[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - IntegerNumericPrimitives.sum(new IntVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - IntegerNumericPrimitives.sum(new IntVectorDirect(NULL_INT))) == 0.0);
        assertTrue(Math.abs(20 - IntegerNumericPrimitives.sum(new IntVectorDirect(new int[]{5, NULL_INT, 15}))) == 0.0);
        assertEquals(NULL_INT, IntegerNumericPrimitives.sum((IntVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - IntegerNumericPrimitives.sum(new int[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - IntegerNumericPrimitives.sum(new int[]{})) == 0.0);
        assertTrue(Math.abs(0 - IntegerNumericPrimitives.sum(new int[]{NULL_INT})) == 0.0);
        assertTrue(Math.abs(20 - IntegerNumericPrimitives.sum(new int[]{5, NULL_INT, 15})) == 0.0);
        assertEquals(NULL_INT, IntegerNumericPrimitives.sum((int[]) null));
    }

    public void testSumVector() {
        assertEquals(new int[]{4, 15}, IntegerNumericPrimitives.sum(new ObjectVectorDirect<>(new int[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new int[]{4, NULL_INT}, IntegerNumericPrimitives.sum(new ObjectVectorDirect<>(new int[][]{{5, NULL_INT}, {-3, 5}, {2, 6}})));
        assertEquals(null, IntegerNumericPrimitives.sum((ObjectVector<int[]>) null));

        try {
            IntegerNumericPrimitives.sum(new ObjectVectorDirect<>(new int[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new int[]{4, 15}, IntegerNumericPrimitives.sum(new int[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new int[]{4, NULL_INT}, IntegerNumericPrimitives.sum(new int[][]{{5, NULL_INT}, {-3, 5}, {2, 6}}));
        assertEquals(null, IntegerNumericPrimitives.sum((int[][]) null));

        try {
            IntegerNumericPrimitives.sum(new int[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - IntegerNumericPrimitives.product(new int[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_INT, IntegerNumericPrimitives.product(new int[]{}));
        assertEquals(NULL_INT, IntegerNumericPrimitives.product(new int[]{NULL_INT}));
        assertTrue(Math.abs(75 - IntegerNumericPrimitives.product(new int[]{5, NULL_INT, 15})) == 0.0);
        assertEquals(NULL_INT, IntegerNumericPrimitives.product((int[]) null));

        assertTrue(Math.abs(120 - IntegerNumericPrimitives.product(new IntVectorDirect(new int[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_INT, IntegerNumericPrimitives.product(new IntVectorDirect()));
        assertEquals(NULL_INT, IntegerNumericPrimitives.product(new IntVectorDirect(NULL_INT)));
        assertTrue(Math.abs(75 - IntegerNumericPrimitives.product(new IntVectorDirect(new int[]{5, NULL_INT, 15}))) == 0.0);
        assertEquals(NULL_INT, IntegerNumericPrimitives.product((IntVector) null));
    }

//    public void testProdVector() {
//        assertEquals(new int[]{-30, 120}, product(new ObjectVectorDirect<>(new int[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new int[]{-30, NULL_INT}, product(new ObjectVectorDirect<>(new int[][]{{5, NULL_INT}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((Vector<int[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new int[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void testProdArray() {
//        assertEquals(new int[]{-30, 120}, product(new int[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new int[]{-30, NULL_INT}, product(new int[][]{{5, NULL_INT}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((int[][]) null));
//
//        try {
//            product(new int[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void testCumSumArray() {
        assertEquals(new int[]{1, 3, 6, 10, 15}, IntegerNumericPrimitives.cumsum(new int[]{1, 2, 3, 4, 5}));
        assertEquals(new int[]{1, 3, 6, 6, 11}, IntegerNumericPrimitives.cumsum(new int[]{1, 2, 3, NULL_INT, 5}));
        assertEquals(new int[]{NULL_INT, 2, 5, 9, 14}, IntegerNumericPrimitives.cumsum(new int[]{NULL_INT, 2, 3, 4, 5}));
        assertEquals(new int[0], IntegerNumericPrimitives.cumsum());
        assertEquals(null, IntegerNumericPrimitives.cumsum((int[]) null));

        assertEquals(new int[]{1, 3, 6, 10, 15}, IntegerNumericPrimitives.cumsum(new IntVectorDirect(new int[]{1, 2, 3, 4, 5})));
        assertEquals(new int[]{1, 3, 6, 6, 11}, IntegerNumericPrimitives.cumsum(new IntVectorDirect(new int[]{1, 2, 3, NULL_INT, 5})));
        assertEquals(new int[]{NULL_INT, 2, 5, 9, 14}, IntegerNumericPrimitives.cumsum(new IntVectorDirect(new int[]{NULL_INT, 2, 3, 4, 5})));
        assertEquals(new int[0], IntegerNumericPrimitives.cumsum(new IntVectorDirect()));
        assertEquals(null, IntegerNumericPrimitives.cumsum((IntVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new int[]{1, 2, 6, 24, 120}, IntegerNumericPrimitives.cumprod(new int[]{1, 2, 3, 4, 5}));
        assertEquals(new int[]{1, 2, 6, 6, 30}, IntegerNumericPrimitives.cumprod(new int[]{1, 2, 3, NULL_INT, 5}));
        assertEquals(new int[]{NULL_INT, 2, 6, 24, 120}, IntegerNumericPrimitives.cumprod(new int[]{NULL_INT, 2, 3, 4, 5}));
        assertEquals(new int[0], IntegerNumericPrimitives.cumprod());
        assertEquals(null, IntegerNumericPrimitives.cumprod((int[]) null));

        assertEquals(new int[]{1, 2, 6, 24, 120}, IntegerNumericPrimitives.cumprod(new IntVectorDirect(new int[]{1, 2, 3, 4, 5})));
        assertEquals(new int[]{1, 2, 6, 6, 30}, IntegerNumericPrimitives.cumprod(new IntVectorDirect(new int[]{1, 2, 3, NULL_INT, 5})));
        assertEquals(new int[]{NULL_INT, 2, 6, 24, 120}, IntegerNumericPrimitives.cumprod(new IntVectorDirect(new int[]{NULL_INT, 2, 3, 4, 5})));
        assertEquals(new int[0], IntegerNumericPrimitives.cumprod(new IntVectorDirect()));
        assertEquals(null, IntegerNumericPrimitives.cumprod((IntVector) null));
    }

    public void testAbs() {
        int value = -5;
        assertEquals((int) Math.abs(value), IntegerNumericPrimitives.abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_INT, IntegerNumericPrimitives.abs(NULL_INT), 1e-10);
    }

    public void testAcos() {
        int value = -5;
        assertEquals(Math.acos(value), IntegerNumericPrimitives.acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.acos(NULL_INT), 1e-10);
    }

    public void testAsin() {
        int value = -5;
        assertEquals(Math.asin(value), IntegerNumericPrimitives.asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.asin(NULL_INT), 1e-10);
    }

    public void testAtan() {
        int value = -5;
        assertEquals(Math.atan(value), IntegerNumericPrimitives.atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.atan(NULL_INT), 1e-10);
    }

    public void testCeil() {
        int value = -5;
        assertEquals(Math.ceil(value), IntegerNumericPrimitives.ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.ceil(NULL_INT), 1e-10);
    }

    public void testCos() {
        int value = -5;
        assertEquals(Math.cos(value), IntegerNumericPrimitives.cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.cos(NULL_INT), 1e-10);
    }

    public void testExp() {
        int value = -5;
        assertEquals(Math.exp(value), IntegerNumericPrimitives.exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.exp(NULL_INT), 1e-10);
    }

    public void testFloor() {
        int value = -5;
        assertEquals(Math.floor(value), IntegerNumericPrimitives.floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.floor(NULL_INT), 1e-10);
    }

    public void testLog() {
        int value = -5;
        assertEquals(Math.log(value), IntegerNumericPrimitives.log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.log(NULL_INT), 1e-10);
    }

    public void testPow() {
        int value0 = -5;
        int value1 = 2;
        assertEquals(Math.pow(value0, value1), IntegerNumericPrimitives.pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.pow(NULL_INT, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.pow(value0, NULL_INT), 1e-10);
    }

    public void testRint() {
        int value = -5;
        assertEquals(Math.rint(value), IntegerNumericPrimitives.rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.rint(NULL_INT), 1e-10);
    }

    public void testRound() {
        int value = -5;
        assertEquals(Math.round(value), IntegerNumericPrimitives.round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, IntegerNumericPrimitives.round(NULL_INT), 1e-10);
    }

    public void testSin() {
        int value = -5;
        assertEquals(Math.sin(value), IntegerNumericPrimitives.sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.sin(NULL_INT), 1e-10);
    }

    public void testSqrt() {
        int value = -5;
        assertEquals(Math.sqrt(value), IntegerNumericPrimitives.sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.sqrt(NULL_INT), 1e-10);
    }

    public void testTan() {
        int value = -5;
        assertEquals(Math.tan(value), IntegerNumericPrimitives.tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, IntegerNumericPrimitives.tan(NULL_INT), 1e-10);
    }

    public void testLowerBin() {
        int value = (int) 114;

        assertEquals((int) 110, IntegerNumericPrimitives.lowerBin(value, (int) 5));
        assertEquals((int) 110, IntegerNumericPrimitives.lowerBin(value, (int) 10));
        assertEquals((int) 100, IntegerNumericPrimitives.lowerBin(value, (int) 20));
        assertEquals(NULL_INT, IntegerNumericPrimitives.lowerBin(NULL_INT, (int) 5));
        assertEquals(NULL_INT, IntegerNumericPrimitives.lowerBin(value, NULL_INT));

        assertEquals(IntegerNumericPrimitives.lowerBin(value, (int) 5), IntegerNumericPrimitives.lowerBin(IntegerNumericPrimitives.lowerBin(value, (int) 5), (int) 5));
    }

    public void testLowerBinWithOffset() {
        int value = (int) 114;
        int offset = (int) 3;

        assertEquals((int) 113, IntegerNumericPrimitives.lowerBin(value, (int) 5, offset));
        assertEquals((int) 113, IntegerNumericPrimitives.lowerBin(value, (int) 10, offset));
        assertEquals((int) 103, IntegerNumericPrimitives.lowerBin(value, (int) 20, offset));
        assertEquals(NULL_INT, IntegerNumericPrimitives.lowerBin(NULL_INT, (int) 5, offset));
        assertEquals(NULL_INT, IntegerNumericPrimitives.lowerBin(value, NULL_INT, offset));

        assertEquals(IntegerNumericPrimitives.lowerBin(value, (int) 5, offset), IntegerNumericPrimitives.lowerBin(IntegerNumericPrimitives.lowerBin(value, (int) 5, offset), (int) 5, offset));
    }

    public void testUpperBin() {
        int value = (int) 114;

        assertEquals((int) 115, IntegerNumericPrimitives.upperBin(value, (int) 5));
        assertEquals((int) 120, IntegerNumericPrimitives.upperBin(value, (int) 10));
        assertEquals((int) 120, IntegerNumericPrimitives.upperBin(value, (int) 20));
        assertEquals(NULL_INT, IntegerNumericPrimitives.upperBin(NULL_INT, (int) 5));
        assertEquals(NULL_INT, IntegerNumericPrimitives.upperBin(value, NULL_INT));

        assertEquals(IntegerNumericPrimitives.upperBin(value, (int) 5), IntegerNumericPrimitives.upperBin(IntegerNumericPrimitives.upperBin(value, (int) 5), (int) 5));
    }

    public void testUpperBinWithOffset() {
        int value = (int) 114;
        int offset = (int) 3;

        assertEquals((int) 118, IntegerNumericPrimitives.upperBin(value, (int) 5, offset));
        assertEquals((int) 123, IntegerNumericPrimitives.upperBin(value, (int) 10, offset));
        assertEquals((int) 123, IntegerNumericPrimitives.upperBin(value, (int) 20, offset));
        assertEquals(NULL_INT, IntegerNumericPrimitives.upperBin(NULL_INT, (int) 5, offset));
        assertEquals(NULL_INT, IntegerNumericPrimitives.upperBin(value, NULL_INT, offset));

        assertEquals(IntegerNumericPrimitives.upperBin(value, (int) 5, offset), IntegerNumericPrimitives.upperBin(IntegerNumericPrimitives.upperBin(value, (int) 5, offset), (int) 5, offset));
    }

    public void testClamp() {
        assertEquals((int) 3, IntegerNumericPrimitives.clamp((int) 3, (int) -6, (int) 5));
        assertEquals((int) -6, IntegerNumericPrimitives.clamp((int) -7, (int) -6, (int) 5));
        assertEquals((int) 5, IntegerNumericPrimitives.clamp((int) 7, (int) -6, (int) 5));
        assertEquals(NULL_INT, IntegerNumericPrimitives.clamp(NULL_INT, (int) -6, (int) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, IntegerNumericPrimitives.binSearchIndex((int[]) null, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.binSearchIndex(new int[]{1,3,4}, (int)0, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.binSearchIndex(new int[]{1,3,4}, (int)1, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.binSearchIndex(new int[]{1,3,4}, (int)2, BinSearch.BS_ANY));
        assertEquals(1, IntegerNumericPrimitives.binSearchIndex(new int[]{1,3,4}, (int)3, BinSearch.BS_ANY));
        assertEquals(2, IntegerNumericPrimitives.binSearchIndex(new int[]{1,3,4}, (int)4, BinSearch.BS_ANY));
        assertEquals(2, IntegerNumericPrimitives.binSearchIndex(new int[]{1,3,4}, (int)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, IntegerNumericPrimitives.binSearchIndex((IntVector) null, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.binSearchIndex(new IntVectorDirect(new int[]{1,3,4}), (int)0, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.binSearchIndex(new IntVectorDirect(new int[]{1,3,4}), (int)1, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.binSearchIndex(new IntVectorDirect(new int[]{1,3,4}), (int)2, BinSearch.BS_ANY));
        assertEquals(1, IntegerNumericPrimitives.binSearchIndex(new IntVectorDirect(new int[]{1,3,4}), (int)3, BinSearch.BS_ANY));
        assertEquals(2, IntegerNumericPrimitives.binSearchIndex(new IntVectorDirect(new int[]{1,3,4}), (int)4, BinSearch.BS_ANY));
        assertEquals(2, IntegerNumericPrimitives.binSearchIndex(new IntVectorDirect(new int[]{1,3,4}), (int)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, IntegerNumericPrimitives.rawBinSearchIndex((IntVector)null, (int) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, IntegerNumericPrimitives.rawBinSearchIndex((IntVector)null, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, IntegerNumericPrimitives.rawBinSearchIndex((IntVector)null, (int) 0, BinSearch.BS_LOWEST));

        int[] empty = {};
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(empty), (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(empty), (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(empty), (int) 0, BinSearch.BS_LOWEST));

        int[] one = {11};
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 12, BinSearch.BS_ANY));
        assertEquals(-2, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 12, BinSearch.BS_LOWEST));

        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 11, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(one), (int) 11, BinSearch.BS_LOWEST));


        int[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        IntegerNumericPrimitives.rawBinSearchIndex((IntVector)null, (int) 0, null);

        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 26, BinSearch.BS_LOWEST));

        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 1, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 1, BinSearch.BS_LOWEST));

        assertEquals(2, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 2, BinSearch.BS_LOWEST));

        assertEquals(5, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 3, BinSearch.BS_LOWEST));

        assertEquals(9, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 4, BinSearch.BS_LOWEST));

        assertEquals(14, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 7, BinSearch.BS_ANY));
        assertEquals(-16, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 7, BinSearch.BS_LOWEST));

        assertEquals(19, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 10, BinSearch.BS_LOWEST));

        assertEquals(24, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 11, BinSearch.BS_LOWEST));

        assertEquals(25, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 15, BinSearch.BS_ANY));
        assertEquals(25, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 15, BinSearch.BS_LOWEST));

        assertEquals(29, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, IntegerNumericPrimitives.rawBinSearchIndex(new IntVectorDirect(v), (int) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, IntegerNumericPrimitives.rawBinSearchIndex((int[]) null, (int) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, IntegerNumericPrimitives.rawBinSearchIndex((int[])null, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, IntegerNumericPrimitives.rawBinSearchIndex((int[])null, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(empty, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(empty, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(empty, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 12, BinSearch.BS_ANY));
        assertEquals(-2, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 12, BinSearch.BS_LOWEST));

        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 11, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(one, (int) 11, BinSearch.BS_LOWEST));


        IntegerNumericPrimitives.rawBinSearchIndex((int[])null, (int) 0, null);

        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 26, BinSearch.BS_LOWEST));

        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 1, BinSearch.BS_ANY));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 1, BinSearch.BS_LOWEST));

        assertEquals(2, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 2, BinSearch.BS_LOWEST));

        assertEquals(5, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 3, BinSearch.BS_LOWEST));

        assertEquals(9, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 4, BinSearch.BS_LOWEST));

        assertEquals(14, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 7, BinSearch.BS_ANY));
        assertEquals(-16, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 7, BinSearch.BS_LOWEST));

        assertEquals(19, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 10, BinSearch.BS_LOWEST));

        assertEquals(24, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 11, BinSearch.BS_LOWEST));

        assertEquals(25, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 15, BinSearch.BS_ANY));
        assertEquals(25, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 15, BinSearch.BS_LOWEST));

        assertEquals(29, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, IntegerNumericPrimitives.rawBinSearchIndex(v, (int) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final int[] ints = new int[]{1, -5, -2, -2, 96, 0, 12, NULL_INT, NULL_INT};
        final IntVector sort = IntegerNumericPrimitives.sort(new IntVectorDirect(ints));
        final IntVector expected = new IntVectorDirect(new int[]{NULL_INT, NULL_INT, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        int[] sortedArray = IntegerNumericPrimitives.sort(ints);
        assertEquals(new int[]{NULL_INT, NULL_INT, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(IntegerNumericPrimitives.sort((IntVector)null));
        assertNull(IntegerNumericPrimitives.sort((int[])null));
        assertNull(IntegerNumericPrimitives.sort((Integer[])null));
        assertEquals(new IntVectorDirect(), IntegerNumericPrimitives.sort(new IntVectorDirect()));
        assertEquals(new int[]{}, IntegerNumericPrimitives.sort(new int[]{}));
        assertEquals(new int[]{}, IntegerNumericPrimitives.sort(new Integer[]{}));
    }

    public void testSortDescending() {
        final int[] ints = new int[]{1, -5, -2, -2, 96, 0, 12, NULL_INT, NULL_INT};
        final IntVector sort = IntegerNumericPrimitives.sortDescending(new IntVectorDirect(ints));
        final IntVector expected = new IntVectorDirect(new int[]{96, 12, 1, 0, -2, -2, -5, NULL_INT, NULL_INT});
        assertEquals(expected, sort);

        int[] sortedArray = IntegerNumericPrimitives.sortDescending(ints);
        assertEquals(new int[]{96, 12, 1, 0, -2, -2, -5, NULL_INT, NULL_INT}, sortedArray);

        assertNull(IntegerNumericPrimitives.sortDescending((IntVector)null));
        assertNull(IntegerNumericPrimitives.sortDescending((int[])null));
        assertNull(IntegerNumericPrimitives.sortDescending((Integer[])null));
        assertEquals(new IntVectorDirect(), IntegerNumericPrimitives.sortDescending(new IntVectorDirect()));
        assertEquals(new int[]{}, IntegerNumericPrimitives.sortDescending(new int[]{}));
        assertEquals(new int[]{}, IntegerNumericPrimitives.sortDescending(new Integer[]{}));
    }

    public void testSortsExceptions() {
        IntVector intVector = null;
        IntVector sort = IntegerNumericPrimitives.sort(intVector);
        assertNull(sort);

        int[] ints = null;
        int[] sortArray = IntegerNumericPrimitives.sort(ints);
        assertNull(sortArray);

        ints = new int[]{};
        sort = IntegerNumericPrimitives.sort(new IntVectorDirect(ints));
        assertEquals(new IntVectorDirect(), sort);

        sortArray = IntegerNumericPrimitives.sort(ints);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        IntVector intVector = null;
        IntVector sort = IntegerNumericPrimitives.sortDescending(intVector);
        assertNull(sort);

        int[] ints = null;
        int[] sortArray = IntegerNumericPrimitives.sortDescending(ints);
        assertNull(sortArray);

        ints = new int[]{};
        sort = IntegerNumericPrimitives.sortDescending(new IntVectorDirect(ints));
        assertEquals(new IntVectorDirect(), sort);

        sortArray = IntegerNumericPrimitives.sortDescending(ints);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSequence(){
        assertEquals(new int[]{0,1,2,3,4,5}, IntegerNumericPrimitives.sequence((int)0, (int)5, (int)1));
        assertEquals(new int[]{-5,-4,-3,-2,-1,0}, IntegerNumericPrimitives.sequence((int)-5, (int)0, (int)1));

        assertEquals(new int[]{0,2,4}, IntegerNumericPrimitives.sequence((int)0, (int)5, (int)2));
        assertEquals(new int[]{-5,-3,-1}, IntegerNumericPrimitives.sequence((int)-5, (int)0, (int)2));

        assertEquals(new int[]{5,3,1}, IntegerNumericPrimitives.sequence((int)5, (int)0, (int)-2));
        assertEquals(new int[]{0,-2,-4}, IntegerNumericPrimitives.sequence((int)0, (int)-5, (int)-2));

        assertEquals(new int[]{}, IntegerNumericPrimitives.sequence((int)0, (int)5, (int)0));
        assertEquals(new int[]{}, IntegerNumericPrimitives.sequence((int)5, (int)0, (int)1));
    }

    public void testMedian() {
        assertEquals(3.0, IntegerNumericPrimitives.median(new int[]{4,2,3}));
        assertEquals(3.5, IntegerNumericPrimitives.median(new int[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.median((int[])null));

        assertEquals(3.0, IntegerNumericPrimitives.median(new Integer[]{(int)4,(int)2,(int)3}));
        assertEquals(3.5, IntegerNumericPrimitives.median(new Integer[]{(int)5,(int)4,(int)2,(int)3}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.median((Integer[])null));

        assertEquals(3.0, IntegerNumericPrimitives.median(new IntVectorDirect(new int[]{4,2,3})));
        assertEquals(3.5, IntegerNumericPrimitives.median(new IntVectorDirect(new int[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.median((IntVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, IntegerNumericPrimitives.percentile(0.00, new int[]{4,2,3}));
        assertEquals(3.0, IntegerNumericPrimitives.percentile(0.50, new int[]{4,2,3}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.percentile(0.25, (int[])null));

        assertEquals(2.0, IntegerNumericPrimitives.percentile(0.00, new IntVectorDirect(new int[]{4,2,3})));
        assertEquals(3.0, IntegerNumericPrimitives.percentile(0.50, new IntVectorDirect(new int[]{4,2,3})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.percentile(0.25, (IntVector) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wsum(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedSum(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wavg(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.weightedAvg(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wvar(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wstd(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wste(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wste(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }

    public void testWtstat() {
        final double target = IntegerNumericPrimitives.wavg(new int[]{1,2,3}, new int[]{4,5,6}) / IntegerNumericPrimitives.wste(new int[]{1,2,3}, new int[]{4,5,6});

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new int[]{1,2,3,NULL_INT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((int[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new int[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (IntVector) null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (LongVector) null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3,NULL_INT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat((IntVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, IntegerNumericPrimitives.wtstat(new IntVectorDirect(new int[]{1,2,3}), (FloatVector)null));
    }
}
