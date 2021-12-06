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
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.function.LongPrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestLongNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((long) 1, LongNumericPrimitives.signum((long) 5));
        assertEquals((long) 0, LongNumericPrimitives.signum((long) 0));
        assertEquals((long) -1, LongNumericPrimitives.signum((long) -5));
        assertEquals(NULL_LONG, LongNumericPrimitives.signum(NULL_LONG));
    }

    public void testAvg() {
        assertEquals(50.0, LongNumericPrimitives.avg(new long[]{40, 50, 60}));
        assertEquals(45.5, LongNumericPrimitives.avg(new long[]{40, 51}));
        assertTrue(Double.isNaN(LongNumericPrimitives.avg(new long[]{})));
        assertTrue(Double.isNaN(LongNumericPrimitives.avg(new long[]{NULL_LONG})));
        assertEquals(10.0, LongNumericPrimitives.avg(new long[]{5, NULL_LONG, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.avg((long[])null));

        assertEquals(50.0, LongNumericPrimitives.avg(new Long[]{(long)40, (long)50, (long)60}));
        assertEquals(45.5, LongNumericPrimitives.avg(new Long[]{(long)40, (long)51}));
        assertTrue(Double.isNaN(LongNumericPrimitives.avg(new Long[]{})));
        assertTrue(Double.isNaN(LongNumericPrimitives.avg(new Long[]{NULL_LONG})));
        assertEquals(10.0, LongNumericPrimitives.avg(new Long[]{(long)5, NULL_LONG, (long)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.avg((Long[])null));

        assertEquals(50.0, LongNumericPrimitives.avg(new LongVectorDirect(new long[]{40, 50, 60})));
        assertEquals(45.5, LongNumericPrimitives.avg(new LongVectorDirect(new long[]{40, 51})));
        assertTrue(Double.isNaN(LongNumericPrimitives.avg(new LongVectorDirect())));
        assertTrue(Double.isNaN(LongNumericPrimitives.avg(new LongVectorDirect(NULL_LONG))));
        assertEquals(10.0, LongNumericPrimitives.avg(new LongVectorDirect(new long[]{5, NULL_LONG, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.avg((LongVectorDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, LongNumericPrimitives.absAvg(new long[]{40, (long) 50, 60}));
        assertEquals(45.5, LongNumericPrimitives.absAvg(new long[]{(long) 40, 51}));
        assertTrue(Double.isNaN(LongNumericPrimitives.absAvg(new long[]{})));
        assertTrue(Double.isNaN(LongNumericPrimitives.absAvg(new long[]{NULL_LONG})));
        assertEquals(10.0, LongNumericPrimitives.absAvg(new long[]{(long) 5, NULL_LONG, (long) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.absAvg((long[])null));

        assertEquals(50.0, LongNumericPrimitives.absAvg(new Long[]{(long)40, (long) 50, (long)60}));
        assertEquals(45.5, LongNumericPrimitives.absAvg(new Long[]{(long) 40, (long)51}));
        assertTrue(Double.isNaN(LongNumericPrimitives.absAvg(new Long[]{})));
        assertTrue(Double.isNaN(LongNumericPrimitives.absAvg(new Long[]{NULL_LONG})));
        assertEquals(10.0, LongNumericPrimitives.absAvg(new Long[]{(long) 5, NULL_LONG, (long) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.absAvg((Long[])null));

        assertEquals(50.0, LongNumericPrimitives.absAvg(new LongVectorDirect(new long[]{40, (long) 50, 60})));
        assertEquals(45.5, LongNumericPrimitives.absAvg(new LongVectorDirect(new long[]{(long) 40, 51})));
        assertTrue(Double.isNaN(LongNumericPrimitives.absAvg(new LongVectorDirect())));
        assertTrue(Double.isNaN(LongNumericPrimitives.absAvg(new LongVectorDirect(NULL_LONG))));
        assertEquals(10.0, LongNumericPrimitives.absAvg(new LongVectorDirect((long) 5, NULL_LONG, (long) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.absAvg((LongVectorDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, LongNumericPrimitives.countPos(new long[]{40, 50, 60, (long) 1, 0}));
        assertEquals(0, LongNumericPrimitives.countPos(new long[]{}));
        assertEquals(0, LongNumericPrimitives.countPos(new long[]{NULL_LONG}));
        assertEquals(3, LongNumericPrimitives.countPos(new long[]{5, NULL_LONG, 15, (long) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countPos((long[])null));

        assertEquals(4, LongNumericPrimitives.countPos(new Long[]{(long)40, (long)50, (long)60, (long) 1, (long)0}));
        assertEquals(0, LongNumericPrimitives.countPos(new Long[]{}));
        assertEquals(0, LongNumericPrimitives.countPos(new Long[]{NULL_LONG}));
        assertEquals(3, LongNumericPrimitives.countPos(new Long[]{(long)5, NULL_LONG, (long)15, (long) 1, (long)0}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countPos((long[])null));

        assertEquals(4, LongNumericPrimitives.countPos(new LongVectorDirect(new long[]{40, 50, 60, (long) 1, 0})));
        assertEquals(0, LongNumericPrimitives.countPos(new LongVectorDirect()));
        assertEquals(0, LongNumericPrimitives.countPos(new LongVectorDirect(NULL_LONG)));
        assertEquals(3, LongNumericPrimitives.countPos(new LongVectorDirect(new long[]{5, NULL_LONG, 15, (long) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countPos((LongVectorDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, LongNumericPrimitives.countNeg(new long[]{40, (long) -50, 60, (long) -1, 0}));
        assertEquals(0, LongNumericPrimitives.countNeg(new long[]{}));
        assertEquals(0, LongNumericPrimitives.countNeg(new long[]{NULL_LONG}));
        assertEquals(1, LongNumericPrimitives.countNeg(new long[]{5, NULL_LONG, 15, (long) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countNeg((long[])null));

        assertEquals(2, LongNumericPrimitives.countNeg(new Long[]{(long)40, (long) -50, (long)60, (long) -1, (long)0}));
        assertEquals(0, LongNumericPrimitives.countNeg(new Long[]{}));
        assertEquals(0, LongNumericPrimitives.countNeg(new Long[]{NULL_LONG}));
        assertEquals(1, LongNumericPrimitives.countNeg(new Long[]{(long)5, NULL_LONG, (long)15, (long) -1, (long)0}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countNeg((Long[])null));

        assertEquals(2, LongNumericPrimitives.countNeg(new LongVectorDirect(new long[]{40, (long) -50, 60, (long) -1, 0})));
        assertEquals(0, LongNumericPrimitives.countNeg(new LongVectorDirect()));
        assertEquals(0, LongNumericPrimitives.countNeg(new LongVectorDirect(NULL_LONG)));
        assertEquals(1, LongNumericPrimitives.countNeg(new LongVectorDirect(new long[]{5, NULL_LONG, 15, (long) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countNeg((LongVectorDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, LongNumericPrimitives.countZero(new long[]{0, 40, 50, 60, (long) -1, 0}));
        assertEquals(0, LongNumericPrimitives.countZero(new long[]{}));
        assertEquals(0, LongNumericPrimitives.countZero(new long[]{NULL_LONG}));
        assertEquals(2, LongNumericPrimitives.countZero(new long[]{0, 5, NULL_LONG, 0, (long) -15}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countZero((long[])null));

        assertEquals(2, LongNumericPrimitives.countZero(new Long[]{(long)0, (long)40, (long)50, (long)60, (long) -1, (long)0}));
        assertEquals(0, LongNumericPrimitives.countZero(new Long[]{}));
        assertEquals(0, LongNumericPrimitives.countZero(new Long[]{NULL_LONG}));
        assertEquals(2, LongNumericPrimitives.countZero(new Long[]{(long)0, (long)5, NULL_LONG, (long)0, (long) -15}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countZero((Long[])null));

        assertEquals(2, LongNumericPrimitives.countZero(new LongVectorDirect(new long[]{0, 40, 50, 60, (long) -1, 0})));
        assertEquals(0, LongNumericPrimitives.countZero(new LongVectorDirect()));
        assertEquals(0, LongNumericPrimitives.countZero(new LongVectorDirect(NULL_LONG)));
        assertEquals(2, LongNumericPrimitives.countZero(new LongVectorDirect(new long[]{0, 5, NULL_LONG, 0, (long) -15})));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.countZero((LongVectorDirect)null));
    }

    public void testMax() {
        assertEquals((long) 60, LongNumericPrimitives.max(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals((long) 60, LongNumericPrimitives.max(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1)));
        assertEquals(NULL_LONG, LongNumericPrimitives.max(new LongVectorDirect()));
        assertEquals(NULL_LONG, LongNumericPrimitives.max(new LongVectorDirect(NULL_LONG)));
        assertEquals(NULL_LONG, LongNumericPrimitives.max((LongVector) null));

        assertEquals((long) 60, LongNumericPrimitives.max((long) 0, (long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1, (long) 0));
        assertEquals((long) 60, LongNumericPrimitives.max((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1));
        assertEquals(NULL_LONG, LongNumericPrimitives.max());
        assertEquals(NULL_LONG, LongNumericPrimitives.max(NULL_LONG));
        assertEquals(NULL_LONG, LongNumericPrimitives.max((long[]) null));
        assertEquals(NULL_LONG, LongNumericPrimitives.max((Long[]) null));
    }

    public void testMin() {
        assertEquals((long) 0, LongNumericPrimitives.min(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals((long) -1, LongNumericPrimitives.min(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1)));
        assertEquals(NULL_LONG, LongNumericPrimitives.min(new LongVectorDirect()));
        assertEquals(NULL_LONG, LongNumericPrimitives.min(new LongVectorDirect(NULL_LONG)));
        assertEquals(NULL_LONG, LongNumericPrimitives.min((LongVector) null));

        assertEquals((long) 0, LongNumericPrimitives.min((long) 0, (long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1, (long) 0));
        assertEquals((long) -1, LongNumericPrimitives.min((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1));
        assertEquals(NULL_LONG, LongNumericPrimitives.min());
        assertEquals(NULL_LONG, LongNumericPrimitives.min(NULL_LONG));
        assertEquals(NULL_LONG, LongNumericPrimitives.min((long[]) null));
        assertEquals(NULL_LONG, LongNumericPrimitives.min((Long[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, LongNumericPrimitives.firstIndexOf(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}, (long)40));
        assertEquals(4, LongNumericPrimitives.firstIndexOf(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}, (long)60));
        assertEquals(NULL_INT, LongNumericPrimitives.firstIndexOf(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}, (long)1));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.firstIndexOf((long[])null, (long)40));

        assertEquals(1, LongNumericPrimitives.firstIndexOf(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)40));
        assertEquals(4, LongNumericPrimitives.firstIndexOf(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)60));
        assertEquals(NULL_INT, LongNumericPrimitives.firstIndexOf(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)1));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.firstIndexOf((LongVector) null, (long)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, LongNumericPrimitives.indexOfMax(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0}));
        assertEquals(3, LongNumericPrimitives.indexOfMax(new long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1}));
        assertEquals(-1, LongNumericPrimitives.indexOfMax(new long[]{}));
        assertEquals(-1, LongNumericPrimitives.indexOfMax(new long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.indexOfMax((long[])null));

        assertEquals(4, LongNumericPrimitives.indexOfMax(new Long[]{(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) 1, (long)0}));
        assertEquals(3, LongNumericPrimitives.indexOfMax(new Long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1}));
        assertEquals(-1, LongNumericPrimitives.indexOfMax(new Long[]{}));
        assertEquals(-1, LongNumericPrimitives.indexOfMax(new Long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.indexOfMax((Long[])null));

        assertEquals(4, LongNumericPrimitives.indexOfMax(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals(3, LongNumericPrimitives.indexOfMax(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1)));
        assertEquals(-1, LongNumericPrimitives.indexOfMax(new LongVectorDirect()));
        assertEquals(-1, LongNumericPrimitives.indexOfMax(new LongVectorDirect(NULL_LONG)));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.indexOfMax((LongVectorDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, LongNumericPrimitives.indexOfMin(new long[]{40, 0, NULL_LONG, 50, 60, (long) 1, 0}));
        assertEquals(4, LongNumericPrimitives.indexOfMin(new long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1}));
        assertEquals(-1, LongNumericPrimitives.indexOfMin(new long[]{}));
        assertEquals(-1, LongNumericPrimitives.indexOfMin(new long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.indexOfMin((long[])null));

        assertEquals(1, LongNumericPrimitives.indexOfMin(new Long[]{(long)40, (long)0, NULL_LONG, (long)50, (long)60, (long) 1, (long)0}));
        assertEquals(4, LongNumericPrimitives.indexOfMin(new Long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1}));
        assertEquals(-1, LongNumericPrimitives.indexOfMin(new Long[]{}));
        assertEquals(-1, LongNumericPrimitives.indexOfMin(new Long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.indexOfMin((Long[])null));

        assertEquals(1, LongNumericPrimitives.indexOfMin(new LongVectorDirect(new long[]{40, 0, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals(4, LongNumericPrimitives.indexOfMin(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1)));
        assertEquals(-1, LongNumericPrimitives.indexOfMin(new LongVectorDirect()));
        assertEquals(-1, LongNumericPrimitives.indexOfMin(new LongVectorDirect(NULL_LONG)));
        assertEquals(QueryConstants.NULL_INT, LongNumericPrimitives.indexOfMin((LongVectorDirect)null));
    }


    public void testVar() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, LongNumericPrimitives.var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.var((long[])null));

        assertEquals(var, LongNumericPrimitives.var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.var((Long[])null));

        assertEquals(var, LongNumericPrimitives.var(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.var((LongVectorDirect)null));
    }

    public void testStd() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(Math.sqrt(LongNumericPrimitives.var(new LongVectorDirect(v))), LongNumericPrimitives.std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.std((long[])null));

        assertEquals(Math.sqrt(LongNumericPrimitives.var(new LongVectorDirect(v))), LongNumericPrimitives.std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.std((Long[])null));

        assertEquals(Math.sqrt(LongNumericPrimitives.var(new LongVectorDirect(v))), LongNumericPrimitives.std(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.std((LongVectorDirect)null));
    }

    public void testSte() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(LongNumericPrimitives.std(new LongVectorDirect(v)) / Math.sqrt(count(new LongVectorDirect(v))), LongNumericPrimitives.ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.ste((long[])null));

        assertEquals(LongNumericPrimitives.std(new LongVectorDirect(v)) / Math.sqrt(count(new LongVectorDirect(v))), LongNumericPrimitives.ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.ste((Long[])null));

        assertEquals(LongNumericPrimitives.std(new LongVectorDirect(v)) / Math.sqrt(count(new LongVectorDirect(v))), LongNumericPrimitives.ste(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.ste((LongVectorDirect)null));
    }

    public void testTstat() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(LongNumericPrimitives.avg(new LongVectorDirect(v)) / LongNumericPrimitives.ste(new LongVectorDirect(v)), LongNumericPrimitives.tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.tstat((long[])null));

        assertEquals(LongNumericPrimitives.avg(new LongVectorDirect(v)) / LongNumericPrimitives.ste(new LongVectorDirect(v)), LongNumericPrimitives.tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.tstat((Long[])null));

        assertEquals(LongNumericPrimitives.avg(new LongVectorDirect(v)) / LongNumericPrimitives.ste(new LongVectorDirect(v)), LongNumericPrimitives.tstat(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.tstat((LongVectorDirect)null));
    }

    public void testCov() {
        long[] a = {10, 40, NULL_LONG, 50, NULL_LONG, (long) -1, 0, (long) -7};
        long[] b = {0, (long) -40, NULL_LONG, NULL_LONG, 6, (long) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, LongNumericPrimitives.cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov(a, (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((long[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((long[])null, (long[]) null));

        assertEquals(cov, LongNumericPrimitives.cov(a, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov(a, (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((long[])null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((long[])null, (LongVectorDirect)null));

        assertEquals(cov, LongNumericPrimitives.cov(new LongVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov(new LongVectorDirect(a), (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((LongVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((LongVectorDirect)null, (long[])null));

        assertEquals(cov, LongNumericPrimitives.cov(new LongVectorDirect(a), new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov(new LongVectorDirect(a), (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((LongVectorDirect)null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cov((LongVectorDirect)null, (LongVectorDirect)null));
    }

    public void testCor() {
        long[] a = {10, 40, NULL_LONG, 50, NULL_LONG, (long) -1, 0, (long) -7};
        long[] b = {0, (long) -40, NULL_LONG, NULL_LONG, 6, (long) -1, 11, 3};
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

        assertEquals(cor, LongNumericPrimitives.cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor(a, (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((long[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((long[])null, (long[])null));

        assertEquals(cor, LongNumericPrimitives.cor(a, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor(a, (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((long[])null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((long[])null, (LongVectorDirect)null));

        assertEquals(cor, LongNumericPrimitives.cor(new LongVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor(new LongVectorDirect(a), (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((LongVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((LongVectorDirect)null, (long[])null));

        assertEquals(cor, LongNumericPrimitives.cor(new LongVectorDirect(a), new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor(new LongVectorDirect(a), (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((LongVectorDirect)null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cor((LongVectorDirect)null, (LongVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - LongNumericPrimitives.sum(new LongVectorDirect(new long[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - LongNumericPrimitives.sum(new LongVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - LongNumericPrimitives.sum(new LongVectorDirect(NULL_LONG))) == 0.0);
        assertTrue(Math.abs(20 - LongNumericPrimitives.sum(new LongVectorDirect(new long[]{5, NULL_LONG, 15}))) == 0.0);
        assertEquals(NULL_LONG, LongNumericPrimitives.sum((LongVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - LongNumericPrimitives.sum(new long[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - LongNumericPrimitives.sum(new long[]{})) == 0.0);
        assertTrue(Math.abs(0 - LongNumericPrimitives.sum(new long[]{NULL_LONG})) == 0.0);
        assertTrue(Math.abs(20 - LongNumericPrimitives.sum(new long[]{5, NULL_LONG, 15})) == 0.0);
        assertEquals(NULL_LONG, LongNumericPrimitives.sum((long[]) null));
    }

    public void testSumVector() {
        assertEquals(new long[]{4, 15}, LongNumericPrimitives.sum(new ObjectVectorDirect<>(new long[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new long[]{4, NULL_LONG}, LongNumericPrimitives.sum(new ObjectVectorDirect<>(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}})));
        assertEquals(null, LongNumericPrimitives.sum((ObjectVector<long[]>) null));

        try {
            LongNumericPrimitives.sum(new ObjectVectorDirect<>(new long[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new long[]{4, 15}, LongNumericPrimitives.sum(new long[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new long[]{4, NULL_LONG}, LongNumericPrimitives.sum(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}}));
        assertEquals(null, LongNumericPrimitives.sum((long[][]) null));

        try {
            LongNumericPrimitives.sum(new long[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - LongNumericPrimitives.product(new long[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_LONG, LongNumericPrimitives.product(new long[]{}));
        assertEquals(NULL_LONG, LongNumericPrimitives.product(new long[]{NULL_LONG}));
        assertTrue(Math.abs(75 - LongNumericPrimitives.product(new long[]{5, NULL_LONG, 15})) == 0.0);
        assertEquals(NULL_LONG, LongNumericPrimitives.product((long[]) null));

        assertTrue(Math.abs(120 - LongNumericPrimitives.product(new LongVectorDirect(new long[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_LONG, LongNumericPrimitives.product(new LongVectorDirect()));
        assertEquals(NULL_LONG, LongNumericPrimitives.product(new LongVectorDirect(NULL_LONG)));
        assertTrue(Math.abs(75 - LongNumericPrimitives.product(new LongVectorDirect(new long[]{5, NULL_LONG, 15}))) == 0.0);
        assertEquals(NULL_LONG, LongNumericPrimitives.product((LongVector) null));
    }

//    public void testProdVector() {
//        assertEquals(new long[]{-30, 120}, product(new ObjectVectorDirect<>(new long[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new long[]{-30, NULL_LONG}, product(new ObjectVectorDirect<>(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((Vector<long[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new long[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void testProdArray() {
//        assertEquals(new long[]{-30, 120}, product(new long[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new long[]{-30, NULL_LONG}, product(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((long[][]) null));
//
//        try {
//            product(new long[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void testCumSumArray() {
        assertEquals(new long[]{1, 3, 6, 10, 15}, LongNumericPrimitives.cumsum(new long[]{1, 2, 3, 4, 5}));
        assertEquals(new long[]{1, 3, 6, 6, 11}, LongNumericPrimitives.cumsum(new long[]{1, 2, 3, NULL_LONG, 5}));
        assertEquals(new long[]{NULL_LONG, 2, 5, 9, 14}, LongNumericPrimitives.cumsum(new long[]{NULL_LONG, 2, 3, 4, 5}));
        assertEquals(new long[0], LongNumericPrimitives.cumsum());
        assertEquals(null, LongNumericPrimitives.cumsum((long[]) null));

        assertEquals(new long[]{1, 3, 6, 10, 15}, LongNumericPrimitives.cumsum(new LongVectorDirect(new long[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 3, 6, 6, 11}, LongNumericPrimitives.cumsum(new LongVectorDirect(new long[]{1, 2, 3, NULL_LONG, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 5, 9, 14}, LongNumericPrimitives.cumsum(new LongVectorDirect(new long[]{NULL_LONG, 2, 3, 4, 5})));
        assertEquals(new long[0], LongNumericPrimitives.cumsum(new LongVectorDirect()));
        assertEquals(null, LongNumericPrimitives.cumsum((LongVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new long[]{1, 2, 6, 24, 120}, LongNumericPrimitives.cumprod(new long[]{1, 2, 3, 4, 5}));
        assertEquals(new long[]{1, 2, 6, 6, 30}, LongNumericPrimitives.cumprod(new long[]{1, 2, 3, NULL_LONG, 5}));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, LongNumericPrimitives.cumprod(new long[]{NULL_LONG, 2, 3, 4, 5}));
        assertEquals(new long[0], LongNumericPrimitives.cumprod());
        assertEquals(null, LongNumericPrimitives.cumprod((long[]) null));

        assertEquals(new long[]{1, 2, 6, 24, 120}, LongNumericPrimitives.cumprod(new LongVectorDirect(new long[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 2, 6, 6, 30}, LongNumericPrimitives.cumprod(new LongVectorDirect(new long[]{1, 2, 3, NULL_LONG, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, LongNumericPrimitives.cumprod(new LongVectorDirect(new long[]{NULL_LONG, 2, 3, 4, 5})));
        assertEquals(new long[0], LongNumericPrimitives.cumprod(new LongVectorDirect()));
        assertEquals(null, LongNumericPrimitives.cumprod((LongVector) null));
    }

    public void testAbs() {
        long value = -5;
        assertEquals((long) Math.abs(value), LongNumericPrimitives.abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, LongNumericPrimitives.abs(NULL_LONG), 1e-10);
    }

    public void testAcos() {
        long value = -5;
        assertEquals(Math.acos(value), LongNumericPrimitives.acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.acos(NULL_LONG), 1e-10);
    }

    public void testAsin() {
        long value = -5;
        assertEquals(Math.asin(value), LongNumericPrimitives.asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.asin(NULL_LONG), 1e-10);
    }

    public void testAtan() {
        long value = -5;
        assertEquals(Math.atan(value), LongNumericPrimitives.atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.atan(NULL_LONG), 1e-10);
    }

    public void testCeil() {
        long value = -5;
        assertEquals(Math.ceil(value), LongNumericPrimitives.ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.ceil(NULL_LONG), 1e-10);
    }

    public void testCos() {
        long value = -5;
        assertEquals(Math.cos(value), LongNumericPrimitives.cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.cos(NULL_LONG), 1e-10);
    }

    public void testExp() {
        long value = -5;
        assertEquals(Math.exp(value), LongNumericPrimitives.exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.exp(NULL_LONG), 1e-10);
    }

    public void testFloor() {
        long value = -5;
        assertEquals(Math.floor(value), LongNumericPrimitives.floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.floor(NULL_LONG), 1e-10);
    }

    public void testLog() {
        long value = -5;
        assertEquals(Math.log(value), LongNumericPrimitives.log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.log(NULL_LONG), 1e-10);
    }

    public void testPow() {
        long value0 = -5;
        long value1 = 2;
        assertEquals(Math.pow(value0, value1), LongNumericPrimitives.pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.pow(NULL_LONG, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.pow(value0, NULL_LONG), 1e-10);
    }

    public void testRint() {
        long value = -5;
        assertEquals(Math.rint(value), LongNumericPrimitives.rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.rint(NULL_LONG), 1e-10);
    }

    public void testRound() {
        long value = -5;
        assertEquals(Math.round(value), LongNumericPrimitives.round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, LongNumericPrimitives.round(NULL_LONG), 1e-10);
    }

    public void testSin() {
        long value = -5;
        assertEquals(Math.sin(value), LongNumericPrimitives.sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.sin(NULL_LONG), 1e-10);
    }

    public void testSqrt() {
        long value = -5;
        assertEquals(Math.sqrt(value), LongNumericPrimitives.sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.sqrt(NULL_LONG), 1e-10);
    }

    public void testTan() {
        long value = -5;
        assertEquals(Math.tan(value), LongNumericPrimitives.tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, LongNumericPrimitives.tan(NULL_LONG), 1e-10);
    }

    public void testLowerBin() {
        long value = (long) 114;

        assertEquals((long) 110, LongNumericPrimitives.lowerBin(value, (long) 5));
        assertEquals((long) 110, LongNumericPrimitives.lowerBin(value, (long) 10));
        assertEquals((long) 100, LongNumericPrimitives.lowerBin(value, (long) 20));
        assertEquals(NULL_LONG, LongNumericPrimitives.lowerBin(NULL_LONG, (long) 5));
        assertEquals(NULL_LONG, LongNumericPrimitives.lowerBin(value, NULL_LONG));

        assertEquals(LongNumericPrimitives.lowerBin(value, (long) 5), LongNumericPrimitives.lowerBin(LongNumericPrimitives.lowerBin(value, (long) 5), (long) 5));
    }

    public void testLowerBinWithOffset() {
        long value = (long) 114;
        long offset = (long) 3;

        assertEquals((long) 113, LongNumericPrimitives.lowerBin(value, (long) 5, offset));
        assertEquals((long) 113, LongNumericPrimitives.lowerBin(value, (long) 10, offset));
        assertEquals((long) 103, LongNumericPrimitives.lowerBin(value, (long) 20, offset));
        assertEquals(NULL_LONG, LongNumericPrimitives.lowerBin(NULL_LONG, (long) 5, offset));
        assertEquals(NULL_LONG, LongNumericPrimitives.lowerBin(value, NULL_LONG, offset));

        assertEquals(LongNumericPrimitives.lowerBin(value, (long) 5, offset), LongNumericPrimitives.lowerBin(LongNumericPrimitives.lowerBin(value, (long) 5, offset), (long) 5, offset));
    }

    public void testUpperBin() {
        long value = (long) 114;

        assertEquals((long) 115, LongNumericPrimitives.upperBin(value, (long) 5));
        assertEquals((long) 120, LongNumericPrimitives.upperBin(value, (long) 10));
        assertEquals((long) 120, LongNumericPrimitives.upperBin(value, (long) 20));
        assertEquals(NULL_LONG, LongNumericPrimitives.upperBin(NULL_LONG, (long) 5));
        assertEquals(NULL_LONG, LongNumericPrimitives.upperBin(value, NULL_LONG));

        assertEquals(LongNumericPrimitives.upperBin(value, (long) 5), LongNumericPrimitives.upperBin(LongNumericPrimitives.upperBin(value, (long) 5), (long) 5));
    }

    public void testUpperBinWithOffset() {
        long value = (long) 114;
        long offset = (long) 3;

        assertEquals((long) 118, LongNumericPrimitives.upperBin(value, (long) 5, offset));
        assertEquals((long) 123, LongNumericPrimitives.upperBin(value, (long) 10, offset));
        assertEquals((long) 123, LongNumericPrimitives.upperBin(value, (long) 20, offset));
        assertEquals(NULL_LONG, LongNumericPrimitives.upperBin(NULL_LONG, (long) 5, offset));
        assertEquals(NULL_LONG, LongNumericPrimitives.upperBin(value, NULL_LONG, offset));

        assertEquals(LongNumericPrimitives.upperBin(value, (long) 5, offset), LongNumericPrimitives.upperBin(LongNumericPrimitives.upperBin(value, (long) 5, offset), (long) 5, offset));
    }

    public void testClamp() {
        assertEquals((long) 3, LongNumericPrimitives.clamp((long) 3, (long) -6, (long) 5));
        assertEquals((long) -6, LongNumericPrimitives.clamp((long) -7, (long) -6, (long) 5));
        assertEquals((long) 5, LongNumericPrimitives.clamp((long) 7, (long) -6, (long) 5));
        assertEquals(NULL_LONG, LongNumericPrimitives.clamp(NULL_LONG, (long) -6, (long) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, LongNumericPrimitives.binSearchIndex((long[]) null, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.binSearchIndex(new long[]{1,3,4}, (long)0, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.binSearchIndex(new long[]{1,3,4}, (long)1, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.binSearchIndex(new long[]{1,3,4}, (long)2, BinSearch.BS_ANY));
        assertEquals(1, LongNumericPrimitives.binSearchIndex(new long[]{1,3,4}, (long)3, BinSearch.BS_ANY));
        assertEquals(2, LongNumericPrimitives.binSearchIndex(new long[]{1,3,4}, (long)4, BinSearch.BS_ANY));
        assertEquals(2, LongNumericPrimitives.binSearchIndex(new long[]{1,3,4}, (long)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, LongNumericPrimitives.binSearchIndex((LongVector) null, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)0, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)1, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)2, BinSearch.BS_ANY));
        assertEquals(1, LongNumericPrimitives.binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)3, BinSearch.BS_ANY));
        assertEquals(2, LongNumericPrimitives.binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)4, BinSearch.BS_ANY));
        assertEquals(2, LongNumericPrimitives.binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, LongNumericPrimitives.rawBinSearchIndex((LongVector)null, (long) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, LongNumericPrimitives.rawBinSearchIndex((LongVector)null, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, LongNumericPrimitives.rawBinSearchIndex((LongVector)null, (long) 0, BinSearch.BS_LOWEST));

        long[] empty = {};
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(empty), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(empty), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(empty), (long) 0, BinSearch.BS_LOWEST));

        long[] one = {11};
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 12, BinSearch.BS_ANY));
        assertEquals(-2, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 12, BinSearch.BS_LOWEST));

        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 11, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(one), (long) 11, BinSearch.BS_LOWEST));


        long[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        LongNumericPrimitives.rawBinSearchIndex((LongVector)null, (long) 0, null);

        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 26, BinSearch.BS_LOWEST));

        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 1, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 1, BinSearch.BS_LOWEST));

        assertEquals(2, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 2, BinSearch.BS_LOWEST));

        assertEquals(5, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 3, BinSearch.BS_LOWEST));

        assertEquals(9, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 4, BinSearch.BS_LOWEST));

        assertEquals(14, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 7, BinSearch.BS_ANY));
        assertEquals(-16, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 7, BinSearch.BS_LOWEST));

        assertEquals(19, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 10, BinSearch.BS_LOWEST));

        assertEquals(24, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 11, BinSearch.BS_LOWEST));

        assertEquals(25, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 15, BinSearch.BS_ANY));
        assertEquals(25, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 15, BinSearch.BS_LOWEST));

        assertEquals(29, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, LongNumericPrimitives.rawBinSearchIndex(new LongVectorDirect(v), (long) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, LongNumericPrimitives.rawBinSearchIndex((long[]) null, (long) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, LongNumericPrimitives.rawBinSearchIndex((long[])null, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, LongNumericPrimitives.rawBinSearchIndex((long[])null, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(empty, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(empty, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(empty, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(one, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(one, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(one, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, LongNumericPrimitives.rawBinSearchIndex(one, (long) 12, BinSearch.BS_ANY));
        assertEquals(-2, LongNumericPrimitives.rawBinSearchIndex(one, (long) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, LongNumericPrimitives.rawBinSearchIndex(one, (long) 12, BinSearch.BS_LOWEST));

        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(one, (long) 11, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(one, (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(one, (long) 11, BinSearch.BS_LOWEST));


        LongNumericPrimitives.rawBinSearchIndex((long[])null, (long) 0, null);

        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(v, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(v, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, LongNumericPrimitives.rawBinSearchIndex(v, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, LongNumericPrimitives.rawBinSearchIndex(v, (long) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, LongNumericPrimitives.rawBinSearchIndex(v, (long) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, LongNumericPrimitives.rawBinSearchIndex(v, (long) 26, BinSearch.BS_LOWEST));

        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(v, (long) 1, BinSearch.BS_ANY));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(v, (long) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, LongNumericPrimitives.rawBinSearchIndex(v, (long) 1, BinSearch.BS_LOWEST));

        assertEquals(2, LongNumericPrimitives.rawBinSearchIndex(v, (long) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, LongNumericPrimitives.rawBinSearchIndex(v, (long) 2, BinSearch.BS_LOWEST));

        assertEquals(5, LongNumericPrimitives.rawBinSearchIndex(v, (long) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, LongNumericPrimitives.rawBinSearchIndex(v, (long) 3, BinSearch.BS_LOWEST));

        assertEquals(9, LongNumericPrimitives.rawBinSearchIndex(v, (long) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, LongNumericPrimitives.rawBinSearchIndex(v, (long) 4, BinSearch.BS_LOWEST));

        assertEquals(14, LongNumericPrimitives.rawBinSearchIndex(v, (long) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, LongNumericPrimitives.rawBinSearchIndex(v, (long) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, LongNumericPrimitives.rawBinSearchIndex(v, (long) 7, BinSearch.BS_ANY));
        assertEquals(-16, LongNumericPrimitives.rawBinSearchIndex(v, (long) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, LongNumericPrimitives.rawBinSearchIndex(v, (long) 7, BinSearch.BS_LOWEST));

        assertEquals(19, LongNumericPrimitives.rawBinSearchIndex(v, (long) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, LongNumericPrimitives.rawBinSearchIndex(v, (long) 10, BinSearch.BS_LOWEST));

        assertEquals(24, LongNumericPrimitives.rawBinSearchIndex(v, (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, LongNumericPrimitives.rawBinSearchIndex(v, (long) 11, BinSearch.BS_LOWEST));

        assertEquals(25, LongNumericPrimitives.rawBinSearchIndex(v, (long) 15, BinSearch.BS_ANY));
        assertEquals(25, LongNumericPrimitives.rawBinSearchIndex(v, (long) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, LongNumericPrimitives.rawBinSearchIndex(v, (long) 15, BinSearch.BS_LOWEST));

        assertEquals(29, LongNumericPrimitives.rawBinSearchIndex(v, (long) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, LongNumericPrimitives.rawBinSearchIndex(v, (long) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final long[] longs = new long[]{1, -5, -2, -2, 96, 0, 12, NULL_LONG, NULL_LONG};
        final LongVector sort = LongNumericPrimitives.sort(new LongVectorDirect(longs));
        final LongVector expected = new LongVectorDirect(new long[]{NULL_LONG, NULL_LONG, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        long[] sortedArray = LongNumericPrimitives.sort(longs);
        assertEquals(new long[]{NULL_LONG, NULL_LONG, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(LongNumericPrimitives.sort((LongVector)null));
        assertNull(LongNumericPrimitives.sort((long[])null));
        assertNull(LongNumericPrimitives.sort((Long[])null));
        assertEquals(new LongVectorDirect(), LongNumericPrimitives.sort(new LongVectorDirect()));
        assertEquals(new long[]{}, LongNumericPrimitives.sort(new long[]{}));
        assertEquals(new long[]{}, LongNumericPrimitives.sort(new Long[]{}));
    }

    public void testSortDescending() {
        final long[] longs = new long[]{1, -5, -2, -2, 96, 0, 12, NULL_LONG, NULL_LONG};
        final LongVector sort = LongNumericPrimitives.sortDescending(new LongVectorDirect(longs));
        final LongVector expected = new LongVectorDirect(new long[]{96, 12, 1, 0, -2, -2, -5, NULL_LONG, NULL_LONG});
        assertEquals(expected, sort);

        long[] sortedArray = LongNumericPrimitives.sortDescending(longs);
        assertEquals(new long[]{96, 12, 1, 0, -2, -2, -5, NULL_LONG, NULL_LONG}, sortedArray);

        assertNull(LongNumericPrimitives.sortDescending((LongVector)null));
        assertNull(LongNumericPrimitives.sortDescending((long[])null));
        assertNull(LongNumericPrimitives.sortDescending((Long[])null));
        assertEquals(new LongVectorDirect(), LongNumericPrimitives.sortDescending(new LongVectorDirect()));
        assertEquals(new long[]{}, LongNumericPrimitives.sortDescending(new long[]{}));
        assertEquals(new long[]{}, LongNumericPrimitives.sortDescending(new Long[]{}));
    }

    public void testSortsExceptions() {
        LongVector longVector = null;
        LongVector sort = LongNumericPrimitives.sort(longVector);
        assertNull(sort);

        long[] longs = null;
        long[] sortArray = LongNumericPrimitives.sort(longs);
        assertNull(sortArray);

        longs = new long[]{};
        sort = LongNumericPrimitives.sort(new LongVectorDirect(longs));
        assertEquals(new LongVectorDirect(), sort);

        sortArray = LongNumericPrimitives.sort(longs);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        LongVector longVector = null;
        LongVector sort = LongNumericPrimitives.sortDescending(longVector);
        assertNull(sort);

        long[] longs = null;
        long[] sortArray = LongNumericPrimitives.sortDescending(longs);
        assertNull(sortArray);

        longs = new long[]{};
        sort = LongNumericPrimitives.sortDescending(new LongVectorDirect(longs));
        assertEquals(new LongVectorDirect(), sort);

        sortArray = LongNumericPrimitives.sortDescending(longs);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSequence(){
        assertEquals(new long[]{0,1,2,3,4,5}, LongNumericPrimitives.sequence((long)0, (long)5, (long)1));
        assertEquals(new long[]{-5,-4,-3,-2,-1,0}, LongNumericPrimitives.sequence((long)-5, (long)0, (long)1));

        assertEquals(new long[]{0,2,4}, LongNumericPrimitives.sequence((long)0, (long)5, (long)2));
        assertEquals(new long[]{-5,-3,-1}, LongNumericPrimitives.sequence((long)-5, (long)0, (long)2));

        assertEquals(new long[]{5,3,1}, LongNumericPrimitives.sequence((long)5, (long)0, (long)-2));
        assertEquals(new long[]{0,-2,-4}, LongNumericPrimitives.sequence((long)0, (long)-5, (long)-2));

        assertEquals(new long[]{}, LongNumericPrimitives.sequence((long)0, (long)5, (long)0));
        assertEquals(new long[]{}, LongNumericPrimitives.sequence((long)5, (long)0, (long)1));
    }

    public void testMedian() {
        assertEquals(3.0, LongNumericPrimitives.median(new long[]{4,2,3}));
        assertEquals(3.5, LongNumericPrimitives.median(new long[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.median((long[])null));

        assertEquals(3.0, LongNumericPrimitives.median(new Long[]{(long)4,(long)2,(long)3}));
        assertEquals(3.5, LongNumericPrimitives.median(new Long[]{(long)5,(long)4,(long)2,(long)3}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.median((Long[])null));

        assertEquals(3.0, LongNumericPrimitives.median(new LongVectorDirect(new long[]{4,2,3})));
        assertEquals(3.5, LongNumericPrimitives.median(new LongVectorDirect(new long[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.median((LongVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, LongNumericPrimitives.percentile(0.00, new long[]{4,2,3}));
        assertEquals(3.0, LongNumericPrimitives.percentile(0.50, new long[]{4,2,3}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.percentile(0.25, (long[])null));

        assertEquals(2.0, LongNumericPrimitives.percentile(0.00, new LongVectorDirect(new long[]{4,2,3})));
        assertEquals(3.0, LongNumericPrimitives.percentile(0.50, new LongVectorDirect(new long[]{4,2,3})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.percentile(0.25, (LongVector) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wsum(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedSum(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wavg(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (int[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (double[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wvar(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wvar(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (int[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (double[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wstd(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wstd(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (int[])null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (double[])null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wste(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wste(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWtstat() {
        final double target = LongNumericPrimitives.wavg(new long[]{1,2,3}, new long[]{4,5,6}) / LongNumericPrimitives.wste(new long[]{1,2,3}, new long[]{4,5,6});

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (int[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (long[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (double[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wtstat(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, LongNumericPrimitives.wtstat(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }
}
