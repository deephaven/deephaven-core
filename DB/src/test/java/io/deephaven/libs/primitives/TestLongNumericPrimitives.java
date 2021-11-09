/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestShortNumericPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.libs.primitives.LongNumericPrimitives.*;
import static io.deephaven.libs.primitives.LongPrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestLongNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((long) 1, signum((long) 5));
        assertEquals((long) 0, signum((long) 0));
        assertEquals((long) -1, signum((long) -5));
        assertEquals(NULL_LONG, signum(NULL_LONG));
    }

    public void testAvg() {
        assertEquals(50.0, avg(new long[]{40, 50, 60}));
        assertEquals(45.5, avg(new long[]{40, 51}));
        assertTrue(Double.isNaN(avg(new long[]{})));
        assertTrue(Double.isNaN(avg(new long[]{NULL_LONG})));
        assertEquals(10.0, avg(new long[]{5, NULL_LONG, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((long[])null));

        assertEquals(50.0, avg(new Long[]{(long)40, (long)50, (long)60}));
        assertEquals(45.5, avg(new Long[]{(long)40, (long)51}));
        assertTrue(Double.isNaN(avg(new Long[]{})));
        assertTrue(Double.isNaN(avg(new Long[]{NULL_LONG})));
        assertEquals(10.0, avg(new Long[]{(long)5, NULL_LONG, (long)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((Long[])null));

        assertEquals(50.0, avg(new LongVectorDirect(new long[]{40, 50, 60})));
        assertEquals(45.5, avg(new LongVectorDirect(new long[]{40, 51})));
        assertTrue(Double.isNaN(avg(new LongVectorDirect())));
        assertTrue(Double.isNaN(avg(new LongVectorDirect(NULL_LONG))));
        assertEquals(10.0, avg(new LongVectorDirect(new long[]{5, NULL_LONG, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((LongVectorDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, absAvg(new long[]{40, (long) 50, 60}));
        assertEquals(45.5, absAvg(new long[]{(long) 40, 51}));
        assertTrue(Double.isNaN(absAvg(new long[]{})));
        assertTrue(Double.isNaN(absAvg(new long[]{NULL_LONG})));
        assertEquals(10.0, absAvg(new long[]{(long) 5, NULL_LONG, (long) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((long[])null));

        assertEquals(50.0, absAvg(new Long[]{(long)40, (long) 50, (long)60}));
        assertEquals(45.5, absAvg(new Long[]{(long) 40, (long)51}));
        assertTrue(Double.isNaN(absAvg(new Long[]{})));
        assertTrue(Double.isNaN(absAvg(new Long[]{NULL_LONG})));
        assertEquals(10.0, absAvg(new Long[]{(long) 5, NULL_LONG, (long) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((Long[])null));

        assertEquals(50.0, absAvg(new LongVectorDirect(new long[]{40, (long) 50, 60})));
        assertEquals(45.5, absAvg(new LongVectorDirect(new long[]{(long) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new LongVectorDirect())));
        assertTrue(Double.isNaN(absAvg(new LongVectorDirect(NULL_LONG))));
        assertEquals(10.0, absAvg(new LongVectorDirect((long) 5, NULL_LONG, (long) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((LongVectorDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, countPos(new long[]{40, 50, 60, (long) 1, 0}));
        assertEquals(0, countPos(new long[]{}));
        assertEquals(0, countPos(new long[]{NULL_LONG}));
        assertEquals(3, countPos(new long[]{5, NULL_LONG, 15, (long) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, countPos((long[])null));

        assertEquals(4, countPos(new Long[]{(long)40, (long)50, (long)60, (long) 1, (long)0}));
        assertEquals(0, countPos(new Long[]{}));
        assertEquals(0, countPos(new Long[]{NULL_LONG}));
        assertEquals(3, countPos(new Long[]{(long)5, NULL_LONG, (long)15, (long) 1, (long)0}));
        assertEquals(QueryConstants.NULL_INT, countPos((long[])null));

        assertEquals(4, countPos(new LongVectorDirect(new long[]{40, 50, 60, (long) 1, 0})));
        assertEquals(0, countPos(new LongVectorDirect()));
        assertEquals(0, countPos(new LongVectorDirect(NULL_LONG)));
        assertEquals(3, countPos(new LongVectorDirect(new long[]{5, NULL_LONG, 15, (long) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((LongVectorDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, countNeg(new long[]{40, (long) -50, 60, (long) -1, 0}));
        assertEquals(0, countNeg(new long[]{}));
        assertEquals(0, countNeg(new long[]{NULL_LONG}));
        assertEquals(1, countNeg(new long[]{5, NULL_LONG, 15, (long) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((long[])null));

        assertEquals(2, countNeg(new Long[]{(long)40, (long) -50, (long)60, (long) -1, (long)0}));
        assertEquals(0, countNeg(new Long[]{}));
        assertEquals(0, countNeg(new Long[]{NULL_LONG}));
        assertEquals(1, countNeg(new Long[]{(long)5, NULL_LONG, (long)15, (long) -1, (long)0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((Long[])null));

        assertEquals(2, countNeg(new LongVectorDirect(new long[]{40, (long) -50, 60, (long) -1, 0})));
        assertEquals(0, countNeg(new LongVectorDirect()));
        assertEquals(0, countNeg(new LongVectorDirect(NULL_LONG)));
        assertEquals(1, countNeg(new LongVectorDirect(new long[]{5, NULL_LONG, 15, (long) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((LongVectorDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, countZero(new long[]{0, 40, 50, 60, (long) -1, 0}));
        assertEquals(0, countZero(new long[]{}));
        assertEquals(0, countZero(new long[]{NULL_LONG}));
        assertEquals(2, countZero(new long[]{0, 5, NULL_LONG, 0, (long) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((long[])null));

        assertEquals(2, countZero(new Long[]{(long)0, (long)40, (long)50, (long)60, (long) -1, (long)0}));
        assertEquals(0, countZero(new Long[]{}));
        assertEquals(0, countZero(new Long[]{NULL_LONG}));
        assertEquals(2, countZero(new Long[]{(long)0, (long)5, NULL_LONG, (long)0, (long) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((Long[])null));

        assertEquals(2, countZero(new LongVectorDirect(new long[]{0, 40, 50, 60, (long) -1, 0})));
        assertEquals(0, countZero(new LongVectorDirect()));
        assertEquals(0, countZero(new LongVectorDirect(NULL_LONG)));
        assertEquals(2, countZero(new LongVectorDirect(new long[]{0, 5, NULL_LONG, 0, (long) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((LongVectorDirect)null));
    }

    public void testMax() {
        assertEquals((long) 60, max(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals((long) 60, max(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1)));
        assertEquals(NULL_LONG, max(new LongVectorDirect()));
        assertEquals(NULL_LONG, max(new LongVectorDirect(NULL_LONG)));
        assertEquals(NULL_LONG, max((LongVector) null));

        assertEquals((long) 60, max((long) 0, (long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1, (long) 0));
        assertEquals((long) 60, max((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1));
        assertEquals(NULL_LONG, max());
        assertEquals(NULL_LONG, max(NULL_LONG));
        assertEquals(NULL_LONG, max((long[]) null));
        assertEquals(NULL_LONG, max((Long[]) null));
    }

    public void testMin() {
        assertEquals((long) 0, min(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals((long) -1, min(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1)));
        assertEquals(NULL_LONG, min(new LongVectorDirect()));
        assertEquals(NULL_LONG, min(new LongVectorDirect(NULL_LONG)));
        assertEquals(NULL_LONG, min((LongVector) null));

        assertEquals((long) 0, min((long) 0, (long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1, (long) 0));
        assertEquals((long) -1, min((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1));
        assertEquals(NULL_LONG, min());
        assertEquals(NULL_LONG, min(NULL_LONG));
        assertEquals(NULL_LONG, min((long[]) null));
        assertEquals(NULL_LONG, min((Long[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, firstIndexOf(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}, (long)40));
        assertEquals(4, firstIndexOf(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}, (long)60));
        assertEquals(NULL_INT, firstIndexOf(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}, (long)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((long[])null, (long)40));

        assertEquals(1, firstIndexOf(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)40));
        assertEquals(4, firstIndexOf(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)60));
        assertEquals(NULL_INT, firstIndexOf(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((LongVector) null, (long)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, indexOfMax(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0}));
        assertEquals(3, indexOfMax(new long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1}));
        assertEquals(-1, indexOfMax(new long[]{}));
        assertEquals(-1, indexOfMax(new long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((long[])null));

        assertEquals(4, indexOfMax(new Long[]{(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) 1, (long)0}));
        assertEquals(3, indexOfMax(new Long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1}));
        assertEquals(-1, indexOfMax(new Long[]{}));
        assertEquals(-1, indexOfMax(new Long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((Long[])null));

        assertEquals(4, indexOfMax(new LongVectorDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals(3, indexOfMax(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1)));
        assertEquals(-1, indexOfMax(new LongVectorDirect()));
        assertEquals(-1, indexOfMax(new LongVectorDirect(NULL_LONG)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((LongVectorDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, indexOfMin(new long[]{40, 0, NULL_LONG, 50, 60, (long) 1, 0}));
        assertEquals(4, indexOfMin(new long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1}));
        assertEquals(-1, indexOfMin(new long[]{}));
        assertEquals(-1, indexOfMin(new long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((long[])null));

        assertEquals(1, indexOfMin(new Long[]{(long)40, (long)0, NULL_LONG, (long)50, (long)60, (long) 1, (long)0}));
        assertEquals(4, indexOfMin(new Long[]{(long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1}));
        assertEquals(-1, indexOfMin(new Long[]{}));
        assertEquals(-1, indexOfMin(new Long[]{NULL_LONG}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((Long[])null));

        assertEquals(1, indexOfMin(new LongVectorDirect(new long[]{40, 0, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals(4, indexOfMin(new LongVectorDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1)));
        assertEquals(-1, indexOfMin(new LongVectorDirect()));
        assertEquals(-1, indexOfMin(new LongVectorDirect(NULL_LONG)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((LongVectorDirect)null));
    }


    public void testVar() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, var((long[])null));

        assertEquals(var, var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, var((Long[])null));

        assertEquals(var, var(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((LongVectorDirect)null));
    }

    public void testStd() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(Math.sqrt(var(new LongVectorDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((long[])null));

        assertEquals(Math.sqrt(var(new LongVectorDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Long[])null));

        assertEquals(Math.sqrt(var(new LongVectorDirect(v))), std(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((LongVectorDirect)null));
    }

    public void testSte() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(std(new LongVectorDirect(v)) / Math.sqrt(count(new LongVectorDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((long[])null));

        assertEquals(std(new LongVectorDirect(v)) / Math.sqrt(count(new LongVectorDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Long[])null));

        assertEquals(std(new LongVectorDirect(v)) / Math.sqrt(count(new LongVectorDirect(v))), ste(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((LongVectorDirect)null));
    }

    public void testTstat() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(avg(new LongVectorDirect(v)) / ste(new LongVectorDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((long[])null));

        assertEquals(avg(new LongVectorDirect(v)) / ste(new LongVectorDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Long[])null));

        assertEquals(avg(new LongVectorDirect(v)) / ste(new LongVectorDirect(v)), tstat(new LongVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((LongVectorDirect)null));
    }

    public void testCov() {
        long[] a = {10, 40, NULL_LONG, 50, NULL_LONG, (long) -1, 0, (long) -7};
        long[] b = {0, (long) -40, NULL_LONG, NULL_LONG, 6, (long) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((long[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((long[])null, (long[]) null));

        assertEquals(cov, cov(a, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((long[])null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((long[])null, (LongVectorDirect)null));

        assertEquals(cov, cov(new LongVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new LongVectorDirect(a), (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((LongVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((LongVectorDirect)null, (long[])null));

        assertEquals(cov, cov(new LongVectorDirect(a), new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new LongVectorDirect(a), (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((LongVectorDirect)null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((LongVectorDirect)null, (LongVectorDirect)null));
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

        assertEquals(cor, cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((long[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((long[])null, (long[])null));

        assertEquals(cor, cor(a, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((long[])null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((long[])null, (LongVectorDirect)null));

        assertEquals(cor, cor(new LongVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new LongVectorDirect(a), (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((LongVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((LongVectorDirect)null, (long[])null));

        assertEquals(cor, cor(new LongVectorDirect(a), new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new LongVectorDirect(a), (LongVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((LongVectorDirect)null, new LongVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((LongVectorDirect)null, (LongVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new LongVectorDirect(new long[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new LongVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new LongVectorDirect(NULL_LONG))) == 0.0);
        assertTrue(Math.abs(20 - sum(new LongVectorDirect(new long[]{5, NULL_LONG, 15}))) == 0.0);
        assertEquals(NULL_LONG, sum((LongVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new long[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new long[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new long[]{NULL_LONG})) == 0.0);
        assertTrue(Math.abs(20 - sum(new long[]{5, NULL_LONG, 15})) == 0.0);
        assertEquals(NULL_LONG, sum((long[]) null));
    }

    public void testSumDbArray() {
        assertEquals(new long[]{4, 15}, sum(new ObjectVectorDirect<>(new long[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new long[]{4, NULL_LONG}, sum(new ObjectVectorDirect<>(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((ObjectVector<long[]>) null));

        try {
            sum(new ObjectVectorDirect<>(new long[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new long[]{4, 15}, sum(new long[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new long[]{4, NULL_LONG}, sum(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}}));
        assertEquals(null, sum((long[][]) null));

        try {
            sum(new long[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - product(new long[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_LONG, product(new long[]{}));
        assertEquals(NULL_LONG, product(new long[]{NULL_LONG}));
        assertTrue(Math.abs(75 - product(new long[]{5, NULL_LONG, 15})) == 0.0);
        assertEquals(NULL_LONG, product((long[]) null));

        assertTrue(Math.abs(120 - product(new LongVectorDirect(new long[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_LONG, product(new LongVectorDirect()));
        assertEquals(NULL_LONG, product(new LongVectorDirect(NULL_LONG)));
        assertTrue(Math.abs(75 - product(new LongVectorDirect(new long[]{5, NULL_LONG, 15}))) == 0.0);
        assertEquals(NULL_LONG, product((LongVector) null));
    }

//    public void testProdDbArray() {
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
        assertEquals(new long[]{1, 3, 6, 10, 15}, cumsum(new long[]{1, 2, 3, 4, 5}));
        assertEquals(new long[]{1, 3, 6, 6, 11}, cumsum(new long[]{1, 2, 3, NULL_LONG, 5}));
        assertEquals(new long[]{NULL_LONG, 2, 5, 9, 14}, cumsum(new long[]{NULL_LONG, 2, 3, 4, 5}));
        assertEquals(new long[0], cumsum());
        assertEquals(null, cumsum((long[]) null));

        assertEquals(new long[]{1, 3, 6, 10, 15}, cumsum(new LongVectorDirect(new long[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 3, 6, 6, 11}, cumsum(new LongVectorDirect(new long[]{1, 2, 3, NULL_LONG, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 5, 9, 14}, cumsum(new LongVectorDirect(new long[]{NULL_LONG, 2, 3, 4, 5})));
        assertEquals(new long[0], cumsum(new LongVectorDirect()));
        assertEquals(null, cumsum((LongVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new long[]{1, 2, 6, 24, 120}, cumprod(new long[]{1, 2, 3, 4, 5}));
        assertEquals(new long[]{1, 2, 6, 6, 30}, cumprod(new long[]{1, 2, 3, NULL_LONG, 5}));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, cumprod(new long[]{NULL_LONG, 2, 3, 4, 5}));
        assertEquals(new long[0], cumprod());
        assertEquals(null, cumprod((long[]) null));

        assertEquals(new long[]{1, 2, 6, 24, 120}, cumprod(new LongVectorDirect(new long[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 2, 6, 6, 30}, cumprod(new LongVectorDirect(new long[]{1, 2, 3, NULL_LONG, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, cumprod(new LongVectorDirect(new long[]{NULL_LONG, 2, 3, 4, 5})));
        assertEquals(new long[0], cumprod(new LongVectorDirect()));
        assertEquals(null, cumprod((LongVector) null));
    }

    public void testAbs() {
        long value = -5;
        assertEquals((long) Math.abs(value), abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, abs(NULL_LONG), 1e-10);
    }

    public void testAcos() {
        long value = -5;
        assertEquals(Math.acos(value), acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, acos(NULL_LONG), 1e-10);
    }

    public void testAsin() {
        long value = -5;
        assertEquals(Math.asin(value), asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, asin(NULL_LONG), 1e-10);
    }

    public void testAtan() {
        long value = -5;
        assertEquals(Math.atan(value), atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, atan(NULL_LONG), 1e-10);
    }

    public void testCeil() {
        long value = -5;
        assertEquals(Math.ceil(value), ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ceil(NULL_LONG), 1e-10);
    }

    public void testCos() {
        long value = -5;
        assertEquals(Math.cos(value), cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, cos(NULL_LONG), 1e-10);
    }

    public void testExp() {
        long value = -5;
        assertEquals(Math.exp(value), exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, exp(NULL_LONG), 1e-10);
    }

    public void testFloor() {
        long value = -5;
        assertEquals(Math.floor(value), floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, floor(NULL_LONG), 1e-10);
    }

    public void testLog() {
        long value = -5;
        assertEquals(Math.log(value), log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, log(NULL_LONG), 1e-10);
    }

    public void testPow() {
        long value0 = -5;
        long value1 = 2;
        assertEquals(Math.pow(value0, value1), pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(NULL_LONG, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(value0, NULL_LONG), 1e-10);
    }

    public void testRint() {
        long value = -5;
        assertEquals(Math.rint(value), rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, rint(NULL_LONG), 1e-10);
    }

    public void testRound() {
        long value = -5;
        assertEquals(Math.round(value), round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, round(NULL_LONG), 1e-10);
    }

    public void testSin() {
        long value = -5;
        assertEquals(Math.sin(value), sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sin(NULL_LONG), 1e-10);
    }

    public void testSqrt() {
        long value = -5;
        assertEquals(Math.sqrt(value), sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sqrt(NULL_LONG), 1e-10);
    }

    public void testTan() {
        long value = -5;
        assertEquals(Math.tan(value), tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, tan(NULL_LONG), 1e-10);
    }

    public void testLowerBin() {
        long value = (long) 114;

        assertEquals((long) 110, lowerBin(value, (long) 5));
        assertEquals((long) 110, lowerBin(value, (long) 10));
        assertEquals((long) 100, lowerBin(value, (long) 20));
        assertEquals(NULL_LONG, lowerBin(NULL_LONG, (long) 5));
        assertEquals(NULL_LONG, lowerBin(value, NULL_LONG));

        assertEquals(lowerBin(value, (long) 5), lowerBin(lowerBin(value, (long) 5), (long) 5));
    }

    public void testLowerBinWithOffset() {
        long value = (long) 114;
        long offset = (long) 3;

        assertEquals((long) 113, lowerBin(value, (long) 5, offset));
        assertEquals((long) 113, lowerBin(value, (long) 10, offset));
        assertEquals((long) 103, lowerBin(value, (long) 20, offset));
        assertEquals(NULL_LONG, lowerBin(NULL_LONG, (long) 5, offset));
        assertEquals(NULL_LONG, lowerBin(value, NULL_LONG, offset));

        assertEquals(lowerBin(value, (long) 5, offset), lowerBin(lowerBin(value, (long) 5, offset), (long) 5, offset));
    }

    public void testUpperBin() {
        long value = (long) 114;

        assertEquals((long) 115, upperBin(value, (long) 5));
        assertEquals((long) 120, upperBin(value, (long) 10));
        assertEquals((long) 120, upperBin(value, (long) 20));
        assertEquals(NULL_LONG, upperBin(NULL_LONG, (long) 5));
        assertEquals(NULL_LONG, upperBin(value, NULL_LONG));

        assertEquals(upperBin(value, (long) 5), upperBin(upperBin(value, (long) 5), (long) 5));
    }

    public void testUpperBinWithOffset() {
        long value = (long) 114;
        long offset = (long) 3;

        assertEquals((long) 118, upperBin(value, (long) 5, offset));
        assertEquals((long) 123, upperBin(value, (long) 10, offset));
        assertEquals((long) 123, upperBin(value, (long) 20, offset));
        assertEquals(NULL_LONG, upperBin(NULL_LONG, (long) 5, offset));
        assertEquals(NULL_LONG, upperBin(value, NULL_LONG, offset));

        assertEquals(upperBin(value, (long) 5, offset), upperBin(upperBin(value, (long) 5, offset), (long) 5, offset));
    }

    public void testClamp() {
        assertEquals((long) 3, clamp((long) 3, (long) -6, (long) 5));
        assertEquals((long) -6, clamp((long) -7, (long) -6, (long) 5));
        assertEquals((long) 5, clamp((long) 7, (long) -6, (long) 5));
        assertEquals(NULL_LONG, clamp(NULL_LONG, (long) -6, (long) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, binSearchIndex((long[]) null, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new long[]{1,3,4}, (long)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new long[]{1,3,4}, (long)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new long[]{1,3,4}, (long)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new long[]{1,3,4}, (long)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new long[]{1,3,4}, (long)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new long[]{1,3,4}, (long)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((LongVector) null, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new LongVectorDirect(new long[]{1,3,4}), (long)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((LongVector)null, (long) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((LongVector)null, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((LongVector)null, (long) 0, BinSearch.BS_LOWEST));

        long[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(empty), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(empty), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(empty), (long) 0, BinSearch.BS_LOWEST));

        long[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(one), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(one), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(one), (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new LongVectorDirect(one), (long) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new LongVectorDirect(one), (long) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new LongVectorDirect(one), (long) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new LongVectorDirect(one), (long) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new LongVectorDirect(one), (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new LongVectorDirect(one), (long) 11, BinSearch.BS_LOWEST));


        long[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((LongVector)null, (long) 0, null);

        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(v), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(v), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new LongVectorDirect(v), (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new LongVectorDirect(v), (long) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new LongVectorDirect(v), (long) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new LongVectorDirect(v), (long) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new LongVectorDirect(v), (long) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new LongVectorDirect(v), (long) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new LongVectorDirect(v), (long) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new LongVectorDirect(v), (long) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new LongVectorDirect(v), (long) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new LongVectorDirect(v), (long) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new LongVectorDirect(v), (long) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new LongVectorDirect(v), (long) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new LongVectorDirect(v), (long) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new LongVectorDirect(v), (long) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new LongVectorDirect(v), (long) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new LongVectorDirect(v), (long) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new LongVectorDirect(v), (long) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new LongVectorDirect(v), (long) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new LongVectorDirect(v), (long) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new LongVectorDirect(v), (long) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new LongVectorDirect(v), (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new LongVectorDirect(v), (long) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new LongVectorDirect(v), (long) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new LongVectorDirect(v), (long) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new LongVectorDirect(v), (long) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new LongVectorDirect(v), (long) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new LongVectorDirect(v), (long) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, rawBinSearchIndex((long[]) null, (long) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((long[])null, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((long[])null, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(empty, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(empty, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(empty, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(one, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(one, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(one, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(one, (long) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(one, (long) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(one, (long) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(one, (long) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(one, (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(one, (long) 11, BinSearch.BS_LOWEST));


        rawBinSearchIndex((long[])null, (long) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (long) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (long) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (long) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (long) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (long) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (long) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (long) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (long) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (long) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (long) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (long) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (long) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (long) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (long) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (long) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (long) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (long) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (long) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (long) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (long) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (long) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (long) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (long) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (long) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (long) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final long[] longs = new long[]{1, -5, -2, -2, 96, 0, 12, NULL_LONG, NULL_LONG};
        final LongVector sort = sort(new LongVectorDirect(longs));
        final LongVector expected = new LongVectorDirect(new long[]{NULL_LONG, NULL_LONG, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        long[] sortedArray = sort(longs);
        assertEquals(new long[]{NULL_LONG, NULL_LONG, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((LongVector)null));
        assertNull(sort((long[])null));
        assertNull(sort((Long[])null));
        assertEquals(new LongVectorDirect(), sort(new LongVectorDirect()));
        assertEquals(new long[]{}, sort(new long[]{}));
        assertEquals(new long[]{}, sort(new Long[]{}));
    }

    public void testSortDescending() {
        final long[] longs = new long[]{1, -5, -2, -2, 96, 0, 12, NULL_LONG, NULL_LONG};
        final LongVector sort = sortDescending(new LongVectorDirect(longs));
        final LongVector expected = new LongVectorDirect(new long[]{96, 12, 1, 0, -2, -2, -5, NULL_LONG, NULL_LONG});
        assertEquals(expected, sort);

        long[] sortedArray = sortDescending(longs);
        assertEquals(new long[]{96, 12, 1, 0, -2, -2, -5, NULL_LONG, NULL_LONG}, sortedArray);

        assertNull(sortDescending((LongVector)null));
        assertNull(sortDescending((long[])null));
        assertNull(sortDescending((Long[])null));
        assertEquals(new LongVectorDirect(), sortDescending(new LongVectorDirect()));
        assertEquals(new long[]{}, sortDescending(new long[]{}));
        assertEquals(new long[]{}, sortDescending(new Long[]{}));
    }

    public void testSortsExceptions() {
        LongVector longVector = null;
        LongVector sort = sort(longVector);
        assertNull(sort);

        long[] longs = null;
        long[] sortArray = sort(longs);
        assertNull(sortArray);

        longs = new long[]{};
        sort = sort(new LongVectorDirect(longs));
        assertEquals(new LongVectorDirect(), sort);

        sortArray = sort(longs);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        LongVector longVector = null;
        LongVector sort = sortDescending(longVector);
        assertNull(sort);

        long[] longs = null;
        long[] sortArray = sortDescending(longs);
        assertNull(sortArray);

        longs = new long[]{};
        sort = sortDescending(new LongVectorDirect(longs));
        assertEquals(new LongVectorDirect(), sort);

        sortArray = sortDescending(longs);
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
        assertEquals(3.0, median(new long[]{4,2,3}));
        assertEquals(3.5, median(new long[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((long[])null));

        assertEquals(3.0, median(new Long[]{(long)4,(long)2,(long)3}));
        assertEquals(3.5, median(new Long[]{(long)5,(long)4,(long)2,(long)3}));
        assertEquals(NULL_DOUBLE, median((Long[])null));

        assertEquals(3.0, median(new LongVectorDirect(new long[]{4,2,3})));
        assertEquals(3.5, median(new LongVectorDirect(new long[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((LongVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(0.00, new long[]{4,2,3}));
        assertEquals(3.0, percentile(0.50, new long[]{4,2,3}));
        assertEquals(NULL_DOUBLE, percentile(0.25, (long[])null));

        assertEquals(2.0, percentile(0.00, new LongVectorDirect(new long[]{4,2,3})));
        assertEquals(3.0, percentile(0.50, new LongVectorDirect(new long[]{4,2,3})));
        assertEquals(NULL_DOUBLE, percentile(0.25, (LongVector) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (int[])null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (double[])null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wvar(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (int[])null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (double[])null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wstd(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (int[])null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (double[])null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wste(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }

    public void testWtstat() {
        final double target = wavg(new long[]{1,2,3}, new long[]{4,5,6}) / wste(new long[]{1,2,3}, new long[]{4,5,6});

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (int[])null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (long[])null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (double[])null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((LongVector) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((LongVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((LongVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((LongVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((LongVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (IntVector) null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (LongVector) null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((LongVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (IntVector) null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((LongVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (LongVector) null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((LongVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wtstat(new LongVectorDirect(new long[]{1,2,3,NULL_LONG,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((LongVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new LongVectorDirect(new long[]{1,2,3}), (FloatVector)null));
    }
}
