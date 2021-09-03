/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestShortNumericPrimitives and regenerate
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

        assertEquals(50.0, avg(new DbLongArrayDirect(new long[]{40, 50, 60})));
        assertEquals(45.5, avg(new DbLongArrayDirect(new long[]{40, 51})));
        assertTrue(Double.isNaN(avg(new DbLongArrayDirect())));
        assertTrue(Double.isNaN(avg(new DbLongArrayDirect(NULL_LONG))));
        assertEquals(10.0, avg(new DbLongArrayDirect(new long[]{5, NULL_LONG, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((DbLongArrayDirect)null));
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

        assertEquals(50.0, absAvg(new DbLongArrayDirect(new long[]{40, (long) 50, 60})));
        assertEquals(45.5, absAvg(new DbLongArrayDirect(new long[]{(long) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new DbLongArrayDirect())));
        assertTrue(Double.isNaN(absAvg(new DbLongArrayDirect(NULL_LONG))));
        assertEquals(10.0, absAvg(new DbLongArrayDirect((long) 5, NULL_LONG, (long) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((DbLongArrayDirect)null));
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

        assertEquals(4, countPos(new DbLongArrayDirect(new long[]{40, 50, 60, (long) 1, 0})));
        assertEquals(0, countPos(new DbLongArrayDirect()));
        assertEquals(0, countPos(new DbLongArrayDirect(NULL_LONG)));
        assertEquals(3, countPos(new DbLongArrayDirect(new long[]{5, NULL_LONG, 15, (long) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((DbLongArrayDirect)null));
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

        assertEquals(2, countNeg(new DbLongArrayDirect(new long[]{40, (long) -50, 60, (long) -1, 0})));
        assertEquals(0, countNeg(new DbLongArrayDirect()));
        assertEquals(0, countNeg(new DbLongArrayDirect(NULL_LONG)));
        assertEquals(1, countNeg(new DbLongArrayDirect(new long[]{5, NULL_LONG, 15, (long) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((DbLongArrayDirect)null));
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

        assertEquals(2, countZero(new DbLongArrayDirect(new long[]{0, 40, 50, 60, (long) -1, 0})));
        assertEquals(0, countZero(new DbLongArrayDirect()));
        assertEquals(0, countZero(new DbLongArrayDirect(NULL_LONG)));
        assertEquals(2, countZero(new DbLongArrayDirect(new long[]{0, 5, NULL_LONG, 0, (long) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((DbLongArrayDirect)null));
    }

    public void testMax() {
        assertEquals((long) 60, max(new DbLongArrayDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals((long) 60, max(new DbLongArrayDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1)));
        assertEquals(NULL_LONG, max(new DbLongArrayDirect()));
        assertEquals(NULL_LONG, max(new DbLongArrayDirect(NULL_LONG)));
        assertEquals(NULL_LONG, max((DbLongArray) null));

        assertEquals((long) 60, max((long) 0, (long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1, (long) 0));
        assertEquals((long) 60, max((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1));
        assertEquals(NULL_LONG, max());
        assertEquals(NULL_LONG, max(NULL_LONG));
        assertEquals(NULL_LONG, max((long[]) null));
        assertEquals(NULL_LONG, max((Long[]) null));
    }

    public void testMin() {
        assertEquals((long) 0, min(new DbLongArrayDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals((long) -1, min(new DbLongArrayDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1)));
        assertEquals(NULL_LONG, min(new DbLongArrayDirect()));
        assertEquals(NULL_LONG, min(new DbLongArrayDirect(NULL_LONG)));
        assertEquals(NULL_LONG, min((DbLongArray) null));

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

        assertEquals(1, firstIndexOf(new DbLongArrayDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)40));
        assertEquals(4, firstIndexOf(new DbLongArrayDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)60));
        assertEquals(NULL_INT, firstIndexOf(new DbLongArrayDirect(new long[]{0, 40, NULL_LONG, 40, 60, 40, 0}), (long)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((DbLongArray) null, (long)40));
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

        assertEquals(4, indexOfMax(new DbLongArrayDirect(new long[]{0, 40, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals(3, indexOfMax(new DbLongArrayDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) 1)));
        assertEquals(-1, indexOfMax(new DbLongArrayDirect()));
        assertEquals(-1, indexOfMax(new DbLongArrayDirect(NULL_LONG)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((DbLongArrayDirect)null));
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

        assertEquals(1, indexOfMin(new DbLongArrayDirect(new long[]{40, 0, NULL_LONG, 50, 60, (long) 1, 0})));
        assertEquals(4, indexOfMin(new DbLongArrayDirect((long) 40, NULL_LONG, (long) 50, (long) 60, (long) -1)));
        assertEquals(-1, indexOfMin(new DbLongArrayDirect()));
        assertEquals(-1, indexOfMin(new DbLongArrayDirect(NULL_LONG)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((DbLongArrayDirect)null));
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

        assertEquals(var, var(new DbLongArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((DbLongArrayDirect)null));
    }

    public void testStd() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(Math.sqrt(var(new DbLongArrayDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((long[])null));

        assertEquals(Math.sqrt(var(new DbLongArrayDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Long[])null));

        assertEquals(Math.sqrt(var(new DbLongArrayDirect(v))), std(new DbLongArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((DbLongArrayDirect)null));
    }

    public void testSte() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(std(new DbLongArrayDirect(v)) / Math.sqrt(count(new DbLongArrayDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((long[])null));

        assertEquals(std(new DbLongArrayDirect(v)) / Math.sqrt(count(new DbLongArrayDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Long[])null));

        assertEquals(std(new DbLongArrayDirect(v)) / Math.sqrt(count(new DbLongArrayDirect(v))), ste(new DbLongArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((DbLongArrayDirect)null));
    }

    public void testTstat() {
        long[] v = {0, 40, NULL_LONG, 50, 60, (long) -1, 0};
        Long[] V = {(long)0, (long)40, NULL_LONG, (long)50, (long)60, (long) -1, (long)0};

        assertEquals(avg(new DbLongArrayDirect(v)) / ste(new DbLongArrayDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((long[])null));

        assertEquals(avg(new DbLongArrayDirect(v)) / ste(new DbLongArrayDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Long[])null));

        assertEquals(avg(new DbLongArrayDirect(v)) / ste(new DbLongArrayDirect(v)), tstat(new DbLongArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((DbLongArrayDirect)null));
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

        assertEquals(cov, cov(a, new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (DbLongArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((long[])null, new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((long[])null, (DbLongArrayDirect)null));

        assertEquals(cov, cov(new DbLongArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbLongArrayDirect(a), (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbLongArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbLongArrayDirect)null, (long[])null));

        assertEquals(cov, cov(new DbLongArrayDirect(a), new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbLongArrayDirect(a), (DbLongArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbLongArrayDirect)null, new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbLongArrayDirect)null, (DbLongArrayDirect)null));
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

        assertEquals(cor, cor(a, new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (DbLongArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((long[])null, new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((long[])null, (DbLongArrayDirect)null));

        assertEquals(cor, cor(new DbLongArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbLongArrayDirect(a), (long[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbLongArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbLongArrayDirect)null, (long[])null));

        assertEquals(cor, cor(new DbLongArrayDirect(a), new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbLongArrayDirect(a), (DbLongArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbLongArrayDirect)null, new DbLongArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbLongArrayDirect)null, (DbLongArrayDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new DbLongArrayDirect(new long[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbLongArrayDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbLongArrayDirect(NULL_LONG))) == 0.0);
        assertTrue(Math.abs(20 - sum(new DbLongArrayDirect(new long[]{5, NULL_LONG, 15}))) == 0.0);
        assertEquals(NULL_LONG, sum((DbLongArray) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new long[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new long[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new long[]{NULL_LONG})) == 0.0);
        assertTrue(Math.abs(20 - sum(new long[]{5, NULL_LONG, 15})) == 0.0);
        assertEquals(NULL_LONG, sum((long[]) null));
    }

    public void testSumDbArray() {
        assertEquals(new long[]{4, 15}, sum(new DbArrayDirect<>(new long[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new long[]{4, NULL_LONG}, sum(new DbArrayDirect<>(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((DbArray<long[]>) null));

        try {
            sum(new DbArrayDirect<>(new long[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertTrue(Math.abs(120 - product(new DbLongArrayDirect(new long[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_LONG, product(new DbLongArrayDirect()));
        assertEquals(NULL_LONG, product(new DbLongArrayDirect(NULL_LONG)));
        assertTrue(Math.abs(75 - product(new DbLongArrayDirect(new long[]{5, NULL_LONG, 15}))) == 0.0);
        assertEquals(NULL_LONG, product((DbLongArray) null));
    }

//    public void testProdDbArray() {
//        assertEquals(new long[]{-30, 120}, product(new DbArrayDirect<>(new long[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new long[]{-30, NULL_LONG}, product(new DbArrayDirect<>(new long[][]{{5, NULL_LONG}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((DbArray<long[]>) null));
//
//        try {
//            product(new DbArrayDirect<>(new long[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertEquals(new long[]{1, 3, 6, 10, 15}, cumsum(new DbLongArrayDirect(new long[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 3, 6, 6, 11}, cumsum(new DbLongArrayDirect(new long[]{1, 2, 3, NULL_LONG, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 5, 9, 14}, cumsum(new DbLongArrayDirect(new long[]{NULL_LONG, 2, 3, 4, 5})));
        assertEquals(new long[0], cumsum(new DbLongArrayDirect()));
        assertEquals(null, cumsum((DbLongArray) null));
    }

    public void testCumProdArray() {
        assertEquals(new long[]{1, 2, 6, 24, 120}, cumprod(new long[]{1, 2, 3, 4, 5}));
        assertEquals(new long[]{1, 2, 6, 6, 30}, cumprod(new long[]{1, 2, 3, NULL_LONG, 5}));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, cumprod(new long[]{NULL_LONG, 2, 3, 4, 5}));
        assertEquals(new long[0], cumprod());
        assertEquals(null, cumprod((long[]) null));

        assertEquals(new long[]{1, 2, 6, 24, 120}, cumprod(new DbLongArrayDirect(new long[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 2, 6, 6, 30}, cumprod(new DbLongArrayDirect(new long[]{1, 2, 3, NULL_LONG, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, cumprod(new DbLongArrayDirect(new long[]{NULL_LONG, 2, 3, 4, 5})));
        assertEquals(new long[0], cumprod(new DbLongArrayDirect()));
        assertEquals(null, cumprod((DbLongArray) null));
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

        assertEquals(NULL_INT, binSearchIndex((DbLongArray) null, (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new DbLongArrayDirect(new long[]{1,3,4}), (long)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbLongArrayDirect(new long[]{1,3,4}), (long)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbLongArrayDirect(new long[]{1,3,4}), (long)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new DbLongArrayDirect(new long[]{1,3,4}), (long)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbLongArrayDirect(new long[]{1,3,4}), (long)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbLongArrayDirect(new long[]{1,3,4}), (long)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((DbLongArray)null, (long) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((DbLongArray)null, (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((DbLongArray)null, (long) 0, BinSearch.BS_LOWEST));

        long[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(empty), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(empty), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(empty), (long) 0, BinSearch.BS_LOWEST));

        long[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbLongArrayDirect(one), (long) 11, BinSearch.BS_LOWEST));


        long[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((DbLongArray)null, (long) 0, null);

        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new DbLongArrayDirect(v), (long) 25, BinSearch.BS_LOWEST));

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
        final DbLongArray sort = sort(new DbLongArrayDirect(longs));
        final DbLongArray expected = new DbLongArrayDirect(new long[]{NULL_LONG, NULL_LONG, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        long[] sortedArray = sort(longs);
        assertEquals(new long[]{NULL_LONG, NULL_LONG, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((DbLongArray)null));
        assertNull(sort((long[])null));
        assertNull(sort((Long[])null));
        assertEquals(new DbLongArrayDirect(), sort(new DbLongArrayDirect()));
        assertEquals(new long[]{}, sort(new long[]{}));
        assertEquals(new long[]{}, sort(new Long[]{}));
    }

    public void testSortDescending() {
        final long[] longs = new long[]{1, -5, -2, -2, 96, 0, 12, NULL_LONG, NULL_LONG};
        final DbLongArray sort = sortDescending(new DbLongArrayDirect(longs));
        final DbLongArray expected = new DbLongArrayDirect(new long[]{96, 12, 1, 0, -2, -2, -5, NULL_LONG, NULL_LONG});
        assertEquals(expected, sort);

        long[] sortedArray = sortDescending(longs);
        assertEquals(new long[]{96, 12, 1, 0, -2, -2, -5, NULL_LONG, NULL_LONG}, sortedArray);

        assertNull(sortDescending((DbLongArray)null));
        assertNull(sortDescending((long[])null));
        assertNull(sortDescending((Long[])null));
        assertEquals(new DbLongArrayDirect(), sortDescending(new DbLongArrayDirect()));
        assertEquals(new long[]{}, sortDescending(new long[]{}));
        assertEquals(new long[]{}, sortDescending(new Long[]{}));
    }

    public void testSortsExceptions() {
        DbLongArray dbLongArray = null;
        DbLongArray sort = sort(dbLongArray);
        assertNull(sort);

        long[] longs = null;
        long[] sortArray = sort(longs);
        assertNull(sortArray);

        longs = new long[]{};
        sort = sort(new DbLongArrayDirect(longs));
        assertEquals(new DbLongArrayDirect(), sort);

        sortArray = sort(longs);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DbLongArray dbLongArray = null;
        DbLongArray sort = sortDescending(dbLongArray);
        assertNull(sort);

        long[] longs = null;
        long[] sortArray = sortDescending(longs);
        assertNull(sortArray);

        longs = new long[]{};
        sort = sortDescending(new DbLongArrayDirect(longs));
        assertEquals(new DbLongArrayDirect(), sort);

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

        assertEquals(3.0, median(new DbLongArrayDirect(new long[]{4,2,3})));
        assertEquals(3.5, median(new DbLongArrayDirect(new long[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((DbLongArray) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(0.00, new long[]{4,2,3}));
        assertEquals(3.0, percentile(0.50, new long[]{4,2,3}));
        assertEquals(NULL_DOUBLE, percentile(0.25, (long[])null));

        assertEquals(2.0, percentile(0.00, new DbLongArrayDirect(new long[]{4,2,3})));
        assertEquals(3.0, percentile(0.50, new DbLongArrayDirect(new long[]{4,2,3})));
        assertEquals(NULL_DOUBLE, percentile(0.25, (DbLongArray) null));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
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

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wvar(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wvar(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
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

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wstd(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wstd(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
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

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wste(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wste(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
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

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray) null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wtstat(new long[]{1,2,3,NULL_LONG,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((long[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new long[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wtstat(new DbLongArrayDirect(new long[]{1,2,3,NULL_LONG,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((DbLongArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbLongArrayDirect(new long[]{1,2,3}), (DbFloatArray)null));
    }
}
