/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestShortNumericPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.libs.primitives.IntegerNumericPrimitives.*;
import static io.deephaven.libs.primitives.IntegerPrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestIntegerNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((int) 1, signum((int) 5));
        assertEquals((int) 0, signum((int) 0));
        assertEquals((int) -1, signum((int) -5));
        assertEquals(NULL_INT, signum(NULL_INT));
    }

    public void testAvg() {
        assertEquals(50.0, avg(new int[]{40, 50, 60}));
        assertEquals(45.5, avg(new int[]{40, 51}));
        assertTrue(Double.isNaN(avg(new int[]{})));
        assertTrue(Double.isNaN(avg(new int[]{NULL_INT})));
        assertEquals(10.0, avg(new int[]{5, NULL_INT, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((int[])null));

        assertEquals(50.0, avg(new Integer[]{(int)40, (int)50, (int)60}));
        assertEquals(45.5, avg(new Integer[]{(int)40, (int)51}));
        assertTrue(Double.isNaN(avg(new Integer[]{})));
        assertTrue(Double.isNaN(avg(new Integer[]{NULL_INT})));
        assertEquals(10.0, avg(new Integer[]{(int)5, NULL_INT, (int)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((Integer[])null));

        assertEquals(50.0, avg(new DbIntArrayDirect(new int[]{40, 50, 60})));
        assertEquals(45.5, avg(new DbIntArrayDirect(new int[]{40, 51})));
        assertTrue(Double.isNaN(avg(new DbIntArrayDirect())));
        assertTrue(Double.isNaN(avg(new DbIntArrayDirect(NULL_INT))));
        assertEquals(10.0, avg(new DbIntArrayDirect(new int[]{5, NULL_INT, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((DbIntArrayDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, absAvg(new int[]{40, (int) 50, 60}));
        assertEquals(45.5, absAvg(new int[]{(int) 40, 51}));
        assertTrue(Double.isNaN(absAvg(new int[]{})));
        assertTrue(Double.isNaN(absAvg(new int[]{NULL_INT})));
        assertEquals(10.0, absAvg(new int[]{(int) 5, NULL_INT, (int) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((int[])null));

        assertEquals(50.0, absAvg(new Integer[]{(int)40, (int) 50, (int)60}));
        assertEquals(45.5, absAvg(new Integer[]{(int) 40, (int)51}));
        assertTrue(Double.isNaN(absAvg(new Integer[]{})));
        assertTrue(Double.isNaN(absAvg(new Integer[]{NULL_INT})));
        assertEquals(10.0, absAvg(new Integer[]{(int) 5, NULL_INT, (int) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((Integer[])null));

        assertEquals(50.0, absAvg(new DbIntArrayDirect(new int[]{40, (int) 50, 60})));
        assertEquals(45.5, absAvg(new DbIntArrayDirect(new int[]{(int) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new DbIntArrayDirect())));
        assertTrue(Double.isNaN(absAvg(new DbIntArrayDirect(NULL_INT))));
        assertEquals(10.0, absAvg(new DbIntArrayDirect((int) 5, NULL_INT, (int) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((DbIntArrayDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, countPos(new int[]{40, 50, 60, (int) 1, 0}));
        assertEquals(0, countPos(new int[]{}));
        assertEquals(0, countPos(new int[]{NULL_INT}));
        assertEquals(3, countPos(new int[]{5, NULL_INT, 15, (int) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, countPos((int[])null));

        assertEquals(4, countPos(new Integer[]{(int)40, (int)50, (int)60, (int) 1, (int)0}));
        assertEquals(0, countPos(new Integer[]{}));
        assertEquals(0, countPos(new Integer[]{NULL_INT}));
        assertEquals(3, countPos(new Integer[]{(int)5, NULL_INT, (int)15, (int) 1, (int)0}));
        assertEquals(QueryConstants.NULL_INT, countPos((int[])null));

        assertEquals(4, countPos(new DbIntArrayDirect(new int[]{40, 50, 60, (int) 1, 0})));
        assertEquals(0, countPos(new DbIntArrayDirect()));
        assertEquals(0, countPos(new DbIntArrayDirect(NULL_INT)));
        assertEquals(3, countPos(new DbIntArrayDirect(new int[]{5, NULL_INT, 15, (int) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((DbIntArrayDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, countNeg(new int[]{40, (int) -50, 60, (int) -1, 0}));
        assertEquals(0, countNeg(new int[]{}));
        assertEquals(0, countNeg(new int[]{NULL_INT}));
        assertEquals(1, countNeg(new int[]{5, NULL_INT, 15, (int) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((int[])null));

        assertEquals(2, countNeg(new Integer[]{(int)40, (int) -50, (int)60, (int) -1, (int)0}));
        assertEquals(0, countNeg(new Integer[]{}));
        assertEquals(0, countNeg(new Integer[]{NULL_INT}));
        assertEquals(1, countNeg(new Integer[]{(int)5, NULL_INT, (int)15, (int) -1, (int)0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((Integer[])null));

        assertEquals(2, countNeg(new DbIntArrayDirect(new int[]{40, (int) -50, 60, (int) -1, 0})));
        assertEquals(0, countNeg(new DbIntArrayDirect()));
        assertEquals(0, countNeg(new DbIntArrayDirect(NULL_INT)));
        assertEquals(1, countNeg(new DbIntArrayDirect(new int[]{5, NULL_INT, 15, (int) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((DbIntArrayDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, countZero(new int[]{0, 40, 50, 60, (int) -1, 0}));
        assertEquals(0, countZero(new int[]{}));
        assertEquals(0, countZero(new int[]{NULL_INT}));
        assertEquals(2, countZero(new int[]{0, 5, NULL_INT, 0, (int) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((int[])null));

        assertEquals(2, countZero(new Integer[]{(int)0, (int)40, (int)50, (int)60, (int) -1, (int)0}));
        assertEquals(0, countZero(new Integer[]{}));
        assertEquals(0, countZero(new Integer[]{NULL_INT}));
        assertEquals(2, countZero(new Integer[]{(int)0, (int)5, NULL_INT, (int)0, (int) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((Integer[])null));

        assertEquals(2, countZero(new DbIntArrayDirect(new int[]{0, 40, 50, 60, (int) -1, 0})));
        assertEquals(0, countZero(new DbIntArrayDirect()));
        assertEquals(0, countZero(new DbIntArrayDirect(NULL_INT)));
        assertEquals(2, countZero(new DbIntArrayDirect(new int[]{0, 5, NULL_INT, 0, (int) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((DbIntArrayDirect)null));
    }

    public void testMax() {
        assertEquals((int) 60, max(new DbIntArrayDirect(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals((int) 60, max(new DbIntArrayDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) 1)));
        assertEquals(NULL_INT, max(new DbIntArrayDirect()));
        assertEquals(NULL_INT, max(new DbIntArrayDirect(NULL_INT)));
        assertEquals(NULL_INT, max((DbIntArray) null));

        assertEquals((int) 60, max((int) 0, (int) 40, NULL_INT, (int) 50, (int) 60, (int) 1, (int) 0));
        assertEquals((int) 60, max((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1));
        assertEquals(NULL_INT, max());
        assertEquals(NULL_INT, max(NULL_INT));
        assertEquals(NULL_INT, max((int[]) null));
        assertEquals(NULL_INT, max((Integer[]) null));
    }

    public void testMin() {
        assertEquals((int) 0, min(new DbIntArrayDirect(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals((int) -1, min(new DbIntArrayDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1)));
        assertEquals(NULL_INT, min(new DbIntArrayDirect()));
        assertEquals(NULL_INT, min(new DbIntArrayDirect(NULL_INT)));
        assertEquals(NULL_INT, min((DbIntArray) null));

        assertEquals((int) 0, min((int) 0, (int) 40, NULL_INT, (int) 50, (int) 60, (int) 1, (int) 0));
        assertEquals((int) -1, min((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1));
        assertEquals(NULL_INT, min());
        assertEquals(NULL_INT, min(NULL_INT));
        assertEquals(NULL_INT, min((int[]) null));
        assertEquals(NULL_INT, min((Integer[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, firstIndexOf(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}, (int)40));
        assertEquals(4, firstIndexOf(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}, (int)60));
        assertEquals(NULL_INT, firstIndexOf(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}, (int)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((int[])null, (int)40));

        assertEquals(1, firstIndexOf(new DbIntArrayDirect(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}), (int)40));
        assertEquals(4, firstIndexOf(new DbIntArrayDirect(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}), (int)60));
        assertEquals(NULL_INT, firstIndexOf(new DbIntArrayDirect(new int[]{0, 40, NULL_INT, 40, 60, 40, 0}), (int)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((DbIntArray) null, (int)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, indexOfMax(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0}));
        assertEquals(3, indexOfMax(new int[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) 1}));
        assertEquals(-1, indexOfMax(new int[]{}));
        assertEquals(-1, indexOfMax(new int[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((int[])null));

        assertEquals(4, indexOfMax(new Integer[]{(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) 1, (int)0}));
        assertEquals(3, indexOfMax(new Integer[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) 1}));
        assertEquals(-1, indexOfMax(new Integer[]{}));
        assertEquals(-1, indexOfMax(new Integer[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((Integer[])null));

        assertEquals(4, indexOfMax(new DbIntArrayDirect(new int[]{0, 40, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals(3, indexOfMax(new DbIntArrayDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) 1)));
        assertEquals(-1, indexOfMax(new DbIntArrayDirect()));
        assertEquals(-1, indexOfMax(new DbIntArrayDirect(NULL_INT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((DbIntArrayDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, indexOfMin(new int[]{40, 0, NULL_INT, 50, 60, (int) 1, 0}));
        assertEquals(4, indexOfMin(new int[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) -1}));
        assertEquals(-1, indexOfMin(new int[]{}));
        assertEquals(-1, indexOfMin(new int[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((int[])null));

        assertEquals(1, indexOfMin(new Integer[]{(int)40, (int)0, NULL_INT, (int)50, (int)60, (int) 1, (int)0}));
        assertEquals(4, indexOfMin(new Integer[]{(int) 40, NULL_INT, (int) 50, (int) 60, (int) -1}));
        assertEquals(-1, indexOfMin(new Integer[]{}));
        assertEquals(-1, indexOfMin(new Integer[]{NULL_INT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((Integer[])null));

        assertEquals(1, indexOfMin(new DbIntArrayDirect(new int[]{40, 0, NULL_INT, 50, 60, (int) 1, 0})));
        assertEquals(4, indexOfMin(new DbIntArrayDirect((int) 40, NULL_INT, (int) 50, (int) 60, (int) -1)));
        assertEquals(-1, indexOfMin(new DbIntArrayDirect()));
        assertEquals(-1, indexOfMin(new DbIntArrayDirect(NULL_INT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((DbIntArrayDirect)null));
    }


    public void testVar() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, var((int[])null));

        assertEquals(var, var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, var((Integer[])null));

        assertEquals(var, var(new DbIntArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((DbIntArrayDirect)null));
    }

    public void testStd() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};

        assertEquals(Math.sqrt(var(new DbIntArrayDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((int[])null));

        assertEquals(Math.sqrt(var(new DbIntArrayDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Integer[])null));

        assertEquals(Math.sqrt(var(new DbIntArrayDirect(v))), std(new DbIntArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((DbIntArrayDirect)null));
    }

    public void testSte() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};

        assertEquals(std(new DbIntArrayDirect(v)) / Math.sqrt(count(new DbIntArrayDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((int[])null));

        assertEquals(std(new DbIntArrayDirect(v)) / Math.sqrt(count(new DbIntArrayDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Integer[])null));

        assertEquals(std(new DbIntArrayDirect(v)) / Math.sqrt(count(new DbIntArrayDirect(v))), ste(new DbIntArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((DbIntArrayDirect)null));
    }

    public void testTstat() {
        int[] v = {0, 40, NULL_INT, 50, 60, (int) -1, 0};
        Integer[] V = {(int)0, (int)40, NULL_INT, (int)50, (int)60, (int) -1, (int)0};

        assertEquals(avg(new DbIntArrayDirect(v)) / ste(new DbIntArrayDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((int[])null));

        assertEquals(avg(new DbIntArrayDirect(v)) / ste(new DbIntArrayDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Integer[])null));

        assertEquals(avg(new DbIntArrayDirect(v)) / ste(new DbIntArrayDirect(v)), tstat(new DbIntArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((DbIntArrayDirect)null));
    }

    public void testCov() {
        int[] a = {10, 40, NULL_INT, 50, NULL_INT, (int) -1, 0, (int) -7};
        int[] b = {0, (int) -40, NULL_INT, NULL_INT, 6, (int) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((int[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((int[])null, (int[]) null));

        assertEquals(cov, cov(a, new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (DbIntArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((int[])null, new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((int[])null, (DbIntArrayDirect)null));

        assertEquals(cov, cov(new DbIntArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbIntArrayDirect(a), (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbIntArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbIntArrayDirect)null, (int[])null));

        assertEquals(cov, cov(new DbIntArrayDirect(a), new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbIntArrayDirect(a), (DbIntArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbIntArrayDirect)null, new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbIntArrayDirect)null, (DbIntArrayDirect)null));
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

        assertEquals(cor, cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((int[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((int[])null, (int[])null));

        assertEquals(cor, cor(a, new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (DbIntArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((int[])null, new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((int[])null, (DbIntArrayDirect)null));

        assertEquals(cor, cor(new DbIntArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbIntArrayDirect(a), (int[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbIntArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbIntArrayDirect)null, (int[])null));

        assertEquals(cor, cor(new DbIntArrayDirect(a), new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbIntArrayDirect(a), (DbIntArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbIntArrayDirect)null, new DbIntArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbIntArrayDirect)null, (DbIntArrayDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new DbIntArrayDirect(new int[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbIntArrayDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbIntArrayDirect(NULL_INT))) == 0.0);
        assertTrue(Math.abs(20 - sum(new DbIntArrayDirect(new int[]{5, NULL_INT, 15}))) == 0.0);
        assertEquals(NULL_INT, sum((DbIntArray) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new int[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new int[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new int[]{NULL_INT})) == 0.0);
        assertTrue(Math.abs(20 - sum(new int[]{5, NULL_INT, 15})) == 0.0);
        assertEquals(NULL_INT, sum((int[]) null));
    }

    public void testSumDbArray() {
        assertEquals(new int[]{4, 15}, sum(new DbArrayDirect<>(new int[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new int[]{4, NULL_INT}, sum(new DbArrayDirect<>(new int[][]{{5, NULL_INT}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((DbArray<int[]>) null));

        try {
            sum(new DbArrayDirect<>(new int[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new int[]{4, 15}, sum(new int[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new int[]{4, NULL_INT}, sum(new int[][]{{5, NULL_INT}, {-3, 5}, {2, 6}}));
        assertEquals(null, sum((int[][]) null));

        try {
            sum(new int[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - product(new int[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_INT, product(new int[]{}));
        assertEquals(NULL_INT, product(new int[]{NULL_INT}));
        assertTrue(Math.abs(75 - product(new int[]{5, NULL_INT, 15})) == 0.0);
        assertEquals(NULL_INT, product((int[]) null));

        assertTrue(Math.abs(120 - product(new DbIntArrayDirect(new int[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_INT, product(new DbIntArrayDirect()));
        assertEquals(NULL_INT, product(new DbIntArrayDirect(NULL_INT)));
        assertTrue(Math.abs(75 - product(new DbIntArrayDirect(new int[]{5, NULL_INT, 15}))) == 0.0);
        assertEquals(NULL_INT, product((DbIntArray) null));
    }

//    public void testProdDbArray() {
//        assertEquals(new int[]{-30, 120}, product(new DbArrayDirect<>(new int[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new int[]{-30, NULL_INT}, product(new DbArrayDirect<>(new int[][]{{5, NULL_INT}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((DbArray<int[]>) null));
//
//        try {
//            product(new DbArrayDirect<>(new int[][]{{5}, {-3, 5}, {2, 6}}));
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
        assertEquals(new int[]{1, 3, 6, 10, 15}, cumsum(new int[]{1, 2, 3, 4, 5}));
        assertEquals(new int[]{1, 3, 6, 6, 11}, cumsum(new int[]{1, 2, 3, NULL_INT, 5}));
        assertEquals(new int[]{NULL_INT, 2, 5, 9, 14}, cumsum(new int[]{NULL_INT, 2, 3, 4, 5}));
        assertEquals(new int[0], cumsum());
        assertEquals(null, cumsum((int[]) null));

        assertEquals(new int[]{1, 3, 6, 10, 15}, cumsum(new DbIntArrayDirect(new int[]{1, 2, 3, 4, 5})));
        assertEquals(new int[]{1, 3, 6, 6, 11}, cumsum(new DbIntArrayDirect(new int[]{1, 2, 3, NULL_INT, 5})));
        assertEquals(new int[]{NULL_INT, 2, 5, 9, 14}, cumsum(new DbIntArrayDirect(new int[]{NULL_INT, 2, 3, 4, 5})));
        assertEquals(new int[0], cumsum(new DbIntArrayDirect()));
        assertEquals(null, cumsum((DbIntArray) null));
    }

    public void testCumProdArray() {
        assertEquals(new int[]{1, 2, 6, 24, 120}, cumprod(new int[]{1, 2, 3, 4, 5}));
        assertEquals(new int[]{1, 2, 6, 6, 30}, cumprod(new int[]{1, 2, 3, NULL_INT, 5}));
        assertEquals(new int[]{NULL_INT, 2, 6, 24, 120}, cumprod(new int[]{NULL_INT, 2, 3, 4, 5}));
        assertEquals(new int[0], cumprod());
        assertEquals(null, cumprod((int[]) null));

        assertEquals(new int[]{1, 2, 6, 24, 120}, cumprod(new DbIntArrayDirect(new int[]{1, 2, 3, 4, 5})));
        assertEquals(new int[]{1, 2, 6, 6, 30}, cumprod(new DbIntArrayDirect(new int[]{1, 2, 3, NULL_INT, 5})));
        assertEquals(new int[]{NULL_INT, 2, 6, 24, 120}, cumprod(new DbIntArrayDirect(new int[]{NULL_INT, 2, 3, 4, 5})));
        assertEquals(new int[0], cumprod(new DbIntArrayDirect()));
        assertEquals(null, cumprod((DbIntArray) null));
    }

    public void testAbs() {
        int value = -5;
        assertEquals((int) Math.abs(value), abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_INT, abs(NULL_INT), 1e-10);
    }

    public void testAcos() {
        int value = -5;
        assertEquals(Math.acos(value), acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, acos(NULL_INT), 1e-10);
    }

    public void testAsin() {
        int value = -5;
        assertEquals(Math.asin(value), asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, asin(NULL_INT), 1e-10);
    }

    public void testAtan() {
        int value = -5;
        assertEquals(Math.atan(value), atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, atan(NULL_INT), 1e-10);
    }

    public void testCeil() {
        int value = -5;
        assertEquals(Math.ceil(value), ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ceil(NULL_INT), 1e-10);
    }

    public void testCos() {
        int value = -5;
        assertEquals(Math.cos(value), cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, cos(NULL_INT), 1e-10);
    }

    public void testExp() {
        int value = -5;
        assertEquals(Math.exp(value), exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, exp(NULL_INT), 1e-10);
    }

    public void testFloor() {
        int value = -5;
        assertEquals(Math.floor(value), floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, floor(NULL_INT), 1e-10);
    }

    public void testLog() {
        int value = -5;
        assertEquals(Math.log(value), log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, log(NULL_INT), 1e-10);
    }

    public void testPow() {
        int value0 = -5;
        int value1 = 2;
        assertEquals(Math.pow(value0, value1), pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(NULL_INT, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(value0, NULL_INT), 1e-10);
    }

    public void testRint() {
        int value = -5;
        assertEquals(Math.rint(value), rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, rint(NULL_INT), 1e-10);
    }

    public void testRound() {
        int value = -5;
        assertEquals(Math.round(value), round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, round(NULL_INT), 1e-10);
    }

    public void testSin() {
        int value = -5;
        assertEquals(Math.sin(value), sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sin(NULL_INT), 1e-10);
    }

    public void testSqrt() {
        int value = -5;
        assertEquals(Math.sqrt(value), sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sqrt(NULL_INT), 1e-10);
    }

    public void testTan() {
        int value = -5;
        assertEquals(Math.tan(value), tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, tan(NULL_INT), 1e-10);
    }

    public void testLowerBin() {
        int value = (int) 114;

        assertEquals((int) 110, lowerBin(value, (int) 5));
        assertEquals((int) 110, lowerBin(value, (int) 10));
        assertEquals((int) 100, lowerBin(value, (int) 20));
        assertEquals(NULL_INT, lowerBin(NULL_INT, (int) 5));
        assertEquals(NULL_INT, lowerBin(value, NULL_INT));

        assertEquals(lowerBin(value, (int) 5), lowerBin(lowerBin(value, (int) 5), (int) 5));
    }

    public void testLowerBinWithOffset() {
        int value = (int) 114;
        int offset = (int) 3;

        assertEquals((int) 113, lowerBin(value, (int) 5, offset));
        assertEquals((int) 113, lowerBin(value, (int) 10, offset));
        assertEquals((int) 103, lowerBin(value, (int) 20, offset));
        assertEquals(NULL_INT, lowerBin(NULL_INT, (int) 5, offset));
        assertEquals(NULL_INT, lowerBin(value, NULL_INT, offset));

        assertEquals(lowerBin(value, (int) 5, offset), lowerBin(lowerBin(value, (int) 5, offset), (int) 5, offset));
    }

    public void testUpperBin() {
        int value = (int) 114;

        assertEquals((int) 115, upperBin(value, (int) 5));
        assertEquals((int) 120, upperBin(value, (int) 10));
        assertEquals((int) 120, upperBin(value, (int) 20));
        assertEquals(NULL_INT, upperBin(NULL_INT, (int) 5));
        assertEquals(NULL_INT, upperBin(value, NULL_INT));

        assertEquals(upperBin(value, (int) 5), upperBin(upperBin(value, (int) 5), (int) 5));
    }

    public void testUpperBinWithOffset() {
        int value = (int) 114;
        int offset = (int) 3;

        assertEquals((int) 118, upperBin(value, (int) 5, offset));
        assertEquals((int) 123, upperBin(value, (int) 10, offset));
        assertEquals((int) 123, upperBin(value, (int) 20, offset));
        assertEquals(NULL_INT, upperBin(NULL_INT, (int) 5, offset));
        assertEquals(NULL_INT, upperBin(value, NULL_INT, offset));

        assertEquals(upperBin(value, (int) 5, offset), upperBin(upperBin(value, (int) 5, offset), (int) 5, offset));
    }

    public void testClamp() {
        assertEquals((int) 3, clamp((int) 3, (int) -6, (int) 5));
        assertEquals((int) -6, clamp((int) -7, (int) -6, (int) 5));
        assertEquals((int) 5, clamp((int) 7, (int) -6, (int) 5));
        assertEquals(NULL_INT, clamp(NULL_INT, (int) -6, (int) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, binSearchIndex((int[]) null, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new int[]{1,3,4}, (int)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new int[]{1,3,4}, (int)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new int[]{1,3,4}, (int)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new int[]{1,3,4}, (int)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new int[]{1,3,4}, (int)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new int[]{1,3,4}, (int)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((DbIntArray) null, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new DbIntArrayDirect(new int[]{1,3,4}), (int)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbIntArrayDirect(new int[]{1,3,4}), (int)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbIntArrayDirect(new int[]{1,3,4}), (int)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new DbIntArrayDirect(new int[]{1,3,4}), (int)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbIntArrayDirect(new int[]{1,3,4}), (int)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbIntArrayDirect(new int[]{1,3,4}), (int)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((DbIntArray)null, (int) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((DbIntArray)null, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((DbIntArray)null, (int) 0, BinSearch.BS_LOWEST));

        int[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(empty), (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(empty), (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(empty), (int) 0, BinSearch.BS_LOWEST));

        int[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbIntArrayDirect(one), (int) 11, BinSearch.BS_LOWEST));


        int[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((DbIntArray)null, (int) 0, null);

        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new DbIntArrayDirect(v), (int) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, rawBinSearchIndex((int[]) null, (int) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((int[])null, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((int[])null, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(empty, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(empty, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(empty, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(one, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(one, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(one, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(one, (int) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(one, (int) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(one, (int) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(one, (int) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(one, (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(one, (int) 11, BinSearch.BS_LOWEST));


        rawBinSearchIndex((int[])null, (int) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (int) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (int) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (int) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (int) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (int) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (int) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (int) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (int) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (int) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (int) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (int) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (int) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (int) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (int) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (int) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (int) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (int) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (int) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (int) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (int) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (int) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (int) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (int) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (int) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (int) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (int) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (int) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (int) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (int) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final int[] ints = new int[]{1, -5, -2, -2, 96, 0, 12, NULL_INT, NULL_INT};
        final DbIntArray sort = sort(new DbIntArrayDirect(ints));
        final DbIntArray expected = new DbIntArrayDirect(new int[]{NULL_INT, NULL_INT, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        int[] sortedArray = sort(ints);
        assertEquals(new int[]{NULL_INT, NULL_INT, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((DbIntArray)null));
        assertNull(sort((int[])null));
        assertNull(sort((Integer[])null));
        assertEquals(new DbIntArrayDirect(), sort(new DbIntArrayDirect()));
        assertEquals(new int[]{}, sort(new int[]{}));
        assertEquals(new int[]{}, sort(new Integer[]{}));
    }

    public void testSortDescending() {
        final int[] ints = new int[]{1, -5, -2, -2, 96, 0, 12, NULL_INT, NULL_INT};
        final DbIntArray sort = sortDescending(new DbIntArrayDirect(ints));
        final DbIntArray expected = new DbIntArrayDirect(new int[]{96, 12, 1, 0, -2, -2, -5, NULL_INT, NULL_INT});
        assertEquals(expected, sort);

        int[] sortedArray = sortDescending(ints);
        assertEquals(new int[]{96, 12, 1, 0, -2, -2, -5, NULL_INT, NULL_INT}, sortedArray);

        assertNull(sortDescending((DbIntArray)null));
        assertNull(sortDescending((int[])null));
        assertNull(sortDescending((Integer[])null));
        assertEquals(new DbIntArrayDirect(), sortDescending(new DbIntArrayDirect()));
        assertEquals(new int[]{}, sortDescending(new int[]{}));
        assertEquals(new int[]{}, sortDescending(new Integer[]{}));
    }

    public void testSortsExceptions() {
        DbIntArray dbIntegerArray = null;
        DbIntArray sort = sort(dbIntegerArray);
        assertNull(sort);

        int[] ints = null;
        int[] sortArray = sort(ints);
        assertNull(sortArray);

        ints = new int[]{};
        sort = sort(new DbIntArrayDirect(ints));
        assertEquals(new DbIntArrayDirect(), sort);

        sortArray = sort(ints);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DbIntArray dbIntegerArray = null;
        DbIntArray sort = sortDescending(dbIntegerArray);
        assertNull(sort);

        int[] ints = null;
        int[] sortArray = sortDescending(ints);
        assertNull(sortArray);

        ints = new int[]{};
        sort = sortDescending(new DbIntArrayDirect(ints));
        assertEquals(new DbIntArrayDirect(), sort);

        sortArray = sortDescending(ints);
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
        assertEquals(3.0, median(new int[]{4,2,3}));
        assertEquals(3.5, median(new int[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((int[])null));

        assertEquals(3.0, median(new Integer[]{(int)4,(int)2,(int)3}));
        assertEquals(3.5, median(new Integer[]{(int)5,(int)4,(int)2,(int)3}));
        assertEquals(NULL_DOUBLE, median((Integer[])null));

        assertEquals(3.0, median(new DbIntArrayDirect(new int[]{4,2,3})));
        assertEquals(3.5, median(new DbIntArrayDirect(new int[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((DbIntArray) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(0.00, new int[]{4,2,3}));
        assertEquals(3.0, percentile(0.50, new int[]{4,2,3}));
        assertEquals(NULL_DOUBLE, percentile(0.25, (int[])null));

        assertEquals(2.0, percentile(0.00, new DbIntArrayDirect(new int[]{4,2,3})));
        assertEquals(3.0, percentile(0.50, new DbIntArrayDirect(new int[]{4,2,3})));
        assertEquals(NULL_DOUBLE, percentile(0.25, (DbIntArray) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (long[])null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (double[])null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wvar(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wvar(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (long[])null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (double[])null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wstd(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wstd(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (long[])null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (double[])null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wste(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wste(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWtstat() {
        final double target = wavg(new int[]{1,2,3}, new int[]{4,5,6}) / wste(new int[]{1,2,3}, new int[]{4,5,6});

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (int[])null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (long[])null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (double[])null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray) null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wtstat(new int[]{1,2,3,NULL_INT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((int[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new int[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wtstat(new DbIntArrayDirect(new int[]{1,2,3,NULL_INT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((DbIntArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbIntArrayDirect(new int[]{1,2,3}), (DbFloatArray)null));
    }
}
