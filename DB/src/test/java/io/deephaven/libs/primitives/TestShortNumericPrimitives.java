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
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static io.deephaven.libs.primitives.ShortNumericPrimitives.*;
import static io.deephaven.libs.primitives.ShortPrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestShortNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((short) 1, signum((short) 5));
        assertEquals((short) 0, signum((short) 0));
        assertEquals((short) -1, signum((short) -5));
        assertEquals(NULL_SHORT, signum(NULL_SHORT));
    }

    public void testAvg() {
        assertEquals(50.0, avg(new short[]{40, 50, 60}));
        assertEquals(45.5, avg(new short[]{40, 51}));
        assertTrue(Double.isNaN(avg(new short[]{})));
        assertTrue(Double.isNaN(avg(new short[]{NULL_SHORT})));
        assertEquals(10.0, avg(new short[]{5, NULL_SHORT, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((short[])null));

        assertEquals(50.0, avg(new Short[]{(short)40, (short)50, (short)60}));
        assertEquals(45.5, avg(new Short[]{(short)40, (short)51}));
        assertTrue(Double.isNaN(avg(new Short[]{})));
        assertTrue(Double.isNaN(avg(new Short[]{NULL_SHORT})));
        assertEquals(10.0, avg(new Short[]{(short)5, NULL_SHORT, (short)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((Short[])null));

        assertEquals(50.0, avg(new DbShortArrayDirect(new short[]{40, 50, 60})));
        assertEquals(45.5, avg(new DbShortArrayDirect(new short[]{40, 51})));
        assertTrue(Double.isNaN(avg(new DbShortArrayDirect())));
        assertTrue(Double.isNaN(avg(new DbShortArrayDirect(NULL_SHORT))));
        assertEquals(10.0, avg(new DbShortArrayDirect(new short[]{5, NULL_SHORT, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((DbShortArrayDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, absAvg(new short[]{40, (short) 50, 60}));
        assertEquals(45.5, absAvg(new short[]{(short) 40, 51}));
        assertTrue(Double.isNaN(absAvg(new short[]{})));
        assertTrue(Double.isNaN(absAvg(new short[]{NULL_SHORT})));
        assertEquals(10.0, absAvg(new short[]{(short) 5, NULL_SHORT, (short) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((short[])null));

        assertEquals(50.0, absAvg(new Short[]{(short)40, (short) 50, (short)60}));
        assertEquals(45.5, absAvg(new Short[]{(short) 40, (short)51}));
        assertTrue(Double.isNaN(absAvg(new Short[]{})));
        assertTrue(Double.isNaN(absAvg(new Short[]{NULL_SHORT})));
        assertEquals(10.0, absAvg(new Short[]{(short) 5, NULL_SHORT, (short) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((Short[])null));

        assertEquals(50.0, absAvg(new DbShortArrayDirect(new short[]{40, (short) 50, 60})));
        assertEquals(45.5, absAvg(new DbShortArrayDirect(new short[]{(short) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new DbShortArrayDirect())));
        assertTrue(Double.isNaN(absAvg(new DbShortArrayDirect(NULL_SHORT))));
        assertEquals(10.0, absAvg(new DbShortArrayDirect((short) 5, NULL_SHORT, (short) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((DbShortArrayDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, countPos(new short[]{40, 50, 60, (short) 1, 0}));
        assertEquals(0, countPos(new short[]{}));
        assertEquals(0, countPos(new short[]{NULL_SHORT}));
        assertEquals(3, countPos(new short[]{5, NULL_SHORT, 15, (short) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, countPos((short[])null));

        assertEquals(4, countPos(new Short[]{(short)40, (short)50, (short)60, (short) 1, (short)0}));
        assertEquals(0, countPos(new Short[]{}));
        assertEquals(0, countPos(new Short[]{NULL_SHORT}));
        assertEquals(3, countPos(new Short[]{(short)5, NULL_SHORT, (short)15, (short) 1, (short)0}));
        assertEquals(QueryConstants.NULL_INT, countPos((short[])null));

        assertEquals(4, countPos(new DbShortArrayDirect(new short[]{40, 50, 60, (short) 1, 0})));
        assertEquals(0, countPos(new DbShortArrayDirect()));
        assertEquals(0, countPos(new DbShortArrayDirect(NULL_SHORT)));
        assertEquals(3, countPos(new DbShortArrayDirect(new short[]{5, NULL_SHORT, 15, (short) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((DbShortArrayDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, countNeg(new short[]{40, (short) -50, 60, (short) -1, 0}));
        assertEquals(0, countNeg(new short[]{}));
        assertEquals(0, countNeg(new short[]{NULL_SHORT}));
        assertEquals(1, countNeg(new short[]{5, NULL_SHORT, 15, (short) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((short[])null));

        assertEquals(2, countNeg(new Short[]{(short)40, (short) -50, (short)60, (short) -1, (short)0}));
        assertEquals(0, countNeg(new Short[]{}));
        assertEquals(0, countNeg(new Short[]{NULL_SHORT}));
        assertEquals(1, countNeg(new Short[]{(short)5, NULL_SHORT, (short)15, (short) -1, (short)0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((Short[])null));

        assertEquals(2, countNeg(new DbShortArrayDirect(new short[]{40, (short) -50, 60, (short) -1, 0})));
        assertEquals(0, countNeg(new DbShortArrayDirect()));
        assertEquals(0, countNeg(new DbShortArrayDirect(NULL_SHORT)));
        assertEquals(1, countNeg(new DbShortArrayDirect(new short[]{5, NULL_SHORT, 15, (short) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((DbShortArrayDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, countZero(new short[]{0, 40, 50, 60, (short) -1, 0}));
        assertEquals(0, countZero(new short[]{}));
        assertEquals(0, countZero(new short[]{NULL_SHORT}));
        assertEquals(2, countZero(new short[]{0, 5, NULL_SHORT, 0, (short) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((short[])null));

        assertEquals(2, countZero(new Short[]{(short)0, (short)40, (short)50, (short)60, (short) -1, (short)0}));
        assertEquals(0, countZero(new Short[]{}));
        assertEquals(0, countZero(new Short[]{NULL_SHORT}));
        assertEquals(2, countZero(new Short[]{(short)0, (short)5, NULL_SHORT, (short)0, (short) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((Short[])null));

        assertEquals(2, countZero(new DbShortArrayDirect(new short[]{0, 40, 50, 60, (short) -1, 0})));
        assertEquals(0, countZero(new DbShortArrayDirect()));
        assertEquals(0, countZero(new DbShortArrayDirect(NULL_SHORT)));
        assertEquals(2, countZero(new DbShortArrayDirect(new short[]{0, 5, NULL_SHORT, 0, (short) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((DbShortArrayDirect)null));
    }

    public void testMax() {
        assertEquals((short) 60, max(new DbShortArrayDirect(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals((short) 60, max(new DbShortArrayDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1)));
        assertEquals(NULL_SHORT, max(new DbShortArrayDirect()));
        assertEquals(NULL_SHORT, max(new DbShortArrayDirect(NULL_SHORT)));
        assertEquals(NULL_SHORT, max((DbShortArray) null));

        assertEquals((short) 60, max((short) 0, (short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1, (short) 0));
        assertEquals((short) 60, max((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1));
        assertEquals(NULL_SHORT, max());
        assertEquals(NULL_SHORT, max(NULL_SHORT));
        assertEquals(NULL_SHORT, max((short[]) null));
        assertEquals(NULL_SHORT, max((Short[]) null));
    }

    public void testMin() {
        assertEquals((short) 0, min(new DbShortArrayDirect(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals((short) -1, min(new DbShortArrayDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1)));
        assertEquals(NULL_SHORT, min(new DbShortArrayDirect()));
        assertEquals(NULL_SHORT, min(new DbShortArrayDirect(NULL_SHORT)));
        assertEquals(NULL_SHORT, min((DbShortArray) null));

        assertEquals((short) 0, min((short) 0, (short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1, (short) 0));
        assertEquals((short) -1, min((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1));
        assertEquals(NULL_SHORT, min());
        assertEquals(NULL_SHORT, min(NULL_SHORT));
        assertEquals(NULL_SHORT, min((short[]) null));
        assertEquals(NULL_SHORT, min((Short[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, firstIndexOf(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}, (short)40));
        assertEquals(4, firstIndexOf(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}, (short)60));
        assertEquals(NULL_INT, firstIndexOf(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}, (short)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((short[])null, (short)40));

        assertEquals(1, firstIndexOf(new DbShortArrayDirect(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}), (short)40));
        assertEquals(4, firstIndexOf(new DbShortArrayDirect(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}), (short)60));
        assertEquals(NULL_INT, firstIndexOf(new DbShortArrayDirect(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}), (short)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((DbShortArray) null, (short)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, indexOfMax(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0}));
        assertEquals(3, indexOfMax(new short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1}));
        assertEquals(-1, indexOfMax(new short[]{}));
        assertEquals(-1, indexOfMax(new short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((short[])null));

        assertEquals(4, indexOfMax(new Short[]{(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) 1, (short)0}));
        assertEquals(3, indexOfMax(new Short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1}));
        assertEquals(-1, indexOfMax(new Short[]{}));
        assertEquals(-1, indexOfMax(new Short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((Short[])null));

        assertEquals(4, indexOfMax(new DbShortArrayDirect(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals(3, indexOfMax(new DbShortArrayDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1)));
        assertEquals(-1, indexOfMax(new DbShortArrayDirect()));
        assertEquals(-1, indexOfMax(new DbShortArrayDirect(NULL_SHORT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((DbShortArrayDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, indexOfMin(new short[]{40, 0, NULL_SHORT, 50, 60, (short) 1, 0}));
        assertEquals(4, indexOfMin(new short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1}));
        assertEquals(-1, indexOfMin(new short[]{}));
        assertEquals(-1, indexOfMin(new short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((short[])null));

        assertEquals(1, indexOfMin(new Short[]{(short)40, (short)0, NULL_SHORT, (short)50, (short)60, (short) 1, (short)0}));
        assertEquals(4, indexOfMin(new Short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1}));
        assertEquals(-1, indexOfMin(new Short[]{}));
        assertEquals(-1, indexOfMin(new Short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((Short[])null));

        assertEquals(1, indexOfMin(new DbShortArrayDirect(new short[]{40, 0, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals(4, indexOfMin(new DbShortArrayDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1)));
        assertEquals(-1, indexOfMin(new DbShortArrayDirect()));
        assertEquals(-1, indexOfMin(new DbShortArrayDirect(NULL_SHORT)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((DbShortArrayDirect)null));
    }


    public void testVar() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, var((short[])null));

        assertEquals(var, var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, var((Short[])null));

        assertEquals(var, var(new DbShortArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((DbShortArrayDirect)null));
    }

    public void testStd() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};

        assertEquals(Math.sqrt(var(new DbShortArrayDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((short[])null));

        assertEquals(Math.sqrt(var(new DbShortArrayDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Short[])null));

        assertEquals(Math.sqrt(var(new DbShortArrayDirect(v))), std(new DbShortArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((DbShortArrayDirect)null));
    }

    public void testSte() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};

        assertEquals(std(new DbShortArrayDirect(v)) / Math.sqrt(count(new DbShortArrayDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((short[])null));

        assertEquals(std(new DbShortArrayDirect(v)) / Math.sqrt(count(new DbShortArrayDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Short[])null));

        assertEquals(std(new DbShortArrayDirect(v)) / Math.sqrt(count(new DbShortArrayDirect(v))), ste(new DbShortArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((DbShortArrayDirect)null));
    }

    public void testTstat() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};

        assertEquals(avg(new DbShortArrayDirect(v)) / ste(new DbShortArrayDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((short[])null));

        assertEquals(avg(new DbShortArrayDirect(v)) / ste(new DbShortArrayDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Short[])null));

        assertEquals(avg(new DbShortArrayDirect(v)) / ste(new DbShortArrayDirect(v)), tstat(new DbShortArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((DbShortArrayDirect)null));
    }

    public void testCov() {
        short[] a = {10, 40, NULL_SHORT, 50, NULL_SHORT, (short) -1, 0, (short) -7};
        short[] b = {0, (short) -40, NULL_SHORT, NULL_SHORT, 6, (short) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((short[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((short[])null, (short[]) null));

        assertEquals(cov, cov(a, new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (DbShortArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((short[])null, new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((short[])null, (DbShortArrayDirect)null));

        assertEquals(cov, cov(new DbShortArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbShortArrayDirect(a), (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbShortArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbShortArrayDirect)null, (short[])null));

        assertEquals(cov, cov(new DbShortArrayDirect(a), new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbShortArrayDirect(a), (DbShortArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbShortArrayDirect)null, new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbShortArrayDirect)null, (DbShortArrayDirect)null));
    }

    public void testCor() {
        short[] a = {10, 40, NULL_SHORT, 50, NULL_SHORT, (short) -1, 0, (short) -7};
        short[] b = {0, (short) -40, NULL_SHORT, NULL_SHORT, 6, (short) -1, 11, 3};
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
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((short[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((short[])null, (short[])null));

        assertEquals(cor, cor(a, new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (DbShortArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((short[])null, new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((short[])null, (DbShortArrayDirect)null));

        assertEquals(cor, cor(new DbShortArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbShortArrayDirect(a), (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbShortArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbShortArrayDirect)null, (short[])null));

        assertEquals(cor, cor(new DbShortArrayDirect(a), new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbShortArrayDirect(a), (DbShortArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbShortArrayDirect)null, new DbShortArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbShortArrayDirect)null, (DbShortArrayDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new DbShortArrayDirect(new short[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbShortArrayDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbShortArrayDirect(NULL_SHORT))) == 0.0);
        assertTrue(Math.abs(20 - sum(new DbShortArrayDirect(new short[]{5, NULL_SHORT, 15}))) == 0.0);
        assertEquals(NULL_SHORT, sum((DbShortArray) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new short[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new short[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new short[]{NULL_SHORT})) == 0.0);
        assertTrue(Math.abs(20 - sum(new short[]{5, NULL_SHORT, 15})) == 0.0);
        assertEquals(NULL_SHORT, sum((short[]) null));
    }

    public void testSumDbArray() {
        assertEquals(new short[]{4, 15}, sum(new DbArrayDirect<>(new short[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new short[]{4, NULL_SHORT}, sum(new DbArrayDirect<>(new short[][]{{5, NULL_SHORT}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((DbArray<short[]>) null));

        try {
            sum(new DbArrayDirect<>(new short[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new short[]{4, 15}, sum(new short[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new short[]{4, NULL_SHORT}, sum(new short[][]{{5, NULL_SHORT}, {-3, 5}, {2, 6}}));
        assertEquals(null, sum((short[][]) null));

        try {
            sum(new short[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - product(new short[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_SHORT, product(new short[]{}));
        assertEquals(NULL_SHORT, product(new short[]{NULL_SHORT}));
        assertTrue(Math.abs(75 - product(new short[]{5, NULL_SHORT, 15})) == 0.0);
        assertEquals(NULL_SHORT, product((short[]) null));

        assertTrue(Math.abs(120 - product(new DbShortArrayDirect(new short[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_SHORT, product(new DbShortArrayDirect()));
        assertEquals(NULL_SHORT, product(new DbShortArrayDirect(NULL_SHORT)));
        assertTrue(Math.abs(75 - product(new DbShortArrayDirect(new short[]{5, NULL_SHORT, 15}))) == 0.0);
        assertEquals(NULL_SHORT, product((DbShortArray) null));
    }

//    public void testProdDbArray() {
//        assertEquals(new short[]{-30, 120}, product(new DbArrayDirect<>(new short[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new short[]{-30, NULL_SHORT}, product(new DbArrayDirect<>(new short[][]{{5, NULL_SHORT}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((DbArray<short[]>) null));
//
//        try {
//            product(new DbArrayDirect<>(new short[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void testProdArray() {
//        assertEquals(new short[]{-30, 120}, product(new short[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new short[]{-30, NULL_SHORT}, product(new short[][]{{5, NULL_SHORT}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((short[][]) null));
//
//        try {
//            product(new short[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void testCumSumArray() {
        assertEquals(new short[]{1, 3, 6, 10, 15}, cumsum(new short[]{1, 2, 3, 4, 5}));
        assertEquals(new short[]{1, 3, 6, 6, 11}, cumsum(new short[]{1, 2, 3, NULL_SHORT, 5}));
        assertEquals(new short[]{NULL_SHORT, 2, 5, 9, 14}, cumsum(new short[]{NULL_SHORT, 2, 3, 4, 5}));
        assertEquals(new short[0], cumsum());
        assertEquals(null, cumsum((short[]) null));

        assertEquals(new short[]{1, 3, 6, 10, 15}, cumsum(new DbShortArrayDirect(new short[]{1, 2, 3, 4, 5})));
        assertEquals(new short[]{1, 3, 6, 6, 11}, cumsum(new DbShortArrayDirect(new short[]{1, 2, 3, NULL_SHORT, 5})));
        assertEquals(new short[]{NULL_SHORT, 2, 5, 9, 14}, cumsum(new DbShortArrayDirect(new short[]{NULL_SHORT, 2, 3, 4, 5})));
        assertEquals(new short[0], cumsum(new DbShortArrayDirect()));
        assertEquals(null, cumsum((DbShortArray) null));
    }

    public void testCumProdArray() {
        assertEquals(new short[]{1, 2, 6, 24, 120}, cumprod(new short[]{1, 2, 3, 4, 5}));
        assertEquals(new short[]{1, 2, 6, 6, 30}, cumprod(new short[]{1, 2, 3, NULL_SHORT, 5}));
        assertEquals(new short[]{NULL_SHORT, 2, 6, 24, 120}, cumprod(new short[]{NULL_SHORT, 2, 3, 4, 5}));
        assertEquals(new short[0], cumprod());
        assertEquals(null, cumprod((short[]) null));

        assertEquals(new short[]{1, 2, 6, 24, 120}, cumprod(new DbShortArrayDirect(new short[]{1, 2, 3, 4, 5})));
        assertEquals(new short[]{1, 2, 6, 6, 30}, cumprod(new DbShortArrayDirect(new short[]{1, 2, 3, NULL_SHORT, 5})));
        assertEquals(new short[]{NULL_SHORT, 2, 6, 24, 120}, cumprod(new DbShortArrayDirect(new short[]{NULL_SHORT, 2, 3, 4, 5})));
        assertEquals(new short[0], cumprod(new DbShortArrayDirect()));
        assertEquals(null, cumprod((DbShortArray) null));
    }

    public void testAbs() {
        short value = -5;
        assertEquals((short) Math.abs(value), abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_SHORT, abs(NULL_SHORT), 1e-10);
    }

    public void testAcos() {
        short value = -5;
        assertEquals(Math.acos(value), acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, acos(NULL_SHORT), 1e-10);
    }

    public void testAsin() {
        short value = -5;
        assertEquals(Math.asin(value), asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, asin(NULL_SHORT), 1e-10);
    }

    public void testAtan() {
        short value = -5;
        assertEquals(Math.atan(value), atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, atan(NULL_SHORT), 1e-10);
    }

    public void testCeil() {
        short value = -5;
        assertEquals(Math.ceil(value), ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ceil(NULL_SHORT), 1e-10);
    }

    public void testCos() {
        short value = -5;
        assertEquals(Math.cos(value), cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, cos(NULL_SHORT), 1e-10);
    }

    public void testExp() {
        short value = -5;
        assertEquals(Math.exp(value), exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, exp(NULL_SHORT), 1e-10);
    }

    public void testFloor() {
        short value = -5;
        assertEquals(Math.floor(value), floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, floor(NULL_SHORT), 1e-10);
    }

    public void testLog() {
        short value = -5;
        assertEquals(Math.log(value), log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, log(NULL_SHORT), 1e-10);
    }

    public void testPow() {
        short value0 = -5;
        short value1 = 2;
        assertEquals(Math.pow(value0, value1), pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(NULL_SHORT, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(value0, NULL_SHORT), 1e-10);
    }

    public void testRint() {
        short value = -5;
        assertEquals(Math.rint(value), rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, rint(NULL_SHORT), 1e-10);
    }

    public void testRound() {
        short value = -5;
        assertEquals(Math.round(value), round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, round(NULL_SHORT), 1e-10);
    }

    public void testSin() {
        short value = -5;
        assertEquals(Math.sin(value), sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sin(NULL_SHORT), 1e-10);
    }

    public void testSqrt() {
        short value = -5;
        assertEquals(Math.sqrt(value), sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sqrt(NULL_SHORT), 1e-10);
    }

    public void testTan() {
        short value = -5;
        assertEquals(Math.tan(value), tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, tan(NULL_SHORT), 1e-10);
    }

    public void testLowerBin() {
        short value = (short) 114;

        assertEquals((short) 110, lowerBin(value, (short) 5));
        assertEquals((short) 110, lowerBin(value, (short) 10));
        assertEquals((short) 100, lowerBin(value, (short) 20));
        assertEquals(NULL_SHORT, lowerBin(NULL_SHORT, (short) 5));
        assertEquals(NULL_SHORT, lowerBin(value, NULL_SHORT));

        assertEquals(lowerBin(value, (short) 5), lowerBin(lowerBin(value, (short) 5), (short) 5));
    }

    public void testLowerBinWithOffset() {
        short value = (short) 114;
        short offset = (short) 3;

        assertEquals((short) 113, lowerBin(value, (short) 5, offset));
        assertEquals((short) 113, lowerBin(value, (short) 10, offset));
        assertEquals((short) 103, lowerBin(value, (short) 20, offset));
        assertEquals(NULL_SHORT, lowerBin(NULL_SHORT, (short) 5, offset));
        assertEquals(NULL_SHORT, lowerBin(value, NULL_SHORT, offset));

        assertEquals(lowerBin(value, (short) 5, offset), lowerBin(lowerBin(value, (short) 5, offset), (short) 5, offset));
    }

    public void testUpperBin() {
        short value = (short) 114;

        assertEquals((short) 115, upperBin(value, (short) 5));
        assertEquals((short) 120, upperBin(value, (short) 10));
        assertEquals((short) 120, upperBin(value, (short) 20));
        assertEquals(NULL_SHORT, upperBin(NULL_SHORT, (short) 5));
        assertEquals(NULL_SHORT, upperBin(value, NULL_SHORT));

        assertEquals(upperBin(value, (short) 5), upperBin(upperBin(value, (short) 5), (short) 5));
    }

    public void testUpperBinWithOffset() {
        short value = (short) 114;
        short offset = (short) 3;

        assertEquals((short) 118, upperBin(value, (short) 5, offset));
        assertEquals((short) 123, upperBin(value, (short) 10, offset));
        assertEquals((short) 123, upperBin(value, (short) 20, offset));
        assertEquals(NULL_SHORT, upperBin(NULL_SHORT, (short) 5, offset));
        assertEquals(NULL_SHORT, upperBin(value, NULL_SHORT, offset));

        assertEquals(upperBin(value, (short) 5, offset), upperBin(upperBin(value, (short) 5, offset), (short) 5, offset));
    }

    public void testClamp() {
        assertEquals((short) 3, clamp((short) 3, (short) -6, (short) 5));
        assertEquals((short) -6, clamp((short) -7, (short) -6, (short) 5));
        assertEquals((short) 5, clamp((short) 7, (short) -6, (short) 5));
        assertEquals(NULL_SHORT, clamp(NULL_SHORT, (short) -6, (short) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, binSearchIndex((short[]) null, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new short[]{1,3,4}, (short)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new short[]{1,3,4}, (short)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new short[]{1,3,4}, (short)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new short[]{1,3,4}, (short)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new short[]{1,3,4}, (short)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new short[]{1,3,4}, (short)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((DbShortArray) null, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new DbShortArrayDirect(new short[]{1,3,4}), (short)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbShortArrayDirect(new short[]{1,3,4}), (short)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbShortArrayDirect(new short[]{1,3,4}), (short)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new DbShortArrayDirect(new short[]{1,3,4}), (short)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbShortArrayDirect(new short[]{1,3,4}), (short)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbShortArrayDirect(new short[]{1,3,4}), (short)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((DbShortArray)null, (short) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((DbShortArray)null, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((DbShortArray)null, (short) 0, BinSearch.BS_LOWEST));

        short[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(empty), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(empty), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(empty), (short) 0, BinSearch.BS_LOWEST));

        short[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbShortArrayDirect(one), (short) 11, BinSearch.BS_LOWEST));


        short[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((DbShortArray)null, (short) 0, null);

        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new DbShortArrayDirect(v), (short) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, rawBinSearchIndex((short[]) null, (short) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((short[])null, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((short[])null, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(empty, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(empty, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(empty, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(one, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(one, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(one, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(one, (short) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(one, (short) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(one, (short) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(one, (short) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(one, (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(one, (short) 11, BinSearch.BS_LOWEST));


        rawBinSearchIndex((short[])null, (short) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (short) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (short) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (short) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (short) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (short) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (short) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (short) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (short) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (short) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (short) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (short) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (short) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (short) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (short) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (short) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (short) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (short) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (short) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (short) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (short) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (short) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (short) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (short) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (short) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (short) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final short[] shorts = new short[]{1, -5, -2, -2, 96, 0, 12, NULL_SHORT, NULL_SHORT};
        final DbShortArray sort = sort(new DbShortArrayDirect(shorts));
        final DbShortArray expected = new DbShortArrayDirect(new short[]{NULL_SHORT, NULL_SHORT, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        short[] sortedArray = sort(shorts);
        assertEquals(new short[]{NULL_SHORT, NULL_SHORT, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((DbShortArray)null));
        assertNull(sort((short[])null));
        assertNull(sort((Short[])null));
        assertEquals(new DbShortArrayDirect(), sort(new DbShortArrayDirect()));
        assertEquals(new short[]{}, sort(new short[]{}));
        assertEquals(new short[]{}, sort(new Short[]{}));
    }

    public void testSortDescending() {
        final short[] shorts = new short[]{1, -5, -2, -2, 96, 0, 12, NULL_SHORT, NULL_SHORT};
        final DbShortArray sort = sortDescending(new DbShortArrayDirect(shorts));
        final DbShortArray expected = new DbShortArrayDirect(new short[]{96, 12, 1, 0, -2, -2, -5, NULL_SHORT, NULL_SHORT});
        assertEquals(expected, sort);

        short[] sortedArray = sortDescending(shorts);
        assertEquals(new short[]{96, 12, 1, 0, -2, -2, -5, NULL_SHORT, NULL_SHORT}, sortedArray);

        assertNull(sortDescending((DbShortArray)null));
        assertNull(sortDescending((short[])null));
        assertNull(sortDescending((Short[])null));
        assertEquals(new DbShortArrayDirect(), sortDescending(new DbShortArrayDirect()));
        assertEquals(new short[]{}, sortDescending(new short[]{}));
        assertEquals(new short[]{}, sortDescending(new Short[]{}));
    }

    public void testSortsExceptions() {
        DbShortArray dbShortArray = null;
        DbShortArray sort = sort(dbShortArray);
        assertNull(sort);

        short[] shorts = null;
        short[] sortArray = sort(shorts);
        assertNull(sortArray);

        shorts = new short[]{};
        sort = sort(new DbShortArrayDirect(shorts));
        assertEquals(new DbShortArrayDirect(), sort);

        sortArray = sort(shorts);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DbShortArray dbShortArray = null;
        DbShortArray sort = sortDescending(dbShortArray);
        assertNull(sort);

        short[] shorts = null;
        short[] sortArray = sortDescending(shorts);
        assertNull(sortArray);

        shorts = new short[]{};
        sort = sortDescending(new DbShortArrayDirect(shorts));
        assertEquals(new DbShortArrayDirect(), sort);

        sortArray = sortDescending(shorts);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSequence(){
        assertEquals(new short[]{0,1,2,3,4,5}, ShortNumericPrimitives.sequence((short)0, (short)5, (short)1));
        assertEquals(new short[]{-5,-4,-3,-2,-1,0}, ShortNumericPrimitives.sequence((short)-5, (short)0, (short)1));

        assertEquals(new short[]{0,2,4}, ShortNumericPrimitives.sequence((short)0, (short)5, (short)2));
        assertEquals(new short[]{-5,-3,-1}, ShortNumericPrimitives.sequence((short)-5, (short)0, (short)2));

        assertEquals(new short[]{5,3,1}, ShortNumericPrimitives.sequence((short)5, (short)0, (short)-2));
        assertEquals(new short[]{0,-2,-4}, ShortNumericPrimitives.sequence((short)0, (short)-5, (short)-2));

        assertEquals(new short[]{}, ShortNumericPrimitives.sequence((short)0, (short)5, (short)0));
        assertEquals(new short[]{}, ShortNumericPrimitives.sequence((short)5, (short)0, (short)1));
    }

    public void testMedian() {
        assertEquals(3.0, median(new short[]{4,2,3}));
        assertEquals(3.5, median(new short[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((short[])null));

        assertEquals(3.0, median(new Short[]{(short)4,(short)2,(short)3}));
        assertEquals(3.5, median(new Short[]{(short)5,(short)4,(short)2,(short)3}));
        assertEquals(NULL_DOUBLE, median((Short[])null));

        assertEquals(3.0, median(new DbShortArrayDirect(new short[]{4,2,3})));
        assertEquals(3.5, median(new DbShortArrayDirect(new short[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((DbShortArray) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(0.00, new short[]{4,2,3}));
        assertEquals(3.0, percentile(0.50, new short[]{4,2,3}));
        assertEquals(NULL_DOUBLE, percentile(0.25, (short[])null));

        assertEquals(2.0, percentile(0.00, new DbShortArrayDirect(new short[]{4,2,3})));
        assertEquals(3.0, percentile(0.50, new DbShortArrayDirect(new short[]{4,2,3})));
        assertEquals(NULL_DOUBLE, percentile(0.25, (DbShortArray) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (short[])null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (int[])null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (long[])null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (double[])null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wvar(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wvar(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (short[])null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (int[])null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (long[])null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (double[])null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wstd(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wstd(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (short[])null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (int[])null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (long[])null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (double[])null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wste((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wste(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wste((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wste(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWtstat() {
        final double target = wavg(new short[]{1,2,3}, new short[]{4,5,6}) / wste(new short[]{1,2,3}, new short[]{4,5,6});

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (short[])null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (int[])null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (long[])null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (double[])null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (DbShortArray) null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wtstat(new short[]{1,2,3,NULL_SHORT,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((short[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new short[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbShortArrayDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray) null, new DbShortArrayDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (DbShortArray) null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wtstat(new DbShortArrayDirect(new short[]{1,2,3,NULL_SHORT,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((DbShortArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbShortArrayDirect(new short[]{1,2,3}), (DbFloatArray)null));
    }
}
