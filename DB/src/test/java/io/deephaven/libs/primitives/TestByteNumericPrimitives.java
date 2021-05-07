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
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.libs.primitives.ByteNumericPrimitives.*;
import static io.deephaven.libs.primitives.BytePrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestByteNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((byte) 1, signum((byte) 5));
        assertEquals((byte) 0, signum((byte) 0));
        assertEquals((byte) -1, signum((byte) -5));
        assertEquals(NULL_BYTE, signum(NULL_BYTE));
    }

    public void testAvg() {
        assertEquals(50.0, avg(new byte[]{40, 50, 60}));
        assertEquals(45.5, avg(new byte[]{40, 51}));
        assertTrue(Double.isNaN(avg(new byte[]{})));
        assertTrue(Double.isNaN(avg(new byte[]{NULL_BYTE})));
        assertEquals(10.0, avg(new byte[]{5, NULL_BYTE, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((byte[])null));

        assertEquals(50.0, avg(new Byte[]{(byte)40, (byte)50, (byte)60}));
        assertEquals(45.5, avg(new Byte[]{(byte)40, (byte)51}));
        assertTrue(Double.isNaN(avg(new Byte[]{})));
        assertTrue(Double.isNaN(avg(new Byte[]{NULL_BYTE})));
        assertEquals(10.0, avg(new Byte[]{(byte)5, NULL_BYTE, (byte)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((Byte[])null));

        assertEquals(50.0, avg(new DbByteArrayDirect(new byte[]{40, 50, 60})));
        assertEquals(45.5, avg(new DbByteArrayDirect(new byte[]{40, 51})));
        assertTrue(Double.isNaN(avg(new DbByteArrayDirect())));
        assertTrue(Double.isNaN(avg(new DbByteArrayDirect(NULL_BYTE))));
        assertEquals(10.0, avg(new DbByteArrayDirect(new byte[]{5, NULL_BYTE, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((DbByteArrayDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, absAvg(new byte[]{40, (byte) 50, 60}));
        assertEquals(45.5, absAvg(new byte[]{(byte) 40, 51}));
        assertTrue(Double.isNaN(absAvg(new byte[]{})));
        assertTrue(Double.isNaN(absAvg(new byte[]{NULL_BYTE})));
        assertEquals(10.0, absAvg(new byte[]{(byte) 5, NULL_BYTE, (byte) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((byte[])null));

        assertEquals(50.0, absAvg(new Byte[]{(byte)40, (byte) 50, (byte)60}));
        assertEquals(45.5, absAvg(new Byte[]{(byte) 40, (byte)51}));
        assertTrue(Double.isNaN(absAvg(new Byte[]{})));
        assertTrue(Double.isNaN(absAvg(new Byte[]{NULL_BYTE})));
        assertEquals(10.0, absAvg(new Byte[]{(byte) 5, NULL_BYTE, (byte) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((Byte[])null));

        assertEquals(50.0, absAvg(new DbByteArrayDirect(new byte[]{40, (byte) 50, 60})));
        assertEquals(45.5, absAvg(new DbByteArrayDirect(new byte[]{(byte) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new DbByteArrayDirect())));
        assertTrue(Double.isNaN(absAvg(new DbByteArrayDirect(NULL_BYTE))));
        assertEquals(10.0, absAvg(new DbByteArrayDirect((byte) 5, NULL_BYTE, (byte) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((DbByteArrayDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, countPos(new byte[]{40, 50, 60, (byte) 1, 0}));
        assertEquals(0, countPos(new byte[]{}));
        assertEquals(0, countPos(new byte[]{NULL_BYTE}));
        assertEquals(3, countPos(new byte[]{5, NULL_BYTE, 15, (byte) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, countPos((byte[])null));

        assertEquals(4, countPos(new Byte[]{(byte)40, (byte)50, (byte)60, (byte) 1, (byte)0}));
        assertEquals(0, countPos(new Byte[]{}));
        assertEquals(0, countPos(new Byte[]{NULL_BYTE}));
        assertEquals(3, countPos(new Byte[]{(byte)5, NULL_BYTE, (byte)15, (byte) 1, (byte)0}));
        assertEquals(QueryConstants.NULL_INT, countPos((byte[])null));

        assertEquals(4, countPos(new DbByteArrayDirect(new byte[]{40, 50, 60, (byte) 1, 0})));
        assertEquals(0, countPos(new DbByteArrayDirect()));
        assertEquals(0, countPos(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(3, countPos(new DbByteArrayDirect(new byte[]{5, NULL_BYTE, 15, (byte) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((DbByteArrayDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, countNeg(new byte[]{40, (byte) -50, 60, (byte) -1, 0}));
        assertEquals(0, countNeg(new byte[]{}));
        assertEquals(0, countNeg(new byte[]{NULL_BYTE}));
        assertEquals(1, countNeg(new byte[]{5, NULL_BYTE, 15, (byte) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((byte[])null));

        assertEquals(2, countNeg(new Byte[]{(byte)40, (byte) -50, (byte)60, (byte) -1, (byte)0}));
        assertEquals(0, countNeg(new Byte[]{}));
        assertEquals(0, countNeg(new Byte[]{NULL_BYTE}));
        assertEquals(1, countNeg(new Byte[]{(byte)5, NULL_BYTE, (byte)15, (byte) -1, (byte)0}));
        assertEquals(QueryConstants.NULL_INT, countNeg((Byte[])null));

        assertEquals(2, countNeg(new DbByteArrayDirect(new byte[]{40, (byte) -50, 60, (byte) -1, 0})));
        assertEquals(0, countNeg(new DbByteArrayDirect()));
        assertEquals(0, countNeg(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(1, countNeg(new DbByteArrayDirect(new byte[]{5, NULL_BYTE, 15, (byte) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((DbByteArrayDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, countZero(new byte[]{0, 40, 50, 60, (byte) -1, 0}));
        assertEquals(0, countZero(new byte[]{}));
        assertEquals(0, countZero(new byte[]{NULL_BYTE}));
        assertEquals(2, countZero(new byte[]{0, 5, NULL_BYTE, 0, (byte) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((byte[])null));

        assertEquals(2, countZero(new Byte[]{(byte)0, (byte)40, (byte)50, (byte)60, (byte) -1, (byte)0}));
        assertEquals(0, countZero(new Byte[]{}));
        assertEquals(0, countZero(new Byte[]{NULL_BYTE}));
        assertEquals(2, countZero(new Byte[]{(byte)0, (byte)5, NULL_BYTE, (byte)0, (byte) -15}));
        assertEquals(QueryConstants.NULL_INT, countZero((Byte[])null));

        assertEquals(2, countZero(new DbByteArrayDirect(new byte[]{0, 40, 50, 60, (byte) -1, 0})));
        assertEquals(0, countZero(new DbByteArrayDirect()));
        assertEquals(0, countZero(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(2, countZero(new DbByteArrayDirect(new byte[]{0, 5, NULL_BYTE, 0, (byte) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((DbByteArrayDirect)null));
    }

    public void testMax() {
        assertEquals((byte) 60, max(new DbByteArrayDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals((byte) 60, max(new DbByteArrayDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1)));
        assertEquals(NULL_BYTE, max(new DbByteArrayDirect()));
        assertEquals(NULL_BYTE, max(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(NULL_BYTE, max((DbByteArray) null));

        assertEquals((byte) 60, max((byte) 0, (byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1, (byte) 0));
        assertEquals((byte) 60, max((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1));
        assertEquals(NULL_BYTE, max());
        assertEquals(NULL_BYTE, max(NULL_BYTE));
        assertEquals(NULL_BYTE, max((byte[]) null));
        assertEquals(NULL_BYTE, max((Byte[]) null));
    }

    public void testMin() {
        assertEquals((byte) 0, min(new DbByteArrayDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals((byte) -1, min(new DbByteArrayDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1)));
        assertEquals(NULL_BYTE, min(new DbByteArrayDirect()));
        assertEquals(NULL_BYTE, min(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(NULL_BYTE, min((DbByteArray) null));

        assertEquals((byte) 0, min((byte) 0, (byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1, (byte) 0));
        assertEquals((byte) -1, min((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1));
        assertEquals(NULL_BYTE, min());
        assertEquals(NULL_BYTE, min(NULL_BYTE));
        assertEquals(NULL_BYTE, min((byte[]) null));
        assertEquals(NULL_BYTE, min((Byte[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, firstIndexOf(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}, (byte)40));
        assertEquals(4, firstIndexOf(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}, (byte)60));
        assertEquals(NULL_INT, firstIndexOf(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}, (byte)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((byte[])null, (byte)40));

        assertEquals(1, firstIndexOf(new DbByteArrayDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)40));
        assertEquals(4, firstIndexOf(new DbByteArrayDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)60));
        assertEquals(NULL_INT, firstIndexOf(new DbByteArrayDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((DbByteArray) null, (byte)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, indexOfMax(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0}));
        assertEquals(3, indexOfMax(new byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1}));
        assertEquals(-1, indexOfMax(new byte[]{}));
        assertEquals(-1, indexOfMax(new byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((byte[])null));

        assertEquals(4, indexOfMax(new Byte[]{(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) 1, (byte)0}));
        assertEquals(3, indexOfMax(new Byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1}));
        assertEquals(-1, indexOfMax(new Byte[]{}));
        assertEquals(-1, indexOfMax(new Byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((Byte[])null));

        assertEquals(4, indexOfMax(new DbByteArrayDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals(3, indexOfMax(new DbByteArrayDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1)));
        assertEquals(-1, indexOfMax(new DbByteArrayDirect()));
        assertEquals(-1, indexOfMax(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((DbByteArrayDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, indexOfMin(new byte[]{40, 0, NULL_BYTE, 50, 60, (byte) 1, 0}));
        assertEquals(4, indexOfMin(new byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1}));
        assertEquals(-1, indexOfMin(new byte[]{}));
        assertEquals(-1, indexOfMin(new byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((byte[])null));

        assertEquals(1, indexOfMin(new Byte[]{(byte)40, (byte)0, NULL_BYTE, (byte)50, (byte)60, (byte) 1, (byte)0}));
        assertEquals(4, indexOfMin(new Byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1}));
        assertEquals(-1, indexOfMin(new Byte[]{}));
        assertEquals(-1, indexOfMin(new Byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((Byte[])null));

        assertEquals(1, indexOfMin(new DbByteArrayDirect(new byte[]{40, 0, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals(4, indexOfMin(new DbByteArrayDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1)));
        assertEquals(-1, indexOfMin(new DbByteArrayDirect()));
        assertEquals(-1, indexOfMin(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((DbByteArrayDirect)null));
    }


    public void testVar() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, var((byte[])null));

        assertEquals(var, var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, var((Byte[])null));

        assertEquals(var, var(new DbByteArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((DbByteArrayDirect)null));
    }

    public void testStd() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(Math.sqrt(var(new DbByteArrayDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((byte[])null));

        assertEquals(Math.sqrt(var(new DbByteArrayDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Byte[])null));

        assertEquals(Math.sqrt(var(new DbByteArrayDirect(v))), std(new DbByteArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((DbByteArrayDirect)null));
    }

    public void testSte() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(std(new DbByteArrayDirect(v)) / Math.sqrt(count(new DbByteArrayDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((byte[])null));

        assertEquals(std(new DbByteArrayDirect(v)) / Math.sqrt(count(new DbByteArrayDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Byte[])null));

        assertEquals(std(new DbByteArrayDirect(v)) / Math.sqrt(count(new DbByteArrayDirect(v))), ste(new DbByteArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((DbByteArrayDirect)null));
    }

    public void testTstat() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(avg(new DbByteArrayDirect(v)) / ste(new DbByteArrayDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((byte[])null));

        assertEquals(avg(new DbByteArrayDirect(v)) / ste(new DbByteArrayDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Byte[])null));

        assertEquals(avg(new DbByteArrayDirect(v)) / ste(new DbByteArrayDirect(v)), tstat(new DbByteArrayDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((DbByteArrayDirect)null));
    }

    public void testCov() {
        byte[] a = {10, 40, NULL_BYTE, 50, NULL_BYTE, (byte) -1, 0, (byte) -7};
        byte[] b = {0, (byte) -40, NULL_BYTE, NULL_BYTE, 6, (byte) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((byte[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((byte[])null, (byte[]) null));

        assertEquals(cov, cov(a, new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (DbByteArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((byte[])null, new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((byte[])null, (DbByteArrayDirect)null));

        assertEquals(cov, cov(new DbByteArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbByteArrayDirect(a), (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbByteArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbByteArrayDirect)null, (byte[])null));

        assertEquals(cov, cov(new DbByteArrayDirect(a), new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new DbByteArrayDirect(a), (DbByteArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbByteArrayDirect)null, new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((DbByteArrayDirect)null, (DbByteArrayDirect)null));
    }

    public void testCor() {
        byte[] a = {10, 40, NULL_BYTE, 50, NULL_BYTE, (byte) -1, 0, (byte) -7};
        byte[] b = {0, (byte) -40, NULL_BYTE, NULL_BYTE, 6, (byte) -1, 11, 3};
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
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((byte[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((byte[])null, (byte[])null));

        assertEquals(cor, cor(a, new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (DbByteArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((byte[])null, new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((byte[])null, (DbByteArrayDirect)null));

        assertEquals(cor, cor(new DbByteArrayDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbByteArrayDirect(a), (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbByteArrayDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbByteArrayDirect)null, (byte[])null));

        assertEquals(cor, cor(new DbByteArrayDirect(a), new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new DbByteArrayDirect(a), (DbByteArrayDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbByteArrayDirect)null, new DbByteArrayDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((DbByteArrayDirect)null, (DbByteArrayDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new DbByteArrayDirect(new byte[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbByteArrayDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new DbByteArrayDirect(NULL_BYTE))) == 0.0);
        assertTrue(Math.abs(20 - sum(new DbByteArrayDirect(new byte[]{5, NULL_BYTE, 15}))) == 0.0);
        assertEquals(NULL_BYTE, sum((DbByteArray) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new byte[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new byte[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new byte[]{NULL_BYTE})) == 0.0);
        assertTrue(Math.abs(20 - sum(new byte[]{5, NULL_BYTE, 15})) == 0.0);
        assertEquals(NULL_BYTE, sum((byte[]) null));
    }

    public void testSumDbArray() {
        assertEquals(new byte[]{4, 15}, sum(new DbArrayDirect<>(new byte[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new byte[]{4, NULL_BYTE}, sum(new DbArrayDirect<>(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((DbArray<byte[]>) null));

        try {
            sum(new DbArrayDirect<>(new byte[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new byte[]{4, 15}, sum(new byte[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new byte[]{4, NULL_BYTE}, sum(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}}));
        assertEquals(null, sum((byte[][]) null));

        try {
            sum(new byte[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - product(new byte[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_BYTE, product(new byte[]{}));
        assertEquals(NULL_BYTE, product(new byte[]{NULL_BYTE}));
        assertTrue(Math.abs(75 - product(new byte[]{5, NULL_BYTE, 15})) == 0.0);
        assertEquals(NULL_BYTE, product((byte[]) null));

        assertTrue(Math.abs(120 - product(new DbByteArrayDirect(new byte[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_BYTE, product(new DbByteArrayDirect()));
        assertEquals(NULL_BYTE, product(new DbByteArrayDirect(NULL_BYTE)));
        assertTrue(Math.abs(75 - product(new DbByteArrayDirect(new byte[]{5, NULL_BYTE, 15}))) == 0.0);
        assertEquals(NULL_BYTE, product((DbByteArray) null));
    }

//    public void testProdDbArray() {
//        assertEquals(new byte[]{-30, 120}, product(new DbArrayDirect<>(new byte[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new byte[]{-30, NULL_BYTE}, product(new DbArrayDirect<>(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((DbArray<byte[]>) null));
//
//        try {
//            product(new DbArrayDirect<>(new byte[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void testProdArray() {
//        assertEquals(new byte[]{-30, 120}, product(new byte[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new byte[]{-30, NULL_BYTE}, product(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((byte[][]) null));
//
//        try {
//            product(new byte[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void testCumSumArray() {
        assertEquals(new byte[]{1, 3, 6, 10, 15}, cumsum(new byte[]{1, 2, 3, 4, 5}));
        assertEquals(new byte[]{1, 3, 6, 6, 11}, cumsum(new byte[]{1, 2, 3, NULL_BYTE, 5}));
        assertEquals(new byte[]{NULL_BYTE, 2, 5, 9, 14}, cumsum(new byte[]{NULL_BYTE, 2, 3, 4, 5}));
        assertEquals(new byte[0], cumsum());
        assertEquals(null, cumsum((byte[]) null));

        assertEquals(new byte[]{1, 3, 6, 10, 15}, cumsum(new DbByteArrayDirect(new byte[]{1, 2, 3, 4, 5})));
        assertEquals(new byte[]{1, 3, 6, 6, 11}, cumsum(new DbByteArrayDirect(new byte[]{1, 2, 3, NULL_BYTE, 5})));
        assertEquals(new byte[]{NULL_BYTE, 2, 5, 9, 14}, cumsum(new DbByteArrayDirect(new byte[]{NULL_BYTE, 2, 3, 4, 5})));
        assertEquals(new byte[0], cumsum(new DbByteArrayDirect()));
        assertEquals(null, cumsum((DbByteArray) null));
    }

    public void testCumProdArray() {
        assertEquals(new byte[]{1, 2, 6, 24, 120}, cumprod(new byte[]{1, 2, 3, 4, 5}));
        assertEquals(new byte[]{1, 2, 6, 6, 30}, cumprod(new byte[]{1, 2, 3, NULL_BYTE, 5}));
        assertEquals(new byte[]{NULL_BYTE, 2, 6, 24, 120}, cumprod(new byte[]{NULL_BYTE, 2, 3, 4, 5}));
        assertEquals(new byte[0], cumprod());
        assertEquals(null, cumprod((byte[]) null));

        assertEquals(new byte[]{1, 2, 6, 24, 120}, cumprod(new DbByteArrayDirect(new byte[]{1, 2, 3, 4, 5})));
        assertEquals(new byte[]{1, 2, 6, 6, 30}, cumprod(new DbByteArrayDirect(new byte[]{1, 2, 3, NULL_BYTE, 5})));
        assertEquals(new byte[]{NULL_BYTE, 2, 6, 24, 120}, cumprod(new DbByteArrayDirect(new byte[]{NULL_BYTE, 2, 3, 4, 5})));
        assertEquals(new byte[0], cumprod(new DbByteArrayDirect()));
        assertEquals(null, cumprod((DbByteArray) null));
    }

    public void testAbs() {
        byte value = -5;
        assertEquals((byte) Math.abs(value), abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_BYTE, abs(NULL_BYTE), 1e-10);
    }

    public void testAcos() {
        byte value = -5;
        assertEquals(Math.acos(value), acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, acos(NULL_BYTE), 1e-10);
    }

    public void testAsin() {
        byte value = -5;
        assertEquals(Math.asin(value), asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, asin(NULL_BYTE), 1e-10);
    }

    public void testAtan() {
        byte value = -5;
        assertEquals(Math.atan(value), atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, atan(NULL_BYTE), 1e-10);
    }

    public void testCeil() {
        byte value = -5;
        assertEquals(Math.ceil(value), ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ceil(NULL_BYTE), 1e-10);
    }

    public void testCos() {
        byte value = -5;
        assertEquals(Math.cos(value), cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, cos(NULL_BYTE), 1e-10);
    }

    public void testExp() {
        byte value = -5;
        assertEquals(Math.exp(value), exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, exp(NULL_BYTE), 1e-10);
    }

    public void testFloor() {
        byte value = -5;
        assertEquals(Math.floor(value), floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, floor(NULL_BYTE), 1e-10);
    }

    public void testLog() {
        byte value = -5;
        assertEquals(Math.log(value), log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, log(NULL_BYTE), 1e-10);
    }

    public void testPow() {
        byte value0 = -5;
        byte value1 = 2;
        assertEquals(Math.pow(value0, value1), pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(NULL_BYTE, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, pow(value0, NULL_BYTE), 1e-10);
    }

    public void testRint() {
        byte value = -5;
        assertEquals(Math.rint(value), rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, rint(NULL_BYTE), 1e-10);
    }

    public void testRound() {
        byte value = -5;
        assertEquals(Math.round(value), round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, round(NULL_BYTE), 1e-10);
    }

    public void testSin() {
        byte value = -5;
        assertEquals(Math.sin(value), sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sin(NULL_BYTE), 1e-10);
    }

    public void testSqrt() {
        byte value = -5;
        assertEquals(Math.sqrt(value), sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, sqrt(NULL_BYTE), 1e-10);
    }

    public void testTan() {
        byte value = -5;
        assertEquals(Math.tan(value), tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, tan(NULL_BYTE), 1e-10);
    }

    public void testLowerBin() {
        byte value = (byte) 114;

        assertEquals((byte) 110, lowerBin(value, (byte) 5));
        assertEquals((byte) 110, lowerBin(value, (byte) 10));
        assertEquals((byte) 100, lowerBin(value, (byte) 20));
        assertEquals(NULL_BYTE, lowerBin(NULL_BYTE, (byte) 5));
        assertEquals(NULL_BYTE, lowerBin(value, NULL_BYTE));

        assertEquals(lowerBin(value, (byte) 5), lowerBin(lowerBin(value, (byte) 5), (byte) 5));
    }

    public void testLowerBinWithOffset() {
        byte value = (byte) 114;
        byte offset = (byte) 3;

        assertEquals((byte) 113, lowerBin(value, (byte) 5, offset));
        assertEquals((byte) 113, lowerBin(value, (byte) 10, offset));
        assertEquals((byte) 103, lowerBin(value, (byte) 20, offset));
        assertEquals(NULL_BYTE, lowerBin(NULL_BYTE, (byte) 5, offset));
        assertEquals(NULL_BYTE, lowerBin(value, NULL_BYTE, offset));

        assertEquals(lowerBin(value, (byte) 5, offset), lowerBin(lowerBin(value, (byte) 5, offset), (byte) 5, offset));
    }

    public void testUpperBin() {
        byte value = (byte) 114;

        assertEquals((byte) 115, upperBin(value, (byte) 5));
        assertEquals((byte) 120, upperBin(value, (byte) 10));
        assertEquals((byte) 120, upperBin(value, (byte) 20));
        assertEquals(NULL_BYTE, upperBin(NULL_BYTE, (byte) 5));
        assertEquals(NULL_BYTE, upperBin(value, NULL_BYTE));

        assertEquals(upperBin(value, (byte) 5), upperBin(upperBin(value, (byte) 5), (byte) 5));
    }

    public void testUpperBinWithOffset() {
        byte value = (byte) 114;
        byte offset = (byte) 3;

        assertEquals((byte) 118, upperBin(value, (byte) 5, offset));
        assertEquals((byte) 123, upperBin(value, (byte) 10, offset));
        assertEquals((byte) 123, upperBin(value, (byte) 20, offset));
        assertEquals(NULL_BYTE, upperBin(NULL_BYTE, (byte) 5, offset));
        assertEquals(NULL_BYTE, upperBin(value, NULL_BYTE, offset));

        assertEquals(upperBin(value, (byte) 5, offset), upperBin(upperBin(value, (byte) 5, offset), (byte) 5, offset));
    }

    public void testClamp() {
        assertEquals((byte) 3, clamp((byte) 3, (byte) -6, (byte) 5));
        assertEquals((byte) -6, clamp((byte) -7, (byte) -6, (byte) 5));
        assertEquals((byte) 5, clamp((byte) 7, (byte) -6, (byte) 5));
        assertEquals(NULL_BYTE, clamp(NULL_BYTE, (byte) -6, (byte) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, binSearchIndex((byte[]) null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new byte[]{1,3,4}, (byte)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new byte[]{1,3,4}, (byte)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new byte[]{1,3,4}, (byte)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new byte[]{1,3,4}, (byte)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new byte[]{1,3,4}, (byte)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new byte[]{1,3,4}, (byte)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((DbByteArray) null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new DbByteArrayDirect(new byte[]{1,3,4}), (byte)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbByteArrayDirect(new byte[]{1,3,4}), (byte)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new DbByteArrayDirect(new byte[]{1,3,4}), (byte)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new DbByteArrayDirect(new byte[]{1,3,4}), (byte)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbByteArrayDirect(new byte[]{1,3,4}), (byte)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new DbByteArrayDirect(new byte[]{1,3,4}), (byte)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((DbByteArray)null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((DbByteArray)null, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((DbByteArray)null, (byte) 0, BinSearch.BS_LOWEST));

        byte[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(empty), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(empty), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(empty), (byte) 0, BinSearch.BS_LOWEST));

        byte[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbByteArrayDirect(one), (byte) 11, BinSearch.BS_LOWEST));


        byte[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((DbByteArray)null, (byte) 0, null);

        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new DbByteArrayDirect(v), (byte) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, rawBinSearchIndex((byte[]) null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((byte[])null, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((byte[])null, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(empty, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(empty, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(empty, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(one, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(one, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(one, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(one, (byte) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(one, (byte) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(one, (byte) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(one, (byte) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(one, (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(one, (byte) 11, BinSearch.BS_LOWEST));


        rawBinSearchIndex((byte[])null, (byte) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (byte) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (byte) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (byte) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (byte) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (byte) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (byte) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (byte) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (byte) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (byte) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (byte) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (byte) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (byte) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (byte) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (byte) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (byte) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (byte) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (byte) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (byte) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (byte) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (byte) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (byte) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (byte) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (byte) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (byte) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (byte) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final byte[] bytes = new byte[]{1, -5, -2, -2, 96, 0, 12, NULL_BYTE, NULL_BYTE};
        final DbByteArray sort = sort(new DbByteArrayDirect(bytes));
        final DbByteArray expected = new DbByteArrayDirect(new byte[]{NULL_BYTE, NULL_BYTE, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        byte[] sortedArray = sort(bytes);
        assertEquals(new byte[]{NULL_BYTE, NULL_BYTE, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((DbByteArray)null));
        assertNull(sort((byte[])null));
        assertNull(sort((Byte[])null));
        assertEquals(new DbByteArrayDirect(), sort(new DbByteArrayDirect()));
        assertEquals(new byte[]{}, sort(new byte[]{}));
        assertEquals(new byte[]{}, sort(new Byte[]{}));
    }

    public void testSortDescending() {
        final byte[] bytes = new byte[]{1, -5, -2, -2, 96, 0, 12, NULL_BYTE, NULL_BYTE};
        final DbByteArray sort = sortDescending(new DbByteArrayDirect(bytes));
        final DbByteArray expected = new DbByteArrayDirect(new byte[]{96, 12, 1, 0, -2, -2, -5, NULL_BYTE, NULL_BYTE});
        assertEquals(expected, sort);

        byte[] sortedArray = sortDescending(bytes);
        assertEquals(new byte[]{96, 12, 1, 0, -2, -2, -5, NULL_BYTE, NULL_BYTE}, sortedArray);

        assertNull(sortDescending((DbByteArray)null));
        assertNull(sortDescending((byte[])null));
        assertNull(sortDescending((Byte[])null));
        assertEquals(new DbByteArrayDirect(), sortDescending(new DbByteArrayDirect()));
        assertEquals(new byte[]{}, sortDescending(new byte[]{}));
        assertEquals(new byte[]{}, sortDescending(new Byte[]{}));
    }

    public void testSortsExceptions() {
        DbByteArray dbByteArray = null;
        DbByteArray sort = sort(dbByteArray);
        assertNull(sort);

        byte[] bytes = null;
        byte[] sortArray = sort(bytes);
        assertNull(sortArray);

        bytes = new byte[]{};
        sort = sort(new DbByteArrayDirect(bytes));
        assertEquals(new DbByteArrayDirect(), sort);

        sortArray = sort(bytes);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DbByteArray dbByteArray = null;
        DbByteArray sort = sortDescending(dbByteArray);
        assertNull(sort);

        byte[] bytes = null;
        byte[] sortArray = sortDescending(bytes);
        assertNull(sortArray);

        bytes = new byte[]{};
        sort = sortDescending(new DbByteArrayDirect(bytes));
        assertEquals(new DbByteArrayDirect(), sort);

        sortArray = sortDescending(bytes);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSequence(){
        assertEquals(new byte[]{0,1,2,3,4,5}, ByteNumericPrimitives.sequence((byte)0, (byte)5, (byte)1));
        assertEquals(new byte[]{-5,-4,-3,-2,-1,0}, ByteNumericPrimitives.sequence((byte)-5, (byte)0, (byte)1));

        assertEquals(new byte[]{0,2,4}, ByteNumericPrimitives.sequence((byte)0, (byte)5, (byte)2));
        assertEquals(new byte[]{-5,-3,-1}, ByteNumericPrimitives.sequence((byte)-5, (byte)0, (byte)2));

        assertEquals(new byte[]{5,3,1}, ByteNumericPrimitives.sequence((byte)5, (byte)0, (byte)-2));
        assertEquals(new byte[]{0,-2,-4}, ByteNumericPrimitives.sequence((byte)0, (byte)-5, (byte)-2));

        assertEquals(new byte[]{}, ByteNumericPrimitives.sequence((byte)0, (byte)5, (byte)0));
        assertEquals(new byte[]{}, ByteNumericPrimitives.sequence((byte)5, (byte)0, (byte)1));
    }

    public void testMedian() {
        assertEquals(3.0, median(new byte[]{4,2,3}));
        assertEquals(3.5, median(new byte[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((byte[])null));

        assertEquals(3.0, median(new Byte[]{(byte)4,(byte)2,(byte)3}));
        assertEquals(3.5, median(new Byte[]{(byte)5,(byte)4,(byte)2,(byte)3}));
        assertEquals(NULL_DOUBLE, median((Byte[])null));

        assertEquals(3.0, median(new DbByteArrayDirect(new byte[]{4,2,3})));
        assertEquals(3.5, median(new DbByteArrayDirect(new byte[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((DbByteArray) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(0.00, new byte[]{4,2,3}));
        assertEquals(3.0, percentile(0.50, new byte[]{4,2,3}));
        assertEquals(NULL_DOUBLE, percentile(0.25, (byte[])null));

        assertEquals(2.0, percentile(0.00, new DbByteArrayDirect(new byte[]{4,2,3})));
        assertEquals(3.0, percentile(0.50, new DbByteArrayDirect(new byte[]{4,2,3})));
        assertEquals(NULL_DOUBLE, percentile(0.25, (DbByteArray) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wvar(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wstd(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wste((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wste((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wste(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }

    public void testWtstat() {
        final double target = wavg(new byte[]{1,2,3}, new byte[]{4,5,6}) / wste(new byte[]{1,2,3}, new byte[]{4,5,6});

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (DbByteArray) null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (DbIntArray) null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (DbLongArray) null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (DbDoubleArray)null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (DbFloatArray)null));

        /////

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbByteArrayDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray) null, new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (DbByteArray) null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbIntArrayDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray) null, new DbIntArrayDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (DbIntArray) null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbLongArrayDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray) null, new DbLongArrayDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (DbLongArray) null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbDoubleArrayDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray) null, new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (DbDoubleArray)null));

        assertEquals(target, wtstat(new DbByteArrayDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DbFloatArrayDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((DbByteArray) null, new DbFloatArrayDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new DbByteArrayDirect(new byte[]{1,2,3}), (DbFloatArray)null));
    }
}
