/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestShortNumericPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.vector.*;
import io.deephaven.engine.vector.*;
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

        assertEquals(50.0, avg(new ByteVectorDirect(new byte[]{40, 50, 60})));
        assertEquals(45.5, avg(new ByteVectorDirect(new byte[]{40, 51})));
        assertTrue(Double.isNaN(avg(new ByteVectorDirect())));
        assertTrue(Double.isNaN(avg(new ByteVectorDirect(NULL_BYTE))));
        assertEquals(10.0, avg(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, avg((ByteVectorDirect)null));
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

        assertEquals(50.0, absAvg(new ByteVectorDirect(new byte[]{40, (byte) 50, 60})));
        assertEquals(45.5, absAvg(new ByteVectorDirect(new byte[]{(byte) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new ByteVectorDirect())));
        assertTrue(Double.isNaN(absAvg(new ByteVectorDirect(NULL_BYTE))));
        assertEquals(10.0, absAvg(new ByteVectorDirect((byte) 5, NULL_BYTE, (byte) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, absAvg((ByteVectorDirect)null));
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

        assertEquals(4, countPos(new ByteVectorDirect(new byte[]{40, 50, 60, (byte) 1, 0})));
        assertEquals(0, countPos(new ByteVectorDirect()));
        assertEquals(0, countPos(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(3, countPos(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15, (byte) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, countPos((ByteVectorDirect)null));
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

        assertEquals(2, countNeg(new ByteVectorDirect(new byte[]{40, (byte) -50, 60, (byte) -1, 0})));
        assertEquals(0, countNeg(new ByteVectorDirect()));
        assertEquals(0, countNeg(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(1, countNeg(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15, (byte) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, countNeg((ByteVectorDirect)null));
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

        assertEquals(2, countZero(new ByteVectorDirect(new byte[]{0, 40, 50, 60, (byte) -1, 0})));
        assertEquals(0, countZero(new ByteVectorDirect()));
        assertEquals(0, countZero(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(2, countZero(new ByteVectorDirect(new byte[]{0, 5, NULL_BYTE, 0, (byte) -15})));
        assertEquals(QueryConstants.NULL_INT, countZero((ByteVectorDirect)null));
    }

    public void testMax() {
        assertEquals((byte) 60, max(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals((byte) 60, max(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1)));
        assertEquals(NULL_BYTE, max(new ByteVectorDirect()));
        assertEquals(NULL_BYTE, max(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(NULL_BYTE, max((ByteVector) null));

        assertEquals((byte) 60, max((byte) 0, (byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1, (byte) 0));
        assertEquals((byte) 60, max((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1));
        assertEquals(NULL_BYTE, max());
        assertEquals(NULL_BYTE, max(NULL_BYTE));
        assertEquals(NULL_BYTE, max((byte[]) null));
        assertEquals(NULL_BYTE, max((Byte[]) null));
    }

    public void testMin() {
        assertEquals((byte) 0, min(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals((byte) -1, min(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1)));
        assertEquals(NULL_BYTE, min(new ByteVectorDirect()));
        assertEquals(NULL_BYTE, min(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(NULL_BYTE, min((ByteVector) null));

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

        assertEquals(1, firstIndexOf(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)40));
        assertEquals(4, firstIndexOf(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)60));
        assertEquals(NULL_INT, firstIndexOf(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)1));
        assertEquals(QueryConstants.NULL_INT, firstIndexOf((ByteVector) null, (byte)40));
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

        assertEquals(4, indexOfMax(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals(3, indexOfMax(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1)));
        assertEquals(-1, indexOfMax(new ByteVectorDirect()));
        assertEquals(-1, indexOfMax(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMax((ByteVectorDirect)null));
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

        assertEquals(1, indexOfMin(new ByteVectorDirect(new byte[]{40, 0, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals(4, indexOfMin(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1)));
        assertEquals(-1, indexOfMin(new ByteVectorDirect()));
        assertEquals(-1, indexOfMin(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(QueryConstants.NULL_INT, indexOfMin((ByteVectorDirect)null));
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

        assertEquals(var, var(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, var((ByteVectorDirect)null));
    }

    public void testStd() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(Math.sqrt(var(new ByteVectorDirect(v))), std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, std((byte[])null));

        assertEquals(Math.sqrt(var(new ByteVectorDirect(v))), std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, std((Byte[])null));

        assertEquals(Math.sqrt(var(new ByteVectorDirect(v))), std(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, std((ByteVectorDirect)null));
    }

    public void testSte() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(std(new ByteVectorDirect(v)) / Math.sqrt(count(new ByteVectorDirect(v))), ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((byte[])null));

        assertEquals(std(new ByteVectorDirect(v)) / Math.sqrt(count(new ByteVectorDirect(v))), ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((Byte[])null));

        assertEquals(std(new ByteVectorDirect(v)) / Math.sqrt(count(new ByteVectorDirect(v))), ste(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ste((ByteVectorDirect)null));
    }

    public void testTstat() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(avg(new ByteVectorDirect(v)) / ste(new ByteVectorDirect(v)), tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((byte[])null));

        assertEquals(avg(new ByteVectorDirect(v)) / ste(new ByteVectorDirect(v)), tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((Byte[])null));

        assertEquals(avg(new ByteVectorDirect(v)) / ste(new ByteVectorDirect(v)), tstat(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, tstat((ByteVectorDirect)null));
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

        assertEquals(cov, cov(a, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(a, (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((byte[])null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((byte[])null, (ByteVectorDirect)null));

        assertEquals(cov, cov(new ByteVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new ByteVectorDirect(a), (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((ByteVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((ByteVectorDirect)null, (byte[])null));

        assertEquals(cov, cov(new ByteVectorDirect(a), new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov(new ByteVectorDirect(a), (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((ByteVectorDirect)null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cov((ByteVectorDirect)null, (ByteVectorDirect)null));
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

        assertEquals(cor, cor(a, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(a, (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((byte[])null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((byte[])null, (ByteVectorDirect)null));

        assertEquals(cor, cor(new ByteVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new ByteVectorDirect(a), (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((ByteVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((ByteVectorDirect)null, (byte[])null));

        assertEquals(cor, cor(new ByteVectorDirect(a), new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor(new ByteVectorDirect(a), (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((ByteVectorDirect)null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, cor((ByteVectorDirect)null, (ByteVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - sum(new ByteVectorDirect(new byte[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new ByteVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - sum(new ByteVectorDirect(NULL_BYTE))) == 0.0);
        assertTrue(Math.abs(20 - sum(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15}))) == 0.0);
        assertEquals(NULL_BYTE, sum((ByteVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - sum(new byte[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new byte[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new byte[]{NULL_BYTE})) == 0.0);
        assertTrue(Math.abs(20 - sum(new byte[]{5, NULL_BYTE, 15})) == 0.0);
        assertEquals(NULL_BYTE, sum((byte[]) null));
    }

    public void testSumVector() {
        assertEquals(new byte[]{4, 15}, sum(new ObjectVectorDirect<>(new byte[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new byte[]{4, NULL_BYTE}, sum(new ObjectVectorDirect<>(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}})));
        assertEquals(null, sum((ObjectVector<byte[]>) null));

        try {
            sum(new ObjectVectorDirect<>(new byte[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertTrue(Math.abs(120 - product(new ByteVectorDirect(new byte[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_BYTE, product(new ByteVectorDirect()));
        assertEquals(NULL_BYTE, product(new ByteVectorDirect(NULL_BYTE)));
        assertTrue(Math.abs(75 - product(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15}))) == 0.0);
        assertEquals(NULL_BYTE, product((ByteVector) null));
    }

//    public void testProdVector() {
//        assertEquals(new byte[]{-30, 120}, product(new ObjectVectorDirect<>(new byte[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new byte[]{-30, NULL_BYTE}, product(new ObjectVectorDirect<>(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((Vector<byte[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new byte[][]{{5}, {-3, 5}, {2, 6}}));
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

        assertEquals(new byte[]{1, 3, 6, 10, 15}, cumsum(new ByteVectorDirect(new byte[]{1, 2, 3, 4, 5})));
        assertEquals(new byte[]{1, 3, 6, 6, 11}, cumsum(new ByteVectorDirect(new byte[]{1, 2, 3, NULL_BYTE, 5})));
        assertEquals(new byte[]{NULL_BYTE, 2, 5, 9, 14}, cumsum(new ByteVectorDirect(new byte[]{NULL_BYTE, 2, 3, 4, 5})));
        assertEquals(new byte[0], cumsum(new ByteVectorDirect()));
        assertEquals(null, cumsum((ByteVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new byte[]{1, 2, 6, 24, 120}, cumprod(new byte[]{1, 2, 3, 4, 5}));
        assertEquals(new byte[]{1, 2, 6, 6, 30}, cumprod(new byte[]{1, 2, 3, NULL_BYTE, 5}));
        assertEquals(new byte[]{NULL_BYTE, 2, 6, 24, 120}, cumprod(new byte[]{NULL_BYTE, 2, 3, 4, 5}));
        assertEquals(new byte[0], cumprod());
        assertEquals(null, cumprod((byte[]) null));

        assertEquals(new byte[]{1, 2, 6, 24, 120}, cumprod(new ByteVectorDirect(new byte[]{1, 2, 3, 4, 5})));
        assertEquals(new byte[]{1, 2, 6, 6, 30}, cumprod(new ByteVectorDirect(new byte[]{1, 2, 3, NULL_BYTE, 5})));
        assertEquals(new byte[]{NULL_BYTE, 2, 6, 24, 120}, cumprod(new ByteVectorDirect(new byte[]{NULL_BYTE, 2, 3, 4, 5})));
        assertEquals(new byte[0], cumprod(new ByteVectorDirect()));
        assertEquals(null, cumprod((ByteVector) null));
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

        assertEquals(NULL_INT, binSearchIndex((ByteVector) null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((ByteVector)null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((ByteVector)null, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((ByteVector)null, (byte) 0, BinSearch.BS_LOWEST));

        byte[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(empty), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(empty), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(empty), (byte) 0, BinSearch.BS_LOWEST));

        byte[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ByteVectorDirect(one), (byte) 11, BinSearch.BS_LOWEST));


        byte[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((ByteVector)null, (byte) 0, null);

        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new ByteVectorDirect(v), (byte) 25, BinSearch.BS_LOWEST));

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
        final ByteVector sort = sort(new ByteVectorDirect(bytes));
        final ByteVector expected = new ByteVectorDirect(new byte[]{NULL_BYTE, NULL_BYTE, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        byte[] sortedArray = sort(bytes);
        assertEquals(new byte[]{NULL_BYTE, NULL_BYTE, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(sort((ByteVector)null));
        assertNull(sort((byte[])null));
        assertNull(sort((Byte[])null));
        assertEquals(new ByteVectorDirect(), sort(new ByteVectorDirect()));
        assertEquals(new byte[]{}, sort(new byte[]{}));
        assertEquals(new byte[]{}, sort(new Byte[]{}));
    }

    public void testSortDescending() {
        final byte[] bytes = new byte[]{1, -5, -2, -2, 96, 0, 12, NULL_BYTE, NULL_BYTE};
        final ByteVector sort = sortDescending(new ByteVectorDirect(bytes));
        final ByteVector expected = new ByteVectorDirect(new byte[]{96, 12, 1, 0, -2, -2, -5, NULL_BYTE, NULL_BYTE});
        assertEquals(expected, sort);

        byte[] sortedArray = sortDescending(bytes);
        assertEquals(new byte[]{96, 12, 1, 0, -2, -2, -5, NULL_BYTE, NULL_BYTE}, sortedArray);

        assertNull(sortDescending((ByteVector)null));
        assertNull(sortDescending((byte[])null));
        assertNull(sortDescending((Byte[])null));
        assertEquals(new ByteVectorDirect(), sortDescending(new ByteVectorDirect()));
        assertEquals(new byte[]{}, sortDescending(new byte[]{}));
        assertEquals(new byte[]{}, sortDescending(new Byte[]{}));
    }

    public void testSortsExceptions() {
        ByteVector byteVector = null;
        ByteVector sort = sort(byteVector);
        assertNull(sort);

        byte[] bytes = null;
        byte[] sortArray = sort(bytes);
        assertNull(sortArray);

        bytes = new byte[]{};
        sort = sort(new ByteVectorDirect(bytes));
        assertEquals(new ByteVectorDirect(), sort);

        sortArray = sort(bytes);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        ByteVector byteVector = null;
        ByteVector sort = sortDescending(byteVector);
        assertNull(sort);

        byte[] bytes = null;
        byte[] sortArray = sortDescending(bytes);
        assertNull(sortArray);

        bytes = new byte[]{};
        sort = sortDescending(new ByteVectorDirect(bytes));
        assertEquals(new ByteVectorDirect(), sort);

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

        assertEquals(3.0, median(new ByteVectorDirect(new byte[]{4,2,3})));
        assertEquals(3.5, median(new ByteVectorDirect(new byte[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((ByteVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, percentile(0.00, new byte[]{4,2,3}));
        assertEquals(3.0, percentile(0.50, new byte[]{4,2,3}));
        assertEquals(NULL_DOUBLE, percentile(0.25, (byte[])null));

        assertEquals(2.0, percentile(0.00, new ByteVectorDirect(new byte[]{4,2,3})));
        assertEquals(3.0, percentile(0.50, new ByteVectorDirect(new byte[]{4,2,3})));
        assertEquals(NULL_DOUBLE, percentile(0.25, (ByteVector) null));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wsum((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wsum((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wsum((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wsum((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wsum((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wsum((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wsum((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wsum((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wsum((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wsum((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
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

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedSum((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wavg((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wavg((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wavg((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wavg((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wavg((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wavg((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wavg((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wavg((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wavg((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wavg((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
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

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, weightedAvg((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wvar((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wvar((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wvar((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wvar((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wvar((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wvar(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wvar((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wvar((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wvar((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wvar((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wvar((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wstd((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wstd((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wstd((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wstd((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wstd((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wstd(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wstd((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wstd((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wstd((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wstd((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wstd((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wste((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wste((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wste((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wste((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wste((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wste(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wste((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wste((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wste((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wste((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wste((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
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

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, wtstat((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }
}
