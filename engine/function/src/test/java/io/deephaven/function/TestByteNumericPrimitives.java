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
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.function.BytePrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestByteNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((byte) 1, ByteNumericPrimitives.signum((byte) 5));
        assertEquals((byte) 0, ByteNumericPrimitives.signum((byte) 0));
        assertEquals((byte) -1, ByteNumericPrimitives.signum((byte) -5));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.signum(NULL_BYTE));
    }

    public void testAvg() {
        assertEquals(50.0, ByteNumericPrimitives.avg(new byte[]{40, 50, 60}));
        assertEquals(45.5, ByteNumericPrimitives.avg(new byte[]{40, 51}));
        assertTrue(Double.isNaN(ByteNumericPrimitives.avg(new byte[]{})));
        assertTrue(Double.isNaN(ByteNumericPrimitives.avg(new byte[]{NULL_BYTE})));
        assertEquals(10.0, ByteNumericPrimitives.avg(new byte[]{5, NULL_BYTE, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.avg((byte[])null));

        assertEquals(50.0, ByteNumericPrimitives.avg(new Byte[]{(byte)40, (byte)50, (byte)60}));
        assertEquals(45.5, ByteNumericPrimitives.avg(new Byte[]{(byte)40, (byte)51}));
        assertTrue(Double.isNaN(ByteNumericPrimitives.avg(new Byte[]{})));
        assertTrue(Double.isNaN(ByteNumericPrimitives.avg(new Byte[]{NULL_BYTE})));
        assertEquals(10.0, ByteNumericPrimitives.avg(new Byte[]{(byte)5, NULL_BYTE, (byte)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.avg((Byte[])null));

        assertEquals(50.0, ByteNumericPrimitives.avg(new ByteVectorDirect(new byte[]{40, 50, 60})));
        assertEquals(45.5, ByteNumericPrimitives.avg(new ByteVectorDirect(new byte[]{40, 51})));
        assertTrue(Double.isNaN(ByteNumericPrimitives.avg(new ByteVectorDirect())));
        assertTrue(Double.isNaN(ByteNumericPrimitives.avg(new ByteVectorDirect(NULL_BYTE))));
        assertEquals(10.0, ByteNumericPrimitives.avg(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.avg((ByteVectorDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, ByteNumericPrimitives.absAvg(new byte[]{40, (byte) 50, 60}));
        assertEquals(45.5, ByteNumericPrimitives.absAvg(new byte[]{(byte) 40, 51}));
        assertTrue(Double.isNaN(ByteNumericPrimitives.absAvg(new byte[]{})));
        assertTrue(Double.isNaN(ByteNumericPrimitives.absAvg(new byte[]{NULL_BYTE})));
        assertEquals(10.0, ByteNumericPrimitives.absAvg(new byte[]{(byte) 5, NULL_BYTE, (byte) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.absAvg((byte[])null));

        assertEquals(50.0, ByteNumericPrimitives.absAvg(new Byte[]{(byte)40, (byte) 50, (byte)60}));
        assertEquals(45.5, ByteNumericPrimitives.absAvg(new Byte[]{(byte) 40, (byte)51}));
        assertTrue(Double.isNaN(ByteNumericPrimitives.absAvg(new Byte[]{})));
        assertTrue(Double.isNaN(ByteNumericPrimitives.absAvg(new Byte[]{NULL_BYTE})));
        assertEquals(10.0, ByteNumericPrimitives.absAvg(new Byte[]{(byte) 5, NULL_BYTE, (byte) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.absAvg((Byte[])null));

        assertEquals(50.0, ByteNumericPrimitives.absAvg(new ByteVectorDirect(new byte[]{40, (byte) 50, 60})));
        assertEquals(45.5, ByteNumericPrimitives.absAvg(new ByteVectorDirect(new byte[]{(byte) 40, 51})));
        assertTrue(Double.isNaN(ByteNumericPrimitives.absAvg(new ByteVectorDirect())));
        assertTrue(Double.isNaN(ByteNumericPrimitives.absAvg(new ByteVectorDirect(NULL_BYTE))));
        assertEquals(10.0, ByteNumericPrimitives.absAvg(new ByteVectorDirect((byte) 5, NULL_BYTE, (byte) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.absAvg((ByteVectorDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, ByteNumericPrimitives.countPos(new byte[]{40, 50, 60, (byte) 1, 0}));
        assertEquals(0, ByteNumericPrimitives.countPos(new byte[]{}));
        assertEquals(0, ByteNumericPrimitives.countPos(new byte[]{NULL_BYTE}));
        assertEquals(3, ByteNumericPrimitives.countPos(new byte[]{5, NULL_BYTE, 15, (byte) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countPos((byte[])null));

        assertEquals(4, ByteNumericPrimitives.countPos(new Byte[]{(byte)40, (byte)50, (byte)60, (byte) 1, (byte)0}));
        assertEquals(0, ByteNumericPrimitives.countPos(new Byte[]{}));
        assertEquals(0, ByteNumericPrimitives.countPos(new Byte[]{NULL_BYTE}));
        assertEquals(3, ByteNumericPrimitives.countPos(new Byte[]{(byte)5, NULL_BYTE, (byte)15, (byte) 1, (byte)0}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countPos((byte[])null));

        assertEquals(4, ByteNumericPrimitives.countPos(new ByteVectorDirect(new byte[]{40, 50, 60, (byte) 1, 0})));
        assertEquals(0, ByteNumericPrimitives.countPos(new ByteVectorDirect()));
        assertEquals(0, ByteNumericPrimitives.countPos(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(3, ByteNumericPrimitives.countPos(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15, (byte) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countPos((ByteVectorDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, ByteNumericPrimitives.countNeg(new byte[]{40, (byte) -50, 60, (byte) -1, 0}));
        assertEquals(0, ByteNumericPrimitives.countNeg(new byte[]{}));
        assertEquals(0, ByteNumericPrimitives.countNeg(new byte[]{NULL_BYTE}));
        assertEquals(1, ByteNumericPrimitives.countNeg(new byte[]{5, NULL_BYTE, 15, (byte) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countNeg((byte[])null));

        assertEquals(2, ByteNumericPrimitives.countNeg(new Byte[]{(byte)40, (byte) -50, (byte)60, (byte) -1, (byte)0}));
        assertEquals(0, ByteNumericPrimitives.countNeg(new Byte[]{}));
        assertEquals(0, ByteNumericPrimitives.countNeg(new Byte[]{NULL_BYTE}));
        assertEquals(1, ByteNumericPrimitives.countNeg(new Byte[]{(byte)5, NULL_BYTE, (byte)15, (byte) -1, (byte)0}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countNeg((Byte[])null));

        assertEquals(2, ByteNumericPrimitives.countNeg(new ByteVectorDirect(new byte[]{40, (byte) -50, 60, (byte) -1, 0})));
        assertEquals(0, ByteNumericPrimitives.countNeg(new ByteVectorDirect()));
        assertEquals(0, ByteNumericPrimitives.countNeg(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(1, ByteNumericPrimitives.countNeg(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15, (byte) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countNeg((ByteVectorDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, ByteNumericPrimitives.countZero(new byte[]{0, 40, 50, 60, (byte) -1, 0}));
        assertEquals(0, ByteNumericPrimitives.countZero(new byte[]{}));
        assertEquals(0, ByteNumericPrimitives.countZero(new byte[]{NULL_BYTE}));
        assertEquals(2, ByteNumericPrimitives.countZero(new byte[]{0, 5, NULL_BYTE, 0, (byte) -15}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countZero((byte[])null));

        assertEquals(2, ByteNumericPrimitives.countZero(new Byte[]{(byte)0, (byte)40, (byte)50, (byte)60, (byte) -1, (byte)0}));
        assertEquals(0, ByteNumericPrimitives.countZero(new Byte[]{}));
        assertEquals(0, ByteNumericPrimitives.countZero(new Byte[]{NULL_BYTE}));
        assertEquals(2, ByteNumericPrimitives.countZero(new Byte[]{(byte)0, (byte)5, NULL_BYTE, (byte)0, (byte) -15}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countZero((Byte[])null));

        assertEquals(2, ByteNumericPrimitives.countZero(new ByteVectorDirect(new byte[]{0, 40, 50, 60, (byte) -1, 0})));
        assertEquals(0, ByteNumericPrimitives.countZero(new ByteVectorDirect()));
        assertEquals(0, ByteNumericPrimitives.countZero(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(2, ByteNumericPrimitives.countZero(new ByteVectorDirect(new byte[]{0, 5, NULL_BYTE, 0, (byte) -15})));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.countZero((ByteVectorDirect)null));
    }

    public void testMax() {
        assertEquals((byte) 60, ByteNumericPrimitives.max(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals((byte) 60, ByteNumericPrimitives.max(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1)));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.max(new ByteVectorDirect()));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.max(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.max((ByteVector) null));

        assertEquals((byte) 60, ByteNumericPrimitives.max((byte) 0, (byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1, (byte) 0));
        assertEquals((byte) 60, ByteNumericPrimitives.max((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.max());
        assertEquals(NULL_BYTE, ByteNumericPrimitives.max(NULL_BYTE));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.max((byte[]) null));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.max((Byte[]) null));
    }

    public void testMin() {
        assertEquals((byte) 0, ByteNumericPrimitives.min(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals((byte) -1, ByteNumericPrimitives.min(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1)));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.min(new ByteVectorDirect()));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.min(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.min((ByteVector) null));

        assertEquals((byte) 0, ByteNumericPrimitives.min((byte) 0, (byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1, (byte) 0));
        assertEquals((byte) -1, ByteNumericPrimitives.min((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.min());
        assertEquals(NULL_BYTE, ByteNumericPrimitives.min(NULL_BYTE));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.min((byte[]) null));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.min((Byte[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, ByteNumericPrimitives.firstIndexOf(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}, (byte)40));
        assertEquals(4, ByteNumericPrimitives.firstIndexOf(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}, (byte)60));
        assertEquals(NULL_INT, ByteNumericPrimitives.firstIndexOf(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}, (byte)1));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.firstIndexOf((byte[])null, (byte)40));

        assertEquals(1, ByteNumericPrimitives.firstIndexOf(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)40));
        assertEquals(4, ByteNumericPrimitives.firstIndexOf(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)60));
        assertEquals(NULL_INT, ByteNumericPrimitives.firstIndexOf(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 40, 60, 40, 0}), (byte)1));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.firstIndexOf((ByteVector) null, (byte)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, ByteNumericPrimitives.indexOfMax(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0}));
        assertEquals(3, ByteNumericPrimitives.indexOfMax(new byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMax(new byte[]{}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMax(new byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.indexOfMax((byte[])null));

        assertEquals(4, ByteNumericPrimitives.indexOfMax(new Byte[]{(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) 1, (byte)0}));
        assertEquals(3, ByteNumericPrimitives.indexOfMax(new Byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMax(new Byte[]{}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMax(new Byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.indexOfMax((Byte[])null));

        assertEquals(4, ByteNumericPrimitives.indexOfMax(new ByteVectorDirect(new byte[]{0, 40, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals(3, ByteNumericPrimitives.indexOfMax(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) 1)));
        assertEquals(-1, ByteNumericPrimitives.indexOfMax(new ByteVectorDirect()));
        assertEquals(-1, ByteNumericPrimitives.indexOfMax(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.indexOfMax((ByteVectorDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, ByteNumericPrimitives.indexOfMin(new byte[]{40, 0, NULL_BYTE, 50, 60, (byte) 1, 0}));
        assertEquals(4, ByteNumericPrimitives.indexOfMin(new byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMin(new byte[]{}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMin(new byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.indexOfMin((byte[])null));

        assertEquals(1, ByteNumericPrimitives.indexOfMin(new Byte[]{(byte)40, (byte)0, NULL_BYTE, (byte)50, (byte)60, (byte) 1, (byte)0}));
        assertEquals(4, ByteNumericPrimitives.indexOfMin(new Byte[]{(byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMin(new Byte[]{}));
        assertEquals(-1, ByteNumericPrimitives.indexOfMin(new Byte[]{NULL_BYTE}));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.indexOfMin((Byte[])null));

        assertEquals(1, ByteNumericPrimitives.indexOfMin(new ByteVectorDirect(new byte[]{40, 0, NULL_BYTE, 50, 60, (byte) 1, 0})));
        assertEquals(4, ByteNumericPrimitives.indexOfMin(new ByteVectorDirect((byte) 40, NULL_BYTE, (byte) 50, (byte) 60, (byte) -1)));
        assertEquals(-1, ByteNumericPrimitives.indexOfMin(new ByteVectorDirect()));
        assertEquals(-1, ByteNumericPrimitives.indexOfMin(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(QueryConstants.NULL_INT, ByteNumericPrimitives.indexOfMin((ByteVectorDirect)null));
    }


    public void testVar() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, ByteNumericPrimitives.var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.var((byte[])null));

        assertEquals(var, ByteNumericPrimitives.var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.var((Byte[])null));

        assertEquals(var, ByteNumericPrimitives.var(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.var((ByteVectorDirect)null));
    }

    public void testStd() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(Math.sqrt(ByteNumericPrimitives.var(new ByteVectorDirect(v))), ByteNumericPrimitives.std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.std((byte[])null));

        assertEquals(Math.sqrt(ByteNumericPrimitives.var(new ByteVectorDirect(v))), ByteNumericPrimitives.std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.std((Byte[])null));

        assertEquals(Math.sqrt(ByteNumericPrimitives.var(new ByteVectorDirect(v))), ByteNumericPrimitives.std(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.std((ByteVectorDirect)null));
    }

    public void testSte() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(ByteNumericPrimitives.std(new ByteVectorDirect(v)) / Math.sqrt(count(new ByteVectorDirect(v))), ByteNumericPrimitives.ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.ste((byte[])null));

        assertEquals(ByteNumericPrimitives.std(new ByteVectorDirect(v)) / Math.sqrt(count(new ByteVectorDirect(v))), ByteNumericPrimitives.ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.ste((Byte[])null));

        assertEquals(ByteNumericPrimitives.std(new ByteVectorDirect(v)) / Math.sqrt(count(new ByteVectorDirect(v))), ByteNumericPrimitives.ste(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.ste((ByteVectorDirect)null));
    }

    public void testTstat() {
        byte[] v = {0, 40, NULL_BYTE, 50, 60, (byte) -1, 0};
        Byte[] V = {(byte)0, (byte)40, NULL_BYTE, (byte)50, (byte)60, (byte) -1, (byte)0};

        assertEquals(ByteNumericPrimitives.avg(new ByteVectorDirect(v)) / ByteNumericPrimitives.ste(new ByteVectorDirect(v)), ByteNumericPrimitives.tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.tstat((byte[])null));

        assertEquals(ByteNumericPrimitives.avg(new ByteVectorDirect(v)) / ByteNumericPrimitives.ste(new ByteVectorDirect(v)), ByteNumericPrimitives.tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.tstat((Byte[])null));

        assertEquals(ByteNumericPrimitives.avg(new ByteVectorDirect(v)) / ByteNumericPrimitives.ste(new ByteVectorDirect(v)), ByteNumericPrimitives.tstat(new ByteVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.tstat((ByteVectorDirect)null));
    }

    public void testCov() {
        byte[] a = {10, 40, NULL_BYTE, 50, NULL_BYTE, (byte) -1, 0, (byte) -7};
        byte[] b = {0, (byte) -40, NULL_BYTE, NULL_BYTE, 6, (byte) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, ByteNumericPrimitives.cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov(a, (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((byte[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((byte[])null, (byte[]) null));

        assertEquals(cov, ByteNumericPrimitives.cov(a, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov(a, (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((byte[])null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((byte[])null, (ByteVectorDirect)null));

        assertEquals(cov, ByteNumericPrimitives.cov(new ByteVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov(new ByteVectorDirect(a), (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((ByteVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((ByteVectorDirect)null, (byte[])null));

        assertEquals(cov, ByteNumericPrimitives.cov(new ByteVectorDirect(a), new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov(new ByteVectorDirect(a), (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((ByteVectorDirect)null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cov((ByteVectorDirect)null, (ByteVectorDirect)null));
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

        assertEquals(cor, ByteNumericPrimitives.cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor(a, (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((byte[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((byte[])null, (byte[])null));

        assertEquals(cor, ByteNumericPrimitives.cor(a, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor(a, (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((byte[])null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((byte[])null, (ByteVectorDirect)null));

        assertEquals(cor, ByteNumericPrimitives.cor(new ByteVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor(new ByteVectorDirect(a), (byte[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((ByteVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((ByteVectorDirect)null, (byte[])null));

        assertEquals(cor, ByteNumericPrimitives.cor(new ByteVectorDirect(a), new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor(new ByteVectorDirect(a), (ByteVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((ByteVectorDirect)null, new ByteVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cor((ByteVectorDirect)null, (ByteVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - ByteNumericPrimitives.sum(new ByteVectorDirect(new byte[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - ByteNumericPrimitives.sum(new ByteVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - ByteNumericPrimitives.sum(new ByteVectorDirect(NULL_BYTE))) == 0.0);
        assertTrue(Math.abs(20 - ByteNumericPrimitives.sum(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15}))) == 0.0);
        assertEquals(NULL_BYTE, ByteNumericPrimitives.sum((ByteVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - ByteNumericPrimitives.sum(new byte[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - ByteNumericPrimitives.sum(new byte[]{})) == 0.0);
        assertTrue(Math.abs(0 - ByteNumericPrimitives.sum(new byte[]{NULL_BYTE})) == 0.0);
        assertTrue(Math.abs(20 - ByteNumericPrimitives.sum(new byte[]{5, NULL_BYTE, 15})) == 0.0);
        assertEquals(NULL_BYTE, ByteNumericPrimitives.sum((byte[]) null));
    }

    public void testSumVector() {
        assertEquals(new byte[]{4, 15}, ByteNumericPrimitives.sum(new ObjectVectorDirect<>(new byte[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new byte[]{4, NULL_BYTE}, ByteNumericPrimitives.sum(new ObjectVectorDirect<>(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}})));
        assertEquals(null, ByteNumericPrimitives.sum((ObjectVector<byte[]>) null));

        try {
            ByteNumericPrimitives.sum(new ObjectVectorDirect<>(new byte[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new byte[]{4, 15}, ByteNumericPrimitives.sum(new byte[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new byte[]{4, NULL_BYTE}, ByteNumericPrimitives.sum(new byte[][]{{5, NULL_BYTE}, {-3, 5}, {2, 6}}));
        assertEquals(null, ByteNumericPrimitives.sum((byte[][]) null));

        try {
            ByteNumericPrimitives.sum(new byte[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - ByteNumericPrimitives.product(new byte[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_BYTE, ByteNumericPrimitives.product(new byte[]{}));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.product(new byte[]{NULL_BYTE}));
        assertTrue(Math.abs(75 - ByteNumericPrimitives.product(new byte[]{5, NULL_BYTE, 15})) == 0.0);
        assertEquals(NULL_BYTE, ByteNumericPrimitives.product((byte[]) null));

        assertTrue(Math.abs(120 - ByteNumericPrimitives.product(new ByteVectorDirect(new byte[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_BYTE, ByteNumericPrimitives.product(new ByteVectorDirect()));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.product(new ByteVectorDirect(NULL_BYTE)));
        assertTrue(Math.abs(75 - ByteNumericPrimitives.product(new ByteVectorDirect(new byte[]{5, NULL_BYTE, 15}))) == 0.0);
        assertEquals(NULL_BYTE, ByteNumericPrimitives.product((ByteVector) null));
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
        assertEquals(new byte[]{1, 3, 6, 10, 15}, ByteNumericPrimitives.cumsum(new byte[]{1, 2, 3, 4, 5}));
        assertEquals(new byte[]{1, 3, 6, 6, 11}, ByteNumericPrimitives.cumsum(new byte[]{1, 2, 3, NULL_BYTE, 5}));
        assertEquals(new byte[]{NULL_BYTE, 2, 5, 9, 14}, ByteNumericPrimitives.cumsum(new byte[]{NULL_BYTE, 2, 3, 4, 5}));
        assertEquals(new byte[0], ByteNumericPrimitives.cumsum());
        assertEquals(null, ByteNumericPrimitives.cumsum((byte[]) null));

        assertEquals(new byte[]{1, 3, 6, 10, 15}, ByteNumericPrimitives.cumsum(new ByteVectorDirect(new byte[]{1, 2, 3, 4, 5})));
        assertEquals(new byte[]{1, 3, 6, 6, 11}, ByteNumericPrimitives.cumsum(new ByteVectorDirect(new byte[]{1, 2, 3, NULL_BYTE, 5})));
        assertEquals(new byte[]{NULL_BYTE, 2, 5, 9, 14}, ByteNumericPrimitives.cumsum(new ByteVectorDirect(new byte[]{NULL_BYTE, 2, 3, 4, 5})));
        assertEquals(new byte[0], ByteNumericPrimitives.cumsum(new ByteVectorDirect()));
        assertEquals(null, ByteNumericPrimitives.cumsum((ByteVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new byte[]{1, 2, 6, 24, 120}, ByteNumericPrimitives.cumprod(new byte[]{1, 2, 3, 4, 5}));
        assertEquals(new byte[]{1, 2, 6, 6, 30}, ByteNumericPrimitives.cumprod(new byte[]{1, 2, 3, NULL_BYTE, 5}));
        assertEquals(new byte[]{NULL_BYTE, 2, 6, 24, 120}, ByteNumericPrimitives.cumprod(new byte[]{NULL_BYTE, 2, 3, 4, 5}));
        assertEquals(new byte[0], ByteNumericPrimitives.cumprod());
        assertEquals(null, ByteNumericPrimitives.cumprod((byte[]) null));

        assertEquals(new byte[]{1, 2, 6, 24, 120}, ByteNumericPrimitives.cumprod(new ByteVectorDirect(new byte[]{1, 2, 3, 4, 5})));
        assertEquals(new byte[]{1, 2, 6, 6, 30}, ByteNumericPrimitives.cumprod(new ByteVectorDirect(new byte[]{1, 2, 3, NULL_BYTE, 5})));
        assertEquals(new byte[]{NULL_BYTE, 2, 6, 24, 120}, ByteNumericPrimitives.cumprod(new ByteVectorDirect(new byte[]{NULL_BYTE, 2, 3, 4, 5})));
        assertEquals(new byte[0], ByteNumericPrimitives.cumprod(new ByteVectorDirect()));
        assertEquals(null, ByteNumericPrimitives.cumprod((ByteVector) null));
    }

    public void testAbs() {
        byte value = -5;
        assertEquals((byte) Math.abs(value), ByteNumericPrimitives.abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_BYTE, ByteNumericPrimitives.abs(NULL_BYTE), 1e-10);
    }

    public void testAcos() {
        byte value = -5;
        assertEquals(Math.acos(value), ByteNumericPrimitives.acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.acos(NULL_BYTE), 1e-10);
    }

    public void testAsin() {
        byte value = -5;
        assertEquals(Math.asin(value), ByteNumericPrimitives.asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.asin(NULL_BYTE), 1e-10);
    }

    public void testAtan() {
        byte value = -5;
        assertEquals(Math.atan(value), ByteNumericPrimitives.atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.atan(NULL_BYTE), 1e-10);
    }

    public void testCeil() {
        byte value = -5;
        assertEquals(Math.ceil(value), ByteNumericPrimitives.ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.ceil(NULL_BYTE), 1e-10);
    }

    public void testCos() {
        byte value = -5;
        assertEquals(Math.cos(value), ByteNumericPrimitives.cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.cos(NULL_BYTE), 1e-10);
    }

    public void testExp() {
        byte value = -5;
        assertEquals(Math.exp(value), ByteNumericPrimitives.exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.exp(NULL_BYTE), 1e-10);
    }

    public void testFloor() {
        byte value = -5;
        assertEquals(Math.floor(value), ByteNumericPrimitives.floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.floor(NULL_BYTE), 1e-10);
    }

    public void testLog() {
        byte value = -5;
        assertEquals(Math.log(value), ByteNumericPrimitives.log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.log(NULL_BYTE), 1e-10);
    }

    public void testPow() {
        byte value0 = -5;
        byte value1 = 2;
        assertEquals(Math.pow(value0, value1), ByteNumericPrimitives.pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.pow(NULL_BYTE, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.pow(value0, NULL_BYTE), 1e-10);
    }

    public void testRint() {
        byte value = -5;
        assertEquals(Math.rint(value), ByteNumericPrimitives.rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.rint(NULL_BYTE), 1e-10);
    }

    public void testRound() {
        byte value = -5;
        assertEquals(Math.round(value), ByteNumericPrimitives.round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, ByteNumericPrimitives.round(NULL_BYTE), 1e-10);
    }

    public void testSin() {
        byte value = -5;
        assertEquals(Math.sin(value), ByteNumericPrimitives.sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.sin(NULL_BYTE), 1e-10);
    }

    public void testSqrt() {
        byte value = -5;
        assertEquals(Math.sqrt(value), ByteNumericPrimitives.sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.sqrt(NULL_BYTE), 1e-10);
    }

    public void testTan() {
        byte value = -5;
        assertEquals(Math.tan(value), ByteNumericPrimitives.tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ByteNumericPrimitives.tan(NULL_BYTE), 1e-10);
    }

    public void testLowerBin() {
        byte value = (byte) 114;

        assertEquals((byte) 110, ByteNumericPrimitives.lowerBin(value, (byte) 5));
        assertEquals((byte) 110, ByteNumericPrimitives.lowerBin(value, (byte) 10));
        assertEquals((byte) 100, ByteNumericPrimitives.lowerBin(value, (byte) 20));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.lowerBin(NULL_BYTE, (byte) 5));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.lowerBin(value, NULL_BYTE));

        assertEquals(ByteNumericPrimitives.lowerBin(value, (byte) 5), ByteNumericPrimitives.lowerBin(ByteNumericPrimitives.lowerBin(value, (byte) 5), (byte) 5));
    }

    public void testLowerBinWithOffset() {
        byte value = (byte) 114;
        byte offset = (byte) 3;

        assertEquals((byte) 113, ByteNumericPrimitives.lowerBin(value, (byte) 5, offset));
        assertEquals((byte) 113, ByteNumericPrimitives.lowerBin(value, (byte) 10, offset));
        assertEquals((byte) 103, ByteNumericPrimitives.lowerBin(value, (byte) 20, offset));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.lowerBin(NULL_BYTE, (byte) 5, offset));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.lowerBin(value, NULL_BYTE, offset));

        assertEquals(ByteNumericPrimitives.lowerBin(value, (byte) 5, offset), ByteNumericPrimitives.lowerBin(ByteNumericPrimitives.lowerBin(value, (byte) 5, offset), (byte) 5, offset));
    }

    public void testUpperBin() {
        byte value = (byte) 114;

        assertEquals((byte) 115, ByteNumericPrimitives.upperBin(value, (byte) 5));
        assertEquals((byte) 120, ByteNumericPrimitives.upperBin(value, (byte) 10));
        assertEquals((byte) 120, ByteNumericPrimitives.upperBin(value, (byte) 20));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.upperBin(NULL_BYTE, (byte) 5));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.upperBin(value, NULL_BYTE));

        assertEquals(ByteNumericPrimitives.upperBin(value, (byte) 5), ByteNumericPrimitives.upperBin(ByteNumericPrimitives.upperBin(value, (byte) 5), (byte) 5));
    }

    public void testUpperBinWithOffset() {
        byte value = (byte) 114;
        byte offset = (byte) 3;

        assertEquals((byte) 118, ByteNumericPrimitives.upperBin(value, (byte) 5, offset));
        assertEquals((byte) 123, ByteNumericPrimitives.upperBin(value, (byte) 10, offset));
        assertEquals((byte) 123, ByteNumericPrimitives.upperBin(value, (byte) 20, offset));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.upperBin(NULL_BYTE, (byte) 5, offset));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.upperBin(value, NULL_BYTE, offset));

        assertEquals(ByteNumericPrimitives.upperBin(value, (byte) 5, offset), ByteNumericPrimitives.upperBin(ByteNumericPrimitives.upperBin(value, (byte) 5, offset), (byte) 5, offset));
    }

    public void testClamp() {
        assertEquals((byte) 3, ByteNumericPrimitives.clamp((byte) 3, (byte) -6, (byte) 5));
        assertEquals((byte) -6, ByteNumericPrimitives.clamp((byte) -7, (byte) -6, (byte) 5));
        assertEquals((byte) 5, ByteNumericPrimitives.clamp((byte) 7, (byte) -6, (byte) 5));
        assertEquals(NULL_BYTE, ByteNumericPrimitives.clamp(NULL_BYTE, (byte) -6, (byte) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, ByteNumericPrimitives.binSearchIndex((byte[]) null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.binSearchIndex(new byte[]{1,3,4}, (byte)0, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.binSearchIndex(new byte[]{1,3,4}, (byte)1, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.binSearchIndex(new byte[]{1,3,4}, (byte)2, BinSearch.BS_ANY));
        assertEquals(1, ByteNumericPrimitives.binSearchIndex(new byte[]{1,3,4}, (byte)3, BinSearch.BS_ANY));
        assertEquals(2, ByteNumericPrimitives.binSearchIndex(new byte[]{1,3,4}, (byte)4, BinSearch.BS_ANY));
        assertEquals(2, ByteNumericPrimitives.binSearchIndex(new byte[]{1,3,4}, (byte)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, ByteNumericPrimitives.binSearchIndex((ByteVector) null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)0, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)1, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)2, BinSearch.BS_ANY));
        assertEquals(1, ByteNumericPrimitives.binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)3, BinSearch.BS_ANY));
        assertEquals(2, ByteNumericPrimitives.binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)4, BinSearch.BS_ANY));
        assertEquals(2, ByteNumericPrimitives.binSearchIndex(new ByteVectorDirect(new byte[]{1,3,4}), (byte)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, ByteNumericPrimitives.rawBinSearchIndex((ByteVector)null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, ByteNumericPrimitives.rawBinSearchIndex((ByteVector)null, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, ByteNumericPrimitives.rawBinSearchIndex((ByteVector)null, (byte) 0, BinSearch.BS_LOWEST));

        byte[] empty = {};
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(empty), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(empty), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(empty), (byte) 0, BinSearch.BS_LOWEST));

        byte[] one = {11};
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 12, BinSearch.BS_ANY));
        assertEquals(-2, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 12, BinSearch.BS_LOWEST));

        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 11, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(one), (byte) 11, BinSearch.BS_LOWEST));


        byte[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        ByteNumericPrimitives.rawBinSearchIndex((ByteVector)null, (byte) 0, null);

        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 26, BinSearch.BS_LOWEST));

        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 1, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 1, BinSearch.BS_LOWEST));

        assertEquals(2, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 2, BinSearch.BS_LOWEST));

        assertEquals(5, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 3, BinSearch.BS_LOWEST));

        assertEquals(9, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 4, BinSearch.BS_LOWEST));

        assertEquals(14, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 7, BinSearch.BS_ANY));
        assertEquals(-16, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 7, BinSearch.BS_LOWEST));

        assertEquals(19, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 10, BinSearch.BS_LOWEST));

        assertEquals(24, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 11, BinSearch.BS_LOWEST));

        assertEquals(25, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 15, BinSearch.BS_ANY));
        assertEquals(25, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 15, BinSearch.BS_LOWEST));

        assertEquals(29, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, ByteNumericPrimitives.rawBinSearchIndex(new ByteVectorDirect(v), (byte) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, ByteNumericPrimitives.rawBinSearchIndex((byte[]) null, (byte) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, ByteNumericPrimitives.rawBinSearchIndex((byte[])null, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, ByteNumericPrimitives.rawBinSearchIndex((byte[])null, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(empty, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(empty, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(empty, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 12, BinSearch.BS_ANY));
        assertEquals(-2, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 12, BinSearch.BS_LOWEST));

        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 11, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(one, (byte) 11, BinSearch.BS_LOWEST));


        ByteNumericPrimitives.rawBinSearchIndex((byte[])null, (byte) 0, null);

        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 0, BinSearch.BS_ANY));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 26, BinSearch.BS_LOWEST));

        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 1, BinSearch.BS_ANY));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 1, BinSearch.BS_LOWEST));

        assertEquals(2, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 2, BinSearch.BS_LOWEST));

        assertEquals(5, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 3, BinSearch.BS_LOWEST));

        assertEquals(9, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 4, BinSearch.BS_LOWEST));

        assertEquals(14, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 7, BinSearch.BS_ANY));
        assertEquals(-16, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 7, BinSearch.BS_LOWEST));

        assertEquals(19, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 10, BinSearch.BS_LOWEST));

        assertEquals(24, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 11, BinSearch.BS_LOWEST));

        assertEquals(25, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 15, BinSearch.BS_ANY));
        assertEquals(25, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 15, BinSearch.BS_LOWEST));

        assertEquals(29, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, ByteNumericPrimitives.rawBinSearchIndex(v, (byte) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final byte[] bytes = new byte[]{1, -5, -2, -2, 96, 0, 12, NULL_BYTE, NULL_BYTE};
        final ByteVector sort = ByteNumericPrimitives.sort(new ByteVectorDirect(bytes));
        final ByteVector expected = new ByteVectorDirect(new byte[]{NULL_BYTE, NULL_BYTE, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        byte[] sortedArray = ByteNumericPrimitives.sort(bytes);
        assertEquals(new byte[]{NULL_BYTE, NULL_BYTE, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(ByteNumericPrimitives.sort((ByteVector)null));
        assertNull(ByteNumericPrimitives.sort((byte[])null));
        assertNull(ByteNumericPrimitives.sort((Byte[])null));
        assertEquals(new ByteVectorDirect(), ByteNumericPrimitives.sort(new ByteVectorDirect()));
        assertEquals(new byte[]{}, ByteNumericPrimitives.sort(new byte[]{}));
        assertEquals(new byte[]{}, ByteNumericPrimitives.sort(new Byte[]{}));
    }

    public void testSortDescending() {
        final byte[] bytes = new byte[]{1, -5, -2, -2, 96, 0, 12, NULL_BYTE, NULL_BYTE};
        final ByteVector sort = ByteNumericPrimitives.sortDescending(new ByteVectorDirect(bytes));
        final ByteVector expected = new ByteVectorDirect(new byte[]{96, 12, 1, 0, -2, -2, -5, NULL_BYTE, NULL_BYTE});
        assertEquals(expected, sort);

        byte[] sortedArray = ByteNumericPrimitives.sortDescending(bytes);
        assertEquals(new byte[]{96, 12, 1, 0, -2, -2, -5, NULL_BYTE, NULL_BYTE}, sortedArray);

        assertNull(ByteNumericPrimitives.sortDescending((ByteVector)null));
        assertNull(ByteNumericPrimitives.sortDescending((byte[])null));
        assertNull(ByteNumericPrimitives.sortDescending((Byte[])null));
        assertEquals(new ByteVectorDirect(), ByteNumericPrimitives.sortDescending(new ByteVectorDirect()));
        assertEquals(new byte[]{}, ByteNumericPrimitives.sortDescending(new byte[]{}));
        assertEquals(new byte[]{}, ByteNumericPrimitives.sortDescending(new Byte[]{}));
    }

    public void testSortsExceptions() {
        ByteVector byteVector = null;
        ByteVector sort = ByteNumericPrimitives.sort(byteVector);
        assertNull(sort);

        byte[] bytes = null;
        byte[] sortArray = ByteNumericPrimitives.sort(bytes);
        assertNull(sortArray);

        bytes = new byte[]{};
        sort = ByteNumericPrimitives.sort(new ByteVectorDirect(bytes));
        assertEquals(new ByteVectorDirect(), sort);

        sortArray = ByteNumericPrimitives.sort(bytes);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        ByteVector byteVector = null;
        ByteVector sort = ByteNumericPrimitives.sortDescending(byteVector);
        assertNull(sort);

        byte[] bytes = null;
        byte[] sortArray = ByteNumericPrimitives.sortDescending(bytes);
        assertNull(sortArray);

        bytes = new byte[]{};
        sort = ByteNumericPrimitives.sortDescending(new ByteVectorDirect(bytes));
        assertEquals(new ByteVectorDirect(), sort);

        sortArray = ByteNumericPrimitives.sortDescending(bytes);
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
        assertEquals(3.0, ByteNumericPrimitives.median(new byte[]{4,2,3}));
        assertEquals(3.5, ByteNumericPrimitives.median(new byte[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.median((byte[])null));

        assertEquals(3.0, ByteNumericPrimitives.median(new Byte[]{(byte)4,(byte)2,(byte)3}));
        assertEquals(3.5, ByteNumericPrimitives.median(new Byte[]{(byte)5,(byte)4,(byte)2,(byte)3}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.median((Byte[])null));

        assertEquals(3.0, ByteNumericPrimitives.median(new ByteVectorDirect(new byte[]{4,2,3})));
        assertEquals(3.5, ByteNumericPrimitives.median(new ByteVectorDirect(new byte[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.median((ByteVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, ByteNumericPrimitives.percentile(0.00, new byte[]{4,2,3}));
        assertEquals(3.0, ByteNumericPrimitives.percentile(0.50, new byte[]{4,2,3}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.percentile(0.25, (byte[])null));

        assertEquals(2.0, ByteNumericPrimitives.percentile(0.00, new ByteVectorDirect(new byte[]{4,2,3})));
        assertEquals(3.0, ByteNumericPrimitives.percentile(0.50, new ByteVectorDirect(new byte[]{4,2,3})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.percentile(0.25, (ByteVector) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wsum(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedSum(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wavg(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.weightedAvg(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wvar(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wvar(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wstd(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wstd(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wste(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wste(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }

    public void testWtstat() {
        final double target = ByteNumericPrimitives.wavg(new byte[]{1,2,3}, new byte[]{4,5,6}) / ByteNumericPrimitives.wste(new byte[]{1,2,3}, new byte[]{4,5,6});

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (int[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (long[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (double[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new byte[]{4,5,6,7,NULL_BYTE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector) null, new byte[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (byte[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (int[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (long[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (double[])null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new byte[]{1,2,3,NULL_BYTE,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((byte[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new byte[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new ByteVectorDirect(new byte[]{4,5,6,7,NULL_BYTE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector) null, new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (ByteVector) null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (IntVector) null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (LongVector) null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3,NULL_BYTE,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat((ByteVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ByteNumericPrimitives.wtstat(new ByteVectorDirect(new byte[]{1,2,3}), (FloatVector)null));
    }
}
