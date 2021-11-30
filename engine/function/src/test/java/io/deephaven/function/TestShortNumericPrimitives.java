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
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static io.deephaven.function.ShortPrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestShortNumericPrimitives extends BaseArrayTestCase {
    public void testSignum() {
        assertEquals((short) 1, ShortNumericPrimitives.signum((short) 5));
        assertEquals((short) 0, ShortNumericPrimitives.signum((short) 0));
        assertEquals((short) -1, ShortNumericPrimitives.signum((short) -5));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.signum(NULL_SHORT));
    }

    public void testAvg() {
        assertEquals(50.0, ShortNumericPrimitives.avg(new short[]{40, 50, 60}));
        assertEquals(45.5, ShortNumericPrimitives.avg(new short[]{40, 51}));
        assertTrue(Double.isNaN(ShortNumericPrimitives.avg(new short[]{})));
        assertTrue(Double.isNaN(ShortNumericPrimitives.avg(new short[]{NULL_SHORT})));
        assertEquals(10.0, ShortNumericPrimitives.avg(new short[]{5, NULL_SHORT, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.avg((short[])null));

        assertEquals(50.0, ShortNumericPrimitives.avg(new Short[]{(short)40, (short)50, (short)60}));
        assertEquals(45.5, ShortNumericPrimitives.avg(new Short[]{(short)40, (short)51}));
        assertTrue(Double.isNaN(ShortNumericPrimitives.avg(new Short[]{})));
        assertTrue(Double.isNaN(ShortNumericPrimitives.avg(new Short[]{NULL_SHORT})));
        assertEquals(10.0, ShortNumericPrimitives.avg(new Short[]{(short)5, NULL_SHORT, (short)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.avg((Short[])null));

        assertEquals(50.0, ShortNumericPrimitives.avg(new ShortVectorDirect(new short[]{40, 50, 60})));
        assertEquals(45.5, ShortNumericPrimitives.avg(new ShortVectorDirect(new short[]{40, 51})));
        assertTrue(Double.isNaN(ShortNumericPrimitives.avg(new ShortVectorDirect())));
        assertTrue(Double.isNaN(ShortNumericPrimitives.avg(new ShortVectorDirect(NULL_SHORT))));
        assertEquals(10.0, ShortNumericPrimitives.avg(new ShortVectorDirect(new short[]{5, NULL_SHORT, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.avg((ShortVectorDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, ShortNumericPrimitives.absAvg(new short[]{40, (short) 50, 60}));
        assertEquals(45.5, ShortNumericPrimitives.absAvg(new short[]{(short) 40, 51}));
        assertTrue(Double.isNaN(ShortNumericPrimitives.absAvg(new short[]{})));
        assertTrue(Double.isNaN(ShortNumericPrimitives.absAvg(new short[]{NULL_SHORT})));
        assertEquals(10.0, ShortNumericPrimitives.absAvg(new short[]{(short) 5, NULL_SHORT, (short) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.absAvg((short[])null));

        assertEquals(50.0, ShortNumericPrimitives.absAvg(new Short[]{(short)40, (short) 50, (short)60}));
        assertEquals(45.5, ShortNumericPrimitives.absAvg(new Short[]{(short) 40, (short)51}));
        assertTrue(Double.isNaN(ShortNumericPrimitives.absAvg(new Short[]{})));
        assertTrue(Double.isNaN(ShortNumericPrimitives.absAvg(new Short[]{NULL_SHORT})));
        assertEquals(10.0, ShortNumericPrimitives.absAvg(new Short[]{(short) 5, NULL_SHORT, (short) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.absAvg((Short[])null));

        assertEquals(50.0, ShortNumericPrimitives.absAvg(new ShortVectorDirect(new short[]{40, (short) 50, 60})));
        assertEquals(45.5, ShortNumericPrimitives.absAvg(new ShortVectorDirect(new short[]{(short) 40, 51})));
        assertTrue(Double.isNaN(ShortNumericPrimitives.absAvg(new ShortVectorDirect())));
        assertTrue(Double.isNaN(ShortNumericPrimitives.absAvg(new ShortVectorDirect(NULL_SHORT))));
        assertEquals(10.0, ShortNumericPrimitives.absAvg(new ShortVectorDirect((short) 5, NULL_SHORT, (short) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.absAvg((ShortVectorDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, ShortNumericPrimitives.countPos(new short[]{40, 50, 60, (short) 1, 0}));
        assertEquals(0, ShortNumericPrimitives.countPos(new short[]{}));
        assertEquals(0, ShortNumericPrimitives.countPos(new short[]{NULL_SHORT}));
        assertEquals(3, ShortNumericPrimitives.countPos(new short[]{5, NULL_SHORT, 15, (short) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countPos((short[])null));

        assertEquals(4, ShortNumericPrimitives.countPos(new Short[]{(short)40, (short)50, (short)60, (short) 1, (short)0}));
        assertEquals(0, ShortNumericPrimitives.countPos(new Short[]{}));
        assertEquals(0, ShortNumericPrimitives.countPos(new Short[]{NULL_SHORT}));
        assertEquals(3, ShortNumericPrimitives.countPos(new Short[]{(short)5, NULL_SHORT, (short)15, (short) 1, (short)0}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countPos((short[])null));

        assertEquals(4, ShortNumericPrimitives.countPos(new ShortVectorDirect(new short[]{40, 50, 60, (short) 1, 0})));
        assertEquals(0, ShortNumericPrimitives.countPos(new ShortVectorDirect()));
        assertEquals(0, ShortNumericPrimitives.countPos(new ShortVectorDirect(NULL_SHORT)));
        assertEquals(3, ShortNumericPrimitives.countPos(new ShortVectorDirect(new short[]{5, NULL_SHORT, 15, (short) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countPos((ShortVectorDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, ShortNumericPrimitives.countNeg(new short[]{40, (short) -50, 60, (short) -1, 0}));
        assertEquals(0, ShortNumericPrimitives.countNeg(new short[]{}));
        assertEquals(0, ShortNumericPrimitives.countNeg(new short[]{NULL_SHORT}));
        assertEquals(1, ShortNumericPrimitives.countNeg(new short[]{5, NULL_SHORT, 15, (short) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countNeg((short[])null));

        assertEquals(2, ShortNumericPrimitives.countNeg(new Short[]{(short)40, (short) -50, (short)60, (short) -1, (short)0}));
        assertEquals(0, ShortNumericPrimitives.countNeg(new Short[]{}));
        assertEquals(0, ShortNumericPrimitives.countNeg(new Short[]{NULL_SHORT}));
        assertEquals(1, ShortNumericPrimitives.countNeg(new Short[]{(short)5, NULL_SHORT, (short)15, (short) -1, (short)0}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countNeg((Short[])null));

        assertEquals(2, ShortNumericPrimitives.countNeg(new ShortVectorDirect(new short[]{40, (short) -50, 60, (short) -1, 0})));
        assertEquals(0, ShortNumericPrimitives.countNeg(new ShortVectorDirect()));
        assertEquals(0, ShortNumericPrimitives.countNeg(new ShortVectorDirect(NULL_SHORT)));
        assertEquals(1, ShortNumericPrimitives.countNeg(new ShortVectorDirect(new short[]{5, NULL_SHORT, 15, (short) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countNeg((ShortVectorDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, ShortNumericPrimitives.countZero(new short[]{0, 40, 50, 60, (short) -1, 0}));
        assertEquals(0, ShortNumericPrimitives.countZero(new short[]{}));
        assertEquals(0, ShortNumericPrimitives.countZero(new short[]{NULL_SHORT}));
        assertEquals(2, ShortNumericPrimitives.countZero(new short[]{0, 5, NULL_SHORT, 0, (short) -15}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countZero((short[])null));

        assertEquals(2, ShortNumericPrimitives.countZero(new Short[]{(short)0, (short)40, (short)50, (short)60, (short) -1, (short)0}));
        assertEquals(0, ShortNumericPrimitives.countZero(new Short[]{}));
        assertEquals(0, ShortNumericPrimitives.countZero(new Short[]{NULL_SHORT}));
        assertEquals(2, ShortNumericPrimitives.countZero(new Short[]{(short)0, (short)5, NULL_SHORT, (short)0, (short) -15}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countZero((Short[])null));

        assertEquals(2, ShortNumericPrimitives.countZero(new ShortVectorDirect(new short[]{0, 40, 50, 60, (short) -1, 0})));
        assertEquals(0, ShortNumericPrimitives.countZero(new ShortVectorDirect()));
        assertEquals(0, ShortNumericPrimitives.countZero(new ShortVectorDirect(NULL_SHORT)));
        assertEquals(2, ShortNumericPrimitives.countZero(new ShortVectorDirect(new short[]{0, 5, NULL_SHORT, 0, (short) -15})));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.countZero((ShortVectorDirect)null));
    }

    public void testMax() {
        assertEquals((short) 60, ShortNumericPrimitives.max(new ShortVectorDirect(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals((short) 60, ShortNumericPrimitives.max(new ShortVectorDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1)));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.max(new ShortVectorDirect()));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.max(new ShortVectorDirect(NULL_SHORT)));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.max((ShortVector) null));

        assertEquals((short) 60, ShortNumericPrimitives.max((short) 0, (short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1, (short) 0));
        assertEquals((short) 60, ShortNumericPrimitives.max((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.max());
        assertEquals(NULL_SHORT, ShortNumericPrimitives.max(NULL_SHORT));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.max((short[]) null));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.max((Short[]) null));
    }

    public void testMin() {
        assertEquals((short) 0, ShortNumericPrimitives.min(new ShortVectorDirect(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals((short) -1, ShortNumericPrimitives.min(new ShortVectorDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1)));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.min(new ShortVectorDirect()));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.min(new ShortVectorDirect(NULL_SHORT)));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.min((ShortVector) null));

        assertEquals((short) 0, ShortNumericPrimitives.min((short) 0, (short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1, (short) 0));
        assertEquals((short) -1, ShortNumericPrimitives.min((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.min());
        assertEquals(NULL_SHORT, ShortNumericPrimitives.min(NULL_SHORT));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.min((short[]) null));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.min((Short[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, ShortNumericPrimitives.firstIndexOf(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}, (short)40));
        assertEquals(4, ShortNumericPrimitives.firstIndexOf(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}, (short)60));
        assertEquals(NULL_INT, ShortNumericPrimitives.firstIndexOf(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}, (short)1));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.firstIndexOf((short[])null, (short)40));

        assertEquals(1, ShortNumericPrimitives.firstIndexOf(new ShortVectorDirect(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}), (short)40));
        assertEquals(4, ShortNumericPrimitives.firstIndexOf(new ShortVectorDirect(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}), (short)60));
        assertEquals(NULL_INT, ShortNumericPrimitives.firstIndexOf(new ShortVectorDirect(new short[]{0, 40, NULL_SHORT, 40, 60, 40, 0}), (short)1));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.firstIndexOf((ShortVector) null, (short)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, ShortNumericPrimitives.indexOfMax(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0}));
        assertEquals(3, ShortNumericPrimitives.indexOfMax(new short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMax(new short[]{}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMax(new short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.indexOfMax((short[])null));

        assertEquals(4, ShortNumericPrimitives.indexOfMax(new Short[]{(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) 1, (short)0}));
        assertEquals(3, ShortNumericPrimitives.indexOfMax(new Short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMax(new Short[]{}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMax(new Short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.indexOfMax((Short[])null));

        assertEquals(4, ShortNumericPrimitives.indexOfMax(new ShortVectorDirect(new short[]{0, 40, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals(3, ShortNumericPrimitives.indexOfMax(new ShortVectorDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) 1)));
        assertEquals(-1, ShortNumericPrimitives.indexOfMax(new ShortVectorDirect()));
        assertEquals(-1, ShortNumericPrimitives.indexOfMax(new ShortVectorDirect(NULL_SHORT)));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.indexOfMax((ShortVectorDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, ShortNumericPrimitives.indexOfMin(new short[]{40, 0, NULL_SHORT, 50, 60, (short) 1, 0}));
        assertEquals(4, ShortNumericPrimitives.indexOfMin(new short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMin(new short[]{}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMin(new short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.indexOfMin((short[])null));

        assertEquals(1, ShortNumericPrimitives.indexOfMin(new Short[]{(short)40, (short)0, NULL_SHORT, (short)50, (short)60, (short) 1, (short)0}));
        assertEquals(4, ShortNumericPrimitives.indexOfMin(new Short[]{(short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMin(new Short[]{}));
        assertEquals(-1, ShortNumericPrimitives.indexOfMin(new Short[]{NULL_SHORT}));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.indexOfMin((Short[])null));

        assertEquals(1, ShortNumericPrimitives.indexOfMin(new ShortVectorDirect(new short[]{40, 0, NULL_SHORT, 50, 60, (short) 1, 0})));
        assertEquals(4, ShortNumericPrimitives.indexOfMin(new ShortVectorDirect((short) 40, NULL_SHORT, (short) 50, (short) 60, (short) -1)));
        assertEquals(-1, ShortNumericPrimitives.indexOfMin(new ShortVectorDirect()));
        assertEquals(-1, ShortNumericPrimitives.indexOfMin(new ShortVectorDirect(NULL_SHORT)));
        assertEquals(QueryConstants.NULL_INT, ShortNumericPrimitives.indexOfMin((ShortVectorDirect)null));
    }


    public void testVar() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, ShortNumericPrimitives.var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.var((short[])null));

        assertEquals(var, ShortNumericPrimitives.var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.var((Short[])null));

        assertEquals(var, ShortNumericPrimitives.var(new ShortVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.var((ShortVectorDirect)null));
    }

    public void testStd() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};

        assertEquals(Math.sqrt(ShortNumericPrimitives.var(new ShortVectorDirect(v))), ShortNumericPrimitives.std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.std((short[])null));

        assertEquals(Math.sqrt(ShortNumericPrimitives.var(new ShortVectorDirect(v))), ShortNumericPrimitives.std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.std((Short[])null));

        assertEquals(Math.sqrt(ShortNumericPrimitives.var(new ShortVectorDirect(v))), ShortNumericPrimitives.std(new ShortVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.std((ShortVectorDirect)null));
    }

    public void testSte() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};

        assertEquals(ShortNumericPrimitives.std(new ShortVectorDirect(v)) / Math.sqrt(count(new ShortVectorDirect(v))), ShortNumericPrimitives.ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.ste((short[])null));

        assertEquals(ShortNumericPrimitives.std(new ShortVectorDirect(v)) / Math.sqrt(count(new ShortVectorDirect(v))), ShortNumericPrimitives.ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.ste((Short[])null));

        assertEquals(ShortNumericPrimitives.std(new ShortVectorDirect(v)) / Math.sqrt(count(new ShortVectorDirect(v))), ShortNumericPrimitives.ste(new ShortVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.ste((ShortVectorDirect)null));
    }

    public void testTstat() {
        short[] v = {0, 40, NULL_SHORT, 50, 60, (short) -1, 0};
        Short[] V = {(short)0, (short)40, NULL_SHORT, (short)50, (short)60, (short) -1, (short)0};

        assertEquals(ShortNumericPrimitives.avg(new ShortVectorDirect(v)) / ShortNumericPrimitives.ste(new ShortVectorDirect(v)), ShortNumericPrimitives.tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.tstat((short[])null));

        assertEquals(ShortNumericPrimitives.avg(new ShortVectorDirect(v)) / ShortNumericPrimitives.ste(new ShortVectorDirect(v)), ShortNumericPrimitives.tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.tstat((Short[])null));

        assertEquals(ShortNumericPrimitives.avg(new ShortVectorDirect(v)) / ShortNumericPrimitives.ste(new ShortVectorDirect(v)), ShortNumericPrimitives.tstat(new ShortVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.tstat((ShortVectorDirect)null));
    }

    public void testCov() {
        short[] a = {10, 40, NULL_SHORT, 50, NULL_SHORT, (short) -1, 0, (short) -7};
        short[] b = {0, (short) -40, NULL_SHORT, NULL_SHORT, 6, (short) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, ShortNumericPrimitives.cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov(a, (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((short[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((short[])null, (short[]) null));

        assertEquals(cov, ShortNumericPrimitives.cov(a, new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov(a, (ShortVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((short[])null, new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((short[])null, (ShortVectorDirect)null));

        assertEquals(cov, ShortNumericPrimitives.cov(new ShortVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov(new ShortVectorDirect(a), (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((ShortVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((ShortVectorDirect)null, (short[])null));

        assertEquals(cov, ShortNumericPrimitives.cov(new ShortVectorDirect(a), new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov(new ShortVectorDirect(a), (ShortVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((ShortVectorDirect)null, new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cov((ShortVectorDirect)null, (ShortVectorDirect)null));
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

        assertEquals(cor, ShortNumericPrimitives.cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor(a, (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((short[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((short[])null, (short[])null));

        assertEquals(cor, ShortNumericPrimitives.cor(a, new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor(a, (ShortVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((short[])null, new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((short[])null, (ShortVectorDirect)null));

        assertEquals(cor, ShortNumericPrimitives.cor(new ShortVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor(new ShortVectorDirect(a), (short[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((ShortVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((ShortVectorDirect)null, (short[])null));

        assertEquals(cor, ShortNumericPrimitives.cor(new ShortVectorDirect(a), new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor(new ShortVectorDirect(a), (ShortVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((ShortVectorDirect)null, new ShortVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cor((ShortVectorDirect)null, (ShortVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - ShortNumericPrimitives.sum(new ShortVectorDirect(new short[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - ShortNumericPrimitives.sum(new ShortVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - ShortNumericPrimitives.sum(new ShortVectorDirect(NULL_SHORT))) == 0.0);
        assertTrue(Math.abs(20 - ShortNumericPrimitives.sum(new ShortVectorDirect(new short[]{5, NULL_SHORT, 15}))) == 0.0);
        assertEquals(NULL_SHORT, ShortNumericPrimitives.sum((ShortVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - ShortNumericPrimitives.sum(new short[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - ShortNumericPrimitives.sum(new short[]{})) == 0.0);
        assertTrue(Math.abs(0 - ShortNumericPrimitives.sum(new short[]{NULL_SHORT})) == 0.0);
        assertTrue(Math.abs(20 - ShortNumericPrimitives.sum(new short[]{5, NULL_SHORT, 15})) == 0.0);
        assertEquals(NULL_SHORT, ShortNumericPrimitives.sum((short[]) null));
    }

    public void testSumVector() {
        assertEquals(new short[]{4, 15}, ShortNumericPrimitives.sum(new ObjectVectorDirect<>(new short[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new short[]{4, NULL_SHORT}, ShortNumericPrimitives.sum(new ObjectVectorDirect<>(new short[][]{{5, NULL_SHORT}, {-3, 5}, {2, 6}})));
        assertEquals(null, ShortNumericPrimitives.sum((ObjectVector<short[]>) null));

        try {
            ShortNumericPrimitives.sum(new ObjectVectorDirect<>(new short[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new short[]{4, 15}, ShortNumericPrimitives.sum(new short[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new short[]{4, NULL_SHORT}, ShortNumericPrimitives.sum(new short[][]{{5, NULL_SHORT}, {-3, 5}, {2, 6}}));
        assertEquals(null, ShortNumericPrimitives.sum((short[][]) null));

        try {
            ShortNumericPrimitives.sum(new short[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - ShortNumericPrimitives.product(new short[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_SHORT, ShortNumericPrimitives.product(new short[]{}));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.product(new short[]{NULL_SHORT}));
        assertTrue(Math.abs(75 - ShortNumericPrimitives.product(new short[]{5, NULL_SHORT, 15})) == 0.0);
        assertEquals(NULL_SHORT, ShortNumericPrimitives.product((short[]) null));

        assertTrue(Math.abs(120 - ShortNumericPrimitives.product(new ShortVectorDirect(new short[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_SHORT, ShortNumericPrimitives.product(new ShortVectorDirect()));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.product(new ShortVectorDirect(NULL_SHORT)));
        assertTrue(Math.abs(75 - ShortNumericPrimitives.product(new ShortVectorDirect(new short[]{5, NULL_SHORT, 15}))) == 0.0);
        assertEquals(NULL_SHORT, ShortNumericPrimitives.product((ShortVector) null));
    }

//    public void testProdVector() {
//        assertEquals(new short[]{-30, 120}, product(new ObjectVectorDirect<>(new short[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new short[]{-30, NULL_SHORT}, product(new ObjectVectorDirect<>(new short[][]{{5, NULL_SHORT}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((Vector<short[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new short[][]{{5}, {-3, 5}, {2, 6}}));
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
        assertEquals(new short[]{1, 3, 6, 10, 15}, ShortNumericPrimitives.cumsum(new short[]{1, 2, 3, 4, 5}));
        assertEquals(new short[]{1, 3, 6, 6, 11}, ShortNumericPrimitives.cumsum(new short[]{1, 2, 3, NULL_SHORT, 5}));
        assertEquals(new short[]{NULL_SHORT, 2, 5, 9, 14}, ShortNumericPrimitives.cumsum(new short[]{NULL_SHORT, 2, 3, 4, 5}));
        assertEquals(new short[0], ShortNumericPrimitives.cumsum());
        assertEquals(null, ShortNumericPrimitives.cumsum((short[]) null));

        assertEquals(new short[]{1, 3, 6, 10, 15}, ShortNumericPrimitives.cumsum(new ShortVectorDirect(new short[]{1, 2, 3, 4, 5})));
        assertEquals(new short[]{1, 3, 6, 6, 11}, ShortNumericPrimitives.cumsum(new ShortVectorDirect(new short[]{1, 2, 3, NULL_SHORT, 5})));
        assertEquals(new short[]{NULL_SHORT, 2, 5, 9, 14}, ShortNumericPrimitives.cumsum(new ShortVectorDirect(new short[]{NULL_SHORT, 2, 3, 4, 5})));
        assertEquals(new short[0], ShortNumericPrimitives.cumsum(new ShortVectorDirect()));
        assertEquals(null, ShortNumericPrimitives.cumsum((ShortVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new short[]{1, 2, 6, 24, 120}, ShortNumericPrimitives.cumprod(new short[]{1, 2, 3, 4, 5}));
        assertEquals(new short[]{1, 2, 6, 6, 30}, ShortNumericPrimitives.cumprod(new short[]{1, 2, 3, NULL_SHORT, 5}));
        assertEquals(new short[]{NULL_SHORT, 2, 6, 24, 120}, ShortNumericPrimitives.cumprod(new short[]{NULL_SHORT, 2, 3, 4, 5}));
        assertEquals(new short[0], ShortNumericPrimitives.cumprod());
        assertEquals(null, ShortNumericPrimitives.cumprod((short[]) null));

        assertEquals(new short[]{1, 2, 6, 24, 120}, ShortNumericPrimitives.cumprod(new ShortVectorDirect(new short[]{1, 2, 3, 4, 5})));
        assertEquals(new short[]{1, 2, 6, 6, 30}, ShortNumericPrimitives.cumprod(new ShortVectorDirect(new short[]{1, 2, 3, NULL_SHORT, 5})));
        assertEquals(new short[]{NULL_SHORT, 2, 6, 24, 120}, ShortNumericPrimitives.cumprod(new ShortVectorDirect(new short[]{NULL_SHORT, 2, 3, 4, 5})));
        assertEquals(new short[0], ShortNumericPrimitives.cumprod(new ShortVectorDirect()));
        assertEquals(null, ShortNumericPrimitives.cumprod((ShortVector) null));
    }

    public void testAbs() {
        short value = -5;
        assertEquals((short) Math.abs(value), ShortNumericPrimitives.abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_SHORT, ShortNumericPrimitives.abs(NULL_SHORT), 1e-10);
    }

    public void testAcos() {
        short value = -5;
        assertEquals(Math.acos(value), ShortNumericPrimitives.acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.acos(NULL_SHORT), 1e-10);
    }

    public void testAsin() {
        short value = -5;
        assertEquals(Math.asin(value), ShortNumericPrimitives.asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.asin(NULL_SHORT), 1e-10);
    }

    public void testAtan() {
        short value = -5;
        assertEquals(Math.atan(value), ShortNumericPrimitives.atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.atan(NULL_SHORT), 1e-10);
    }

    public void testCeil() {
        short value = -5;
        assertEquals(Math.ceil(value), ShortNumericPrimitives.ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.ceil(NULL_SHORT), 1e-10);
    }

    public void testCos() {
        short value = -5;
        assertEquals(Math.cos(value), ShortNumericPrimitives.cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.cos(NULL_SHORT), 1e-10);
    }

    public void testExp() {
        short value = -5;
        assertEquals(Math.exp(value), ShortNumericPrimitives.exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.exp(NULL_SHORT), 1e-10);
    }

    public void testFloor() {
        short value = -5;
        assertEquals(Math.floor(value), ShortNumericPrimitives.floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.floor(NULL_SHORT), 1e-10);
    }

    public void testLog() {
        short value = -5;
        assertEquals(Math.log(value), ShortNumericPrimitives.log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.log(NULL_SHORT), 1e-10);
    }

    public void testPow() {
        short value0 = -5;
        short value1 = 2;
        assertEquals(Math.pow(value0, value1), ShortNumericPrimitives.pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.pow(NULL_SHORT, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.pow(value0, NULL_SHORT), 1e-10);
    }

    public void testRint() {
        short value = -5;
        assertEquals(Math.rint(value), ShortNumericPrimitives.rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.rint(NULL_SHORT), 1e-10);
    }

    public void testRound() {
        short value = -5;
        assertEquals(Math.round(value), ShortNumericPrimitives.round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, ShortNumericPrimitives.round(NULL_SHORT), 1e-10);
    }

    public void testSin() {
        short value = -5;
        assertEquals(Math.sin(value), ShortNumericPrimitives.sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.sin(NULL_SHORT), 1e-10);
    }

    public void testSqrt() {
        short value = -5;
        assertEquals(Math.sqrt(value), ShortNumericPrimitives.sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.sqrt(NULL_SHORT), 1e-10);
    }

    public void testTan() {
        short value = -5;
        assertEquals(Math.tan(value), ShortNumericPrimitives.tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, ShortNumericPrimitives.tan(NULL_SHORT), 1e-10);
    }

    public void testLowerBin() {
        short value = (short) 114;

        assertEquals((short) 110, ShortNumericPrimitives.lowerBin(value, (short) 5));
        assertEquals((short) 110, ShortNumericPrimitives.lowerBin(value, (short) 10));
        assertEquals((short) 100, ShortNumericPrimitives.lowerBin(value, (short) 20));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.lowerBin(NULL_SHORT, (short) 5));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.lowerBin(value, NULL_SHORT));

        assertEquals(ShortNumericPrimitives.lowerBin(value, (short) 5), ShortNumericPrimitives.lowerBin(ShortNumericPrimitives.lowerBin(value, (short) 5), (short) 5));
    }

    public void testLowerBinWithOffset() {
        short value = (short) 114;
        short offset = (short) 3;

        assertEquals((short) 113, ShortNumericPrimitives.lowerBin(value, (short) 5, offset));
        assertEquals((short) 113, ShortNumericPrimitives.lowerBin(value, (short) 10, offset));
        assertEquals((short) 103, ShortNumericPrimitives.lowerBin(value, (short) 20, offset));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.lowerBin(NULL_SHORT, (short) 5, offset));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.lowerBin(value, NULL_SHORT, offset));

        assertEquals(ShortNumericPrimitives.lowerBin(value, (short) 5, offset), ShortNumericPrimitives.lowerBin(ShortNumericPrimitives.lowerBin(value, (short) 5, offset), (short) 5, offset));
    }

    public void testUpperBin() {
        short value = (short) 114;

        assertEquals((short) 115, ShortNumericPrimitives.upperBin(value, (short) 5));
        assertEquals((short) 120, ShortNumericPrimitives.upperBin(value, (short) 10));
        assertEquals((short) 120, ShortNumericPrimitives.upperBin(value, (short) 20));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.upperBin(NULL_SHORT, (short) 5));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.upperBin(value, NULL_SHORT));

        assertEquals(ShortNumericPrimitives.upperBin(value, (short) 5), ShortNumericPrimitives.upperBin(ShortNumericPrimitives.upperBin(value, (short) 5), (short) 5));
    }

    public void testUpperBinWithOffset() {
        short value = (short) 114;
        short offset = (short) 3;

        assertEquals((short) 118, ShortNumericPrimitives.upperBin(value, (short) 5, offset));
        assertEquals((short) 123, ShortNumericPrimitives.upperBin(value, (short) 10, offset));
        assertEquals((short) 123, ShortNumericPrimitives.upperBin(value, (short) 20, offset));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.upperBin(NULL_SHORT, (short) 5, offset));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.upperBin(value, NULL_SHORT, offset));

        assertEquals(ShortNumericPrimitives.upperBin(value, (short) 5, offset), ShortNumericPrimitives.upperBin(ShortNumericPrimitives.upperBin(value, (short) 5, offset), (short) 5, offset));
    }

    public void testClamp() {
        assertEquals((short) 3, ShortNumericPrimitives.clamp((short) 3, (short) -6, (short) 5));
        assertEquals((short) -6, ShortNumericPrimitives.clamp((short) -7, (short) -6, (short) 5));
        assertEquals((short) 5, ShortNumericPrimitives.clamp((short) 7, (short) -6, (short) 5));
        assertEquals(NULL_SHORT, ShortNumericPrimitives.clamp(NULL_SHORT, (short) -6, (short) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, ShortNumericPrimitives.binSearchIndex((short[]) null, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.binSearchIndex(new short[]{1,3,4}, (short)0, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.binSearchIndex(new short[]{1,3,4}, (short)1, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.binSearchIndex(new short[]{1,3,4}, (short)2, BinSearch.BS_ANY));
        assertEquals(1, ShortNumericPrimitives.binSearchIndex(new short[]{1,3,4}, (short)3, BinSearch.BS_ANY));
        assertEquals(2, ShortNumericPrimitives.binSearchIndex(new short[]{1,3,4}, (short)4, BinSearch.BS_ANY));
        assertEquals(2, ShortNumericPrimitives.binSearchIndex(new short[]{1,3,4}, (short)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, ShortNumericPrimitives.binSearchIndex((ShortVector) null, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.binSearchIndex(new ShortVectorDirect(new short[]{1,3,4}), (short)0, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.binSearchIndex(new ShortVectorDirect(new short[]{1,3,4}), (short)1, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.binSearchIndex(new ShortVectorDirect(new short[]{1,3,4}), (short)2, BinSearch.BS_ANY));
        assertEquals(1, ShortNumericPrimitives.binSearchIndex(new ShortVectorDirect(new short[]{1,3,4}), (short)3, BinSearch.BS_ANY));
        assertEquals(2, ShortNumericPrimitives.binSearchIndex(new ShortVectorDirect(new short[]{1,3,4}), (short)4, BinSearch.BS_ANY));
        assertEquals(2, ShortNumericPrimitives.binSearchIndex(new ShortVectorDirect(new short[]{1,3,4}), (short)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, ShortNumericPrimitives.rawBinSearchIndex((ShortVector)null, (short) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, ShortNumericPrimitives.rawBinSearchIndex((ShortVector)null, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, ShortNumericPrimitives.rawBinSearchIndex((ShortVector)null, (short) 0, BinSearch.BS_LOWEST));

        short[] empty = {};
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(empty), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(empty), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(empty), (short) 0, BinSearch.BS_LOWEST));

        short[] one = {11};
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 12, BinSearch.BS_ANY));
        assertEquals(-2, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 12, BinSearch.BS_LOWEST));

        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 11, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(one), (short) 11, BinSearch.BS_LOWEST));


        short[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        ShortNumericPrimitives.rawBinSearchIndex((ShortVector)null, (short) 0, null);

        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 26, BinSearch.BS_LOWEST));

        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 1, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 1, BinSearch.BS_LOWEST));

        assertEquals(2, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 2, BinSearch.BS_LOWEST));

        assertEquals(5, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 3, BinSearch.BS_LOWEST));

        assertEquals(9, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 4, BinSearch.BS_LOWEST));

        assertEquals(14, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 7, BinSearch.BS_ANY));
        assertEquals(-16, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 7, BinSearch.BS_LOWEST));

        assertEquals(19, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 10, BinSearch.BS_LOWEST));

        assertEquals(24, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 11, BinSearch.BS_LOWEST));

        assertEquals(25, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 15, BinSearch.BS_ANY));
        assertEquals(25, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 15, BinSearch.BS_LOWEST));

        assertEquals(29, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, ShortNumericPrimitives.rawBinSearchIndex(new ShortVectorDirect(v), (short) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, ShortNumericPrimitives.rawBinSearchIndex((short[]) null, (short) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, ShortNumericPrimitives.rawBinSearchIndex((short[])null, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, ShortNumericPrimitives.rawBinSearchIndex((short[])null, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(empty, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(empty, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(empty, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 12, BinSearch.BS_ANY));
        assertEquals(-2, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 12, BinSearch.BS_LOWEST));

        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 11, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(one, (short) 11, BinSearch.BS_LOWEST));


        ShortNumericPrimitives.rawBinSearchIndex((short[])null, (short) 0, null);

        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 26, BinSearch.BS_LOWEST));

        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 1, BinSearch.BS_ANY));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 1, BinSearch.BS_LOWEST));

        assertEquals(2, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 2, BinSearch.BS_LOWEST));

        assertEquals(5, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 3, BinSearch.BS_LOWEST));

        assertEquals(9, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 4, BinSearch.BS_LOWEST));

        assertEquals(14, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 7, BinSearch.BS_ANY));
        assertEquals(-16, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 7, BinSearch.BS_LOWEST));

        assertEquals(19, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 10, BinSearch.BS_LOWEST));

        assertEquals(24, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 11, BinSearch.BS_LOWEST));

        assertEquals(25, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 15, BinSearch.BS_ANY));
        assertEquals(25, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 15, BinSearch.BS_LOWEST));

        assertEquals(29, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, ShortNumericPrimitives.rawBinSearchIndex(v, (short) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final short[] shorts = new short[]{1, -5, -2, -2, 96, 0, 12, NULL_SHORT, NULL_SHORT};
        final ShortVector sort = ShortNumericPrimitives.sort(new ShortVectorDirect(shorts));
        final ShortVector expected = new ShortVectorDirect(new short[]{NULL_SHORT, NULL_SHORT, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        short[] sortedArray = ShortNumericPrimitives.sort(shorts);
        assertEquals(new short[]{NULL_SHORT, NULL_SHORT, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(ShortNumericPrimitives.sort((ShortVector)null));
        assertNull(ShortNumericPrimitives.sort((short[])null));
        assertNull(ShortNumericPrimitives.sort((Short[])null));
        assertEquals(new ShortVectorDirect(), ShortNumericPrimitives.sort(new ShortVectorDirect()));
        assertEquals(new short[]{}, ShortNumericPrimitives.sort(new short[]{}));
        assertEquals(new short[]{}, ShortNumericPrimitives.sort(new Short[]{}));
    }

    public void testSortDescending() {
        final short[] shorts = new short[]{1, -5, -2, -2, 96, 0, 12, NULL_SHORT, NULL_SHORT};
        final ShortVector sort = ShortNumericPrimitives.sortDescending(new ShortVectorDirect(shorts));
        final ShortVector expected = new ShortVectorDirect(new short[]{96, 12, 1, 0, -2, -2, -5, NULL_SHORT, NULL_SHORT});
        assertEquals(expected, sort);

        short[] sortedArray = ShortNumericPrimitives.sortDescending(shorts);
        assertEquals(new short[]{96, 12, 1, 0, -2, -2, -5, NULL_SHORT, NULL_SHORT}, sortedArray);

        assertNull(ShortNumericPrimitives.sortDescending((ShortVector)null));
        assertNull(ShortNumericPrimitives.sortDescending((short[])null));
        assertNull(ShortNumericPrimitives.sortDescending((Short[])null));
        assertEquals(new ShortVectorDirect(), ShortNumericPrimitives.sortDescending(new ShortVectorDirect()));
        assertEquals(new short[]{}, ShortNumericPrimitives.sortDescending(new short[]{}));
        assertEquals(new short[]{}, ShortNumericPrimitives.sortDescending(new Short[]{}));
    }

    public void testSortsExceptions() {
        ShortVector shortVector = null;
        ShortVector sort = ShortNumericPrimitives.sort(shortVector);
        assertNull(sort);

        short[] shorts = null;
        short[] sortArray = ShortNumericPrimitives.sort(shorts);
        assertNull(sortArray);

        shorts = new short[]{};
        sort = ShortNumericPrimitives.sort(new ShortVectorDirect(shorts));
        assertEquals(new ShortVectorDirect(), sort);

        sortArray = ShortNumericPrimitives.sort(shorts);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        ShortVector shortVector = null;
        ShortVector sort = ShortNumericPrimitives.sortDescending(shortVector);
        assertNull(sort);

        short[] shorts = null;
        short[] sortArray = ShortNumericPrimitives.sortDescending(shorts);
        assertNull(sortArray);

        shorts = new short[]{};
        sort = ShortNumericPrimitives.sortDescending(new ShortVectorDirect(shorts));
        assertEquals(new ShortVectorDirect(), sort);

        sortArray = ShortNumericPrimitives.sortDescending(shorts);
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
        assertEquals(3.0, ShortNumericPrimitives.median(new short[]{4,2,3}));
        assertEquals(3.5, ShortNumericPrimitives.median(new short[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.median((short[])null));

        assertEquals(3.0, ShortNumericPrimitives.median(new Short[]{(short)4,(short)2,(short)3}));
        assertEquals(3.5, ShortNumericPrimitives.median(new Short[]{(short)5,(short)4,(short)2,(short)3}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.median((Short[])null));

        assertEquals(3.0, ShortNumericPrimitives.median(new ShortVectorDirect(new short[]{4,2,3})));
        assertEquals(3.5, ShortNumericPrimitives.median(new ShortVectorDirect(new short[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.median((ShortVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, ShortNumericPrimitives.percentile(0.00, new short[]{4,2,3}));
        assertEquals(3.0, ShortNumericPrimitives.percentile(0.50, new short[]{4,2,3}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.percentile(0.25, (short[])null));

        assertEquals(2.0, ShortNumericPrimitives.percentile(0.00, new ShortVectorDirect(new short[]{4,2,3})));
        assertEquals(3.0, ShortNumericPrimitives.percentile(0.50, new ShortVectorDirect(new short[]{4,2,3})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.percentile(0.25, (ShortVector) null));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wsum(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedSum(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wavg(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.weightedAvg(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (short[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (int[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (long[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (double[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wvar(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wvar(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (short[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (int[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (long[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (double[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wstd(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wstd(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (short[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (int[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (long[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (double[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wste(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wste(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }

    public void testWtstat() {
        final double target = ShortNumericPrimitives.wavg(new short[]{1,2,3}, new short[]{4,5,6}) / ShortNumericPrimitives.wste(new short[]{1,2,3}, new short[]{4,5,6});

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (short[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (int[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (long[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (double[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (short[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (int[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (long[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (double[])null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new float[]{4,5,6,7,NULL_FLOAT}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector)null, new float[]{4,5,6}));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (float[])null));

        /////

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new short[]{1,2,3,NULL_SHORT,5}, new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((short[])null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new short[]{1,2,3}, (FloatVector)null));

        /////

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (ShortVector) null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (IntVector) null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (LongVector) null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3,NULL_SHORT,5}), new FloatVectorDirect(new float[]{4,5,6,7,NULL_FLOAT})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat((ShortVector) null, new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(NULL_DOUBLE, ShortNumericPrimitives.wtstat(new ShortVectorDirect(new short[]{1,2,3}), (FloatVector)null));
    }
}
