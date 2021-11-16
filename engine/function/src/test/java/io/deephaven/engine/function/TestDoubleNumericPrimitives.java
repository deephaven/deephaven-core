/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestFloatNumericPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.vector.*;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.engine.function.DoublePrimitives.count;
import static io.deephaven.util.QueryConstants.*;

public class TestDoubleNumericPrimitives extends BaseArrayTestCase {

    public void testSignum() {
        assertEquals((double) 1, DoubleNumericPrimitives.signum((double) 5));
        assertEquals((double) 0, DoubleNumericPrimitives.signum((double) 0));
        assertEquals((double) -1, DoubleNumericPrimitives.signum((double) -5));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.signum(NULL_DOUBLE));
    }

    public void testAvg() {
        assertEquals(50.0, DoubleNumericPrimitives.avg(new double[]{40, 50, 60}));
        assertEquals(45.5, DoubleNumericPrimitives.avg(new double[]{40, 51}));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.avg(new double[]{})));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.avg(new double[]{NULL_DOUBLE})));
        assertEquals(10.0, DoubleNumericPrimitives.avg(new double[]{5, NULL_DOUBLE, 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.avg((double[])null));

        assertEquals(50.0, DoubleNumericPrimitives.avg(new Double[]{(double)40, (double)50, (double)60}));
        assertEquals(45.5, DoubleNumericPrimitives.avg(new Double[]{(double)40, (double)51}));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.avg(new Double[]{})));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.avg(new Double[]{NULL_DOUBLE})));
        assertEquals(10.0, DoubleNumericPrimitives.avg(new Double[]{(double)5, NULL_DOUBLE, (double)15}));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.avg((Double[])null));

        assertEquals(50.0, DoubleNumericPrimitives.avg(new DoubleVectorDirect(new double[]{40, 50, 60})));
        assertEquals(45.5, DoubleNumericPrimitives.avg(new DoubleVectorDirect(new double[]{40, 51})));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.avg(new DoubleVectorDirect())));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.avg(new DoubleVectorDirect(NULL_DOUBLE))));
        assertEquals(10.0, DoubleNumericPrimitives.avg(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15})));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.avg((DoubleVectorDirect)null));
    }

    public void testAbsAvg() {
        assertEquals(50.0, DoubleNumericPrimitives.absAvg(new double[]{40, (double) 50, 60}));
        assertEquals(45.5, DoubleNumericPrimitives.absAvg(new double[]{(double) 40, 51}));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.absAvg(new double[]{})));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.absAvg(new double[]{NULL_DOUBLE})));
        assertEquals(10.0, DoubleNumericPrimitives.absAvg(new double[]{(double) 5, NULL_DOUBLE, (double) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.absAvg((double[])null));

        assertEquals(50.0, DoubleNumericPrimitives.absAvg(new Double[]{(double)40, (double) 50, (double)60}));
        assertEquals(45.5, DoubleNumericPrimitives.absAvg(new Double[]{(double) 40, (double)51}));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.absAvg(new Double[]{})));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.absAvg(new Double[]{NULL_DOUBLE})));
        assertEquals(10.0, DoubleNumericPrimitives.absAvg(new Double[]{(double) 5, NULL_DOUBLE, (double) 15}));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.absAvg((Double[])null));

        assertEquals(50.0, DoubleNumericPrimitives.absAvg(new DoubleVectorDirect(new double[]{40, (double) 50, 60})));
        assertEquals(45.5, DoubleNumericPrimitives.absAvg(new DoubleVectorDirect(new double[]{(double) 40, 51})));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.absAvg(new DoubleVectorDirect())));
        assertTrue(Double.isNaN(DoubleNumericPrimitives.absAvg(new DoubleVectorDirect(NULL_DOUBLE))));
        assertEquals(10.0, DoubleNumericPrimitives.absAvg(new DoubleVectorDirect((double) 5, NULL_DOUBLE, (double) 15)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.absAvg((DoubleVectorDirect)null));
    }

    public void testCountPos() {
        assertEquals(4, DoubleNumericPrimitives.countPos(new double[]{40, 50, 60, (double) 1, 0}));
        assertEquals(0, DoubleNumericPrimitives.countPos(new double[]{}));
        assertEquals(0, DoubleNumericPrimitives.countPos(new double[]{NULL_DOUBLE}));
        assertEquals(3, DoubleNumericPrimitives.countPos(new double[]{5, NULL_DOUBLE, 15, (double) 1, 0}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countPos((double[])null));

        assertEquals(4, DoubleNumericPrimitives.countPos(new Double[]{(double)40, (double)50, (double)60, (double) 1, (double)0}));
        assertEquals(0, DoubleNumericPrimitives.countPos(new Double[]{}));
        assertEquals(0, DoubleNumericPrimitives.countPos(new Double[]{NULL_DOUBLE}));
        assertEquals(3, DoubleNumericPrimitives.countPos(new Double[]{(double)5, NULL_DOUBLE, (double)15, (double) 1, (double)0}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countPos((double[])null));

        assertEquals(4, DoubleNumericPrimitives.countPos(new DoubleVectorDirect(new double[]{40, 50, 60, (double) 1, 0})));
        assertEquals(0, DoubleNumericPrimitives.countPos(new DoubleVectorDirect()));
        assertEquals(0, DoubleNumericPrimitives.countPos(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(3, DoubleNumericPrimitives.countPos(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15, (double) 1, 0})));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countPos((DoubleVectorDirect)null));
    }

    public void testCountNeg() {
        assertEquals(2, DoubleNumericPrimitives.countNeg(new double[]{40, (double) -50, 60, (double) -1, 0}));
        assertEquals(0, DoubleNumericPrimitives.countNeg(new double[]{}));
        assertEquals(0, DoubleNumericPrimitives.countNeg(new double[]{NULL_DOUBLE}));
        assertEquals(1, DoubleNumericPrimitives.countNeg(new double[]{5, NULL_DOUBLE, 15, (double) -1, 0}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countNeg((double[])null));

        assertEquals(2, DoubleNumericPrimitives.countNeg(new Double[]{(double)40, (double) -50, (double)60, (double) -1, (double)0}));
        assertEquals(0, DoubleNumericPrimitives.countNeg(new Double[]{}));
        assertEquals(0, DoubleNumericPrimitives.countNeg(new Double[]{NULL_DOUBLE}));
        assertEquals(1, DoubleNumericPrimitives.countNeg(new Double[]{(double)5, NULL_DOUBLE, (double)15, (double) -1, (double)0}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countNeg((Double[])null));

        assertEquals(2, DoubleNumericPrimitives.countNeg(new DoubleVectorDirect(new double[]{40, (double) -50, 60, (double) -1, 0})));
        assertEquals(0, DoubleNumericPrimitives.countNeg(new DoubleVectorDirect()));
        assertEquals(0, DoubleNumericPrimitives.countNeg(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(1, DoubleNumericPrimitives.countNeg(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15, (double) -1, 0})));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countNeg((DoubleVectorDirect)null));
    }

    public void testCountZero() {
        assertEquals(2, DoubleNumericPrimitives.countZero(new double[]{0, 40, 50, 60, (double) -1, 0}));
        assertEquals(0, DoubleNumericPrimitives.countZero(new double[]{}));
        assertEquals(0, DoubleNumericPrimitives.countZero(new double[]{NULL_DOUBLE}));
        assertEquals(2, DoubleNumericPrimitives.countZero(new double[]{0, 5, NULL_DOUBLE, 0, (double) -15}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countZero((double[])null));

        assertEquals(2, DoubleNumericPrimitives.countZero(new Double[]{(double)0, (double)40, (double)50, (double)60, (double) -1, (double)0}));
        assertEquals(0, DoubleNumericPrimitives.countZero(new Double[]{}));
        assertEquals(0, DoubleNumericPrimitives.countZero(new Double[]{NULL_DOUBLE}));
        assertEquals(2, DoubleNumericPrimitives.countZero(new Double[]{(double)0, (double)5, NULL_DOUBLE, (double)0, (double) -15}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countZero((Double[])null));

        assertEquals(2, DoubleNumericPrimitives.countZero(new DoubleVectorDirect(new double[]{0, 40, 50, 60, (double) -1, 0})));
        assertEquals(0, DoubleNumericPrimitives.countZero(new DoubleVectorDirect()));
        assertEquals(0, DoubleNumericPrimitives.countZero(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(2, DoubleNumericPrimitives.countZero(new DoubleVectorDirect(new double[]{0, 5, NULL_DOUBLE, 0, (double) -15})));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.countZero((DoubleVectorDirect)null));
    }

    public void testMax() {
        assertEquals((double) 60, DoubleNumericPrimitives.max(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals((double) 60, DoubleNumericPrimitives.max(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1)));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.max(new DoubleVectorDirect()));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.max(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.max((DoubleVector) null));

        assertEquals((double) 60, DoubleNumericPrimitives.max((double) 0, (double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1, (double) 0));
        assertEquals((double) 60, DoubleNumericPrimitives.max((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.max());
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.max(NULL_DOUBLE));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.max((double[]) null));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.max((Double[]) null));
    }

    public void testMin() {
        assertEquals((double) 0, DoubleNumericPrimitives.min(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals((double) -1, DoubleNumericPrimitives.min(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1)));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.min(new DoubleVectorDirect()));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.min(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.min((DoubleVector) null));

        assertEquals((double) 0, DoubleNumericPrimitives.min((double) 0, (double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1, (double) 0));
        assertEquals((double) -1, DoubleNumericPrimitives.min((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.min());
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.min(NULL_DOUBLE));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.min((double[]) null));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.min((Double[]) null));
    }

    public void testFirstIndexOf() {
        assertEquals(1, DoubleNumericPrimitives.firstIndexOf(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}, (double)40));
        assertEquals(4, DoubleNumericPrimitives.firstIndexOf(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}, (double)60));
        assertEquals(NULL_INT, DoubleNumericPrimitives.firstIndexOf(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}, (double)1));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.firstIndexOf((double[])null, (double)40));

        assertEquals(1, DoubleNumericPrimitives.firstIndexOf(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)40));
        assertEquals(4, DoubleNumericPrimitives.firstIndexOf(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)60));
        assertEquals(NULL_INT, DoubleNumericPrimitives.firstIndexOf(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 40, 60, 40, 0}), (double)1));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.firstIndexOf((DoubleVector) null, (double)40));
    }

    public void testIndexOfMax() {
        assertEquals(4, DoubleNumericPrimitives.indexOfMax(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0}));
        assertEquals(3, DoubleNumericPrimitives.indexOfMax(new double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMax(new double[]{}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMax(new double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.indexOfMax((double[])null));

        assertEquals(4, DoubleNumericPrimitives.indexOfMax(new Double[]{(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) 1, (double)0}));
        assertEquals(3, DoubleNumericPrimitives.indexOfMax(new Double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMax(new Double[]{}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMax(new Double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.indexOfMax((Double[])null));

        assertEquals(4, DoubleNumericPrimitives.indexOfMax(new DoubleVectorDirect(new double[]{0, 40, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals(3, DoubleNumericPrimitives.indexOfMax(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) 1)));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMax(new DoubleVectorDirect()));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMax(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.indexOfMax((DoubleVectorDirect)null));
    }

    public void testIndexOfMin() {
        assertEquals(1, DoubleNumericPrimitives.indexOfMin(new double[]{40, 0, NULL_DOUBLE, 50, 60, (double) 1, 0}));
        assertEquals(4, DoubleNumericPrimitives.indexOfMin(new double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMin(new double[]{}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMin(new double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.indexOfMin((double[])null));

        assertEquals(1, DoubleNumericPrimitives.indexOfMin(new Double[]{(double)40, (double)0, NULL_DOUBLE, (double)50, (double)60, (double) 1, (double)0}));
        assertEquals(4, DoubleNumericPrimitives.indexOfMin(new Double[]{(double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMin(new Double[]{}));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMin(new Double[]{NULL_DOUBLE}));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.indexOfMin((Double[])null));

        assertEquals(1, DoubleNumericPrimitives.indexOfMin(new DoubleVectorDirect(new double[]{40, 0, NULL_DOUBLE, 50, 60, (double) 1, 0})));
        assertEquals(4, DoubleNumericPrimitives.indexOfMin(new DoubleVectorDirect((double) 40, NULL_DOUBLE, (double) 50, (double) 60, (double) -1)));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMin(new DoubleVectorDirect()));
        assertEquals(-1, DoubleNumericPrimitives.indexOfMin(new DoubleVectorDirect(NULL_DOUBLE)));
        assertEquals(QueryConstants.NULL_INT, DoubleNumericPrimitives.indexOfMin((DoubleVectorDirect)null));
    }


    public void testVar() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, DoubleNumericPrimitives.var(v));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.var((double[])null));

        assertEquals(var, DoubleNumericPrimitives.var(V));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.var((Double[])null));

        assertEquals(var, DoubleNumericPrimitives.var(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.var((DoubleVectorDirect)null));
    }

    public void testStd() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(Math.sqrt(DoubleNumericPrimitives.var(new DoubleVectorDirect(v))), DoubleNumericPrimitives.std(v));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.std((double[])null));

        assertEquals(Math.sqrt(DoubleNumericPrimitives.var(new DoubleVectorDirect(v))), DoubleNumericPrimitives.std(V));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.std((Double[])null));

        assertEquals(Math.sqrt(DoubleNumericPrimitives.var(new DoubleVectorDirect(v))), DoubleNumericPrimitives.std(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.std((DoubleVectorDirect)null));
    }

    public void testSte() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(DoubleNumericPrimitives.std(new DoubleVectorDirect(v)) / Math.sqrt(DoublePrimitives.count(new DoubleVectorDirect(v))), DoubleNumericPrimitives.ste(v));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.ste((double[])null));

        assertEquals(DoubleNumericPrimitives.std(new DoubleVectorDirect(v)) / Math.sqrt(DoublePrimitives.count(new DoubleVectorDirect(v))), DoubleNumericPrimitives.ste(V));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.ste((Double[])null));

        assertEquals(DoubleNumericPrimitives.std(new DoubleVectorDirect(v)) / Math.sqrt(DoublePrimitives.count(new DoubleVectorDirect(v))), DoubleNumericPrimitives.ste(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.ste((DoubleVectorDirect)null));
    }

    public void testTstat() {
        double[] v = {0, 40, NULL_DOUBLE, 50, 60, (double) -1, 0};
        Double[] V = {(double)0, (double)40, NULL_DOUBLE, (double)50, (double)60, (double) -1, (double)0};

        assertEquals(DoubleNumericPrimitives.avg(new DoubleVectorDirect(v)) / DoubleNumericPrimitives.ste(new DoubleVectorDirect(v)), DoubleNumericPrimitives.tstat(v));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.tstat((double[])null));

        assertEquals(DoubleNumericPrimitives.avg(new DoubleVectorDirect(v)) / DoubleNumericPrimitives.ste(new DoubleVectorDirect(v)), DoubleNumericPrimitives.tstat(V));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.tstat((Double[])null));

        assertEquals(DoubleNumericPrimitives.avg(new DoubleVectorDirect(v)) / DoubleNumericPrimitives.ste(new DoubleVectorDirect(v)), DoubleNumericPrimitives.tstat(new DoubleVectorDirect(v)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.tstat((DoubleVectorDirect)null));
    }

    public void testCov() {
        double[] a = {10, 40, NULL_DOUBLE, 50, NULL_DOUBLE, (double) -1, 0, (double) -7};
        double[] b = {0, (double) -40, NULL_DOUBLE, NULL_DOUBLE, 6, (double) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, DoubleNumericPrimitives.cov(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov(a, (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((double[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((double[])null, (double[]) null));

        assertEquals(cov, DoubleNumericPrimitives.cov(a, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov(a, (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((double[])null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((double[])null, (DoubleVectorDirect)null));

        assertEquals(cov, DoubleNumericPrimitives.cov(new DoubleVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov(new DoubleVectorDirect(a), (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((DoubleVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((DoubleVectorDirect)null, (double[])null));

        assertEquals(cov, DoubleNumericPrimitives.cov(new DoubleVectorDirect(a), new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov(new DoubleVectorDirect(a), (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((DoubleVectorDirect)null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cov((DoubleVectorDirect)null, (DoubleVectorDirect)null));
    }

    public void testCor() {
        double[] a = {10, 40, NULL_DOUBLE, 50, NULL_DOUBLE, (double) -1, 0, (double) -7};
        double[] b = {0, (double) -40, NULL_DOUBLE, NULL_DOUBLE, 6, (double) -1, 11, 3};
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

        assertEquals(cor, DoubleNumericPrimitives.cor(a, b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor(a, (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((double[])null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((double[])null, (double[])null));

        assertEquals(cor, DoubleNumericPrimitives.cor(a, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor(a, (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((double[])null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((double[])null, (DoubleVectorDirect)null));

        assertEquals(cor, DoubleNumericPrimitives.cor(new DoubleVectorDirect(a), b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor(new DoubleVectorDirect(a), (double[])null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((DoubleVectorDirect)null, b));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((DoubleVectorDirect)null, (double[])null));

        assertEquals(cor, DoubleNumericPrimitives.cor(new DoubleVectorDirect(a), new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor(new DoubleVectorDirect(a), (DoubleVectorDirect)null));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((DoubleVectorDirect)null, new DoubleVectorDirect(b)));
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cor((DoubleVectorDirect)null, (DoubleVectorDirect)null));
    }

    public void testSum1() {
        assertTrue(Math.abs(15 - DoubleNumericPrimitives.sum(new DoubleVectorDirect(new double[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - DoubleNumericPrimitives.sum(new DoubleVectorDirect())) == 0.0);
        assertTrue(Math.abs(0 - DoubleNumericPrimitives.sum(new DoubleVectorDirect(NULL_DOUBLE))) == 0.0);
        assertTrue(Math.abs(20 - DoubleNumericPrimitives.sum(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15}))) == 0.0);
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.sum((DoubleVector) null));
    }

    public void testSum2() {
        assertTrue(Math.abs(15 - DoubleNumericPrimitives.sum(new double[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - DoubleNumericPrimitives.sum(new double[]{})) == 0.0);
        assertTrue(Math.abs(0 - DoubleNumericPrimitives.sum(new double[]{NULL_DOUBLE})) == 0.0);
        assertTrue(Math.abs(20 - DoubleNumericPrimitives.sum(new double[]{5, NULL_DOUBLE, 15})) == 0.0);
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.sum((double[]) null));
    }

    public void testSumVector() {
        assertEquals(new double[]{4, 15}, DoubleNumericPrimitives.sum(new ObjectVectorDirect<>(new double[][]{{5, 4}, {-3, 5}, {2, 6}})));
        assertEquals(new double[]{4, NULL_DOUBLE}, DoubleNumericPrimitives.sum(new ObjectVectorDirect<>(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}})));
        assertEquals(null, DoubleNumericPrimitives.sum((ObjectVector<double[]>) null));

        try {
            DoubleNumericPrimitives.sum(new ObjectVectorDirect<>(new double[][]{{5}, {-3, 5}, {2, 6}}));
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testSumArray() {
        assertEquals(new double[]{4, 15}, DoubleNumericPrimitives.sum(new double[][]{{5, 4}, {-3, 5}, {2, 6}}));
        assertEquals(new double[]{4, NULL_DOUBLE}, DoubleNumericPrimitives.sum(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}}));
        assertEquals(null, DoubleNumericPrimitives.sum((double[][]) null));

        try {
            DoubleNumericPrimitives.sum(new double[][]{{5}, {-3, 5}, {2, 6}});
            fail("Should have failed on different length arrays");
        } catch (RequirementFailure e) {
            //pass
        }
    }

    public void testProduct() {
        assertTrue(Math.abs(120 - DoubleNumericPrimitives.product(new double[]{4, 5, 6})) == 0.0);
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.product(new double[]{}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.product(new double[]{NULL_DOUBLE}));
        assertTrue(Math.abs(75 - DoubleNumericPrimitives.product(new double[]{5, NULL_DOUBLE, 15})) == 0.0);
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.product((double[]) null));

        assertTrue(Math.abs(120 - DoubleNumericPrimitives.product(new DoubleVectorDirect(new double[]{4, 5, 6}))) == 0.0);
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.product(new DoubleVectorDirect()));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.product(new DoubleVectorDirect(NULL_DOUBLE)));
        assertTrue(Math.abs(75 - DoubleNumericPrimitives.product(new DoubleVectorDirect(new double[]{5, NULL_DOUBLE, 15}))) == 0.0);
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.product((DoubleVector) null));
    }

//    public void testProdVector() {
//        assertEquals(new double[]{-30, 120}, product(new ObjectVectorDirect<>(new double[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new double[]{-30, NULL_DOUBLE}, product(new ObjectVectorDirect<>(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((Vector<double[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new double[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void testProdArray() {
//        assertEquals(new double[]{-30, 120}, product(new double[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new double[]{-30, NULL_DOUBLE}, product(new double[][]{{5, NULL_DOUBLE}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((double[][]) null));
//
//        try {
//            product(new double[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void testCumSumArray() {
        assertEquals(new double[]{1, 3, 6, 10, 15}, DoubleNumericPrimitives.cumsum(new double[]{1, 2, 3, 4, 5}));
        assertEquals(new double[]{1, 3, 6, 6, 11}, DoubleNumericPrimitives.cumsum(new double[]{1, 2, 3, NULL_DOUBLE, 5}));
        assertEquals(new double[]{NULL_DOUBLE, 2, 5, 9, 14}, DoubleNumericPrimitives.cumsum(new double[]{NULL_DOUBLE, 2, 3, 4, 5}));
        assertEquals(new double[0], DoubleNumericPrimitives.cumsum());
        assertEquals(null, DoubleNumericPrimitives.cumsum((double[]) null));

        assertEquals(new double[]{1, 3, 6, 10, 15}, DoubleNumericPrimitives.cumsum(new DoubleVectorDirect(new double[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 3, 6, 6, 11}, DoubleNumericPrimitives.cumsum(new DoubleVectorDirect(new double[]{1, 2, 3, NULL_DOUBLE, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 5, 9, 14}, DoubleNumericPrimitives.cumsum(new DoubleVectorDirect(new double[]{NULL_DOUBLE, 2, 3, 4, 5})));
        assertEquals(new double[0], DoubleNumericPrimitives.cumsum(new DoubleVectorDirect()));
        assertEquals(null, DoubleNumericPrimitives.cumsum((DoubleVector) null));
    }

    public void testCumProdArray() {
        assertEquals(new double[]{1, 2, 6, 24, 120}, DoubleNumericPrimitives.cumprod(new double[]{1, 2, 3, 4, 5}));
        assertEquals(new double[]{1, 2, 6, 6, 30}, DoubleNumericPrimitives.cumprod(new double[]{1, 2, 3, NULL_DOUBLE, 5}));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, DoubleNumericPrimitives.cumprod(new double[]{NULL_DOUBLE, 2, 3, 4, 5}));
        assertEquals(new double[0], DoubleNumericPrimitives.cumprod());
        assertEquals(null, DoubleNumericPrimitives.cumprod((double[]) null));

        assertEquals(new double[]{1, 2, 6, 24, 120}, DoubleNumericPrimitives.cumprod(new DoubleVectorDirect(new double[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 2, 6, 6, 30}, DoubleNumericPrimitives.cumprod(new DoubleVectorDirect(new double[]{1, 2, 3, NULL_DOUBLE, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, DoubleNumericPrimitives.cumprod(new DoubleVectorDirect(new double[]{NULL_DOUBLE, 2, 3, 4, 5})));
        assertEquals(new double[0], DoubleNumericPrimitives.cumprod(new DoubleVectorDirect()));
        assertEquals(null, DoubleNumericPrimitives.cumprod((DoubleVector) null));
    }

    public void testAbs() {
        double value = -5;
        assertEquals((double) Math.abs(value), DoubleNumericPrimitives.abs(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.abs(NULL_DOUBLE), 1e-10);
    }

    public void testAcos() {
        double value = -5;
        assertEquals(Math.acos(value), DoubleNumericPrimitives.acos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.acos(NULL_DOUBLE), 1e-10);
    }

    public void testAsin() {
        double value = -5;
        assertEquals(Math.asin(value), DoubleNumericPrimitives.asin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.asin(NULL_DOUBLE), 1e-10);
    }

    public void testAtan() {
        double value = -5;
        assertEquals(Math.atan(value), DoubleNumericPrimitives.atan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.atan(NULL_DOUBLE), 1e-10);
    }

    public void testCeil() {
        double value = -5;
        assertEquals(Math.ceil(value), DoubleNumericPrimitives.ceil(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.ceil(NULL_DOUBLE), 1e-10);
    }

    public void testCos() {
        double value = -5;
        assertEquals(Math.cos(value), DoubleNumericPrimitives.cos(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.cos(NULL_DOUBLE), 1e-10);
    }

    public void testExp() {
        double value = -5;
        assertEquals(Math.exp(value), DoubleNumericPrimitives.exp(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.exp(NULL_DOUBLE), 1e-10);
    }

    public void testFloor() {
        double value = -5;
        assertEquals(Math.floor(value), DoubleNumericPrimitives.floor(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.floor(NULL_DOUBLE), 1e-10);
    }

    public void testLog() {
        double value = -5;
        assertEquals(Math.log(value), DoubleNumericPrimitives.log(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.log(NULL_DOUBLE), 1e-10);
    }

    public void testPow() {
        double value0 = -5;
        double value1 = 2;
        assertEquals(Math.pow(value0, value1), DoubleNumericPrimitives.pow(value0, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.pow(NULL_DOUBLE, value1), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.pow(value0, NULL_DOUBLE), 1e-10);
    }

    public void testRint() {
        double value = -5;
        assertEquals(Math.rint(value), DoubleNumericPrimitives.rint(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.rint(NULL_DOUBLE), 1e-10);
    }

    public void testRound() {
        double value = -5;
        assertEquals(Math.round(value), DoubleNumericPrimitives.round(value), 1e-10);
        assertEquals(QueryConstants.NULL_LONG, DoubleNumericPrimitives.round(NULL_DOUBLE), 1e-10);
    }

    public void testSin() {
        double value = -5;
        assertEquals(Math.sin(value), DoubleNumericPrimitives.sin(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.sin(NULL_DOUBLE), 1e-10);
    }

    public void testSqrt() {
        double value = -5;
        assertEquals(Math.sqrt(value), DoubleNumericPrimitives.sqrt(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.sqrt(NULL_DOUBLE), 1e-10);
    }

    public void testTan() {
        double value = -5;
        assertEquals(Math.tan(value), DoubleNumericPrimitives.tan(value), 1e-10);
        assertEquals(QueryConstants.NULL_DOUBLE, DoubleNumericPrimitives.tan(NULL_DOUBLE), 1e-10);
    }

    public void testLowerBin() {
        double value = (double) 114;

        assertEquals((double) 110, DoubleNumericPrimitives.lowerBin(value, (double) 5));
        assertEquals((double) 110, DoubleNumericPrimitives.lowerBin(value, (double) 10));
        assertEquals((double) 100, DoubleNumericPrimitives.lowerBin(value, (double) 20));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.lowerBin(NULL_DOUBLE, (double) 5));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.lowerBin(value, NULL_DOUBLE));

        assertEquals(DoubleNumericPrimitives.lowerBin(value, (double) 5), DoubleNumericPrimitives.lowerBin(DoubleNumericPrimitives.lowerBin(value, (double) 5), (double) 5));
    }

    public void testLowerBinWithOffset() {
        double value = (double) 114;
        double offset = (double) 3;

        assertEquals((double) 113, DoubleNumericPrimitives.lowerBin(value, (double) 5, offset));
        assertEquals((double) 113, DoubleNumericPrimitives.lowerBin(value, (double) 10, offset));
        assertEquals((double) 103, DoubleNumericPrimitives.lowerBin(value, (double) 20, offset));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.lowerBin(NULL_DOUBLE, (double) 5, offset));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.lowerBin(value, NULL_DOUBLE, offset));

        assertEquals(DoubleNumericPrimitives.lowerBin(value, (double) 5, offset), DoubleNumericPrimitives.lowerBin(DoubleNumericPrimitives.lowerBin(value, (double) 5, offset), (double) 5, offset));
    }

    public void testUpperBin() {
        double value = (double) 114;

        assertEquals((double) 115, DoubleNumericPrimitives.upperBin(value, (double) 5));
        assertEquals((double) 120, DoubleNumericPrimitives.upperBin(value, (double) 10));
        assertEquals((double) 120, DoubleNumericPrimitives.upperBin(value, (double) 20));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.upperBin(NULL_DOUBLE, (double) 5));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.upperBin(value, NULL_DOUBLE));

        assertEquals(DoubleNumericPrimitives.upperBin(value, (double) 5), DoubleNumericPrimitives.upperBin(DoubleNumericPrimitives.upperBin(value, (double) 5), (double) 5));
    }

    public void testUpperBinWithOffset() {
        double value = (double) 114;
        double offset = (double) 3;

        assertEquals((double) 118, DoubleNumericPrimitives.upperBin(value, (double) 5, offset));
        assertEquals((double) 123, DoubleNumericPrimitives.upperBin(value, (double) 10, offset));
        assertEquals((double) 123, DoubleNumericPrimitives.upperBin(value, (double) 20, offset));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.upperBin(NULL_DOUBLE, (double) 5, offset));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.upperBin(value, NULL_DOUBLE, offset));

        assertEquals(DoubleNumericPrimitives.upperBin(value, (double) 5, offset), DoubleNumericPrimitives.upperBin(DoubleNumericPrimitives.upperBin(value, (double) 5, offset), (double) 5, offset));
    }

    public void testClamp() {
        assertEquals((double) 3, DoubleNumericPrimitives.clamp((double) 3, (double) -6, (double) 5));
        assertEquals((double) -6, DoubleNumericPrimitives.clamp((double) -7, (double) -6, (double) 5));
        assertEquals((double) 5, DoubleNumericPrimitives.clamp((double) 7, (double) -6, (double) 5));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.clamp(NULL_DOUBLE, (double) -6, (double) 5));
    }

    public void testBinSearchIndex() {
        assertEquals(NULL_INT, DoubleNumericPrimitives.binSearchIndex((double[]) null, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.binSearchIndex(new double[]{1,3,4}, (double)0, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.binSearchIndex(new double[]{1,3,4}, (double)1, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.binSearchIndex(new double[]{1,3,4}, (double)2, BinSearch.BS_ANY));
        assertEquals(1, DoubleNumericPrimitives.binSearchIndex(new double[]{1,3,4}, (double)3, BinSearch.BS_ANY));
        assertEquals(2, DoubleNumericPrimitives.binSearchIndex(new double[]{1,3,4}, (double)4, BinSearch.BS_ANY));
        assertEquals(2, DoubleNumericPrimitives.binSearchIndex(new double[]{1,3,4}, (double)5, BinSearch.BS_ANY));

        assertEquals(NULL_INT, DoubleNumericPrimitives.binSearchIndex((DoubleVector) null, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)0, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)1, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)2, BinSearch.BS_ANY));
        assertEquals(1, DoubleNumericPrimitives.binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)3, BinSearch.BS_ANY));
        assertEquals(2, DoubleNumericPrimitives.binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)4, BinSearch.BS_ANY));
        assertEquals(2, DoubleNumericPrimitives.binSearchIndex(new DoubleVectorDirect(new double[]{1,3,4}), (double)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(NULL_INT, DoubleNumericPrimitives.rawBinSearchIndex((DoubleVector)null, (double) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, DoubleNumericPrimitives.rawBinSearchIndex((DoubleVector)null, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, DoubleNumericPrimitives.rawBinSearchIndex((DoubleVector)null, (double) 0, BinSearch.BS_LOWEST));

        double[] empty = {};
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(empty), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(empty), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(empty), (double) 0, BinSearch.BS_LOWEST));

        double[] one = {11};
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 12, BinSearch.BS_ANY));
        assertEquals(-2, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 12, BinSearch.BS_LOWEST));

        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 11, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(one), (double) 11, BinSearch.BS_LOWEST));


        double[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        DoubleNumericPrimitives.rawBinSearchIndex((DoubleVector)null, (double) 0, null);

        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 26, BinSearch.BS_LOWEST));

        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 1, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 1, BinSearch.BS_LOWEST));

        assertEquals(2, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 2, BinSearch.BS_LOWEST));

        assertEquals(5, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 3, BinSearch.BS_LOWEST));

        assertEquals(9, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 4, BinSearch.BS_LOWEST));

        assertEquals(14, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 7, BinSearch.BS_ANY));
        assertEquals(-16, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 7, BinSearch.BS_LOWEST));

        assertEquals(19, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 10, BinSearch.BS_LOWEST));

        assertEquals(24, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 11, BinSearch.BS_LOWEST));

        assertEquals(25, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 15, BinSearch.BS_ANY));
        assertEquals(25, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 15, BinSearch.BS_LOWEST));

        assertEquals(29, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, DoubleNumericPrimitives.rawBinSearchIndex(new DoubleVectorDirect(v), (double) 25, BinSearch.BS_LOWEST));

        /////

        assertEquals(NULL_INT, DoubleNumericPrimitives.rawBinSearchIndex((double[]) null, (double) 0, BinSearch.BS_ANY));
        assertEquals(NULL_INT, DoubleNumericPrimitives.rawBinSearchIndex((double[])null, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(NULL_INT, DoubleNumericPrimitives.rawBinSearchIndex((double[])null, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(empty, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(empty, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(empty, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 12, BinSearch.BS_ANY));
        assertEquals(-2, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 12, BinSearch.BS_LOWEST));

        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 11, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(one, (double) 11, BinSearch.BS_LOWEST));


        DoubleNumericPrimitives.rawBinSearchIndex((double[])null, (double) 0, null);

        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 0, BinSearch.BS_ANY));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 26, BinSearch.BS_LOWEST));

        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 1, BinSearch.BS_ANY));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 1, BinSearch.BS_LOWEST));

        assertEquals(2, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 2, BinSearch.BS_LOWEST));

        assertEquals(5, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 3, BinSearch.BS_LOWEST));

        assertEquals(9, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 4, BinSearch.BS_LOWEST));

        assertEquals(14, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 7, BinSearch.BS_ANY));
        assertEquals(-16, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 7, BinSearch.BS_LOWEST));

        assertEquals(19, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 10, BinSearch.BS_LOWEST));

        assertEquals(24, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 11, BinSearch.BS_LOWEST));

        assertEquals(25, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 15, BinSearch.BS_ANY));
        assertEquals(25, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 15, BinSearch.BS_LOWEST));

        assertEquals(29, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, DoubleNumericPrimitives.rawBinSearchIndex(v, (double) 25, BinSearch.BS_LOWEST));
    }

    public void testSort() {
        final double[] doubles = new double[]{1, -5, -2, -2, 96, 0, 12, NULL_DOUBLE, NULL_DOUBLE};
        final DoubleVector sort = DoubleNumericPrimitives.sort(new DoubleVectorDirect(doubles));
        final DoubleVector expected = new DoubleVectorDirect(new double[]{NULL_DOUBLE, NULL_DOUBLE, -5, -2, -2, 0, 1, 12, 96});
        assertEquals(expected, sort);

        double[] sortedArray = DoubleNumericPrimitives.sort(doubles);
        assertEquals(new double[]{NULL_DOUBLE, NULL_DOUBLE, -5, -2, -2, 0, 1, 12, 96}, sortedArray);

        assertNull(DoubleNumericPrimitives.sort((DoubleVector)null));
        assertNull(DoubleNumericPrimitives.sort((double[])null));
        assertNull(DoubleNumericPrimitives.sort((Double[])null));
        assertEquals(new DoubleVectorDirect(), DoubleNumericPrimitives.sort(new DoubleVectorDirect()));
        assertEquals(new double[]{}, DoubleNumericPrimitives.sort(new double[]{}));
        assertEquals(new double[]{}, DoubleNumericPrimitives.sort(new Double[]{}));
    }

    public void testSortDescending() {
        final double[] doubles = new double[]{1, -5, -2, -2, 96, 0, 12, NULL_DOUBLE, NULL_DOUBLE};
        final DoubleVector sort = DoubleNumericPrimitives.sortDescending(new DoubleVectorDirect(doubles));
        final DoubleVector expected = new DoubleVectorDirect(new double[]{96, 12, 1, 0, -2, -2, -5, NULL_DOUBLE, NULL_DOUBLE});
        assertEquals(expected, sort);

        double[] sortedArray = DoubleNumericPrimitives.sortDescending(doubles);
        assertEquals(new double[]{96, 12, 1, 0, -2, -2, -5, NULL_DOUBLE, NULL_DOUBLE}, sortedArray);

        assertNull(DoubleNumericPrimitives.sortDescending((DoubleVector)null));
        assertNull(DoubleNumericPrimitives.sortDescending((double[])null));
        assertNull(DoubleNumericPrimitives.sortDescending((Double[])null));
        assertEquals(new DoubleVectorDirect(), DoubleNumericPrimitives.sortDescending(new DoubleVectorDirect()));
        assertEquals(new double[]{}, DoubleNumericPrimitives.sortDescending(new double[]{}));
        assertEquals(new double[]{}, DoubleNumericPrimitives.sortDescending(new Double[]{}));
    }

    public void testSortsExceptions() {
        DoubleVector doubleVector = null;
        DoubleVector sort = DoubleNumericPrimitives.sort(doubleVector);
        assertNull(sort);

        double[] doubles = null;
        double[] sortArray = DoubleNumericPrimitives.sort(doubles);
        assertNull(sortArray);

        doubles = new double[]{};
        sort = DoubleNumericPrimitives.sort(new DoubleVectorDirect(doubles));
        assertEquals(new DoubleVectorDirect(), sort);

        sortArray = DoubleNumericPrimitives.sort(doubles);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSortDescendingExceptions() {
        DoubleVector doubleVector = null;
        DoubleVector sort = DoubleNumericPrimitives.sortDescending(doubleVector);
        assertNull(sort);

        double[] doubles = null;
        double[] sortArray = DoubleNumericPrimitives.sortDescending(doubles);
        assertNull(sortArray);

        doubles = new double[]{};
        sort = DoubleNumericPrimitives.sortDescending(new DoubleVectorDirect(doubles));
        assertEquals(new DoubleVectorDirect(), sort);

        sortArray = DoubleNumericPrimitives.sortDescending(doubles);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void testSequence(){
        assertEquals(new double[]{0,1,2,3,4,5}, DoubleNumericPrimitives.sequence((double)0, (double)5, (double)1));
        assertEquals(new double[]{-5,-4,-3,-2,-1,0}, DoubleNumericPrimitives.sequence((double)-5, (double)0, (double)1));

        assertEquals(new double[]{0,2,4}, DoubleNumericPrimitives.sequence((double)0, (double)5, (double)2));
        assertEquals(new double[]{-5,-3,-1}, DoubleNumericPrimitives.sequence((double)-5, (double)0, (double)2));

        assertEquals(new double[]{5,3,1}, DoubleNumericPrimitives.sequence((double)5, (double)0, (double)-2));
        assertEquals(new double[]{0,-2,-4}, DoubleNumericPrimitives.sequence((double)0, (double)-5, (double)-2));

        assertEquals(new double[]{}, DoubleNumericPrimitives.sequence((double)0, (double)5, (double)0));
        assertEquals(new double[]{}, DoubleNumericPrimitives.sequence((double)5, (double)0, (double)1));
    }

    public void testMedian() {
        assertEquals(3.0, DoubleNumericPrimitives.median(new double[]{4,2,3}));
        assertEquals(3.5, DoubleNumericPrimitives.median(new double[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.median((double[])null));

        assertEquals(3.0, DoubleNumericPrimitives.median(new Double[]{(double)4,(double)2,(double)3}));
        assertEquals(3.5, DoubleNumericPrimitives.median(new Double[]{(double)5,(double)4,(double)2,(double)3}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.median((Double[])null));

        assertEquals(3.0, DoubleNumericPrimitives.median(new DoubleVectorDirect(new double[]{4,2,3})));
        assertEquals(3.5, DoubleNumericPrimitives.median(new DoubleVectorDirect(new double[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.median((DoubleVector) null));
    }

    public void testPercentile() {
        assertEquals(2.0, DoubleNumericPrimitives.percentile(new double[]{4,2,3},0.00));
        assertEquals(3.0, DoubleNumericPrimitives.percentile(new double[]{4,2,3},0.50));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.percentile((double[])null, 0.25));

        assertEquals(2.0, DoubleNumericPrimitives.percentile(new DoubleVectorDirect(new double[]{4,2,3}),0.00));
        assertEquals(3.0, DoubleNumericPrimitives.percentile(new DoubleVectorDirect(new double[]{4,2,3}),0.50));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.percentile((DoubleVector) null, 0.25));
    }

    public void testWsum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wsum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }

    public void testWeightedSum() {
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedSum(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }

    public void testWAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wavg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }

    public void testWeightedAvg() {
        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.weightedAvg(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }

    public void testWvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = sum2/w - sum * sum / w / w;

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wvar(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }

    public void testWstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt(sum2/w - sum * sum / w / w);

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wstd(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }

    public void testWste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt(sum2/w - sum * sum / w / w);
        final double target = std * Math.sqrt( w2 / w / w);

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wste(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wste(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }

    public void testWtstat() {
        final double target = DoubleNumericPrimitives.wavg(new double[]{1,2,3}, new double[]{4,5,6}) / DoubleNumericPrimitives.wste(new double[]{1,2,3}, new double[]{4,5,6});

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new short[]{4,5,6,7,NULL_SHORT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector) null, new short[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (short[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new int[]{4,5,6,7,NULL_INT}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector)null, new int[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (int[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new long[]{4,5,6,7,NULL_LONG}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector)null, new long[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (long[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new double[]{4,5,6,7,NULL_DOUBLE}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector)null, new double[]{4,5,6}));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (double[])null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new double[]{1,2,3,NULL_DOUBLE,5}, new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((double[])null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new double[]{1,2,3}, (DoubleVector)null));

        /////

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new ShortVectorDirect(new short[]{4,5,6,7,NULL_SHORT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector) null, new ShortVectorDirect(new short[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (ShortVector) null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new IntVectorDirect(new int[]{4,5,6,7,NULL_INT})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector) null, new IntVectorDirect(new int[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (IntVector) null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new LongVectorDirect(new long[]{4,5,6,7,NULL_LONG})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector) null, new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (LongVector) null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));

        assertEquals(target, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3,NULL_DOUBLE,5}), new DoubleVectorDirect(new double[]{4,5,6,7,NULL_DOUBLE})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat((DoubleVector) null, new DoubleVectorDirect(new double[]{4,5,6})));
        assertEquals(NULL_DOUBLE, DoubleNumericPrimitives.wtstat(new DoubleVectorDirect(new double[]{1,2,3}), (DoubleVector)null));
    }
}
