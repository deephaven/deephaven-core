/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Basic.count;
import static io.deephaven.function.Numeric.*;

/**
 * Test Numeric.
 */
@SuppressWarnings({"RedundantCast", "RedundantArrayCreation", "PointlessArithmeticExpression", "ConstantConditions", "SimplifiableAssertion", "Convert2Diamond"})
public class TestNumeric extends BaseArrayTestCase {

    //////////////////////////// Object ////////////////////////////

    public void testObjMin() {
        assertEquals(BigInteger.valueOf(1), minObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)}));
        assertEquals(BigInteger.valueOf(-2), minObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1)}));
        assertEquals(null, minObj(new BigInteger[0]));

        assertEquals(BigInteger.valueOf(1), minObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2))));
        assertEquals(BigInteger.valueOf(-2), minObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1))));
        assertEquals(null, minObj(new ObjectVectorDirect<BigInteger>()));

        // check that functions can be resolved with varargs
        assertEquals(BigInteger.valueOf(1), minObj(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)));
    }

    public void testObjMax() {
        assertEquals(BigInteger.valueOf(3), maxObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)}));
        assertEquals(BigInteger.valueOf(10), maxObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1)}));
        assertEquals(null, maxObj(new BigInteger[0]));

        assertEquals(BigInteger.valueOf(3), maxObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2))));
        assertEquals(BigInteger.valueOf(10), maxObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1))));
        assertEquals(null, maxObj(new ObjectVectorDirect<BigInteger>()));

        // check that functions can be resolved with varargs
        assertEquals(BigInteger.valueOf(3), maxObj(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)));
    }

    public void testObjIndexOfMin() {
        assertEquals(1, indexOfMinObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)}));
        assertEquals(2, indexOfMinObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1)}));
        assertEquals(NULL_LONG, indexOfMinObj(new BigInteger[0]));
        assertEquals(NULL_LONG, indexOfMinObj((BigInteger[])null));

        assertEquals(1, indexOfMinObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2))));
        assertEquals(2, indexOfMinObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1))));
        assertEquals(NULL_LONG, indexOfMinObj(new ObjectVectorDirect<BigInteger>()));
        assertEquals(NULL_LONG, indexOfMinObj((ObjectVector<BigInteger>)null));

        // check that functions can be resolved with varargs
        assertEquals(1, indexOfMinObj(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)));
    }

    public void testObjIndexOfMax() {
        assertEquals(0, indexOfMaxObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)}));
        assertEquals(1, indexOfMaxObj(new BigInteger[]{BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1)}));
        assertEquals(NULL_LONG, indexOfMaxObj(new BigInteger[0]));
        assertEquals(NULL_LONG, indexOfMaxObj((BigInteger[])null));

        assertEquals(0, indexOfMaxObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2))));
        assertEquals(1, indexOfMaxObj(new ObjectVectorDirect<BigInteger>(BigInteger.valueOf(3), BigInteger.valueOf(10), BigInteger.valueOf(-2), BigInteger.valueOf(1))));
        assertEquals(NULL_LONG, indexOfMaxObj(new ObjectVectorDirect<BigInteger>()));
        assertEquals(NULL_LONG, indexOfMaxObj((ObjectVector<BigInteger>)null));

        // check that functions can be resolved with varargs
        assertEquals(0, indexOfMaxObj(BigInteger.valueOf(3), BigInteger.valueOf(1), BigInteger.valueOf(2)));
    }


    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    public void test${pt.boxed}Signum() {
        assertEquals(1, signum((${pt.primitive}) 5));
        assertEquals( 0, signum((${pt.primitive}) 0));
        assertEquals( -1, signum((${pt.primitive}) -5));
        assertEquals(NULL_INT, signum(${pt.null}));
    }

    public void test${pt.boxed}Avg() {
        assertEquals(50.0, avg(new ${pt.primitive}[]{40, 50, 60}));
        assertEquals(45.5, avg(new ${pt.primitive}[]{40, 51}));
        assertTrue(Double.isNaN(avg(new ${pt.primitive}[]{})));
        assertTrue(Double.isNaN(avg(new ${pt.primitive}[]{${pt.null}})));
        assertEquals(10.0, avg(new ${pt.primitive}[]{5, ${pt.null}, 15}));
        assertEquals(NULL_DOUBLE, avg((${pt.primitive}[])null));

        assertEquals(50.0, avg(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive})50, (${pt.primitive})60}));
        assertEquals(45.5, avg(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive})51}));
        assertTrue(Double.isNaN(avg(new ${pt.boxed}[]{})));
        assertTrue(Double.isNaN(avg(new ${pt.boxed}[]{${pt.null}})));
        assertEquals(10.0, avg(new ${pt.boxed}[]{(${pt.primitive})5, ${pt.null}, (${pt.primitive})15}));
        assertEquals(NULL_DOUBLE, avg((${pt.boxed}[])null));

        assertEquals(50.0, avg(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, 50, 60})));
        assertEquals(45.5, avg(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, 51})));
        assertTrue(Double.isNaN(avg(new ${pt.vectorDirect}())));
        assertTrue(Double.isNaN(avg(new ${pt.vectorDirect}(${pt.null}))));
        assertEquals(10.0, avg(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15})));
        assertEquals(NULL_DOUBLE, avg((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(45.0, avg((${pt.primitive})40, (${pt.primitive})50));
    }

    public void test${pt.boxed}AbsAvg() {
        assertEquals(50.0, absAvg(new ${pt.primitive}[]{40, (${pt.primitive}) 50, 60}));
        assertEquals(45.5, absAvg(new ${pt.primitive}[]{(${pt.primitive}) 40, 51}));
        assertTrue(Double.isNaN(absAvg(new ${pt.primitive}[]{})));
        assertTrue(Double.isNaN(absAvg(new ${pt.primitive}[]{${pt.null}})));
        assertEquals(10.0, absAvg(new ${pt.primitive}[]{(${pt.primitive}) 5, ${pt.null}, (${pt.primitive}) 15}));
        assertEquals(NULL_DOUBLE, absAvg((${pt.primitive}[])null));

        assertEquals(50.0, absAvg(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive}) 50, (${pt.primitive})60}));
        assertEquals(45.5, absAvg(new ${pt.boxed}[]{(${pt.primitive}) 40, (${pt.primitive})51}));
        assertTrue(Double.isNaN(absAvg(new ${pt.boxed}[]{})));
        assertTrue(Double.isNaN(absAvg(new ${pt.boxed}[]{${pt.null}})));
        assertEquals(10.0, absAvg(new ${pt.boxed}[]{(${pt.primitive}) 5, ${pt.null}, (${pt.primitive}) 15}));
        assertEquals(NULL_DOUBLE, absAvg((${pt.boxed}[])null));

        assertEquals(50.0, absAvg(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, (${pt.primitive}) 50, 60})));
        assertEquals(45.5, absAvg(new ${pt.vectorDirect}(new ${pt.primitive}[]{(${pt.primitive}) 40, 51})));
        assertTrue(Double.isNaN(absAvg(new ${pt.vectorDirect}())));
        assertTrue(Double.isNaN(absAvg(new ${pt.vectorDirect}(${pt.null}))));
        assertEquals(10.0, absAvg(new ${pt.vectorDirect}((${pt.primitive}) 5, ${pt.null}, (${pt.primitive}) 15)));
        assertEquals(NULL_DOUBLE, absAvg((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(45.0, absAvg((${pt.primitive})40, (${pt.primitive})50));
    }

    public void test${pt.boxed}CountPos() {
        assertEquals(4, countPos(new ${pt.primitive}[]{40, 50, 60, (${pt.primitive}) 1, 0}));
        assertEquals(0, countPos(new ${pt.primitive}[]{}));
        assertEquals(0, countPos(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(3, countPos(new ${pt.primitive}[]{5, ${pt.null}, 15, (${pt.primitive}) 1, 0}));
        assertEquals(NULL_LONG, countPos((${pt.primitive}[])null));

        assertEquals(4, countPos(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) 1, (${pt.primitive})0}));
        assertEquals(0, countPos(new ${pt.boxed}[]{}));
        assertEquals(0, countPos(new ${pt.boxed}[]{${pt.null}}));
        assertEquals(3, countPos(new ${pt.boxed}[]{(${pt.primitive})5, ${pt.null}, (${pt.primitive})15, (${pt.primitive}) 1, (${pt.primitive})0}));
        assertEquals(NULL_LONG, countPos((${pt.primitive}[])null));

        assertEquals(4, countPos(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, 50, 60, (${pt.primitive}) 1, 0})));
        assertEquals(0, countPos(new ${pt.vectorDirect}()));
        assertEquals(0, countPos(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(3, countPos(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15, (${pt.primitive}) 1, 0})));
        assertEquals(NULL_LONG, countPos((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(4, countPos((${pt.primitive})40, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive})1, (${pt.primitive})0));
    }

    public void test${pt.boxed}CountNeg() {
        assertEquals(2, countNeg(new ${pt.primitive}[]{40, (${pt.primitive}) -50, 60, (${pt.primitive}) -1, 0}));
        assertEquals(0, countNeg(new ${pt.primitive}[]{}));
        assertEquals(0, countNeg(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(1, countNeg(new ${pt.primitive}[]{5, ${pt.null}, 15, (${pt.primitive}) -1, 0}));
        assertEquals(NULL_LONG, countNeg((${pt.primitive}[])null));

        assertEquals(2, countNeg(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive}) -50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0}));
        assertEquals(0, countNeg(new ${pt.boxed}[]{}));
        assertEquals(0, countNeg(new ${pt.boxed}[]{${pt.null}}));
        assertEquals(1, countNeg(new ${pt.boxed}[]{(${pt.primitive})5, ${pt.null}, (${pt.primitive})15, (${pt.primitive}) -1, (${pt.primitive})0}));
        assertEquals(NULL_LONG, countNeg((${pt.boxed}[])null));

        assertEquals(2, countNeg(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, (${pt.primitive}) -50, 60, (${pt.primitive}) -1, 0})));
        assertEquals(0, countNeg(new ${pt.vectorDirect}()));
        assertEquals(0, countNeg(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(1, countNeg(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15, (${pt.primitive}) -1, 0})));
        assertEquals(NULL_LONG, countNeg((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(2, countNeg((${pt.primitive})40, (${pt.primitive})-50, (${pt.primitive})60, (${pt.primitive})-1, (${pt.primitive})0));
    }

    public void test${pt.boxed}CountZero() {
        assertEquals(2, countZero(new ${pt.primitive}[]{0, 40, 50, 60, (${pt.primitive}) -1, 0}));
        assertEquals(0, countZero(new ${pt.primitive}[]{}));
        assertEquals(0, countZero(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(2, countZero(new ${pt.primitive}[]{0, 5, ${pt.null}, 0, (${pt.primitive}) -15}));
        assertEquals(NULL_LONG, countZero((${pt.primitive}[])null));

        assertEquals(2, countZero(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})40, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0}));
        assertEquals(0, countZero(new ${pt.boxed}[]{}));
        assertEquals(0, countZero(new ${pt.boxed}[]{${pt.null}}));
        assertEquals(2, countZero(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})5, ${pt.null}, (${pt.primitive})0, (${pt.primitive}) -15}));
        assertEquals(NULL_LONG, countZero((${pt.boxed}[])null));

        assertEquals(2, countZero(new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 40, 50, 60, (${pt.primitive}) -1, 0})));
        assertEquals(0, countZero(new ${pt.vectorDirect}()));
        assertEquals(0, countZero(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(2, countZero(new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 5, ${pt.null}, 0, (${pt.primitive}) -15})));
        assertEquals(NULL_LONG, countZero((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(2, countZero((${pt.primitive})0, (${pt.primitive})40, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive})-1, (${pt.primitive})0));
    }

    public void test${pt.boxed}Max() {
        assertEquals((${pt.primitive}) 60, max(new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 50, 60, (${pt.primitive}) 1, 0})));
        assertEquals((${pt.primitive}) 60, max(new ${pt.vectorDirect}((${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1)));
        assertEquals(${pt.null}, max(new ${pt.vectorDirect}()));
        assertEquals(${pt.null}, max(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(${pt.null}, max((${pt.vector}) null));

        assertEquals((${pt.primitive}) 60, max((${pt.primitive}) 0, (${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1, (${pt.primitive}) 0));
        assertEquals((${pt.primitive}) 60, max((${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1));
        assertEquals((${pt.primitive}) 60, max(new ${pt.boxed}[]{(${pt.primitive}) 0, (${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1, (${pt.primitive}) 0}));
        assertEquals((${pt.primitive}) 60, max(new ${pt.boxed}[]{(${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1}));
        assertEquals(${pt.null}, max(new ${pt.primitive}[0]));
        assertEquals(${pt.null}, max(new ${pt.boxed}[0]));
        assertEquals(${pt.null}, max(${pt.null}));
        assertEquals(${pt.null}, max((${pt.primitive}[]) null));
        assertEquals(${pt.null}, max((${pt.boxed}[]) null));
    }

    public void test${pt.boxed}Min() {
        assertEquals((${pt.primitive}) 0, min(new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 50, 60, (${pt.primitive}) 1, 0})));
        assertEquals((${pt.primitive}) -1, min(new ${pt.vectorDirect}((${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1)));
        assertEquals(${pt.null}, min(new ${pt.vectorDirect}()));
        assertEquals(${pt.null}, min(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(${pt.null}, min((${pt.vector}) null));

        assertEquals((${pt.primitive}) 0, min((${pt.primitive}) 0, (${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1, (${pt.primitive}) 0));
        assertEquals((${pt.primitive}) -1, min((${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1));
        assertEquals((${pt.primitive}) 0, min(new ${pt.boxed}[]{(${pt.primitive}) 0, (${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1, (${pt.primitive}) 0}));
        assertEquals((${pt.primitive}) -1, min(new ${pt.boxed}[]{(${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1}));
        assertEquals(${pt.null}, min(new ${pt.primitive}[0]));
        assertEquals(${pt.null}, min(new ${pt.boxed}[0]));
        assertEquals(${pt.null}, min(${pt.null}));
        assertEquals(${pt.null}, min((${pt.primitive}[]) null));
        assertEquals(${pt.null}, min((${pt.boxed}[]) null));
    }

    public void test${pt.boxed}IndexOfMax() {
        assertEquals(4, indexOfMax(new ${pt.primitive}[]{0, 40, ${pt.null}, 50, 60, (${pt.primitive}) 1, 0}));
        assertEquals(3, indexOfMax(new ${pt.primitive}[]{(${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1}));
        assertEquals(NULL_LONG, indexOfMax(new ${pt.primitive}[]{}));
        assertEquals(NULL_LONG, indexOfMax(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(NULL_LONG, indexOfMax((${pt.primitive}[])null));

        assertEquals(4, indexOfMax(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) 1, (${pt.primitive})0}));
        assertEquals(3, indexOfMax(new ${pt.boxed}[]{(${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1}));
        assertEquals(NULL_LONG, indexOfMax(new ${pt.boxed}[]{}));
        assertEquals(NULL_LONG, indexOfMax(new ${pt.boxed}[]{${pt.null}}));
        assertEquals(NULL_LONG, indexOfMax((${pt.boxed}[])null));

        assertEquals(4, indexOfMax(new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 50, 60, (${pt.primitive}) 1, 0})));
        assertEquals(3, indexOfMax(new ${pt.vectorDirect}((${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) 1)));
        assertEquals(NULL_LONG, indexOfMax(new ${pt.vectorDirect}()));
        assertEquals(NULL_LONG, indexOfMax(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(NULL_LONG, indexOfMax((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(4, indexOfMax((${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0));
    }

    public void test${pt.boxed}IndexOfMin() {
        assertEquals(1, indexOfMin(new ${pt.primitive}[]{40, 0, ${pt.null}, 50, 60, (${pt.primitive}) 1, 0}));
        assertEquals(4, indexOfMin(new ${pt.primitive}[]{(${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1}));
        assertEquals(NULL_LONG, indexOfMin(new ${pt.primitive}[]{}));
        assertEquals(NULL_LONG, indexOfMin(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(NULL_LONG, indexOfMin((${pt.primitive}[])null));

        assertEquals(1, indexOfMin(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive})0, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) 1, (${pt.primitive})0}));
        assertEquals(4, indexOfMin(new ${pt.boxed}[]{(${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1}));
        assertEquals(NULL_LONG, indexOfMin(new ${pt.boxed}[]{}));
        assertEquals(NULL_LONG, indexOfMin(new ${pt.boxed}[]{${pt.null}}));
        assertEquals(NULL_LONG, indexOfMin((${pt.boxed}[])null));

        assertEquals(1, indexOfMin(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, 0, ${pt.null}, 50, 60, (${pt.primitive}) 1, 0})));
        assertEquals(4, indexOfMin(new ${pt.vectorDirect}((${pt.primitive}) 40, ${pt.null}, (${pt.primitive}) 50, (${pt.primitive}) 60, (${pt.primitive}) -1)));
        assertEquals(NULL_LONG, indexOfMin(new ${pt.vectorDirect}()));
        assertEquals(NULL_LONG, indexOfMin(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(NULL_LONG, indexOfMin((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(1, indexOfMin((${pt.primitive})40, (${pt.primitive})0, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) 1, (${pt.primitive})0));
    }


    public void test${pt.boxed}Var() {
        ${pt.primitive}[] v = {0, 40, ${pt.null}, 50, 60, (${pt.primitive}) -1, 0};
        ${pt.boxed}[] V = {(${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0};
        double count = 6;
        double sum = v[0] + v[1] + v[3] + v[4] + v[5] + v[6];
        double sumsq = v[0] * v[0] + v[1] * v[1] + v[3] * v[3] + v[4] * v[4] + v[5] * v[5] + v[6] * v[6];
        double var = sumsq / (count - 1) - sum * sum / count / (count - 1);

        assertEquals(var, var(v));
        assertEquals(NULL_DOUBLE, var((${pt.primitive}[])null));

        assertEquals(var, var(V));
        assertEquals(NULL_DOUBLE, var((${pt.boxed}[])null));

        assertEquals(var, var(new ${pt.vectorDirect}(v)));
        assertEquals(NULL_DOUBLE, var((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(var, var((${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0));
    }

    public void test${pt.boxed}Std() {
        ${pt.primitive}[] v = {0, 40, ${pt.null}, 50, 60, (${pt.primitive}) -1, 0};
        ${pt.boxed}[] V = {(${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0};

        assertEquals(Math.sqrt(var(new ${pt.vectorDirect}(v))), std(v));
        assertEquals(NULL_DOUBLE, std((${pt.primitive}[])null));

        assertEquals(Math.sqrt(var(new ${pt.vectorDirect}(v))), std(V));
        assertEquals(NULL_DOUBLE, std((${pt.boxed}[])null));

        assertEquals(Math.sqrt(var(new ${pt.vectorDirect}(v))), std(new ${pt.vectorDirect}(v)));
        assertEquals(NULL_DOUBLE, std((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(std(v), std((${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0));
    }

    public void test${pt.boxed}Ste() {
        ${pt.primitive}[] v = {0, 40, ${pt.null}, 50, 60, (${pt.primitive}) -1, 0};
        ${pt.boxed}[] V = {(${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0};

        assertEquals(std(new ${pt.vectorDirect}(v)) / Math.sqrt(count(new ${pt.vectorDirect}(v))), ste(v));
        assertEquals(NULL_DOUBLE, ste((${pt.primitive}[])null));

        assertEquals(std(new ${pt.vectorDirect}(v)) / Math.sqrt(count(new ${pt.vectorDirect}(v))), ste(V));
        assertEquals(NULL_DOUBLE, ste((${pt.boxed}[])null));

        assertEquals(std(new ${pt.vectorDirect}(v)) / Math.sqrt(count(new ${pt.vectorDirect}(v))), ste(new ${pt.vectorDirect}(v)));
        assertEquals(NULL_DOUBLE, ste((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(ste(v), ste((${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0));
    }

    public void test${pt.boxed}Tstat() {
        ${pt.primitive}[] v = {0, 40, ${pt.null}, 50, 60, (${pt.primitive}) -1, 0};
        ${pt.boxed}[] V = {(${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0};

        assertEquals(avg(new ${pt.vectorDirect}(v)) / ste(new ${pt.vectorDirect}(v)), tstat(v));
        assertEquals(NULL_DOUBLE, tstat((${pt.primitive}[])null));

        assertEquals(avg(new ${pt.vectorDirect}(v)) / ste(new ${pt.vectorDirect}(v)), tstat(V));
        assertEquals(NULL_DOUBLE, tstat((${pt.boxed}[])null));

        assertEquals(avg(new ${pt.vectorDirect}(v)) / ste(new ${pt.vectorDirect}(v)), tstat(new ${pt.vectorDirect}(v)));
        assertEquals(NULL_DOUBLE, tstat((${pt.vectorDirect})null));

        // check that functions can be resolved with varargs
        assertEquals(tstat(v), tstat((${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0));
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    public void test${pt.boxed}${pt2.boxed}Cov() {

        ${pt.primitive}[] a = {10, 40, ${pt.null}, 50, ${pt.null}, (${pt.primitive}) -1, 0, (${pt.primitive}) -7};
        ${pt2.primitive}[] b = {0, (${pt2.primitive}) -40, ${pt2.null}, ${pt2.null}, 6, (${pt2.primitive}) -1, 11, 3};
        double count = 5;
        double sumA = a[0] + a[1] + a[5] + a[6] + a[7];
        double sumB = b[0] + b[1] + b[5] + b[6] + b[7];
        double sumAB = a[0] * b[0] + a[1] * b[1] + a[5] * b[5] + a[6] * b[6] + a[7] * b[7];
        double cov = sumAB / count - sumA * sumB / count / count;

        assertEquals(cov, cov(a, b));
        assertEquals(NULL_DOUBLE, cov(a, (${pt2.primitive}[])null));
        assertEquals(NULL_DOUBLE, cov((${pt.primitive}[])null, b));
        assertEquals(NULL_DOUBLE, cov((${pt.primitive}[])null, (${pt2.primitive}[]) null));

        assertEquals(cov, cov(a, new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cov(a, (${pt2.vectorDirect})null));
        assertEquals(NULL_DOUBLE, cov((${pt.primitive}[])null, new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cov((${pt.primitive}[])null, (${pt2.vectorDirect})null));

        assertEquals(cov, cov(new ${pt.vectorDirect}(a), b));
        assertEquals(NULL_DOUBLE, cov(new ${pt.vectorDirect}(a), (${pt2.primitive}[])null));
        assertEquals(NULL_DOUBLE, cov((${pt.vectorDirect})null, b));
        assertEquals(NULL_DOUBLE, cov((${pt.vectorDirect})null, (${pt2.primitive}[])null));

        assertEquals(cov, cov(new ${pt.vectorDirect}(a), new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cov(new ${pt.vectorDirect}(a), (${pt2.vectorDirect})null));
        assertEquals(NULL_DOUBLE, cov((${pt.vectorDirect})null, new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cov((${pt.vectorDirect})null, (${pt2.vectorDirect})null));


        try {
            cov(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }
    }

    public void test${pt.boxed}${pt2.boxed}Cor() {
        ${pt.primitive}[] a = {10, 40, ${pt.null}, 50, ${pt.null}, (${pt.primitive}) -1, 0, (${pt.primitive}) -7};
        ${pt2.primitive}[] b = {0, (${pt2.primitive}) -40, ${pt2.null}, ${pt2.null}, 6, (${pt2.primitive}) -1, 11, 3};
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
        assertEquals(NULL_DOUBLE, cor(a, (${pt2.primitive}[])null));
        assertEquals(NULL_DOUBLE, cor((${pt.primitive}[])null, b));
        assertEquals(NULL_DOUBLE, cor((${pt.primitive}[])null, (${pt2.primitive}[])null));

        assertEquals(cor, cor(a, new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cor(a, (${pt2.vectorDirect})null));
        assertEquals(NULL_DOUBLE, cor((${pt.primitive}[])null, new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cor((${pt.primitive}[])null, (${pt2.vectorDirect})null));

        assertEquals(cor, cor(new ${pt.vectorDirect}(a), b));
        assertEquals(NULL_DOUBLE, cor(new ${pt.vectorDirect}(a), (${pt2.primitive}[])null));
        assertEquals(NULL_DOUBLE, cor((${pt.vectorDirect})null, b));
        assertEquals(NULL_DOUBLE, cor((${pt.vectorDirect})null, (${pt2.primitive}[])null));

        assertEquals(cor, cor(new ${pt.vectorDirect}(a), new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cor(new ${pt.vectorDirect}(a), (${pt2.vectorDirect})null));
        assertEquals(NULL_DOUBLE, cor((${pt.vectorDirect})null, new ${pt2.vectorDirect}(b)));
        assertEquals(NULL_DOUBLE, cor((${pt.vectorDirect})null, (${pt2.vectorDirect})null));

        try {
            cor(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }
    }


    </#if>
    </#list>


    public void test${pt.boxed}Sum1() {
        assertTrue(Math.abs(15 - sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, 5, 6}))) == 0.0);
        assertTrue(Math.abs(0 - sum(new ${pt.vectorDirect}())) == 0.0);
        assertTrue(Math.abs(0 - sum(new ${pt.vectorDirect}(${pt.null}))) == 0.0);
        assertTrue(Math.abs(20 - sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15}))) == 0.0);
        assertEquals(${pt.null}, sum((${pt.vector}) null));
    }

    public void test${pt.boxed}Sum2() {
        assertTrue(Math.abs(15 - sum(new ${pt.primitive}[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(0 - sum(new ${pt.primitive}[]{})) == 0.0);
        assertTrue(Math.abs(0 - sum(new ${pt.primitive}[]{${pt.null}})) == 0.0);
        assertTrue(Math.abs(20 - sum(new ${pt.primitive}[]{5, ${pt.null}, 15})) == 0.0);
        assertEquals(${pt.null}, sum((${pt.primitive}[]) null));
    }

//    public void test${pt.boxed}SumObjectVector() {
//        assertEquals(new ${pt.primitive}[]{4, 15}, sum(new ObjectVectorDirect<>(new ${pt.primitive}[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new ${pt.primitive}[]{4, ${pt.null}}, sum(new ObjectVectorDirect<>(new ${pt.primitive}[][]{{5, ${pt.null}}, {-3, 5}, {2, 6}})));
//        assertEquals(null, sum((ObjectVector<${pt.primitive}[]>) null));
//
//        try {
//            sum(new ObjectVectorDirect<>(new ${pt.primitive}[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void test${pt.boxed}SumArray() {
//        assertEquals(new ${pt.primitive}[]{4, 15}, sum(new ${pt.primitive}[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new ${pt.primitive}[]{4, ${pt.null}}, sum(new ${pt.primitive}[][]{{5, ${pt.null}}, {-3, 5}, {2, 6}}));
//        assertEquals(null, sum((${pt.primitive}[][]) null));
//
//        try {
//            sum(new ${pt.primitive}[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void test${pt.boxed}Product() {
        assertTrue(Math.abs(120 - product(new ${pt.primitive}[]{4, 5, 6})) == 0.0);
        assertEquals(${pt.null}, product(new ${pt.primitive}[]{}));
        assertEquals(${pt.null}, product(new ${pt.primitive}[]{${pt.null}}));
        assertTrue(Math.abs(75 - product(new ${pt.primitive}[]{5, ${pt.null}, 15})) == 0.0);
        assertEquals(${pt.null}, product((${pt.primitive}[]) null));

        assertTrue(Math.abs(120 - product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, 5, 6}))) == 0.0);
        assertEquals(${pt.null}, product(new ${pt.vectorDirect}()));
        assertEquals(${pt.null}, product(new ${pt.vectorDirect}(${pt.null})));
        assertTrue(Math.abs(75 - product(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15}))) == 0.0);
        assertEquals(${pt.null}, product((${pt.vector}) null));
    }

//    public void test${pt.boxed}ProdObjectVector() {
//        assertEquals(new ${pt.primitive}[]{-30, 120}, product(new ObjectVectorDirect<>(new ${pt.primitive}[][]{{5, 4}, {-3, 5}, {2, 6}})));
//        assertEquals(new ${pt.primitive}[]{-30, ${pt.null}}, product(new ObjectVectorDirect<>(new ${pt.primitive}[][]{{5, ${pt.null}}, {-3, 5}, {2, 6}})));
//        assertEquals(null, product((ObjectVector<${pt.primitive}[]>) null));
//
//        try {
//            product(new ObjectVectorDirect<>(new ${pt.primitive}[][]{{5}, {-3, 5}, {2, 6}}));
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }
//
//    public void test${pt.boxed}ProdArray() {
//        assertEquals(new ${pt.primitive}[]{-30, 120}, product(new ${pt.primitive}[][]{{5, 4}, {-3, 5}, {2, 6}}));
//        assertEquals(new ${pt.primitive}[]{-30, ${pt.null}}, product(new ${pt.primitive}[][]{{5, ${pt.null}}, {-3, 5}, {2, 6}}));
//        assertEquals(null, product((${pt.primitive}[][]) null));
//
//        try {
//            product(new ${pt.primitive}[][]{{5}, {-3, 5}, {2, 6}});
//            fail("Should have failed on different length arrays");
//        } catch (RequirementFailure e) {
//            //pass
//        }
//    }

    public void test${pt.boxed}CumMinArray() {
        assertEquals(new ${pt.primitive}[]{1, 1, 1, 1, 1}, cummin(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[]{5, 4, 3, 2, 1}, cummin(new ${pt.primitive}[]{5, 4, 3, 2, 1}));
        assertEquals(new ${pt.primitive}[]{1, 1, 1, 1, 1}, cummin(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new ${pt.primitive}[]{5, 4, 3, 3, 1}, cummin(new ${pt.primitive}[]{5, 4, 3, ${pt.null}, 1}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 2, 2, 2}, cummin(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 4, 3, 2, 1}, cummin(new ${pt.primitive}[]{${pt.null}, 4, 3, 2, 1}));
        assertEquals(new ${pt.primitive}[0], cummin(new ${pt.primitive}[0]));
        assertEquals(new ${pt.primitive}[0], cummin(new ${pt.boxed}[0]));
        assertEquals(null, cummin((${pt.primitive}[]) null));

        assertEquals(new ${pt.primitive}[]{1, 1, 1, 1, 1}, cummin(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[]{5, 4, 3, 2, 1}, cummin(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, 4, 3, 2, 1})));
        assertEquals(new ${pt.primitive}[]{1, 1, 1, 1, 1}, cummin(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new ${pt.primitive}[]{5, 4, 3, 3, 1}, cummin(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, 4, 3, ${pt.null}, 1})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 2, 2, 2}, cummin(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 4, 3, 2, 1}, cummin(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 4, 3, 2, 1})));
        assertEquals(new ${pt.primitive}[0], cummin(new ${pt.vectorDirect}(new ${pt.primitive}[0])));
        assertEquals(null, cummin((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(new ${pt.primitive}[]{1, 1, 1, 1, 1}, cummin((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));
        assertEquals(new ${pt.primitive}[]{5, 4, 3, 2, 1}, cummin((${pt.primitive})5, (${pt.primitive})4, (${pt.primitive})3, (${pt.primitive})2, (${pt.primitive})1));
    }

    public void test${pt.boxed}CumMaxArray() {
        assertEquals(new ${pt.primitive}[]{1, 2, 3, 4, 5}, cummax(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[]{5, 5, 5, 5, 5}, cummax(new ${pt.primitive}[]{5, 4, 3, 2, 1}));
        assertEquals(new ${pt.primitive}[]{1, 2, 3, 3, 5}, cummax(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new ${pt.primitive}[]{5, 5, 5, 5, 5}, cummax(new ${pt.primitive}[]{5, 4, 3, ${pt.null}, 1}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}, cummax(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 4, 4, 4, 4}, cummax(new ${pt.primitive}[]{${pt.null}, 4, 3, 2, 1}));
        assertEquals(new ${pt.primitive}[0], cummax(new ${pt.primitive}[0]));
        assertEquals(new ${pt.primitive}[0], cummax(new ${pt.boxed}[0]));
        assertEquals(null, cummax((${pt.primitive}[]) null));

        assertEquals(new ${pt.primitive}[]{1, 2, 3, 4, 5}, cummax(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[]{5, 5, 5, 5, 5}, cummax(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, 4, 3, 2, 1})));
        assertEquals(new ${pt.primitive}[]{1, 2, 3, 3, 5}, cummax(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new ${pt.primitive}[]{5, 5, 5, 5, 5}, cummax(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, 4, 3, ${pt.null}, 1})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}, cummax(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 4, 4, 4, 4}, cummax(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 4, 3, 2, 1})));
        assertEquals(new ${pt.primitive}[0], cummax(new ${pt.vectorDirect}(new ${pt.primitive}[0])));
        assertEquals(null, cummax((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(new ${pt.primitive}[]{1, 2, 3, 4, 5}, cummax((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));
        assertEquals(new ${pt.primitive}[]{5, 5, 5, 5, 5}, cummax((${pt.primitive})5, (${pt.primitive})4, (${pt.primitive})3, (${pt.primitive})2, (${pt.primitive})1));
    }

    public void test${pt.boxed}CumSumArray() {
        assertEquals(new ${pt.primitive}[]{1, 3, 6, 10, 15}, cumsum(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[]{1, 3, 6, 6, 11}, cumsum(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 5, 9, 14}, cumsum(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[0], cumsum(new ${pt.primitive}[0]));
        assertEquals(new ${pt.primitive}[0], cumsum(new ${pt.boxed}[0]));
        assertEquals(null, cumsum((${pt.primitive}[]) null));

        assertEquals(new ${pt.primitive}[]{1, 3, 6, 10, 15}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[]{1, 3, 6, 6, 11}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 5, 9, 14}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[0], cumsum(new ${pt.vectorDirect}()));
        assertEquals(null, cumsum((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(new ${pt.primitive}[]{1, 3, 6, 10, 15}, cumsum((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));
    }

    public void test${pt.boxed}CumProdArray() {
        assertEquals(new ${pt.primitive}[]{1, 2, 6, 24, 120}, cumprod(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[]{1, 2, 6, 6, 30}, cumprod(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 6, 24, 120}, cumprod(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new ${pt.primitive}[0], cumprod(new ${pt.primitive}[0]));
        assertEquals(new ${pt.primitive}[0], cumprod(new ${pt.boxed}[0]));
        assertEquals(null, cumprod((${pt.primitive}[]) null));

        assertEquals(new ${pt.primitive}[]{1, 2, 6, 24, 120}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[]{1, 2, 6, 6, 30}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 6, 24, 120}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new ${pt.primitive}[0], cumprod(new ${pt.vectorDirect}()));
        assertEquals(null, cumprod((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(new ${pt.primitive}[]{1, 2, 6, 24, 120}, cumprod((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));
    }

    public void test${pt.boxed}Abs() {
        ${pt.primitive} value = -5;
        assertEquals((${pt.primitive}) Math.abs(value), abs(value), 1e-10);
        assertEquals(${pt.null}, abs(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Acos() {
        ${pt.primitive} value = (${pt.primitive}) 0.8;
        assertEquals(Math.acos(value), acos(value), 1e-10);
        assertEquals(NULL_DOUBLE, acos(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Asin() {
        ${pt.primitive} value = (${pt.primitive}) 0.8;
        assertEquals(Math.asin(value), asin(value), 1e-10);
        assertEquals(NULL_DOUBLE, asin(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Atan() {
        ${pt.primitive} value = -5;
        assertEquals(Math.atan(value), atan(value), 1e-10);
        assertEquals(NULL_DOUBLE, atan(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Ceil() {
        ${pt.primitive} value = -5;
        assertEquals(Math.ceil(value), ceil(value), 1e-10);
        assertEquals(NULL_DOUBLE, ceil(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Cos() {
        ${pt.primitive} value = -5;
        assertEquals(Math.cos(value), cos(value), 1e-10);
        assertEquals(NULL_DOUBLE, cos(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Exp() {
        ${pt.primitive} value = -5;
        assertEquals(Math.exp(value), exp(value), 1e-10);
        assertEquals(NULL_DOUBLE, exp(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Floor() {
        ${pt.primitive} value = -5;
        assertEquals(Math.floor(value), floor(value), 1e-10);
        assertEquals(NULL_DOUBLE, floor(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Log() {
        ${pt.primitive} value = 3;
        assertEquals(Math.log(value), log(value), 1e-10);
        assertEquals(NULL_DOUBLE, log(${pt.null}), 1e-10);
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    public void test${pt.boxed}${pt2.boxed}Pow() {
        ${pt.primitive} value0 = -5;
        ${pt2.primitive} value1 = 2;
        assertEquals(Math.pow(value0, value1), pow(value0, value1), 1e-10);
        assertEquals(NULL_DOUBLE, pow(${pt.null}, value1), 1e-10);
        assertEquals(NULL_DOUBLE, pow(value0, ${pt2.null}), 1e-10);
    }

    </#if>
    </#list>

    public void test${pt.boxed}Rint() {
        ${pt.primitive} value = -5;
        assertEquals(Math.rint(value), rint(value), 1e-10);
        assertEquals(NULL_DOUBLE, rint(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Round() {
        ${pt.primitive} value = -5;
        assertEquals(Math.round(value), round(value), 1e-10);
        assertEquals(NULL_LONG, round(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Sin() {
        ${pt.primitive} value = -5;
        assertEquals(Math.sin(value), sin(value), 1e-10);
        assertEquals(NULL_DOUBLE, sin(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Sqrt() {
        ${pt.primitive} value = 3;
        assertEquals(Math.sqrt(value), sqrt(value), 1e-10);
        assertEquals(NULL_DOUBLE, sqrt(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}Tan() {
        ${pt.primitive} value = -5;
        assertEquals(Math.tan(value), tan(value), 1e-10);
        assertEquals(NULL_DOUBLE, tan(${pt.null}), 1e-10);
    }

    public void test${pt.boxed}LowerBin() {
        ${pt.primitive} value = (${pt.primitive}) 114;

        assertEquals((${pt.primitive}) 110, lowerBin(value, (${pt.primitive}) 5));
        assertEquals((${pt.primitive}) 110, lowerBin(value, (${pt.primitive}) 10));
        assertEquals((${pt.primitive}) 100, lowerBin(value, (${pt.primitive}) 20));
        assertEquals(${pt.null}, lowerBin(${pt.null}, (${pt.primitive}) 5));
        assertEquals(${pt.null}, lowerBin(value, ${pt.null}));

        assertEquals(lowerBin(value, (${pt.primitive}) 5), lowerBin(lowerBin(value, (${pt.primitive}) 5), (${pt.primitive}) 5));

        try {
            lowerBin(value, (${pt.primitive}) -5);
        } catch( IllegalArgumentException e ){
            // pass
        }

        for(int i=-10; i<10; i++) {
            assertEquals((double) lowerBin((double)i, (double) 3), (double) lowerBin((${pt.primitive})i, (${pt.primitive}) 3), 0.0);
        }
    }

    public void test${pt.boxed}LowerBinWithOffset() {
        ${pt.primitive} value = (${pt.primitive}) 114;
        ${pt.primitive} offset = (${pt.primitive}) 3;

        assertEquals((${pt.primitive}) 113, lowerBin(value, (${pt.primitive}) 5, offset));
        assertEquals((${pt.primitive}) 113, lowerBin(value, (${pt.primitive}) 10, offset));
        assertEquals((${pt.primitive}) 103, lowerBin(value, (${pt.primitive}) 20, offset));
        assertEquals(${pt.null}, lowerBin(${pt.null}, (${pt.primitive}) 5, offset));
        assertEquals(${pt.null}, lowerBin(value, ${pt.null}, offset));

        assertEquals(lowerBin(value, (${pt.primitive}) 5, offset), lowerBin(lowerBin(value, (${pt.primitive}) 5, offset), (${pt.primitive}) 5, offset));
    }

    public void test${pt.boxed}UpperBin() {
        ${pt.primitive} value = (${pt.primitive}) 114;

        assertEquals((${pt.primitive}) 115, upperBin(value, (${pt.primitive}) 5));
        assertEquals((${pt.primitive}) 120, upperBin(value, (${pt.primitive}) 10));
        assertEquals((${pt.primitive}) 120, upperBin(value, (${pt.primitive}) 20));
        assertEquals(${pt.null}, upperBin(${pt.null}, (${pt.primitive}) 5));
        assertEquals(${pt.null}, upperBin(value, ${pt.null}));

        assertEquals(upperBin(value, (${pt.primitive}) 5), upperBin(upperBin(value, (${pt.primitive}) 5), (${pt.primitive}) 5));

        try {
            upperBin(value, (${pt.primitive}) -5);
        } catch( IllegalArgumentException e ){
            // pass
        }

        for(int i=-10; i<10; i++) {
            assertEquals((double) upperBin((double)i, (double) 3), (double) upperBin((${pt.primitive})i, (${pt.primitive}) 3), 0.0);
        }
    }

    public void test${pt.boxed}UpperBinWithOffset() {
        ${pt.primitive} value = (${pt.primitive}) 114;
        ${pt.primitive} offset = (${pt.primitive}) 3;

        assertEquals((${pt.primitive}) 118, upperBin(value, (${pt.primitive}) 5, offset));
        assertEquals((${pt.primitive}) 123, upperBin(value, (${pt.primitive}) 10, offset));
        assertEquals((${pt.primitive}) 123, upperBin(value, (${pt.primitive}) 20, offset));
        assertEquals(${pt.null}, upperBin(${pt.null}, (${pt.primitive}) 5, offset));
        assertEquals(${pt.null}, upperBin(value, ${pt.null}, offset));

        assertEquals(upperBin(value, (${pt.primitive}) 5, offset), upperBin(upperBin(value, (${pt.primitive}) 5, offset), (${pt.primitive}) 5, offset));
    }

    public void test${pt.boxed}Clamp() {
        assertEquals((${pt.primitive}) 3, clamp((${pt.primitive}) 3, (${pt.primitive}) -6, (${pt.primitive}) 5));
        assertEquals((${pt.primitive}) -6, clamp((${pt.primitive}) -7, (${pt.primitive}) -6, (${pt.primitive}) 5));
        assertEquals((${pt.primitive}) 5, clamp((${pt.primitive}) 7, (${pt.primitive}) -6, (${pt.primitive}) 5));
        assertEquals(${pt.null}, clamp(${pt.null}, (${pt.primitive}) -6, (${pt.primitive}) 5));
    }

    public void test${pt.boxed}Sequence(){
        assertEquals(new ${pt.primitive}[]{0,1,2,3,4,5}, Numeric.sequence((${pt.primitive})0, (${pt.primitive})5, (${pt.primitive})1));
        assertEquals(new ${pt.primitive}[]{-5,-4,-3,-2,-1,0}, Numeric.sequence((${pt.primitive})-5, (${pt.primitive})0, (${pt.primitive})1));

        assertEquals(new ${pt.primitive}[]{0,2,4}, Numeric.sequence((${pt.primitive})0, (${pt.primitive})5, (${pt.primitive})2));
        assertEquals(new ${pt.primitive}[]{-5,-3,-1}, Numeric.sequence((${pt.primitive})-5, (${pt.primitive})0, (${pt.primitive})2));

        assertEquals(new ${pt.primitive}[]{5,3,1}, Numeric.sequence((${pt.primitive})5, (${pt.primitive})0, (${pt.primitive})-2));
        assertEquals(new ${pt.primitive}[]{0,-2,-4}, Numeric.sequence((${pt.primitive})0, (${pt.primitive})-5, (${pt.primitive})-2));

        assertEquals(new ${pt.primitive}[]{}, Numeric.sequence((${pt.primitive})0, (${pt.primitive})5, (${pt.primitive})0));
        assertEquals(new ${pt.primitive}[]{}, Numeric.sequence((${pt.primitive})5, (${pt.primitive})0, (${pt.primitive})1));
    }

    public void test${pt.boxed}Median() {
        assertEquals(Double.NaN, median(new ${pt.primitive}[]{}));

        assertEquals(3.0, median(new ${pt.primitive}[]{4,2,3}));
        assertEquals(3.5, median(new ${pt.primitive}[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((${pt.primitive}[])null));

        assertEquals(3.0, median(new ${pt.boxed}[]{(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3}));
        assertEquals(3.5, median(new ${pt.boxed}[]{(${pt.primitive})5,(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3}));
        assertEquals(NULL_DOUBLE, median((${pt.boxed}[])null));

        assertEquals(3.0, median(new ${pt.vectorDirect}(new ${pt.primitive}[]{4,2,3})));
        assertEquals(3.5, median(new ${pt.vectorDirect}(new ${pt.primitive}[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(3.0, median((${pt.primitive})4, (${pt.primitive})2, (${pt.primitive})3));
    }

    public void test${pt.boxed}Percentile() {
        assertEquals(2.0, percentile(0.00, new ${pt.primitive}[]{4,2,3}));
        assertEquals(3.0, percentile(0.50, new ${pt.primitive}[]{4,2,3}));
        assertEquals(NULL_DOUBLE, percentile(0.25, (${pt.primitive}[])null));
        assertEquals(NULL_DOUBLE, percentile(0.25, new ${pt.primitive}[]{}));

        assertEquals(2.0, percentile(0.00, new ${pt.vectorDirect}(new ${pt.primitive}[]{4,2,3})));
        assertEquals(3.0, percentile(0.50, new ${pt.vectorDirect}(new ${pt.primitive}[]{4,2,3})));
        assertEquals(NULL_DOUBLE, percentile(0.25, (${pt.vector}) null));
        assertEquals(NULL_DOUBLE, percentile(0.50, new ${pt.vectorDirect}(new ${pt.primitive}[]{})));

        try {
            percentile(-1, new ${pt.primitive}[]{4,2,3});
            fail("Illegal argument check");
        } catch(IllegalArgumentException e) {
            // pass
        }

        try {
            percentile(1.01, new ${pt.primitive}[]{4,2,3});
            fail("Illegal argument check");
        } catch(IllegalArgumentException e) {
            // pass
        }

    }

    public void test${pt.boxed}Wsum() {
        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wsum((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wsum((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wsum((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wsum((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        try {
            wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }

        </#if>
        </#list>
    }

    public void test${pt.boxed}WAvg() {
        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wavg((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wavg((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wavg((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wavg((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wavg(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        try {
            wavg(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }

        </#if>
        </#list>
    }

    public void test${pt.boxed}Wvar() {
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = (w * sum2 - sum * sum) / (w * w - w2);

        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        assertEquals(target, wvar(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wvar((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(target, wvar(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wvar((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wvar(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals(target, wvar(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wvar((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals(target, wvar(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wvar((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wvar(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        try {
            wvar(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }

        assertEquals(var(new ${pt.primitive}[]{1,2,3}), wvar(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{1,1,1,7,${pt2.null}}));
        assertEquals(var(new ${pt.primitive}[]{1,2,3}), wvar(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{2,2,2,7,${pt2.null}}));

        </#if>
        </#list>
    }

    public void test${pt.boxed}Wstd() {
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double target = Math.sqrt((w * sum2 - sum * sum) / (w * w - w2));

        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        assertEquals(target, wstd(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wstd((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(target, wstd(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wstd((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wstd(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals(target, wstd(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wstd((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals(target, wstd(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wstd((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wstd(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        </#if>
        </#list>
    }

    public void test${pt.boxed}Wste() {
        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        final double w = 4.0 + 5.0 + 6.0;
        final double w2 = 4.0*4.0 + 5.0*5.0 + 6.0*6.0;
        final double sum = 1.0*4.0+2.0*5.0+3.0*6.0;
        final double sum2 = 1.0*1.0*4.0 + 2.0*2.0*5.0 + 3.0*3.0*6.0;
        final double std = Math.sqrt((w * sum2 - sum * sum) / (w * w - w2));
        final double target = std * Math.sqrt( w2 / w / w);

        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        assertEquals(target, wste(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wste((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(target, wste(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wste((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wste(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals(target, wste(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wste((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals(target, wste(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wste((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wste(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        try {
            wste(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }


        </#if>
        </#list>
    }

    public void test${pt.boxed}Wtstat() {
        final double target = wavg(new ${pt.primitive}[]{1,2,3}, new ${pt.primitive}[]{4,5,6}) / wste(new ${pt.primitive}[]{1,2,3}, new ${pt.primitive}[]{4,5,6});

        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        assertEquals(target, wtstat(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wtstat((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(target, wtstat(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wtstat((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wtstat(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals(target, wtstat(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wtstat((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals(target, wtstat(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wtstat((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wtstat(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        </#if>
        </#list>
    }


    <#if pt.valueType.isFloat >

    public void test${pt.boxed}IsNan(){
        assertTrue(isNaN(${pt.boxed}.NaN));
        assertFalse(isNaN((${pt.boxed})null));
        assertFalse(isNaN((${pt.primitive})3.0));
    }

    public void test${pt.boxed}IsInf(){
        assertTrue(isInf(${pt.boxed}.POSITIVE_INFINITY));
        assertTrue(isInf(${pt.boxed}.NEGATIVE_INFINITY));
        assertFalse(isInf((${pt.boxed})null));
        assertFalse(isInf((${pt.primitive})3.0));
    }

    public void test${pt.boxed}IsFinite() {
        assertTrue(isFinite((${pt.primitive})0));
        assertTrue(isFinite((${pt.primitive})1));
        assertTrue(isFinite((${pt.primitive})-1));
        assertFalse(isFinite(${pt.boxed}.POSITIVE_INFINITY));
        assertFalse(isFinite(${pt.boxed}.NEGATIVE_INFINITY));
        assertFalse(isFinite(${pt.boxed}.NaN));
        assertFalse(isFinite(${pt.null}));
    }

    public void test${pt.boxed}ContainsNonFinite() {
        assertFalse(containsNonFinite((${pt.primitive})0, (${pt.primitive})0, (${pt.primitive})0));
        assertFalse(containsNonFinite((${pt.primitive})-1, (${pt.primitive})0, (${pt.primitive})1));
        assertTrue(containsNonFinite((${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.NEGATIVE_INFINITY));
        assertTrue(containsNonFinite((${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.POSITIVE_INFINITY));
        assertTrue(containsNonFinite((${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.NaN));
        assertTrue(containsNonFinite((${pt.primitive})0, (${pt.primitive})0, ${pt.null}));

        assertFalse(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})0, (${pt.primitive})0, (${pt.primitive})0}));
        assertFalse(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})-1, (${pt.primitive})0, (${pt.primitive})1}));
        assertTrue(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.NEGATIVE_INFINITY}));
        assertTrue(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.POSITIVE_INFINITY}));
        assertTrue(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.NaN}));
        assertTrue(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.null}}));

        assertFalse(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})0, (${pt.primitive})0}));
        assertFalse(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})-1, (${pt.primitive})0, (${pt.primitive})1}));
        assertTrue(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.NEGATIVE_INFINITY}));
        assertTrue(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.POSITIVE_INFINITY}));
        assertTrue(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.boxed}.NaN}));
        assertTrue(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.null}}));
    }

    public void test${pt.boxed}ReplaceIfNaNScalar() {
        assertEquals((${pt.primitive}) 3, replaceIfNaN((${pt.primitive}) 3, (${pt.primitive}) 7));
        assertEquals((${pt.primitive}) 7, replaceIfNaN(${pt.boxed}.NaN, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}ReplaceIfNaNArray() {
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNaN(new ${pt.vectorDirect}(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.boxed}.NaN, (${pt.primitive}) 11}), (${pt.primitive}) 7));
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNaN(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.boxed}.NaN, (${pt.primitive}) 11}, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}ReplaceIfNullNaNScalar() {
        assertEquals((${pt.primitive}) 3, replaceIfNullNaN((${pt.primitive}) 3, (${pt.primitive}) 7));
        assertEquals((${pt.primitive}) 7, replaceIfNullNaN(${pt.boxed}.NaN, (${pt.primitive}) 7));
        assertEquals((${pt.primitive}) 7, replaceIfNullNaN(${pt.null}, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}ReplaceIfNullNaNArray() {
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNullNaN(new ${pt.vectorDirect}(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.boxed}.NaN, (${pt.primitive}) 11}), (${pt.primitive}) 7));
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNullNaN(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.boxed}.NaN, (${pt.primitive}) 11}, (${pt.primitive}) 7));
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNullNaN(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.null}, (${pt.primitive}) 11}, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}ReplaceIfNonFiniteScalar() {
        assertEquals((${pt.primitive}) 3, replaceIfNonFinite((${pt.primitive}) 3, (${pt.primitive}) 7));
        assertEquals((${pt.primitive}) 7, replaceIfNonFinite(${pt.boxed}.NaN, (${pt.primitive}) 7));
        assertEquals((${pt.primitive}) 7, replaceIfNonFinite(${pt.boxed}.POSITIVE_INFINITY, (${pt.primitive}) 7));
        assertEquals((${pt.primitive}) 7, replaceIfNonFinite(${pt.null}, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}ReplaceIfNonFiniteArray() {
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNonFinite(new ${pt.vectorDirect}(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.boxed}.NaN, (${pt.primitive}) 11}), (${pt.primitive}) 7));
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNonFinite(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.boxed}.NaN, (${pt.primitive}) 11}, (${pt.primitive}) 7));
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNonFinite(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.boxed}.POSITIVE_INFINITY, (${pt.primitive}) 11}, (${pt.primitive}) 7));
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNonFinite(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.null}, (${pt.primitive}) 11}, (${pt.primitive}) 7));
    }

    <#else>

    public void test${pt.boxed}IsNan(){
        assertFalse(isNaN((${pt.primitive})3.0));
        assertFalse(isNaN(new ${pt.boxed}((${pt.primitive})3.0)));
    }

    public void test${pt.boxed}IsInf(){
        assertFalse(isInf((${pt.primitive})3.0));
        assertFalse(isInf(new ${pt.boxed}((${pt.primitive})3.0)));
    }

    public void test${pt.boxed}IsFinite() {
        assertTrue(isFinite((${pt.primitive})0));
        assertTrue(isFinite((${pt.primitive})1));
        assertTrue(isFinite((${pt.primitive})-1));
        assertFalse(isFinite(${pt.null}));

        assertTrue(isFinite(new ${pt.boxed}((${pt.primitive})0)));
        assertTrue(isFinite(new ${pt.boxed}((${pt.primitive})1)));
        assertTrue(isFinite(new ${pt.boxed}((${pt.primitive})-1)));
        assertFalse(isFinite((${pt.boxed})null));
    }

    public void test${pt.boxed}ContainsNonFinite() {
        assertFalse(containsNonFinite((${pt.primitive})0, (${pt.primitive})0, (${pt.primitive})0));
        assertFalse(containsNonFinite((${pt.primitive})-1, (${pt.primitive})0, (${pt.primitive})1));
        assertTrue(containsNonFinite((${pt.primitive})0, (${pt.primitive})0, ${pt.null}));

        assertFalse(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})0, (${pt.primitive})0, (${pt.primitive})0}));
        assertFalse(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})-1, (${pt.primitive})0, (${pt.primitive})1}));
        assertTrue(containsNonFinite(new ${pt.primitive}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.null}}));

        assertFalse(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})0, (${pt.primitive})0}));
        assertFalse(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})-1, (${pt.primitive})0, (${pt.primitive})1}));
        assertTrue(containsNonFinite(new ${pt.boxed}[]{(${pt.primitive})0, (${pt.primitive})0, ${pt.null}}));
    }

    </#if>

    </#if>
    </#list>
}