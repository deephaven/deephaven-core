<#--
  Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
-->

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;

import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Basic.count;
import static io.deephaven.function.Numeric.*;

/**
 * Test Numeric.
 */
@SuppressWarnings({"RedundantCast", "RedundantArrayCreation", "PointlessArithmeticExpression", "ConstantConditions", "SimplifiableAssertion", "Convert2Diamond"})
public class TestNumeric extends BaseArrayTestCase {

    //////////////////////////// Constants ////////////////////////////

    public void testE() {
        assertEquals(Math.E, E, 0.0);
    }

    public void testPI() {
        assertEquals(Math.PI, PI, 0.0);
    }

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
        assertEquals(NULL_DOUBLE, avg(new ${pt.primitive}[]{}));
        assertEquals(NULL_DOUBLE, avg(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(10.0, avg(new ${pt.primitive}[]{5, ${pt.null}, 15}));
        assertEquals(NULL_DOUBLE, avg((${pt.primitive}[])null));

        assertEquals(50.0, avg(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive})50, (${pt.primitive})60}));
        assertEquals(45.5, avg(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive})51}));
        assertEquals(NULL_DOUBLE, avg(new ${pt.boxed}[]{}));
        assertEquals(NULL_DOUBLE, avg(new ${pt.boxed}[]{${pt.null}}));
        assertEquals(10.0, avg(new ${pt.boxed}[]{(${pt.primitive})5, ${pt.null}, (${pt.primitive})15}));
        assertEquals(NULL_DOUBLE, avg((${pt.boxed}[])null));

        assertEquals(50.0, avg(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, 50, 60})));
        assertEquals(45.5, avg(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, 51})));
        assertEquals(NULL_DOUBLE, avg(new ${pt.vectorDirect}()));
        assertEquals(NULL_DOUBLE, avg(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(10.0, avg(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15})));
        assertEquals(NULL_DOUBLE, avg((${pt.vectorDirect})null));

        // verify the all-null case returns null
        assertEquals(NULL_DOUBLE, avg(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(NULL_DOUBLE, avg(new ${pt.boxed}[]{${pt.null}, ${pt.null}, ${pt.null}}));

        <#if pt.valueType.isFloat >
        // verify the NaN short-circuit case
        assertEquals(Double.NaN, avg(new ${pt.primitive}[]{40, ${pt.boxed}.NaN, 60}));
        </#if>

        // check that functions can be resolved with varargs
        assertEquals(45.0, avg((${pt.primitive})40, (${pt.primitive})50));
    }

    public void test${pt.boxed}AbsAvg() {
        assertEquals(50.0, absAvg(new ${pt.primitive}[]{40, (${pt.primitive}) 50, 60}));
        assertEquals(45.5, absAvg(new ${pt.primitive}[]{(${pt.primitive}) 40, 51}));
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.primitive}[]{}));
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(10.0, absAvg(new ${pt.primitive}[]{(${pt.primitive}) 5, ${pt.null}, (${pt.primitive}) 15}));
        assertEquals(NULL_DOUBLE, absAvg((${pt.primitive}[])null));

        assertEquals(50.0, absAvg(new ${pt.boxed}[]{(${pt.primitive})40, (${pt.primitive}) 50, (${pt.primitive})60}));
        assertEquals(45.5, absAvg(new ${pt.boxed}[]{(${pt.primitive}) 40, (${pt.primitive})51}));
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.boxed}[]{}));
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.boxed}[]{${pt.null}}));
        assertEquals(10.0, absAvg(new ${pt.boxed}[]{(${pt.primitive}) 5, ${pt.null}, (${pt.primitive}) 15}));
        assertEquals(NULL_DOUBLE, absAvg((${pt.boxed}[])null));

        assertEquals(50.0, absAvg(new ${pt.vectorDirect}(new ${pt.primitive}[]{40, (${pt.primitive}) 50, 60})));
        assertEquals(45.5, absAvg(new ${pt.vectorDirect}(new ${pt.primitive}[]{(${pt.primitive}) 40, 51})));
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.vectorDirect}()));
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(10.0, absAvg(new ${pt.vectorDirect}((${pt.primitive}) 5, ${pt.null}, (${pt.primitive}) 15)));
        assertEquals(NULL_DOUBLE, absAvg((${pt.vectorDirect})null));

        // verify the all-null case returns null
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(NULL_DOUBLE, absAvg(new ${pt.boxed}[]{${pt.null}, ${pt.null}, ${pt.null}}));

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

        // verify the all-null case returns null
        assertEquals(NULL_DOUBLE, var(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(NULL_DOUBLE, var(new ${pt.boxed}[]{${pt.null}, ${pt.null}, ${pt.null}}));

        // verify size==1
        assertEquals(Double.NaN, var(new ${pt.primitive}[]{40}));

        <#if pt.valueType.isFloat >
        // verify the NaN short-circuit case
        assertEquals(Double.NaN, var(new ${pt.primitive}[]{40, ${pt.boxed}.NaN, 60}));

        </#if>
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

        // verify the all-null case returns null
        assertEquals(NULL_DOUBLE, std(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(NULL_DOUBLE, std(new ${pt.boxed}[]{${pt.null}, ${pt.null}, ${pt.null}}));

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

        // verify the all-null case returns null
        assertEquals(NULL_DOUBLE, ste(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(NULL_DOUBLE, ste(new ${pt.boxed}[]{${pt.null}, ${pt.null}, ${pt.null}}));

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

        // verify the all-null case returns null
        assertEquals(NULL_DOUBLE, tstat(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(NULL_DOUBLE, tstat(new ${pt.boxed}[]{${pt.null}, ${pt.null}, ${pt.null}}));

        // check that functions can be resolved with varargs
        assertEquals(tstat(v), tstat((${pt.primitive})0, (${pt.primitive})40, ${pt.null}, (${pt.primitive})50, (${pt.primitive})60, (${pt.primitive}) -1, (${pt.primitive})0));
    }

<#if pt.valueType.isFloat >
    public void test${pt.boxed}NaNAndInfHandling() {
        double result;

        final ${pt.primitive}[] normal = new ${pt.primitive}[]{1, 2, 3, 4, 5, 6};

        final ${pt.primitive}[] normalWithNaN = new ${pt.primitive}[]{1, 2, 3, ${pt.boxed}.NaN, 4, 5};
        assertTrue(Double.isNaN(avg(normalWithNaN)));
        assertTrue(Double.isNaN(absAvg(normalWithNaN)));
        assertTrue(Double.isNaN(var(normalWithNaN)));
        assertTrue(Double.isNaN(std(normalWithNaN)));
        assertTrue(Double.isNaN(ste(normalWithNaN)));
        assertTrue(Double.isNaN(tstat(normalWithNaN)));

        assertTrue(Double.isNaN(cov(normalWithNaN, normal)));
        assertTrue(Double.isNaN(cor(normalWithNaN, normal)));
        assertTrue(Double.isNaN(wavg(normalWithNaN, normal)));
        assertTrue(Double.isNaN(wvar(normalWithNaN, normal)));
        assertTrue(Double.isNaN(wstd(normalWithNaN, normal)));
        assertTrue(Double.isNaN(wste(normalWithNaN, normal)));
        assertTrue(Double.isNaN(wtstat(normalWithNaN, normal)));

        assertTrue(Double.isNaN(cov(normal, normalWithNaN)));
        assertTrue(Double.isNaN(cor(normal, normalWithNaN)));
        assertTrue(Double.isNaN(wavg(normal, normalWithNaN)));
        assertTrue(Double.isNaN(wvar(normal, normalWithNaN)));
        assertTrue(Double.isNaN(wstd(normal, normalWithNaN)));
        assertTrue(Double.isNaN(wste(normal, normalWithNaN)));
        assertTrue(Double.isNaN(wtstat(normal, normalWithNaN)));

        final ${pt.primitive}[] normalWithInf = new ${pt.primitive}[]{1, 2, 3, ${pt.boxed}.POSITIVE_INFINITY, 4, 5};
        result = avg(normalWithInf);
        assertTrue(Double.isInfinite(result) && result > 0); // positive infinity
        result = absAvg(normalWithInf);
        assertTrue(Double.isInfinite(result) && result > 0); // positive infinity

        assertTrue(Double.isNaN(var(normalWithInf)));
        assertTrue(Double.isNaN(std(normalWithInf)));
        assertTrue(Double.isNaN(ste(normalWithInf)));
        assertTrue(Double.isNaN(tstat(normalWithInf)));

        assertTrue(Double.isNaN(cov(normalWithInf, normal)));
        assertTrue(Double.isNaN(cor(normalWithInf, normal)));
        result = wavg(normalWithInf, normal);
        assertTrue(Double.isInfinite(result) && result > 0); // positive infinity
        assertTrue(Double.isNaN(wvar(normalWithInf, normal)));
        assertTrue(Double.isNaN(wstd(normalWithInf, normal)));
        assertTrue(Double.isNaN(wste(normalWithInf, normal)));
        assertTrue(Double.isNaN(wtstat(normalWithInf, normal)));

        assertTrue(Double.isNaN(cov(normal, normalWithInf)));
        assertTrue(Double.isNaN(cor(normal, normalWithInf)));
        assertTrue(Double.isNaN(wavg(normal, normalWithInf))); // is NaN because of inf/inf division
        assertTrue(Double.isNaN(wvar(normal, normalWithInf)));
        assertTrue(Double.isNaN(wstd(normal, normalWithInf)));
        assertTrue(Double.isNaN(wste(normal, normalWithInf)));
        assertTrue(Double.isNaN(wtstat(normal, normalWithInf)));

        final ${pt.primitive}[] normalWithNegInf = new ${pt.primitive}[]{1, 2, 3, ${pt.boxed}.NEGATIVE_INFINITY, 4, 5};
        result = avg(normalWithNegInf);
        assertTrue(Double.isInfinite(result) && result < 0); // negative infinity
        result = absAvg(normalWithNegInf);
        assertTrue(Double.isInfinite(result) && result > 0); // positive infinity

        assertTrue(Double.isNaN(var(normalWithNegInf)));
        assertTrue(Double.isNaN(std(normalWithNegInf)));
        assertTrue(Double.isNaN(ste(normalWithNegInf)));
        assertTrue(Double.isNaN(tstat(normalWithNegInf)));

        assertTrue(Double.isNaN(cov(normalWithNegInf, normal)));
        assertTrue(Double.isNaN(cor(normalWithNegInf, normal)));
        result = wavg(normalWithNegInf, normal);
        assertTrue(Double.isInfinite(result) && result < 0); // negative infinity
        assertTrue(Double.isNaN(wvar(normalWithNegInf, normal)));
        assertTrue(Double.isNaN(wstd(normalWithNegInf, normal)));
        assertTrue(Double.isNaN(wste(normalWithNegInf, normal)));
        assertTrue(Double.isNaN(wtstat(normalWithNegInf, normal)));

        assertTrue(Double.isNaN(cov(normal, normalWithNegInf)));
        assertTrue(Double.isNaN(cor(normal, normalWithNegInf)));
        assertTrue(Double.isNaN(wavg(normal, normalWithNegInf))); // is NaN because of -inf/-inf division
        assertTrue(Double.isNaN(wvar(normal, normalWithNegInf)));
        assertTrue(Double.isNaN(wstd(normal, normalWithNegInf)));
        assertTrue(Double.isNaN(wste(normal, normalWithNegInf)));
        assertTrue(Double.isNaN(wtstat(normal, normalWithNegInf)));

    <#if pt.primitive == "double" >
        // testing normal value overflow. NOTE: this is testing for doubles only, since overflowing a double using
        // smaller types is quite difficult
        final double LARGE_VALUE = Math.nextDown(Double.MAX_VALUE);

        final double[] overflow = new double[]{1, LARGE_VALUE, LARGE_VALUE};
        assertTrue(Double.isInfinite(avg(overflow)));

        assertTrue(Double.isNaN(var(overflow)));
        assertTrue(Double.isNaN(std(overflow)));
        assertTrue(Double.isNaN(ste(overflow)));
        assertTrue(Double.isNaN(tstat(overflow)));

        final double[] negOverflow = new double[]{1, LARGE_VALUE, -LARGE_VALUE};
        assertTrue(Double.isNaN(var(negOverflow)));
        assertTrue(Double.isNaN(std(negOverflow)));
        assertTrue(Double.isNaN(ste(negOverflow)));
        assertTrue(Double.isNaN(tstat(negOverflow)));

        final double[] negAdditionOverflow = new double[]{1, -LARGE_VALUE, -LARGE_VALUE};
        result = avg(negAdditionOverflow);
        assertTrue(Double.isInfinite(result) && result < 0); // negative infinity
    </#if>
    }
</#if>

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

        // verify the all-null cases return null
        assertEquals(NULL_DOUBLE, cov(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));
        assertEquals(NULL_DOUBLE, cov(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{1, 2, 3}));
        assertEquals(NULL_DOUBLE, cov(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));

        <#if pt.valueType.isFloat >
        // verify the NaN short-circuit case
        assertEquals(Double.NaN, cov(new ${pt.primitive}[]{1, 2, ${pt.boxed}.NaN}, new ${pt2.primitive}[]{1, 2, 3}));
        </#if>
        <#if pt2.valueType.isFloat >
        // verify the NaN short-circuit case
        assertEquals(Double.NaN, cov(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{1, 2, ${pt2.boxed}.NaN}));
        </#if>

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

        // verify the all-null cases return null
        assertEquals(NULL_DOUBLE, cor(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));
        assertEquals(NULL_DOUBLE, cor(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{1, 2, 3}));
        assertEquals(NULL_DOUBLE, cor(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));

        <#if pt.valueType.isFloat >
        // verify the NaN short-circuit case
        assertEquals(Double.NaN, cor(new ${pt.primitive}[]{1, 2, ${pt.boxed}.NaN}, new ${pt2.primitive}[]{1, 2, 3}));
        </#if>
        <#if pt2.valueType.isFloat >
        // verify the NaN short-circuit case
        assertEquals(Double.NaN, cor(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{1, 2, ${pt2.boxed}.NaN}));
        </#if>

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
        assertTrue(Math.abs(20 - sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15}))) == 0.0);
    <#if pt.valueType.isFloat >
        assertEquals(NULL_DOUBLE, sum((${pt.vector}) null));
        assertEquals(NULL_DOUBLE, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{})));
        assertEquals(NULL_DOUBLE, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, ${pt.null}})));
        assertEquals(Double.POSITIVE_INFINITY, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, 6})));
        assertEquals(Double.POSITIVE_INFINITY, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY})));
        assertEquals(Double.NEGATIVE_INFINITY, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, 6})));
        assertEquals(Double.NEGATIVE_INFINITY, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY})));
        assertEquals(Double.NaN, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY})));
        assertEquals(Double.NaN, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.NaN, 6})));
    <#else>
        assertEquals(NULL_LONG, sum((${pt.vector}) null));
        assertEquals(NULL_LONG, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{})));
        assertEquals(NULL_LONG, sum(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, ${pt.null}})));
    </#if>
    }

    public void test${pt.boxed}Sum2() {
        assertTrue(Math.abs(15 - sum(new ${pt.primitive}[]{4, 5, 6})) == 0.0);
        assertTrue(Math.abs(20 - sum(new ${pt.primitive}[]{5, ${pt.null}, 15})) == 0.0);
    <#if pt.valueType.isFloat >
        assertEquals(NULL_DOUBLE, sum((${pt.primitive}[]) null));
        assertEquals(NULL_DOUBLE, sum(new ${pt.primitive}[]{}));
        assertEquals(NULL_DOUBLE, sum(new ${pt.primitive}[]{${pt.null}, ${pt.null}}));
        assertEquals(Double.POSITIVE_INFINITY, sum(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, 6}));
        assertEquals(Double.POSITIVE_INFINITY, sum(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY}));
        assertEquals(Double.NEGATIVE_INFINITY, sum(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, 6}));
        assertEquals(Double.NEGATIVE_INFINITY, sum(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY}));
        assertEquals(Double.NaN, sum(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}));
        assertEquals(Double.NaN, sum(new ${pt.primitive}[]{4, Float.NaN, 6}));
    <#else>
        assertEquals(NULL_LONG, sum((${pt.primitive}[]) null));
        assertEquals(NULL_LONG, sum(new ${pt.primitive}[]{}));
        assertEquals(NULL_LONG, sum(new ${pt.primitive}[]{${pt.null}, ${pt.null}}));
    </#if>
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
        <#if pt.valueType.isFloat >
        final double nullResult = NULL_DOUBLE;
        final double zeroValue = 0.0;
        <#else>
        final long nullResult = NULL_LONG;
        final long zeroValue = 0;
        </#if>

        assertTrue(Math.abs(120 - product(new ${pt.primitive}[]{4, 5, 6})) == 0.0);
        assertEquals(nullResult, product(new ${pt.primitive}[]{}));
        assertEquals(nullResult, product(new ${pt.primitive}[]{${pt.null}}));
        assertTrue(Math.abs(75 - product(new ${pt.primitive}[]{5, ${pt.null}, 15})) == 0.0);
        assertEquals(nullResult, product((${pt.primitive}[]) null));
        assertEquals(zeroValue, product(new ${pt.primitive}[]{4, 0, 5, 6}));

        assertTrue(Math.abs(120 - product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, 5, 6}))) == 0.0);
        assertEquals(nullResult, product(new ${pt.vectorDirect}()));
        assertEquals(nullResult, product(new ${pt.vectorDirect}(${pt.null})));
        assertTrue(Math.abs(75 - product(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null}, 15}))) == 0.0);
        assertEquals(nullResult, product((${pt.vector}) null));
        assertEquals(zeroValue, product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, 0, 5, 6})));


        <#if pt.valueType.isFloat >
        assertEquals(Double.POSITIVE_INFINITY, product(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, 6}));
        assertEquals(Double.POSITIVE_INFINITY, product(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY}));
        assertEquals(Double.NEGATIVE_INFINITY, product(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, 6}));
        assertEquals(Double.POSITIVE_INFINITY, product(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY}));
        assertEquals(Double.NEGATIVE_INFINITY, product(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}));

        assertEquals(Double.POSITIVE_INFINITY, product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, 6})));
        assertEquals(Double.POSITIVE_INFINITY, product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY})));
        assertEquals(Double.NEGATIVE_INFINITY, product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, 6})));
        assertEquals(Double.POSITIVE_INFINITY, product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY})));
        assertEquals(Double.NEGATIVE_INFINITY, product(new ${pt.vectorDirect}(new ${pt.primitive}[]{4, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY})));
        </#if>
    }

<#if pt.valueType.isFloat >
    public void test${pt.boxed}ProductOverflowAndNaN() {
        <#if pt.primitive == "double" >

        final ${pt.primitive} LARGE_VALUE = Math.nextDown(${pt.boxed}.MAX_VALUE);

        final ${pt.primitive}[] overflow = new ${pt.primitive}[]{1, LARGE_VALUE, LARGE_VALUE};
        final double overflowProduct = product(overflow);
        assertTrue(Double.isInfinite(overflowProduct) && overflowProduct > 0);

        final ${pt.primitive}[] negOverflow = new ${pt.primitive}[]{1, LARGE_VALUE, -LARGE_VALUE};
        final double negOverflowProduct = product(negOverflow);
        assertTrue(Double.isInfinite(negOverflowProduct) && negOverflowProduct < 0);

        final ${pt.primitive}[] overflowWithZero = new ${pt.primitive}[]{1, LARGE_VALUE, LARGE_VALUE, 0};
        assertTrue(Math.abs(product(overflowWithZero)) == 0.0);

        </#if>

        final ${pt.primitive}[] normalWithNaN = new ${pt.primitive}[]{1, 2, 3, ${pt.boxed}.NaN, 4, 5};
        assertTrue(Double.isNaN(product(normalWithNaN)));

        final ${pt.primitive}[] posInfAndZero = new ${pt.primitive}[]{1, ${pt.boxed}.POSITIVE_INFINITY, 0};
        assertTrue(Double.isNaN(product(posInfAndZero)));

        final ${pt.primitive}[] negInfAndZero = new ${pt.primitive}[]{1, ${pt.boxed}.NEGATIVE_INFINITY, 0};
        assertTrue(Double.isNaN(product(negInfAndZero)));

        final ${pt.primitive}[] zeroAndPosInf = new ${pt.primitive}[]{1, 0, ${pt.boxed}.POSITIVE_INFINITY};
        assertTrue(Double.isNaN(product(zeroAndPosInf)));

        final ${pt.primitive}[] zeroAndNegInf = new ${pt.primitive}[]{1, 0, ${pt.boxed}.NEGATIVE_INFINITY};
        assertTrue(Double.isNaN(product(zeroAndNegInf)));

    }
</#if>

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

    public void test${pt.boxed}Diff() {
        assertEquals(new ${pt.primitive}[]{1, 2, 4, 8, ${pt.null}}, diff(1, new ${pt.primitive}[]{1, 2, 4, 8, 16}));
        assertEquals(new ${pt.primitive}[]{3, 6, 12, ${pt.null}, ${pt.null}}, diff(2, new ${pt.primitive}[]{1, 2, 4, 8, 16}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, -1, -2, -4, -8}, diff(-1, new ${pt.primitive}[]{1, 2, 4, 8, 16}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, ${pt.null}, -3, -6, -12}, diff(-2, new ${pt.primitive}[]{1, 2, 4, 8, 16}));

        assertEquals(new ${pt.primitive}[]{1, 2, 4, 8, ${pt.null}}, diff(1, new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})4, (${pt.primitive})8, (${pt.primitive})16}));
        assertEquals(new ${pt.primitive}[]{3, 6, 12, ${pt.null}, ${pt.null}}, diff(2, new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})4, (${pt.primitive})8, (${pt.primitive})16}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, -1, -2, -4, -8}, diff(-1, new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})4, (${pt.primitive})8, (${pt.primitive})16}));
        assertEquals(new ${pt.primitive}[]{${pt.null}, ${pt.null}, -3, -6, -12}, diff(-2, new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})4, (${pt.primitive})8, (${pt.primitive})16}));

        assertEquals(new ${pt.primitive}[]{1, 2, 4, 8, ${pt.null}}, diff(1, new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 4, 8, 16})));
        assertEquals(new ${pt.primitive}[]{3, 6, 12, ${pt.null}, ${pt.null}}, diff(2, new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 4, 8, 16})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, -1, -2, -4, -8}, diff(-1, new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 4, 8, 16})));
        assertEquals(new ${pt.primitive}[]{${pt.null}, ${pt.null}, -3, -6, -12}, diff(-2, new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 4, 8, 16})));
    }

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

<#if pt.valueType.isFloat >
    public void test${pt.boxed}CumSumArray() {
        assertEquals(new double[]{1, 3, 6, 10, 15}, cumsum(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new double[]{1, 3, 6, 6, 11}, cumsum(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new double[]{NULL_DOUBLE, 2, 5, 9, 14}, cumsum(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new double[]{NULL_DOUBLE, NULL_DOUBLE, 2, 5, 9, 14}, cumsum(new ${pt.primitive}[]{${pt.null}, ${pt.null}, 2, 3, 4, 5}));
        assertEquals(new double[0], cumsum(new ${pt.primitive}[0]));
        assertEquals(new double[0], cumsum(new ${pt.boxed}[0]));
        assertEquals(null, cumsum((${pt.primitive}[]) null));

        assertEquals(new double[]{1, 3, 6, 10, 15}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 3, 6, 6, 11}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 5, 9, 14}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new double[]{NULL_DOUBLE, NULL_DOUBLE, 2, 5, 9, 14}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, ${pt.null}, 2, 3, 4, 5})));
        assertEquals(new double[0], cumsum(new ${pt.vectorDirect}()));
        assertEquals(null, cumsum((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(new double[]{1, 3, 6, 10, 15}, cumsum((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));

        assertEquals(new double[]{1, 3, 6, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, 5}));
        assertEquals(new double[]{1, 3, 6, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, -5}));
        assertEquals(new double[]{1, 3, 6, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY}));
        assertEquals(new double[]{1, 3, 6, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, 5}));
        assertEquals(new double[]{1, 3, 6, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, -5}));
        assertEquals(new double[]{1, 3, 6, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY}));
        assertEquals(new double[]{1, 3, 6, Double.POSITIVE_INFINITY, Double.NaN}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}));
        assertEquals(new double[]{1, 3, 6, Double.NEGATIVE_INFINITY, Double.NaN}, cumsum(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY}));
    }
<#else>
    public void test${pt.boxed}CumSumArray() {
        assertEquals(new long[]{1, 3, 6, 10, 15}, cumsum(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new long[]{1, 3, 6, 6, 11}, cumsum(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new long[]{NULL_LONG, 2, 5, 9, 14}, cumsum(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new long[]{NULL_LONG, NULL_LONG, 2, 5, 9, 14}, cumsum(new ${pt.primitive}[]{${pt.null}, ${pt.null}, 2, 3, 4, 5}));
        assertEquals(new long[0], cumsum(new ${pt.primitive}[0]));
        assertEquals(new long[0], cumsum(new ${pt.boxed}[0]));
        assertEquals(null, cumsum((${pt.primitive}[]) null));

        assertEquals(new long[]{1, 3, 6, 10, 15}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 3, 6, 6, 11}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 5, 9, 14}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new long[]{NULL_LONG, NULL_LONG, 2, 5, 9, 14}, cumsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, ${pt.null}, 2, 3, 4, 5})));
        assertEquals(new long[0], cumsum(new ${pt.vectorDirect}()));
        assertEquals(null, cumsum((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(new long[]{1, 3, 6, 10, 15}, cumsum((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));
    }
</#if>

<#if pt.valueType.isFloat >
    public void test${pt.boxed}CumProdArray() {
        assertEquals(new double[]{1, 2, 6, 24, 120}, cumprod(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new double[]{1, 2, 6, 6, 30}, cumprod(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new double[]{NULL_DOUBLE, NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new ${pt.primitive}[]{${pt.null}, ${pt.null}, 2, 3, 4, 5}));
        assertEquals(new double[0], cumprod(new ${pt.primitive}[0]));
        assertEquals(new double[0], cumprod(new ${pt.boxed}[0]));
        assertEquals(null, cumprod((${pt.primitive}[]) null));
        assertEquals(new double[]{1, Double.NaN, Double.NaN, Double.NaN, Double.NaN}, cumprod(new ${pt.primitive}[]{1, Float.NaN, 3, 4, 5}));

        assertEquals(new double[]{1, 2, 6, 24, 120}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new double[]{1, 2, 6, 6, 30}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new double[]{NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new double[]{NULL_DOUBLE, NULL_DOUBLE, 2, 6, 24, 120}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, ${pt.null}, 2, 3, 4, 5})));
        assertEquals(new double[0], cumprod(new ${pt.vectorDirect}()));
        assertEquals(null, cumprod((${pt.vector}) null));
        assertEquals(new double[]{1, Double.NaN, Double.NaN, Double.NaN, Double.NaN}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, Float.NaN, 3, 4, 5})));

        // check that functions can be resolved with varargs
        assertEquals(new double[]{1, 2, 6, 24, 120}, cumprod((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));

        assertEquals(new double[]{1, 2, 6, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, 5}));
        assertEquals(new double[]{1, 2, 6, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, -5}));
        assertEquals(new double[]{1, 2, 6, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY}));
        assertEquals(new double[]{1, 2, 6, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, 5}));
        assertEquals(new double[]{1, 2, 6, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, -5}));
        assertEquals(new double[]{1, 2, 6, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY}));
        assertEquals(new double[]{1, 2, 6, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}));
        assertEquals(new double[]{1, 2, 6, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}, cumprod(new ${pt.primitive}[]{1, 2, 3, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY}));
    }
<#else>
    public void test${pt.boxed}CumProdArray() {
        assertEquals(new long[]{1, 2, 6, 24, 120}, cumprod(new ${pt.primitive}[]{1, 2, 3, 4, 5}));
        assertEquals(new long[]{1, 2, 6, 6, 30}, cumprod(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5}));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, cumprod(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5}));
        assertEquals(new long[0], cumprod(new ${pt.primitive}[0]));
        assertEquals(new long[0], cumprod(new ${pt.boxed}[0]));
        assertEquals(null, cumprod((${pt.primitive}[]) null));

        assertEquals(new long[]{1, 2, 6, 24, 120}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, 4, 5})));
        assertEquals(new long[]{1, 2, 6, 6, 30}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3, ${pt.null}, 5})));
        assertEquals(new long[]{NULL_LONG, 2, 6, 24, 120}, cumprod(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}, 2, 3, 4, 5})));
        assertEquals(new long[0], cumprod(new ${pt.vectorDirect}()));
        assertEquals(null, cumprod((${pt.vector}) null));

        // check that functions can be resolved with varargs
        assertEquals(new long[]{1, 2, 6, 24, 120}, cumprod((${pt.primitive})1, (${pt.primitive})2, (${pt.primitive})3, (${pt.primitive})4, (${pt.primitive})5));
    }
</#if>

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
        assertEquals(NULL_DOUBLE, median(new ${pt.primitive}[]{}));

        assertEquals(3.0, median(new ${pt.primitive}[]{4,2,3}));
        assertEquals(3.5, median(new ${pt.primitive}[]{5,4,2,3}));
        assertEquals(NULL_DOUBLE, median((${pt.primitive}[])null));

        assertEquals(3.0, median(new ${pt.boxed}[]{(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3}));
        assertEquals(3.5, median(new ${pt.boxed}[]{(${pt.primitive})5,(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3}));
        assertEquals(NULL_DOUBLE, median((${pt.boxed}[])null));

        assertEquals(3.0, median(new ${pt.vectorDirect}(new ${pt.primitive}[]{4,2,3})));
        assertEquals(3.5, median(new ${pt.vectorDirect}(new ${pt.primitive}[]{5,4,2,3})));
        assertEquals(NULL_DOUBLE, median((${pt.vector}) null));

        // verify the all-null case returns null
        assertEquals(NULL_DOUBLE, median(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(NULL_DOUBLE, median(new ${pt.boxed}[]{${pt.null}, ${pt.null}, ${pt.null}}));

        // verify the mixed-null cases
        assertEquals(3.0, median(new ${pt.primitive}[]{(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3,${pt.null},${pt.null},${pt.null}}));
        assertEquals(3.5, median(new ${pt.primitive}[]{(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3,(${pt.primitive})5, ${pt.null},${pt.null},${pt.null}}));

        assertEquals(3.0, median(new ${pt.boxed}[]{(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3,${pt.null},${pt.null}}));
        assertEquals(3.5, median(new ${pt.boxed}[]{(${pt.primitive})4,(${pt.primitive})2,(${pt.primitive})3,(${pt.primitive})5,${pt.null},${pt.null}}));

    <#if pt.valueType.isFloat >
        assertEquals(Double.NaN, median(new ${pt.primitive}[]{4,2,3, ${pt.boxed}.NaN}));
        assertEquals(3.0, median(new ${pt.primitive}[]{4,2,3, ${pt.boxed}.POSITIVE_INFINITY, ${pt.null}, ${pt.null}, ${pt.boxed}.NEGATIVE_INFINITY}));
        assertEquals(3.5, median(new ${pt.primitive}[]{4,2,3,5, ${pt.boxed}.POSITIVE_INFINITY, ${pt.null}, ${pt.null}, ${pt.boxed}.NEGATIVE_INFINITY}));
    </#if>

        // check that functions can be resolved with varargs
        assertEquals(3.0, median((${pt.primitive})4, (${pt.primitive})2, (${pt.primitive})3));
    }

    public void test${pt.boxed}Percentile() {
        assertEquals((${pt.primitive})2, percentile(0.00, new ${pt.primitive}[]{4,2,3}));
        assertEquals((${pt.primitive})3, percentile(0.50, new ${pt.primitive}[]{4,2,3}));
        assertEquals(${pt.null}, percentile(0.25, (${pt.primitive}[])null));
        assertEquals(${pt.null}, percentile(0.25, new ${pt.primitive}[]{}));

        assertEquals((${pt.primitive})2, percentile(0.00, new ${pt.vectorDirect}(new ${pt.primitive}[]{4,2,3})));
        assertEquals((${pt.primitive})3, percentile(0.50, new ${pt.vectorDirect}(new ${pt.primitive}[]{4,2,3})));
        assertEquals(${pt.null}, percentile(0.25, (${pt.vector}) null));
        assertEquals(${pt.null}, percentile(0.50, new ${pt.vectorDirect}(new ${pt.primitive}[]{})));

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

        // verify the all-null case returns null
        assertEquals(${pt.null}, percentile(0.00, new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(${pt.null}, percentile(0.25, new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));
        assertEquals(${pt.null}, percentile(0.50, new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}));

        // verify the mixed-null cases
        assertEquals((${pt.primitive})2, percentile(0.00, new ${pt.primitive}[]{4,2,3,${pt.null}}));
        assertEquals((${pt.primitive})3, percentile(0.50, new ${pt.primitive}[]{4,2,3,${pt.null},${pt.null}}));
        assertEquals((${pt.primitive})4, percentile(1.0, new ${pt.primitive}[]{4,2,3,${pt.null},${pt.null},${pt.null}}));

        // verify the empty array case
        assertEquals(${pt.null}, percentile(0.00, new ${pt.vectorDirect}(new ${pt.primitive}[]{})));

    <#if pt.valueType.isFloat >
        assertEquals(${pt.boxed}.NaN, percentile(1.0, new ${pt.primitive}[]{4,2,3, ${pt.boxed}.NaN}));

        assertEquals(${pt.boxed}.NEGATIVE_INFINITY, percentile(0.0, new ${pt.primitive}[]{4,2,3, ${pt.boxed}.POSITIVE_INFINITY, ${pt.null}, ${pt.null}, ${pt.boxed}.NEGATIVE_INFINITY}));
        assertEquals((${pt.primitive})2, percentile(0.25, new ${pt.primitive}[]{4,2,3, ${pt.boxed}.POSITIVE_INFINITY, ${pt.null}, ${pt.null}, ${pt.boxed}.NEGATIVE_INFINITY}));
        assertEquals((${pt.primitive})3, percentile(0.5, new ${pt.primitive}[]{4,2,3, ${pt.boxed}.POSITIVE_INFINITY, ${pt.null}, ${pt.null}, ${pt.boxed}.NEGATIVE_INFINITY}));
        assertEquals((${pt.primitive})4, percentile(0.75, new ${pt.primitive}[]{4,2,3, ${pt.boxed}.POSITIVE_INFINITY, ${pt.null}, ${pt.null}, ${pt.boxed}.NEGATIVE_INFINITY}));
        assertEquals(${pt.boxed}.POSITIVE_INFINITY, percentile(1.0, new ${pt.primitive}[]{4,2,3, ${pt.boxed}.POSITIVE_INFINITY, ${pt.null}, ${pt.null}, ${pt.boxed}.NEGATIVE_INFINITY}));
    </#if>
    }

    public void test${pt.boxed}Wsum() {
        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        <#if pt.valueType.isInteger && pt2.valueType.isInteger >
        assertEquals(1*4+2*5+3*6, wsum(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_LONG, wsum((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_LONG, wsum(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(NULL_LONG, wsum(new ${pt.primitive}[]{${pt.null},${pt.null},${pt.null}}, new ${pt2.primitive}[]{${pt2.null},${pt2.null},${pt2.null}}));
        assertEquals(NULL_LONG, wsum(new ${pt.primitive}[]{1,2,3}, new ${pt2.primitive}[]{${pt2.null},${pt2.null},${pt2.null}}));
        assertEquals(NULL_LONG, wsum(new ${pt.primitive}[]{${pt.null},${pt.null},${pt.null}}, new ${pt2.primitive}[]{1,2,3}));

        assertEquals(1*4+2*5+3*6, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_LONG, wsum((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_LONG, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals(1*4+2*5+3*6, wsum(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_LONG, wsum((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_LONG, wsum(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals(1*4+2*5+3*6, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_LONG, wsum((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_LONG, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        try {
            wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }
        <#else>
        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wsum((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(NULL_DOUBLE, wsum(new ${pt.primitive}[]{${pt.null},${pt.null},${pt.null}}, new ${pt2.primitive}[]{${pt2.null},${pt2.null},${pt2.null}}));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.primitive}[]{1,2,3}, new ${pt2.primitive}[]{${pt2.null},${pt2.null},${pt2.null}}));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.primitive}[]{${pt.null},${pt.null},${pt.null}}, new ${pt2.primitive}[]{1,2,3}));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wsum((${pt.vector}) null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.primitive}[])null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wsum((${pt.primitive}[])null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.primitive}[]{1,2,3}, (${pt2.vector}) null));

        assertEquals(1.0*4.0+2.0*5.0+3.0*6.0, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(NULL_DOUBLE, wsum((${pt.vector}) null, new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6})));
        assertEquals(NULL_DOUBLE, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3}), (${pt2.vector}) null));

        <#if pt.valueType.isFloat >
        assertEquals(Double.NaN, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},Float.NaN}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,${pt2.null}})));
        assertEquals(Double.POSITIVE_INFINITY, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},Float.POSITIVE_INFINITY}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,8})));
        assertEquals(Double.NEGATIVE_INFINITY, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},Float.NEGATIVE_INFINITY}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,8})));
        assertEquals(Double.NaN, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,Float.POSITIVE_INFINITY,Float.NEGATIVE_INFINITY}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,7,8})));
        </#if>

        <#if pt2.valueType.isFloat >
        assertEquals(Double.NaN, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,Float.NaN,${pt2.null}})));
        assertEquals(Double.POSITIVE_INFINITY, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,4,5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,Float.POSITIVE_INFINITY,${pt2.null}})));
        assertEquals(Double.NEGATIVE_INFINITY, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,4,5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,Float.NEGATIVE_INFINITY,${pt2.null}})));
        assertEquals(Double.NaN, wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,4,5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,Float.NEGATIVE_INFINITY,Float.POSITIVE_INFINITY})));
        </#if>

        try {
            wsum(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5}));
            fail("Mismatched arguments");
        } catch(IllegalArgumentException e){
            // pass
        }
        </#if>

        </#if>
        </#list>
    }

    public void test${pt.boxed}WAvg() {
        <#list primitiveTypes as pt2>
        <#if pt2.valueType.isNumber >

        assertEquals((1.0*4.0+2.0*5.0+3.0*6.0)/(4.0+5.0+6.0), wavg(new ${pt.primitive}[]{1,2,3,${pt.null},5}, new ${pt2.primitive}[]{4,5,6,7,${pt2.null}}));
        assertEquals(NULL_DOUBLE, wavg((${pt.primitive}[])null, new ${pt2.primitive}[]{4,5,6}));
        assertEquals(NULL_DOUBLE, wavg(new ${pt.primitive}[]{1,2,3}, (${pt2.primitive}[])null));

        assertEquals(NULL_DOUBLE, wavg(new ${pt.primitive}[]{${pt.null},${pt.null},${pt.null}}, new ${pt2.primitive}[]{${pt2.null},${pt2.null},${pt2.null}}));
        assertEquals(NULL_DOUBLE, wavg(new ${pt.primitive}[]{1,2,3}, new ${pt2.primitive}[]{${pt2.null},${pt2.null},${pt2.null}}));
        assertEquals(NULL_DOUBLE, wavg(new ${pt.primitive}[]{${pt.null},${pt.null},${pt.null}}, new ${pt2.primitive}[]{1,2,3}));

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

        // verify the all-null cases return null
        assertEquals(NULL_DOUBLE, wvar(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));
        assertEquals(NULL_DOUBLE, wvar(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{1, 2, 3}));
        assertEquals(NULL_DOUBLE, wvar(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));

        // verify size==1
        assertEquals(Double.NaN, wvar(new ${pt.primitive}[]{1}, new ${pt2.primitive}[]{4}));

        <#if pt2.valueType.isFloat >
        // verify NaN poisoning
        assertEquals(Double.NaN, wvar(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3,${pt.null},5}), new ${pt2.vectorDirect}(new ${pt2.primitive}[]{4,5,6,Float.NaN,${pt2.null}})));
        </#if>

        // verify the zero-weight case returns null
        assertEquals(Double.NaN, wvar(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{0, 0, 0}));
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

        // verify the all-null cases return null
        assertEquals(NULL_DOUBLE, wstd(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));
        assertEquals(NULL_DOUBLE, wstd(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{1, 2, 3}));
        assertEquals(NULL_DOUBLE, wstd(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));
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

        // verify the all-null cases return null
        assertEquals(NULL_DOUBLE, wste(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));
        assertEquals(NULL_DOUBLE, wste(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{1, 2, 3}));
        assertEquals(NULL_DOUBLE, wste(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));

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

        // verify the all-null cases return null
        assertEquals(NULL_DOUBLE, wtstat(new ${pt.primitive}[]{1, 2, 3}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));
        assertEquals(NULL_DOUBLE, wtstat(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{1, 2, 3}));
        assertEquals(NULL_DOUBLE, wtstat(new ${pt.primitive}[]{${pt.null}, ${pt.null}, ${pt.null}}, new ${pt2.primitive}[]{${pt2.null}, ${pt2.null}, ${pt2.null}}));

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

    public void test${pt.boxed}Compare() {
        final ${pt.primitive} v1 = (${pt.primitive})1.4;
        final ${pt.primitive} v2 = (${pt.primitive})2.3;
        final ${pt.primitive} v3 = ${pt.null};
        final ${pt.primitive} v4 = ${pt.null};
        final ${pt.primitive} v5 = ${pt.boxed}.NaN;
        final ${pt.primitive} v6 = ${pt.boxed}.NaN;

        assertEquals(0, compare(v1, v1));
        assertEquals(0, compare(v2, v2));
        assertEquals(0, compare(v3, v3));
        assertEquals(0, compare(v4, v4));
        assertEquals(0, compare(v5, v5));
        assertEquals(0, compare(v6, v6));

        assertEquals(0, compare(v3, v4));
        assertEquals(0, compare(v4, v3));

        assertEquals(0, compare(v5, v6));
        assertEquals(0, compare(v6, v5));

        assertEquals(-1, compare(v1, v2));
        assertEquals(1, compare(v2, v1));

        assertEquals(1, compare(v1, v3));
        assertEquals(-1, compare(v3, v1));

        assertEquals(-1, compare(v1, v5));
        assertEquals(1, compare(v5, v1));
    }

    public void test${pt.boxed}CompareBoxed() {
        final ${pt.boxed} v1 = (${pt.primitive})1.4;
        final ${pt.boxed} v2 = (${pt.primitive})2.3;
        final ${pt.boxed} v3 = null;
        final ${pt.boxed} v4 = null;
        final ${pt.boxed} v5 = ${pt.boxed}.NaN;
        final ${pt.boxed} v6 = ${pt.boxed}.NaN;

        assertEquals(0, compare(v1, v1));
        assertEquals(0, compare(v2, v2));
        assertEquals(0, compare(v3, v3));
        assertEquals(0, compare(v4, v4));
        assertEquals(0, compare(v5, v5));
        assertEquals(0, compare(v6, v6));

        assertEquals(0, compare(v3, v4));
        assertEquals(0, compare(v4, v3));

        assertEquals(0, compare(v5, v6));
        assertEquals(0, compare(v6, v5));

        assertEquals(-1, compare(v1, v2));
        assertEquals(1, compare(v2, v1));

        assertEquals(1, compare(v1, v3));
        assertEquals(-1, compare(v3, v1));

        assertEquals(-1, compare(v1, v5));
        assertEquals(1, compare(v5, v1));
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

    public void test${pt.boxed}Compare() {
        final ${pt.primitive} v1 = (${pt.primitive})1;
        final ${pt.primitive} v2 = (${pt.primitive})2;
        final ${pt.primitive} v3 = ${pt.null};
        final ${pt.primitive} v4 = ${pt.null};

        assertEquals(0, compare(v1, v1));
        assertEquals(0, compare(v2, v2));
        assertEquals(0, compare(v3, v3));
        assertEquals(0, compare(v4, v4));

        assertEquals(0, compare(v3, v4));
        assertEquals(0, compare(v4, v3));

        assertEquals(-1, compare(v1, v2));
        assertEquals(1, compare(v2, v1));

        assertEquals(1, compare(v1, v3));
        assertEquals(-1, compare(v3, v1));
    }

    public void test${pt.boxed}CompareBoxed() {
        final ${pt.boxed} v1 = (${pt.primitive})1;
        final ${pt.boxed} v2 = (${pt.primitive})2;
        final ${pt.boxed} v3 = null;
        final ${pt.boxed} v4 = null;

        assertEquals(0, compare(v1, v1));
        assertEquals(0, compare(v2, v2));
        assertEquals(0, compare(v3, v3));
        assertEquals(0, compare(v4, v4));

        assertEquals(0, compare(v3, v4));
        assertEquals(0, compare(v4, v3));

        assertEquals(-1, compare(v1, v2));
        assertEquals(1, compare(v2, v1));

        assertEquals(1, compare(v1, v3));
        assertEquals(-1, compare(v3, v1));
    }

    </#if>

    public void test${pt.boxed}Atan2(){
        assertEquals(Math.atan2((${pt.primitive})1, (${pt.primitive})2), atan2((${pt.primitive})1, (${pt.primitive})2));
        assertEquals(NULL_DOUBLE, atan2(${pt.null}, (${pt.primitive})2));
        assertEquals(NULL_DOUBLE, atan2((${pt.primitive})2, ${pt.null}));
    }

    public void test${pt.boxed}Cbrt(){
        assertEquals(Math.cbrt((${pt.primitive})2), cbrt((${pt.primitive})2));
        assertEquals(NULL_DOUBLE, cbrt(${pt.null}));
    }

    public void test${pt.boxed}Cosh(){
        assertEquals(Math.cosh((${pt.primitive})2), cosh((${pt.primitive})2));
        assertEquals(NULL_DOUBLE, cosh(${pt.null}));
    }

    public void test${pt.boxed}Expm1(){
        assertEquals(Math.expm1((${pt.primitive})2), expm1((${pt.primitive})2));
        assertEquals(NULL_DOUBLE, expm1(${pt.null}));
    }

    public void test${pt.boxed}Hypot(){
        assertEquals(Math.hypot(7, 3), hypot((${pt.primitive})7, (${pt.primitive})3));
        assertEquals(NULL_DOUBLE, hypot(${pt.null}, (${pt.primitive})3));
        assertEquals(NULL_DOUBLE, hypot((${pt.primitive})7, ${pt.null}));
    }

    public void test${pt.boxed}Log10(){
        assertEquals(Math.log10(7), log10((${pt.primitive})7));
        assertEquals(NULL_DOUBLE, log10(${pt.null}));
    }

    public void test${pt.boxed}Log1p(){
        assertEquals(Math.log1p(7), log1p((${pt.primitive})7));
        assertEquals(NULL_DOUBLE, log1p(${pt.null}));
    }

    <#if pt.valueType.isFloat >
    public void test${pt.boxed}Scalb(){
        assertEquals(Math.scalb((${pt.primitive})7, 3), scalb((${pt.primitive})7, 3));
        assertEquals(${pt.null}, scalb(${pt.null}, 3));
        assertEquals(${pt.null}, scalb((${pt.primitive})7, NULL_INT));
    }
    </#if>

    public void test${pt.boxed}Sinh(){
        assertEquals(Math.sinh((${pt.primitive})7), sinh((${pt.primitive})7));
        assertEquals(NULL_DOUBLE, sinh(${pt.null}));
    }

    public void test${pt.boxed}Tanh(){
        assertEquals(Math.tanh((${pt.primitive})7), tanh((${pt.primitive})7));
        assertEquals(NULL_DOUBLE, tanh(${pt.null}));
    }

    public void test${pt.boxed}CopySign() {
        assertEquals((${pt.primitive})-9, copySign((${pt.primitive})9, (${pt.primitive})-2));
        assertEquals((${pt.primitive})9, copySign((${pt.primitive})9, (${pt.primitive})2));
        assertEquals((${pt.primitive})9, copySign((${pt.primitive})9, (${pt.primitive})0));
        assertEquals((${pt.primitive})-9, copySign((${pt.primitive})-9, (${pt.primitive})-2));
        assertEquals((${pt.primitive})9, copySign((${pt.primitive})-9, (${pt.primitive})2));
        assertEquals((${pt.primitive})9, copySign((${pt.primitive})-9, (${pt.primitive})0));
        assertEquals((${pt.null}), copySign(${pt.null}, (${pt.primitive})-2));
        assertEquals((${pt.null}), copySign((${pt.primitive})1, ${pt.null}));
    }

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}AddExact(){
        assertEquals((${pt.primitive})3, addExact((${pt.primitive})1, (${pt.primitive})2));
        assertEquals(${pt.null}, addExact(${pt.null}, (${pt.primitive})2));
        assertEquals(${pt.null}, addExact((${pt.primitive})2, ${pt.null}));

        try {
            addExact((${pt.primitive})${pt.maxValue}, (${pt.primitive})1);
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}SubtractExact(){
        assertEquals((${pt.primitive})1, subtractExact((${pt.primitive})3, (${pt.primitive})2));
        assertEquals(${pt.null}, subtractExact(${pt.null}, (${pt.primitive})2));
        assertEquals(${pt.null}, subtractExact((${pt.primitive})2, ${pt.null}));

        try {
        subtractExact((${pt.primitive})${pt.minValue}, (${pt.primitive})1);
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}MultiplyExact(){
        assertEquals((${pt.primitive})6, multiplyExact((${pt.primitive})3, (${pt.primitive})2));
        assertEquals(${pt.null}, multiplyExact(${pt.null}, (${pt.primitive})2));
        assertEquals(${pt.null}, multiplyExact((${pt.primitive})2, ${pt.null}));

        try {
            multiplyExact((${pt.primitive})${pt.maxValue}, (${pt.primitive})2);
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}IncrementExact(){
        assertEquals((${pt.primitive})3, incrementExact((${pt.primitive})2));
        assertEquals(${pt.null}, incrementExact(${pt.null}));

        try {
            incrementExact((${pt.primitive})${pt.maxValue});
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}DecrementExact(){
        assertEquals((${pt.primitive})1, decrementExact((${pt.primitive})2));
        assertEquals(${pt.null}, decrementExact(${pt.null}));

        try {
            decrementExact((${pt.primitive})${pt.minValue});
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}NegateExact(){
        assertEquals(Math.negateExact(7), negateExact((${pt.primitive})7));
        assertEquals(${pt.null}, negateExact(${pt.null}));
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}FloorDiv(){
        assertEquals(Math.floorDiv(7, 2), floorDiv((${pt.primitive})7, (${pt.primitive})2));
        assertEquals(${pt.null}, floorDiv(${pt.null}, (${pt.primitive})2));
        assertEquals(${pt.null}, floorDiv((${pt.primitive})7, ${pt.null}));
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}FloorMod(){
        assertEquals(Math.floorMod(7, 2), floorMod((${pt.primitive})7, (${pt.primitive})2));
        assertEquals(${pt.null}, floorMod(${pt.null}, (${pt.primitive})2));
        assertEquals(${pt.null}, floorMod((${pt.primitive})7, ${pt.null}));
    }
    </#if>

    <#if pt.valueType.isFloat >
    public void test${pt.boxed}GetExponent(){
        assertEquals(Math.getExponent(7), getExponent((${pt.primitive})7));
        assertEquals(NULL_INT, getExponent(${pt.null}));
    }
    </#if>

    <#if pt.valueType.isFloat >
    public void test${pt.boxed}IEEEremainder(){
        assertEquals((${pt.primitive}) Math.IEEEremainder(71, 3), IEEEremainder((${pt.primitive})71, (${pt.primitive})3));
        assertEquals(${pt.null}, IEEEremainder(${pt.null}, (${pt.primitive})3));
        assertEquals(${pt.null}, IEEEremainder((${pt.primitive})71, ${pt.null}));
    }
    </#if>

    <#if pt.valueType.isFloat >
    public void test${pt.boxed}NextAfter(){
        assertEquals(Math.nextAfter((${pt.primitive})7, (${pt.primitive})8), nextAfter((${pt.primitive})7, (${pt.primitive})8));
        assertEquals(Math.nextAfter((${pt.primitive})7, (${pt.primitive})-8), nextAfter((${pt.primitive})7, (${pt.primitive})-8));

        assertEquals(Math.nextUp(${pt.null}), nextAfter(Math.nextDown(${pt.null}), (${pt.primitive})8));
        assertEquals(Math.nextDown(${pt.null}), nextAfter(Math.nextUp(${pt.null}), ${pt.boxed}.NEGATIVE_INFINITY));

        assertEquals(${pt.null}, nextAfter(${pt.null}, (${pt.primitive})8));
        assertEquals(${pt.null}, nextAfter((${pt.primitive}) 7, ${pt.null}));
    }
    </#if>

    <#if pt.valueType.isFloat >
    public void test${pt.boxed}NextUp(){
        assertEquals(Math.nextUp((${pt.primitive})7), nextUp((${pt.primitive})7));
        assertEquals(Math.nextUp(${pt.null}), nextUp(Math.nextDown(${pt.null})));
        assertEquals(${pt.null}, nextUp(${pt.null}));
    }
    </#if>

    <#if pt.valueType.isFloat >
    public void test${pt.boxed}NextDown(){
        assertEquals(Math.nextDown((${pt.primitive})7), nextDown((${pt.primitive})7));
        assertEquals(Math.nextDown(${pt.null}), nextDown(Math.nextUp(${pt.null})));
        assertEquals(${pt.null}, nextDown(${pt.null}));
    }
    </#if>

    public void test${pt.boxed}ToDegrees(){
        assertEquals(Math.toDegrees((${pt.primitive})7), toDegrees((${pt.primitive})7));
        assertEquals(NULL_DOUBLE, toDegrees(${pt.null}));
    }

    public void test${pt.boxed}ToRadians(){
        assertEquals(Math.toRadians((${pt.primitive})7), toRadians((${pt.primitive})7));
        assertEquals(NULL_DOUBLE, toRadians(${pt.null}));
    }

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}ToIntExact(){
        assertEquals(Math.toIntExact((${pt.primitive})7), toIntExact((${pt.primitive})7));
        assertEquals(NULL_INT, toIntExact(${pt.null}));

        <#if pt.primitive == "long" >
        try{
            toIntExact((${pt.primitive})${pt.maxValue});
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
        </#if>
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}ToShortExact(){
        assertEquals((short)7, toShortExact((${pt.primitive})7));
        assertEquals(NULL_SHORT, toShortExact(${pt.null}));

        <#if pt.primitive == "int" || pt.primitive == "long" >
        try{
            toShortExact((${pt.primitive})${pt.maxValue});
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
        </#if>
    }
    </#if>

    <#if pt.valueType.isInteger >
    public void test${pt.boxed}ToByteExact(){
        assertEquals((byte)3, toByteExact((${pt.primitive})3));
        assertEquals(NULL_BYTE, toByteExact(${pt.null}));

        <#if pt.primitive == "short" || pt.primitive == "int" || pt.primitive == "long" >
        try{
            toByteExact((${pt.primitive})${pt.maxValue});
            fail("Overflow");
        } catch(ArithmeticException e){
            // pass
        }
        </#if>
    }
    </#if>

    <#if pt.valueType.isFloat >
    public void test${pt.boxed}Ulp(){
        assertEquals(Math.ulp((${pt.primitive})7), ulp((${pt.primitive})7));
        assertEquals(${pt.null}, ulp(${pt.null}));
    }
    </#if>

    </#if>
    </#list>
}