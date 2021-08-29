/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.tables.dbarrays.DbDoubleArray;
import io.deephaven.engine.tables.dbarrays.DbDoubleArrayDirect;

import static io.deephaven.libs.primitives.DoublePrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestDoublePrimitives extends BaseArrayTestCase {

    public void testUnbox(){
        assertNull(unbox((Double[])null));
        assertEquals(new double[]{1, NULL_DOUBLE, 3, NULL_DOUBLE}, unbox((double)1, null, (double)3, NULL_DOUBLE));
    }

    public void testIsNull(){
        assertFalse(isNull((double)3));
        assertTrue(isNull(NULL_DOUBLE));
    }

    public void testNullToValueScalar() {
        assertEquals((double) 3, nullToValue((double) 3, (double) 7));
        assertEquals((double) 7, nullToValue(NULL_DOUBLE, (double) 7));
    }

    public void testNullToValueArray() {
        assertEquals(new double[]{(double) 3, (double) 7, (double) 11}, nullToValue(new DbDoubleArrayDirect(new double[]{(double) 3, NULL_DOUBLE, (double) 11}), (double) 7));

        assertEquals(new double[]{(double) 3, (double) 7, (double) 11}, nullToValue(new double[]{(double) 3, NULL_DOUBLE, (double) 11}, (double) 7));
    }

    public void testCount(){
        assertEquals(0, count((DbDoubleArray)null));
        assertEquals(3,count(new DbDoubleArrayDirect(new double[]{40,50,60})));
        assertEquals(0,count(new DbDoubleArrayDirect()));
        assertEquals(0,count(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertEquals(2,count(new DbDoubleArrayDirect(new double[]{5,NULL_DOUBLE,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60-last(new DbDoubleArrayDirect(new double[]{40,50,60})))==0.0);
        assertEquals(NULL_DOUBLE,last(new DbDoubleArrayDirect()));
        assertEquals(NULL_DOUBLE,last(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertTrue(Math.abs(15-last(new DbDoubleArrayDirect(new double[]{5,NULL_DOUBLE,15})))==0.0);
        assertTrue(Math.abs(40-last(new DbDoubleArrayDirect((double)40)))==0.0);

        assertTrue(Math.abs(60-last(new double[]{40,50,60}))==0.0);
        assertEquals(NULL_DOUBLE,last(new double[]{}));
        assertEquals(NULL_DOUBLE,last(new double[]{NULL_DOUBLE}));
        assertTrue(Math.abs(15-last(new double[]{5,NULL_DOUBLE,15}))==0.0);
        assertTrue(Math.abs(40-last(new double[]{(double)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40-first(new DbDoubleArrayDirect(new double[]{40,50,60})))==0.0);
        assertEquals(NULL_DOUBLE,first(new DbDoubleArrayDirect()));
        assertEquals(NULL_DOUBLE,first(new DbDoubleArrayDirect(NULL_DOUBLE)));
        assertTrue(Math.abs(5-first(new DbDoubleArrayDirect(new double[]{5,NULL_DOUBLE,15})))==0.0);
        assertTrue(Math.abs(40-first(new DbDoubleArrayDirect((double)40)))==0.0);

        assertTrue(Math.abs(40-first(new double[]{40,50,60}))==0.0);
        assertEquals(NULL_DOUBLE,first(new double[]{}));
        assertEquals(NULL_DOUBLE,first(new double[]{NULL_DOUBLE}));
        assertTrue(Math.abs(5-first(new double[]{5,NULL_DOUBLE,15}))==0.0);
        assertTrue(Math.abs(40-first(new double[]{(double)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_DOUBLE, nth(-1,new DbDoubleArrayDirect(new double[]{40,50,60})));
        assertEquals((double)40, nth(0,new DbDoubleArrayDirect(new double[]{40,50,60})));
        assertEquals((double)50, nth(1,new DbDoubleArrayDirect(new double[]{40,50,60})));
        assertEquals((double)60, nth(2,new DbDoubleArrayDirect(new double[]{40,50,60})));
        assertEquals(NULL_DOUBLE, nth(10,new DbDoubleArrayDirect(new double[]{40,50,60})));

        assertEquals(NULL_DOUBLE, nth(-1,new double[]{40,50,60}));
        assertEquals((double)40, nth(0,new double[]{40,50,60}));
        assertEquals((double)50, nth(1,new double[]{40,50,60}));
        assertEquals((double)60, nth(2,new double[]{40,50,60}));
        assertEquals(NULL_DOUBLE, nth(10,new double[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((DbDoubleArrayDirect)null));
        assertEquals(NULL_LONG, countDistinct((DbDoubleArrayDirect)null,true));
        assertEquals(0, countDistinct(new DbDoubleArrayDirect(new double[]{})));
        assertEquals(0, countDistinct(new DbDoubleArrayDirect(new double[]{NULL_DOUBLE})));
        assertEquals(1, countDistinct(new DbDoubleArrayDirect(new double[]{1})));
        assertEquals(2, countDistinct(new DbDoubleArrayDirect(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE})));
        assertEquals(2, countDistinct(new DbDoubleArrayDirect(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}), false));
        assertEquals(3, countDistinct(new DbDoubleArrayDirect(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}), true));

        assertEquals(NULL_LONG, countDistinct((double[])null));
        assertEquals(NULL_LONG, countDistinct((double[])null,true));
        assertEquals(0, countDistinct(new double[]{}));
        assertEquals(0, countDistinct(new double[]{NULL_DOUBLE}));
        assertEquals(1, countDistinct(new double[]{1}));
        assertEquals(2, countDistinct(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}));
        assertEquals(2, countDistinct(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}, false));
        assertEquals(3, countDistinct(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}, true));
    }

    public void testDistinct() {
        assertEquals(null, distinct((DbDoubleArrayDirect)null));
        assertEquals(null, distinct((DbDoubleArrayDirect)null, true, true));
        assertEquals(new DbDoubleArrayDirect(), distinct(new DbDoubleArrayDirect(new double[]{})));
        assertEquals(new DbDoubleArrayDirect(), distinct(new DbDoubleArrayDirect(new double[]{NULL_DOUBLE})));
        assertEquals(new DbDoubleArrayDirect(new double[]{1}), distinct(new DbDoubleArrayDirect(new double[]{1})));
        assertEquals(new DbDoubleArrayDirect(new double[]{1,2}), distinct(new DbDoubleArrayDirect(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE})));
        assertEquals(new DbDoubleArrayDirect(new double[]{1,2}), distinct(new DbDoubleArrayDirect(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}), false, false));
        assertEquals(new DbDoubleArrayDirect(new double[]{1,2,NULL_DOUBLE}), distinct(new DbDoubleArrayDirect(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}), true, false));
        assertEquals(new DbDoubleArrayDirect(new double[]{1,2,3}), distinct(new DbDoubleArrayDirect(new double[]{3,1,2,1,NULL_DOUBLE,NULL_DOUBLE}), false, true));
        assertEquals(new DbDoubleArrayDirect(new double[]{1,2,3,4}), distinct(new DbDoubleArrayDirect(new double[]{3,1,2,4,1,NULL_DOUBLE,NULL_DOUBLE}), false, true));
        assertEquals(new DbDoubleArrayDirect(new double[]{NULL_DOUBLE,1,2,3,4}), distinct(new DbDoubleArrayDirect(new double[]{3,1,2,4,1,NULL_DOUBLE,NULL_DOUBLE}), true, true));

        assertEquals(null, distinct((double[])null));
        assertEquals(null, distinct((double[])null, true, true));
        assertEquals(new double[]{}, distinct(new double[]{}));
        assertEquals(new double[]{}, distinct(new double[]{NULL_DOUBLE}));
        assertEquals(new double[]{1}, distinct(new double[]{1}));
        assertEquals(new double[]{1,2}, distinct(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}));
        assertEquals(new double[]{1,2}, distinct(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}, false, false));
        assertEquals(new double[]{1,2,NULL_DOUBLE}, distinct(new double[]{1,2,1,NULL_DOUBLE,NULL_DOUBLE}, true, false));
        assertEquals(new double[]{1,2,3}, distinct(new double[]{3,1,2,1,NULL_DOUBLE,NULL_DOUBLE}, false, true));
        assertEquals(new double[]{1,2,3,4}, distinct(new double[]{3,1,2,4,1,NULL_DOUBLE,NULL_DOUBLE}, false, true));
        assertEquals(new double[]{NULL_DOUBLE,1,2,3,4}, distinct(new double[]{3,1,2,4,1,NULL_DOUBLE,NULL_DOUBLE}, true, true));
    }

    public void testVec(){
        assertEquals(new double[]{(double)1,(double)3,(double)5}, vec(new DbDoubleArrayDirect((double)1,(double)3,(double)5)));
    }

    public void testArray(){
        assertEquals(new DbDoubleArrayDirect((double)1,(double)3,(double)5), array(new double[]{(double)1,(double)3,(double)5}));
    }

    public void testIn(){
        assertTrue(in((double)1,(double)1,(double)2,(double)3));
        assertFalse(in((double)5,(double)1,(double)2,(double)3));
        assertFalse(in(NULL_DOUBLE,(double)1,(double)2,(double)3));
        assertTrue(in(NULL_DOUBLE,(double)1,(double)2,NULL_DOUBLE,(double)3));
    }

    public void testInRange(){
        assertTrue(inRange((double)2,(double)1,(double)3));
        assertTrue(inRange((double)1,(double)1,(double)3));
        assertFalse(inRange(NULL_DOUBLE,(double)1,(double)3));
        assertTrue(inRange((double)3,(double)1,(double)3));
        assertFalse(inRange((double)4,(double)1,(double)3));
    }

    public void testRepeat() {
        assertEquals(new double[]{5,5,5}, repeat((double) 5, 3));
        assertEquals(new double[]{}, repeat((double) 5, -3));
    }

    public void testEnlist() {
        assertEquals(new double[]{1, 11, 6}, enlist((double)1, (double)11, (double)6));
        assertEquals(new double[]{}, enlist((double[])(null)));
    }

    public void testConcat() {
        assertEquals(new double[]{}, concat((double[][])null));
        assertEquals(new double[]{1,2,3,4,5,6}, concat(new double[]{1,2}, new double[]{3}, new double[]{4,5,6}));
        assertEquals(new double[]{}, concat((double[])(null)));

        assertEquals(new double[]{}, concat((DbDoubleArray[])null));
        assertEquals(new double[]{1,2,3,4,5,6}, concat(new DbDoubleArrayDirect(new double[]{1,2}), new DbDoubleArrayDirect(new double[]{3}), new DbDoubleArrayDirect(new double[]{4,5,6})));
        assertEquals(new double[]{}, concat((DbDoubleArray) (null)));
    }

    public void testReverse() {
        assertEquals(new double[]{3,2,1}, reverse((double)1,(double)2,(double)3));
        assertEquals(null, reverse((double[])(null)));

        assertEquals(new double[]{3,2,1}, reverse(new DbDoubleArrayDirect(new double[]{1,2,3})));
        assertEquals(null, reverse((DbDoubleArray) (null)));
    }
}

