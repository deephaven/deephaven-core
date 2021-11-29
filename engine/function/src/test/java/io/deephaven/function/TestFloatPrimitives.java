/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;

import static io.deephaven.function.FloatPrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestFloatPrimitives extends BaseArrayTestCase {

    public void testUnbox(){
        assertNull(unbox((Float[])null));
        assertEquals(new float[]{1, NULL_FLOAT, 3, NULL_FLOAT}, unbox((float)1, null, (float)3, NULL_FLOAT));
    }

    public void testIsNull(){
        assertFalse(isNull((float)3));
        assertTrue(isNull(NULL_FLOAT));
    }

    public void testNullToValueScalar() {
        assertEquals((float) 3, nullToValue((float) 3, (float) 7));
        assertEquals((float) 7, nullToValue(NULL_FLOAT, (float) 7));
    }

    public void testNullToValueArray() {
        assertEquals(new float[]{(float) 3, (float) 7, (float) 11}, nullToValue(new FloatVectorDirect(new float[]{(float) 3, NULL_FLOAT, (float) 11}), (float) 7));

        assertEquals(new float[]{(float) 3, (float) 7, (float) 11}, nullToValue(new float[]{(float) 3, NULL_FLOAT, (float) 11}, (float) 7));
    }

    public void testCount(){
        assertEquals(0, count((FloatVector)null));
        assertEquals(3,count(new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals(0,count(new FloatVectorDirect()));
        assertEquals(0,count(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(2,count(new FloatVectorDirect(new float[]{5,NULL_FLOAT,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60-last(new FloatVectorDirect(new float[]{40,50,60})))==0.0);
        assertEquals(NULL_FLOAT,last(new FloatVectorDirect()));
        assertEquals(NULL_FLOAT,last(new FloatVectorDirect(NULL_FLOAT)));
        assertTrue(Math.abs(15-last(new FloatVectorDirect(new float[]{5,NULL_FLOAT,15})))==0.0);
        assertTrue(Math.abs(40-last(new FloatVectorDirect((float)40)))==0.0);

        assertTrue(Math.abs(60-last(new float[]{40,50,60}))==0.0);
        assertEquals(NULL_FLOAT,last(new float[]{}));
        assertEquals(NULL_FLOAT,last(new float[]{NULL_FLOAT}));
        assertTrue(Math.abs(15-last(new float[]{5,NULL_FLOAT,15}))==0.0);
        assertTrue(Math.abs(40-last(new float[]{(float)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40-first(new FloatVectorDirect(new float[]{40,50,60})))==0.0);
        assertEquals(NULL_FLOAT,first(new FloatVectorDirect()));
        assertEquals(NULL_FLOAT,first(new FloatVectorDirect(NULL_FLOAT)));
        assertTrue(Math.abs(5-first(new FloatVectorDirect(new float[]{5,NULL_FLOAT,15})))==0.0);
        assertTrue(Math.abs(40-first(new FloatVectorDirect((float)40)))==0.0);

        assertTrue(Math.abs(40-first(new float[]{40,50,60}))==0.0);
        assertEquals(NULL_FLOAT,first(new float[]{}));
        assertEquals(NULL_FLOAT,first(new float[]{NULL_FLOAT}));
        assertTrue(Math.abs(5-first(new float[]{5,NULL_FLOAT,15}))==0.0);
        assertTrue(Math.abs(40-first(new float[]{(float)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_FLOAT, nth(-1,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals((float)40, nth(0,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals((float)50, nth(1,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals((float)60, nth(2,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals(NULL_FLOAT, nth(10,new FloatVectorDirect(new float[]{40,50,60})));

        assertEquals(NULL_FLOAT, nth(-1,new float[]{40,50,60}));
        assertEquals((float)40, nth(0,new float[]{40,50,60}));
        assertEquals((float)50, nth(1,new float[]{40,50,60}));
        assertEquals((float)60, nth(2,new float[]{40,50,60}));
        assertEquals(NULL_FLOAT, nth(10,new float[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((FloatVectorDirect)null));
        assertEquals(NULL_LONG, countDistinct((FloatVectorDirect)null,true));
        assertEquals(0, countDistinct(new FloatVectorDirect(new float[]{})));
        assertEquals(0, countDistinct(new FloatVectorDirect(new float[]{NULL_FLOAT})));
        assertEquals(1, countDistinct(new FloatVectorDirect(new float[]{1})));
        assertEquals(2, countDistinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT})));
        assertEquals(2, countDistinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), false));
        assertEquals(3, countDistinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), true));

        assertEquals(NULL_LONG, countDistinct((float[])null));
        assertEquals(NULL_LONG, countDistinct((float[])null,true));
        assertEquals(0, countDistinct(new float[]{}));
        assertEquals(0, countDistinct(new float[]{NULL_FLOAT}));
        assertEquals(1, countDistinct(new float[]{1}));
        assertEquals(2, countDistinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}));
        assertEquals(2, countDistinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, false));
        assertEquals(3, countDistinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, true));
    }

    public void testDistinct() {
        assertEquals(null, distinct((FloatVectorDirect)null));
        assertEquals(null, distinct((FloatVectorDirect)null, true, true));
        assertEquals(new FloatVectorDirect(), distinct(new FloatVectorDirect(new float[]{})));
        assertEquals(new FloatVectorDirect(), distinct(new FloatVectorDirect(new float[]{NULL_FLOAT})));
        assertEquals(new FloatVectorDirect(new float[]{1}), distinct(new FloatVectorDirect(new float[]{1})));
        assertEquals(new FloatVectorDirect(new float[]{1,2}), distinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT})));
        assertEquals(new FloatVectorDirect(new float[]{1,2}), distinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), false, false));
        assertEquals(new FloatVectorDirect(new float[]{1,2,NULL_FLOAT}), distinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), true, false));
        assertEquals(new FloatVectorDirect(new float[]{1,2,3}), distinct(new FloatVectorDirect(new float[]{3,1,2,1,NULL_FLOAT,NULL_FLOAT}), false, true));
        assertEquals(new FloatVectorDirect(new float[]{1,2,3,4}), distinct(new FloatVectorDirect(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}), false, true));
        assertEquals(new FloatVectorDirect(new float[]{NULL_FLOAT,1,2,3,4}), distinct(new FloatVectorDirect(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}), true, true));

        assertEquals(null, distinct((float[])null));
        assertEquals(null, distinct((float[])null, true, true));
        assertEquals(new float[]{}, distinct(new float[]{}));
        assertEquals(new float[]{}, distinct(new float[]{NULL_FLOAT}));
        assertEquals(new float[]{1}, distinct(new float[]{1}));
        assertEquals(new float[]{1,2}, distinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}));
        assertEquals(new float[]{1,2}, distinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, false, false));
        assertEquals(new float[]{1,2,NULL_FLOAT}, distinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, true, false));
        assertEquals(new float[]{1,2,3}, distinct(new float[]{3,1,2,1,NULL_FLOAT,NULL_FLOAT}, false, true));
        assertEquals(new float[]{1,2,3,4}, distinct(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}, false, true));
        assertEquals(new float[]{NULL_FLOAT,1,2,3,4}, distinct(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}, true, true));
    }

    public void testVec(){
        assertEquals(new float[]{(float)1,(float)3,(float)5}, vec(new FloatVectorDirect((float)1,(float)3,(float)5)));
    }

    public void testArray(){
        assertEquals(new FloatVectorDirect((float)1,(float)3,(float)5), array(new float[]{(float)1,(float)3,(float)5}));
    }

    public void testIn(){
        assertTrue(in((float)1,(float)1,(float)2,(float)3));
        assertFalse(in((float)5,(float)1,(float)2,(float)3));
        assertFalse(in(NULL_FLOAT,(float)1,(float)2,(float)3));
        assertTrue(in(NULL_FLOAT,(float)1,(float)2,NULL_FLOAT,(float)3));
    }

    public void testInRange(){
        assertTrue(inRange((float)2,(float)1,(float)3));
        assertTrue(inRange((float)1,(float)1,(float)3));
        assertFalse(inRange(NULL_FLOAT,(float)1,(float)3));
        assertTrue(inRange((float)3,(float)1,(float)3));
        assertFalse(inRange((float)4,(float)1,(float)3));
    }

    public void testRepeat() {
        assertEquals(new float[]{5,5,5}, repeat((float) 5, 3));
        assertEquals(new float[]{}, repeat((float) 5, -3));
    }

    public void testEnlist() {
        assertEquals(new float[]{1, 11, 6}, enlist((float)1, (float)11, (float)6));
        assertEquals(new float[]{}, enlist((float[])(null)));
    }

    public void testConcat() {
        assertEquals(new float[]{}, concat((float[][])null));
        assertEquals(new float[]{1,2,3,4,5,6}, concat(new float[]{1,2}, new float[]{3}, new float[]{4,5,6}));
        assertEquals(new float[]{}, concat((float[])(null)));

        assertEquals(new float[]{}, concat((FloatVector[])null));
        assertEquals(new float[]{1,2,3,4,5,6}, concat(new FloatVectorDirect(new float[]{1,2}), new FloatVectorDirect(new float[]{3}), new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(new float[]{}, concat((FloatVector) (null)));
    }

    public void testReverse() {
        assertEquals(new float[]{3,2,1}, reverse((float)1,(float)2,(float)3));
        assertEquals(null, reverse((float[])(null)));

        assertEquals(new float[]{3,2,1}, reverse(new FloatVectorDirect(new float[]{1,2,3})));
        assertEquals(null, reverse((FloatVector) (null)));
    }
}

