/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.vector.FloatVector;
import io.deephaven.engine.vector.FloatVectorDirect;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestFloatPrimitives extends BaseArrayTestCase {

    public void testUnbox(){
        assertNull(FloatPrimitives.unbox((Float[])null));
        assertEquals(new float[]{1, NULL_FLOAT, 3, NULL_FLOAT}, FloatPrimitives.unbox((float)1, null, (float)3, NULL_FLOAT));
    }

    public void testIsNull(){
        assertFalse(FloatPrimitives.isNull((float)3));
        assertTrue(FloatPrimitives.isNull(NULL_FLOAT));
    }

    public void testNullToValueScalar() {
        assertEquals((float) 3, FloatPrimitives.nullToValue((float) 3, (float) 7));
        assertEquals((float) 7, FloatPrimitives.nullToValue(NULL_FLOAT, (float) 7));
    }

    public void testNullToValueArray() {
        assertEquals(new float[]{(float) 3, (float) 7, (float) 11}, FloatPrimitives.nullToValue(new FloatVectorDirect(new float[]{(float) 3, NULL_FLOAT, (float) 11}), (float) 7));

        assertEquals(new float[]{(float) 3, (float) 7, (float) 11}, FloatPrimitives.nullToValue(new float[]{(float) 3, NULL_FLOAT, (float) 11}, (float) 7));
    }

    public void testCount(){
        assertEquals(0, FloatPrimitives.count((FloatVector)null));
        assertEquals(3, FloatPrimitives.count(new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals(0, FloatPrimitives.count(new FloatVectorDirect()));
        assertEquals(0, FloatPrimitives.count(new FloatVectorDirect(NULL_FLOAT)));
        assertEquals(2, FloatPrimitives.count(new FloatVectorDirect(new float[]{5,NULL_FLOAT,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60- FloatPrimitives.last(new FloatVectorDirect(new float[]{40,50,60})))==0.0);
        assertEquals(NULL_FLOAT, FloatPrimitives.last(new FloatVectorDirect()));
        assertEquals(NULL_FLOAT, FloatPrimitives.last(new FloatVectorDirect(NULL_FLOAT)));
        assertTrue(Math.abs(15- FloatPrimitives.last(new FloatVectorDirect(new float[]{5,NULL_FLOAT,15})))==0.0);
        assertTrue(Math.abs(40- FloatPrimitives.last(new FloatVectorDirect((float)40)))==0.0);

        assertTrue(Math.abs(60- FloatPrimitives.last(new float[]{40,50,60}))==0.0);
        assertEquals(NULL_FLOAT, FloatPrimitives.last(new float[]{}));
        assertEquals(NULL_FLOAT, FloatPrimitives.last(new float[]{NULL_FLOAT}));
        assertTrue(Math.abs(15- FloatPrimitives.last(new float[]{5,NULL_FLOAT,15}))==0.0);
        assertTrue(Math.abs(40- FloatPrimitives.last(new float[]{(float)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40- FloatPrimitives.first(new FloatVectorDirect(new float[]{40,50,60})))==0.0);
        assertEquals(NULL_FLOAT, FloatPrimitives.first(new FloatVectorDirect()));
        assertEquals(NULL_FLOAT, FloatPrimitives.first(new FloatVectorDirect(NULL_FLOAT)));
        assertTrue(Math.abs(5- FloatPrimitives.first(new FloatVectorDirect(new float[]{5,NULL_FLOAT,15})))==0.0);
        assertTrue(Math.abs(40- FloatPrimitives.first(new FloatVectorDirect((float)40)))==0.0);

        assertTrue(Math.abs(40- FloatPrimitives.first(new float[]{40,50,60}))==0.0);
        assertEquals(NULL_FLOAT, FloatPrimitives.first(new float[]{}));
        assertEquals(NULL_FLOAT, FloatPrimitives.first(new float[]{NULL_FLOAT}));
        assertTrue(Math.abs(5- FloatPrimitives.first(new float[]{5,NULL_FLOAT,15}))==0.0);
        assertTrue(Math.abs(40- FloatPrimitives.first(new float[]{(float)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_FLOAT, FloatPrimitives.nth(-1,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals((float)40, FloatPrimitives.nth(0,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals((float)50, FloatPrimitives.nth(1,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals((float)60, FloatPrimitives.nth(2,new FloatVectorDirect(new float[]{40,50,60})));
        assertEquals(NULL_FLOAT, FloatPrimitives.nth(10,new FloatVectorDirect(new float[]{40,50,60})));

        assertEquals(NULL_FLOAT, FloatPrimitives.nth(-1,new float[]{40,50,60}));
        assertEquals((float)40, FloatPrimitives.nth(0,new float[]{40,50,60}));
        assertEquals((float)50, FloatPrimitives.nth(1,new float[]{40,50,60}));
        assertEquals((float)60, FloatPrimitives.nth(2,new float[]{40,50,60}));
        assertEquals(NULL_FLOAT, FloatPrimitives.nth(10,new float[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, FloatPrimitives.countDistinct((FloatVectorDirect)null));
        assertEquals(NULL_LONG, FloatPrimitives.countDistinct((FloatVectorDirect)null,true));
        assertEquals(0, FloatPrimitives.countDistinct(new FloatVectorDirect(new float[]{})));
        assertEquals(0, FloatPrimitives.countDistinct(new FloatVectorDirect(new float[]{NULL_FLOAT})));
        assertEquals(1, FloatPrimitives.countDistinct(new FloatVectorDirect(new float[]{1})));
        assertEquals(2, FloatPrimitives.countDistinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT})));
        assertEquals(2, FloatPrimitives.countDistinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), false));
        assertEquals(3, FloatPrimitives.countDistinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), true));

        assertEquals(NULL_LONG, FloatPrimitives.countDistinct((float[])null));
        assertEquals(NULL_LONG, FloatPrimitives.countDistinct((float[])null,true));
        assertEquals(0, FloatPrimitives.countDistinct(new float[]{}));
        assertEquals(0, FloatPrimitives.countDistinct(new float[]{NULL_FLOAT}));
        assertEquals(1, FloatPrimitives.countDistinct(new float[]{1}));
        assertEquals(2, FloatPrimitives.countDistinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}));
        assertEquals(2, FloatPrimitives.countDistinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, false));
        assertEquals(3, FloatPrimitives.countDistinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, true));
    }

    public void testDistinct() {
        assertEquals(null, FloatPrimitives.distinct((FloatVectorDirect)null));
        assertEquals(null, FloatPrimitives.distinct((FloatVectorDirect)null, true, true));
        assertEquals(new FloatVectorDirect(), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{})));
        assertEquals(new FloatVectorDirect(), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{NULL_FLOAT})));
        assertEquals(new FloatVectorDirect(new float[]{1}), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{1})));
        assertEquals(new FloatVectorDirect(new float[]{1,2}), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT})));
        assertEquals(new FloatVectorDirect(new float[]{1,2}), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), false, false));
        assertEquals(new FloatVectorDirect(new float[]{1,2,NULL_FLOAT}), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}), true, false));
        assertEquals(new FloatVectorDirect(new float[]{1,2,3}), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{3,1,2,1,NULL_FLOAT,NULL_FLOAT}), false, true));
        assertEquals(new FloatVectorDirect(new float[]{1,2,3,4}), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}), false, true));
        assertEquals(new FloatVectorDirect(new float[]{NULL_FLOAT,1,2,3,4}), FloatPrimitives.distinct(new FloatVectorDirect(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}), true, true));

        assertEquals(null, FloatPrimitives.distinct((float[])null));
        assertEquals(null, FloatPrimitives.distinct((float[])null, true, true));
        assertEquals(new float[]{}, FloatPrimitives.distinct(new float[]{}));
        assertEquals(new float[]{}, FloatPrimitives.distinct(new float[]{NULL_FLOAT}));
        assertEquals(new float[]{1}, FloatPrimitives.distinct(new float[]{1}));
        assertEquals(new float[]{1,2}, FloatPrimitives.distinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}));
        assertEquals(new float[]{1,2}, FloatPrimitives.distinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, false, false));
        assertEquals(new float[]{1,2,NULL_FLOAT}, FloatPrimitives.distinct(new float[]{1,2,1,NULL_FLOAT,NULL_FLOAT}, true, false));
        assertEquals(new float[]{1,2,3}, FloatPrimitives.distinct(new float[]{3,1,2,1,NULL_FLOAT,NULL_FLOAT}, false, true));
        assertEquals(new float[]{1,2,3,4}, FloatPrimitives.distinct(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}, false, true));
        assertEquals(new float[]{NULL_FLOAT,1,2,3,4}, FloatPrimitives.distinct(new float[]{3,1,2,4,1,NULL_FLOAT,NULL_FLOAT}, true, true));
    }

    public void testVec(){
        assertEquals(new float[]{(float)1,(float)3,(float)5}, FloatPrimitives.vec(new FloatVectorDirect((float)1,(float)3,(float)5)));
    }

    public void testArray(){
        assertEquals(new FloatVectorDirect((float)1,(float)3,(float)5), FloatPrimitives.array(new float[]{(float)1,(float)3,(float)5}));
    }

    public void testIn(){
        assertTrue(FloatPrimitives.in((float)1,(float)1,(float)2,(float)3));
        assertFalse(FloatPrimitives.in((float)5,(float)1,(float)2,(float)3));
        assertFalse(FloatPrimitives.in(NULL_FLOAT,(float)1,(float)2,(float)3));
        assertTrue(FloatPrimitives.in(NULL_FLOAT,(float)1,(float)2,NULL_FLOAT,(float)3));
    }

    public void testInRange(){
        assertTrue(FloatPrimitives.inRange((float)2,(float)1,(float)3));
        assertTrue(FloatPrimitives.inRange((float)1,(float)1,(float)3));
        assertFalse(FloatPrimitives.inRange(NULL_FLOAT,(float)1,(float)3));
        assertTrue(FloatPrimitives.inRange((float)3,(float)1,(float)3));
        assertFalse(FloatPrimitives.inRange((float)4,(float)1,(float)3));
    }

    public void testRepeat() {
        assertEquals(new float[]{5,5,5}, FloatPrimitives.repeat((float) 5, 3));
        assertEquals(new float[]{}, FloatPrimitives.repeat((float) 5, -3));
    }

    public void testEnlist() {
        assertEquals(new float[]{1, 11, 6}, FloatPrimitives.enlist((float)1, (float)11, (float)6));
        assertEquals(new float[]{}, FloatPrimitives.enlist((float[])(null)));
    }

    public void testConcat() {
        assertEquals(new float[]{}, FloatPrimitives.concat((float[][])null));
        assertEquals(new float[]{1,2,3,4,5,6}, FloatPrimitives.concat(new float[]{1,2}, new float[]{3}, new float[]{4,5,6}));
        assertEquals(new float[]{}, FloatPrimitives.concat((float[])(null)));

        assertEquals(new float[]{}, FloatPrimitives.concat((FloatVector[])null));
        assertEquals(new float[]{1,2,3,4,5,6}, FloatPrimitives.concat(new FloatVectorDirect(new float[]{1,2}), new FloatVectorDirect(new float[]{3}), new FloatVectorDirect(new float[]{4,5,6})));
        assertEquals(new float[]{}, FloatPrimitives.concat((FloatVector) (null)));
    }

    public void testReverse() {
        assertEquals(new float[]{3,2,1}, FloatPrimitives.reverse((float)1,(float)2,(float)3));
        assertEquals(null, FloatPrimitives.reverse((float[])(null)));

        assertEquals(new float[]{3,2,1}, FloatPrimitives.reverse(new FloatVectorDirect(new float[]{1,2,3})));
        assertEquals(null, FloatPrimitives.reverse((FloatVector) (null)));
    }
}

