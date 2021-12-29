/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;

import static io.deephaven.function.LongPrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestLongPrimitives extends BaseArrayTestCase {

    public void testUnbox(){
        assertNull(unbox((Long[])null));
        assertEquals(new long[]{1, NULL_LONG, 3, NULL_LONG}, unbox((long)1, null, (long)3, NULL_LONG));
    }

    public void testIsNull(){
        assertFalse(isNull((long)3));
        assertTrue(isNull(NULL_LONG));
    }

    public void testNullToValueScalar() {
        assertEquals((long) 3, nullToValue((long) 3, (long) 7));
        assertEquals((long) 7, nullToValue(NULL_LONG, (long) 7));
    }

    public void testNullToValueArray() {
        assertEquals(new long[]{(long) 3, (long) 7, (long) 11}, nullToValue(new LongVectorDirect(new long[]{(long) 3, NULL_LONG, (long) 11}), (long) 7));

        assertEquals(new long[]{(long) 3, (long) 7, (long) 11}, nullToValue(new long[]{(long) 3, NULL_LONG, (long) 11}, (long) 7));
    }

    public void testCount(){
        assertEquals(0, count((LongVector)null));
        assertEquals(3,count(new LongVectorDirect(new long[]{40,50,60})));
        assertEquals(0,count(new LongVectorDirect()));
        assertEquals(0,count(new LongVectorDirect(NULL_LONG)));
        assertEquals(2,count(new LongVectorDirect(new long[]{5,NULL_LONG,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60-last(new LongVectorDirect(new long[]{40,50,60})))==0.0);
        assertEquals(NULL_LONG,last(new LongVectorDirect()));
        assertEquals(NULL_LONG,last(new LongVectorDirect(NULL_LONG)));
        assertTrue(Math.abs(15-last(new LongVectorDirect(new long[]{5,NULL_LONG,15})))==0.0);
        assertTrue(Math.abs(40-last(new LongVectorDirect((long)40)))==0.0);

        assertTrue(Math.abs(60-last(new long[]{40,50,60}))==0.0);
        assertEquals(NULL_LONG,last(new long[]{}));
        assertEquals(NULL_LONG,last(new long[]{NULL_LONG}));
        assertTrue(Math.abs(15-last(new long[]{5,NULL_LONG,15}))==0.0);
        assertTrue(Math.abs(40-last(new long[]{(long)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40-first(new LongVectorDirect(new long[]{40,50,60})))==0.0);
        assertEquals(NULL_LONG,first(new LongVectorDirect()));
        assertEquals(NULL_LONG,first(new LongVectorDirect(NULL_LONG)));
        assertTrue(Math.abs(5-first(new LongVectorDirect(new long[]{5,NULL_LONG,15})))==0.0);
        assertTrue(Math.abs(40-first(new LongVectorDirect((long)40)))==0.0);

        assertTrue(Math.abs(40-first(new long[]{40,50,60}))==0.0);
        assertEquals(NULL_LONG,first(new long[]{}));
        assertEquals(NULL_LONG,first(new long[]{NULL_LONG}));
        assertTrue(Math.abs(5-first(new long[]{5,NULL_LONG,15}))==0.0);
        assertTrue(Math.abs(40-first(new long[]{(long)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_LONG, nth(-1,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals((long)40, nth(0,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals((long)50, nth(1,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals((long)60, nth(2,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals(NULL_LONG, nth(10,new LongVectorDirect(new long[]{40,50,60})));

        assertEquals(NULL_LONG, nth(-1,new long[]{40,50,60}));
        assertEquals((long)40, nth(0,new long[]{40,50,60}));
        assertEquals((long)50, nth(1,new long[]{40,50,60}));
        assertEquals((long)60, nth(2,new long[]{40,50,60}));
        assertEquals(NULL_LONG, nth(10,new long[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((LongVectorDirect)null));
        assertEquals(NULL_LONG, countDistinct((LongVectorDirect)null,true));
        assertEquals(0, countDistinct(new LongVectorDirect(new long[]{})));
        assertEquals(0, countDistinct(new LongVectorDirect(new long[]{NULL_LONG})));
        assertEquals(1, countDistinct(new LongVectorDirect(new long[]{1})));
        assertEquals(2, countDistinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG})));
        assertEquals(2, countDistinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), false));
        assertEquals(3, countDistinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), true));

        assertEquals(NULL_LONG, countDistinct((long[])null));
        assertEquals(NULL_LONG, countDistinct((long[])null,true));
        assertEquals(0, countDistinct(new long[]{}));
        assertEquals(0, countDistinct(new long[]{NULL_LONG}));
        assertEquals(1, countDistinct(new long[]{1}));
        assertEquals(2, countDistinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}));
        assertEquals(2, countDistinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, false));
        assertEquals(3, countDistinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, true));
    }

    public void testDistinct() {
        assertEquals(null, distinct((LongVectorDirect)null));
        assertEquals(null, distinct((LongVectorDirect)null, true, true));
        assertEquals(new LongVectorDirect(), distinct(new LongVectorDirect(new long[]{})));
        assertEquals(new LongVectorDirect(), distinct(new LongVectorDirect(new long[]{NULL_LONG})));
        assertEquals(new LongVectorDirect(new long[]{1}), distinct(new LongVectorDirect(new long[]{1})));
        assertEquals(new LongVectorDirect(new long[]{1,2}), distinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG})));
        assertEquals(new LongVectorDirect(new long[]{1,2}), distinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), false, false));
        assertEquals(new LongVectorDirect(new long[]{1,2,NULL_LONG}), distinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), true, false));
        assertEquals(new LongVectorDirect(new long[]{1,2,3}), distinct(new LongVectorDirect(new long[]{3,1,2,1,NULL_LONG,NULL_LONG}), false, true));
        assertEquals(new LongVectorDirect(new long[]{1,2,3,4}), distinct(new LongVectorDirect(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}), false, true));
        assertEquals(new LongVectorDirect(new long[]{NULL_LONG,1,2,3,4}), distinct(new LongVectorDirect(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}), true, true));

        assertEquals(null, distinct((long[])null));
        assertEquals(null, distinct((long[])null, true, true));
        assertEquals(new long[]{}, distinct(new long[]{}));
        assertEquals(new long[]{}, distinct(new long[]{NULL_LONG}));
        assertEquals(new long[]{1}, distinct(new long[]{1}));
        assertEquals(new long[]{1,2}, distinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}));
        assertEquals(new long[]{1,2}, distinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, false, false));
        assertEquals(new long[]{1,2,NULL_LONG}, distinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, true, false));
        assertEquals(new long[]{1,2,3}, distinct(new long[]{3,1,2,1,NULL_LONG,NULL_LONG}, false, true));
        assertEquals(new long[]{1,2,3,4}, distinct(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}, false, true));
        assertEquals(new long[]{NULL_LONG,1,2,3,4}, distinct(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}, true, true));
    }

    public void testVec(){
        assertEquals(new long[]{(long)1,(long)3,(long)5}, vec(new LongVectorDirect((long)1,(long)3,(long)5)));
    }

    public void testArray(){
        assertEquals(new LongVectorDirect((long)1,(long)3,(long)5), array(new long[]{(long)1,(long)3,(long)5}));
    }

    public void testIn(){
        assertTrue(in((long)1,(long)1,(long)2,(long)3));
        assertFalse(in((long)5,(long)1,(long)2,(long)3));
        assertFalse(in(NULL_LONG,(long)1,(long)2,(long)3));
        assertTrue(in(NULL_LONG,(long)1,(long)2,NULL_LONG,(long)3));
    }

    public void testInRange(){
        assertTrue(inRange((long)2,(long)1,(long)3));
        assertTrue(inRange((long)1,(long)1,(long)3));
        assertFalse(inRange(NULL_LONG,(long)1,(long)3));
        assertTrue(inRange((long)3,(long)1,(long)3));
        assertFalse(inRange((long)4,(long)1,(long)3));
    }

    public void testRepeat() {
        assertEquals(new long[]{5,5,5}, repeat((long) 5, 3));
        assertEquals(new long[]{}, repeat((long) 5, -3));
    }

    public void testEnlist() {
        assertEquals(new long[]{1, 11, 6}, enlist((long)1, (long)11, (long)6));
        assertEquals(new long[]{}, enlist((long[])(null)));
    }

    public void testConcat() {
        assertEquals(new long[]{}, concat((long[][])null));
        assertEquals(new long[]{1,2,3,4,5,6}, concat(new long[]{1,2}, new long[]{3}, new long[]{4,5,6}));
        assertEquals(new long[]{}, concat((long[])(null)));

        assertEquals(new long[]{}, concat((LongVector[])null));
        assertEquals(new long[]{1,2,3,4,5,6}, concat(new LongVectorDirect(new long[]{1,2}), new LongVectorDirect(new long[]{3}), new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(new long[]{}, concat((LongVector) (null)));
    }

    public void testReverse() {
        assertEquals(new long[]{3,2,1}, reverse((long)1,(long)2,(long)3));
        assertEquals(null, reverse((long[])(null)));

        assertEquals(new long[]{3,2,1}, reverse(new LongVectorDirect(new long[]{1,2,3})));
        assertEquals(null, reverse((LongVector) (null)));
    }
}

