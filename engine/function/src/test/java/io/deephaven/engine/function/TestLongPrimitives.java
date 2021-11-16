/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.vector.LongVector;
import io.deephaven.engine.vector.LongVectorDirect;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestLongPrimitives extends BaseArrayTestCase {

    public void testUnbox(){
        assertNull(LongPrimitives.unbox((Long[])null));
        assertEquals(new long[]{1, NULL_LONG, 3, NULL_LONG}, LongPrimitives.unbox((long)1, null, (long)3, NULL_LONG));
    }

    public void testIsNull(){
        assertFalse(LongPrimitives.isNull((long)3));
        assertTrue(LongPrimitives.isNull(NULL_LONG));
    }

    public void testNullToValueScalar() {
        assertEquals((long) 3, LongPrimitives.nullToValue((long) 3, (long) 7));
        assertEquals((long) 7, LongPrimitives.nullToValue(NULL_LONG, (long) 7));
    }

    public void testNullToValueArray() {
        assertEquals(new long[]{(long) 3, (long) 7, (long) 11}, LongPrimitives.nullToValue(new LongVectorDirect(new long[]{(long) 3, NULL_LONG, (long) 11}), (long) 7));

        assertEquals(new long[]{(long) 3, (long) 7, (long) 11}, LongPrimitives.nullToValue(new long[]{(long) 3, NULL_LONG, (long) 11}, (long) 7));
    }

    public void testCount(){
        assertEquals(0, LongPrimitives.count((LongVector)null));
        assertEquals(3, LongPrimitives.count(new LongVectorDirect(new long[]{40,50,60})));
        assertEquals(0, LongPrimitives.count(new LongVectorDirect()));
        assertEquals(0, LongPrimitives.count(new LongVectorDirect(NULL_LONG)));
        assertEquals(2, LongPrimitives.count(new LongVectorDirect(new long[]{5,NULL_LONG,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60- LongPrimitives.last(new LongVectorDirect(new long[]{40,50,60})))==0.0);
        assertEquals(NULL_LONG, LongPrimitives.last(new LongVectorDirect()));
        assertEquals(NULL_LONG, LongPrimitives.last(new LongVectorDirect(NULL_LONG)));
        assertTrue(Math.abs(15- LongPrimitives.last(new LongVectorDirect(new long[]{5,NULL_LONG,15})))==0.0);
        assertTrue(Math.abs(40- LongPrimitives.last(new LongVectorDirect((long)40)))==0.0);

        assertTrue(Math.abs(60- LongPrimitives.last(new long[]{40,50,60}))==0.0);
        assertEquals(NULL_LONG, LongPrimitives.last(new long[]{}));
        assertEquals(NULL_LONG, LongPrimitives.last(new long[]{NULL_LONG}));
        assertTrue(Math.abs(15- LongPrimitives.last(new long[]{5,NULL_LONG,15}))==0.0);
        assertTrue(Math.abs(40- LongPrimitives.last(new long[]{(long)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40- LongPrimitives.first(new LongVectorDirect(new long[]{40,50,60})))==0.0);
        assertEquals(NULL_LONG, LongPrimitives.first(new LongVectorDirect()));
        assertEquals(NULL_LONG, LongPrimitives.first(new LongVectorDirect(NULL_LONG)));
        assertTrue(Math.abs(5- LongPrimitives.first(new LongVectorDirect(new long[]{5,NULL_LONG,15})))==0.0);
        assertTrue(Math.abs(40- LongPrimitives.first(new LongVectorDirect((long)40)))==0.0);

        assertTrue(Math.abs(40- LongPrimitives.first(new long[]{40,50,60}))==0.0);
        assertEquals(NULL_LONG, LongPrimitives.first(new long[]{}));
        assertEquals(NULL_LONG, LongPrimitives.first(new long[]{NULL_LONG}));
        assertTrue(Math.abs(5- LongPrimitives.first(new long[]{5,NULL_LONG,15}))==0.0);
        assertTrue(Math.abs(40- LongPrimitives.first(new long[]{(long)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_LONG, LongPrimitives.nth(-1,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals((long)40, LongPrimitives.nth(0,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals((long)50, LongPrimitives.nth(1,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals((long)60, LongPrimitives.nth(2,new LongVectorDirect(new long[]{40,50,60})));
        assertEquals(NULL_LONG, LongPrimitives.nth(10,new LongVectorDirect(new long[]{40,50,60})));

        assertEquals(NULL_LONG, LongPrimitives.nth(-1,new long[]{40,50,60}));
        assertEquals((long)40, LongPrimitives.nth(0,new long[]{40,50,60}));
        assertEquals((long)50, LongPrimitives.nth(1,new long[]{40,50,60}));
        assertEquals((long)60, LongPrimitives.nth(2,new long[]{40,50,60}));
        assertEquals(NULL_LONG, LongPrimitives.nth(10,new long[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, LongPrimitives.countDistinct((LongVectorDirect)null));
        assertEquals(NULL_LONG, LongPrimitives.countDistinct((LongVectorDirect)null,true));
        assertEquals(0, LongPrimitives.countDistinct(new LongVectorDirect(new long[]{})));
        assertEquals(0, LongPrimitives.countDistinct(new LongVectorDirect(new long[]{NULL_LONG})));
        assertEquals(1, LongPrimitives.countDistinct(new LongVectorDirect(new long[]{1})));
        assertEquals(2, LongPrimitives.countDistinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG})));
        assertEquals(2, LongPrimitives.countDistinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), false));
        assertEquals(3, LongPrimitives.countDistinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), true));

        assertEquals(NULL_LONG, LongPrimitives.countDistinct((long[])null));
        assertEquals(NULL_LONG, LongPrimitives.countDistinct((long[])null,true));
        assertEquals(0, LongPrimitives.countDistinct(new long[]{}));
        assertEquals(0, LongPrimitives.countDistinct(new long[]{NULL_LONG}));
        assertEquals(1, LongPrimitives.countDistinct(new long[]{1}));
        assertEquals(2, LongPrimitives.countDistinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}));
        assertEquals(2, LongPrimitives.countDistinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, false));
        assertEquals(3, LongPrimitives.countDistinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, true));
    }

    public void testDistinct() {
        assertEquals(null, LongPrimitives.distinct((LongVectorDirect)null));
        assertEquals(null, LongPrimitives.distinct((LongVectorDirect)null, true, true));
        assertEquals(new LongVectorDirect(), LongPrimitives.distinct(new LongVectorDirect(new long[]{})));
        assertEquals(new LongVectorDirect(), LongPrimitives.distinct(new LongVectorDirect(new long[]{NULL_LONG})));
        assertEquals(new LongVectorDirect(new long[]{1}), LongPrimitives.distinct(new LongVectorDirect(new long[]{1})));
        assertEquals(new LongVectorDirect(new long[]{1,2}), LongPrimitives.distinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG})));
        assertEquals(new LongVectorDirect(new long[]{1,2}), LongPrimitives.distinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), false, false));
        assertEquals(new LongVectorDirect(new long[]{1,2,NULL_LONG}), LongPrimitives.distinct(new LongVectorDirect(new long[]{1,2,1,NULL_LONG,NULL_LONG}), true, false));
        assertEquals(new LongVectorDirect(new long[]{1,2,3}), LongPrimitives.distinct(new LongVectorDirect(new long[]{3,1,2,1,NULL_LONG,NULL_LONG}), false, true));
        assertEquals(new LongVectorDirect(new long[]{1,2,3,4}), LongPrimitives.distinct(new LongVectorDirect(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}), false, true));
        assertEquals(new LongVectorDirect(new long[]{NULL_LONG,1,2,3,4}), LongPrimitives.distinct(new LongVectorDirect(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}), true, true));

        assertEquals(null, LongPrimitives.distinct((long[])null));
        assertEquals(null, LongPrimitives.distinct((long[])null, true, true));
        assertEquals(new long[]{}, LongPrimitives.distinct(new long[]{}));
        assertEquals(new long[]{}, LongPrimitives.distinct(new long[]{NULL_LONG}));
        assertEquals(new long[]{1}, LongPrimitives.distinct(new long[]{1}));
        assertEquals(new long[]{1,2}, LongPrimitives.distinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}));
        assertEquals(new long[]{1,2}, LongPrimitives.distinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, false, false));
        assertEquals(new long[]{1,2,NULL_LONG}, LongPrimitives.distinct(new long[]{1,2,1,NULL_LONG,NULL_LONG}, true, false));
        assertEquals(new long[]{1,2,3}, LongPrimitives.distinct(new long[]{3,1,2,1,NULL_LONG,NULL_LONG}, false, true));
        assertEquals(new long[]{1,2,3,4}, LongPrimitives.distinct(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}, false, true));
        assertEquals(new long[]{NULL_LONG,1,2,3,4}, LongPrimitives.distinct(new long[]{3,1,2,4,1,NULL_LONG,NULL_LONG}, true, true));
    }

    public void testVec(){
        assertEquals(new long[]{(long)1,(long)3,(long)5}, LongPrimitives.vec(new LongVectorDirect((long)1,(long)3,(long)5)));
    }

    public void testArray(){
        assertEquals(new LongVectorDirect((long)1,(long)3,(long)5), LongPrimitives.array(new long[]{(long)1,(long)3,(long)5}));
    }

    public void testIn(){
        assertTrue(LongPrimitives.in((long)1,(long)1,(long)2,(long)3));
        assertFalse(LongPrimitives.in((long)5,(long)1,(long)2,(long)3));
        assertFalse(LongPrimitives.in(NULL_LONG,(long)1,(long)2,(long)3));
        assertTrue(LongPrimitives.in(NULL_LONG,(long)1,(long)2,NULL_LONG,(long)3));
    }

    public void testInRange(){
        assertTrue(LongPrimitives.inRange((long)2,(long)1,(long)3));
        assertTrue(LongPrimitives.inRange((long)1,(long)1,(long)3));
        assertFalse(LongPrimitives.inRange(NULL_LONG,(long)1,(long)3));
        assertTrue(LongPrimitives.inRange((long)3,(long)1,(long)3));
        assertFalse(LongPrimitives.inRange((long)4,(long)1,(long)3));
    }

    public void testRepeat() {
        assertEquals(new long[]{5,5,5}, LongPrimitives.repeat((long) 5, 3));
        assertEquals(new long[]{}, LongPrimitives.repeat((long) 5, -3));
    }

    public void testEnlist() {
        assertEquals(new long[]{1, 11, 6}, LongPrimitives.enlist((long)1, (long)11, (long)6));
        assertEquals(new long[]{}, LongPrimitives.enlist((long[])(null)));
    }

    public void testConcat() {
        assertEquals(new long[]{}, LongPrimitives.concat((long[][])null));
        assertEquals(new long[]{1,2,3,4,5,6}, LongPrimitives.concat(new long[]{1,2}, new long[]{3}, new long[]{4,5,6}));
        assertEquals(new long[]{}, LongPrimitives.concat((long[])(null)));

        assertEquals(new long[]{}, LongPrimitives.concat((LongVector[])null));
        assertEquals(new long[]{1,2,3,4,5,6}, LongPrimitives.concat(new LongVectorDirect(new long[]{1,2}), new LongVectorDirect(new long[]{3}), new LongVectorDirect(new long[]{4,5,6})));
        assertEquals(new long[]{}, LongPrimitives.concat((LongVector) (null)));
    }

    public void testReverse() {
        assertEquals(new long[]{3,2,1}, LongPrimitives.reverse((long)1,(long)2,(long)3));
        assertEquals(null, LongPrimitives.reverse((long[])(null)));

        assertEquals(new long[]{3,2,1}, LongPrimitives.reverse(new LongVectorDirect(new long[]{1,2,3})));
        assertEquals(null, LongPrimitives.reverse((LongVector) (null)));
    }
}

