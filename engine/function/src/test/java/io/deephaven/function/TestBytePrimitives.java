/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;

import static io.deephaven.function.BytePrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestBytePrimitives extends BaseArrayTestCase {

    public void testUnbox(){
        assertNull(unbox((Byte[])null));
        assertEquals(new byte[]{1, NULL_BYTE, 3, NULL_BYTE}, unbox((byte)1, null, (byte)3, NULL_BYTE));
    }

    public void testIsNull(){
        assertFalse(isNull((byte)3));
        assertTrue(isNull(NULL_BYTE));
    }

    public void testNullToValueScalar() {
        assertEquals((byte) 3, nullToValue((byte) 3, (byte) 7));
        assertEquals((byte) 7, nullToValue(NULL_BYTE, (byte) 7));
    }

    public void testNullToValueArray() {
        assertEquals(new byte[]{(byte) 3, (byte) 7, (byte) 11}, nullToValue(new ByteVectorDirect(new byte[]{(byte) 3, NULL_BYTE, (byte) 11}), (byte) 7));

        assertEquals(new byte[]{(byte) 3, (byte) 7, (byte) 11}, nullToValue(new byte[]{(byte) 3, NULL_BYTE, (byte) 11}, (byte) 7));
    }

    public void testCount(){
        assertEquals(0, count((ByteVector)null));
        assertEquals(3,count(new ByteVectorDirect(new byte[]{40,50,60})));
        assertEquals(0,count(new ByteVectorDirect()));
        assertEquals(0,count(new ByteVectorDirect(NULL_BYTE)));
        assertEquals(2,count(new ByteVectorDirect(new byte[]{5,NULL_BYTE,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60-last(new ByteVectorDirect(new byte[]{40,50,60})))==0.0);
        assertEquals(NULL_BYTE,last(new ByteVectorDirect()));
        assertEquals(NULL_BYTE,last(new ByteVectorDirect(NULL_BYTE)));
        assertTrue(Math.abs(15-last(new ByteVectorDirect(new byte[]{5,NULL_BYTE,15})))==0.0);
        assertTrue(Math.abs(40-last(new ByteVectorDirect((byte)40)))==0.0);

        assertTrue(Math.abs(60-last(new byte[]{40,50,60}))==0.0);
        assertEquals(NULL_BYTE,last(new byte[]{}));
        assertEquals(NULL_BYTE,last(new byte[]{NULL_BYTE}));
        assertTrue(Math.abs(15-last(new byte[]{5,NULL_BYTE,15}))==0.0);
        assertTrue(Math.abs(40-last(new byte[]{(byte)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40-first(new ByteVectorDirect(new byte[]{40,50,60})))==0.0);
        assertEquals(NULL_BYTE,first(new ByteVectorDirect()));
        assertEquals(NULL_BYTE,first(new ByteVectorDirect(NULL_BYTE)));
        assertTrue(Math.abs(5-first(new ByteVectorDirect(new byte[]{5,NULL_BYTE,15})))==0.0);
        assertTrue(Math.abs(40-first(new ByteVectorDirect((byte)40)))==0.0);

        assertTrue(Math.abs(40-first(new byte[]{40,50,60}))==0.0);
        assertEquals(NULL_BYTE,first(new byte[]{}));
        assertEquals(NULL_BYTE,first(new byte[]{NULL_BYTE}));
        assertTrue(Math.abs(5-first(new byte[]{5,NULL_BYTE,15}))==0.0);
        assertTrue(Math.abs(40-first(new byte[]{(byte)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_BYTE, nth(-1,new ByteVectorDirect(new byte[]{40,50,60})));
        assertEquals((byte)40, nth(0,new ByteVectorDirect(new byte[]{40,50,60})));
        assertEquals((byte)50, nth(1,new ByteVectorDirect(new byte[]{40,50,60})));
        assertEquals((byte)60, nth(2,new ByteVectorDirect(new byte[]{40,50,60})));
        assertEquals(NULL_BYTE, nth(10,new ByteVectorDirect(new byte[]{40,50,60})));

        assertEquals(NULL_BYTE, nth(-1,new byte[]{40,50,60}));
        assertEquals((byte)40, nth(0,new byte[]{40,50,60}));
        assertEquals((byte)50, nth(1,new byte[]{40,50,60}));
        assertEquals((byte)60, nth(2,new byte[]{40,50,60}));
        assertEquals(NULL_BYTE, nth(10,new byte[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((ByteVectorDirect)null));
        assertEquals(NULL_LONG, countDistinct((ByteVectorDirect)null,true));
        assertEquals(0, countDistinct(new ByteVectorDirect(new byte[]{})));
        assertEquals(0, countDistinct(new ByteVectorDirect(new byte[]{NULL_BYTE})));
        assertEquals(1, countDistinct(new ByteVectorDirect(new byte[]{1})));
        assertEquals(2, countDistinct(new ByteVectorDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE})));
        assertEquals(2, countDistinct(new ByteVectorDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), false));
        assertEquals(3, countDistinct(new ByteVectorDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), true));

        assertEquals(NULL_LONG, countDistinct((byte[])null));
        assertEquals(NULL_LONG, countDistinct((byte[])null,true));
        assertEquals(0, countDistinct(new byte[]{}));
        assertEquals(0, countDistinct(new byte[]{NULL_BYTE}));
        assertEquals(1, countDistinct(new byte[]{1}));
        assertEquals(2, countDistinct(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}));
        assertEquals(2, countDistinct(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}, false));
        assertEquals(3, countDistinct(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}, true));
    }

    public void testDistinct() {
        assertEquals(null, distinct((ByteVectorDirect)null));
        assertEquals(null, distinct((ByteVectorDirect)null, true, true));
        assertEquals(new ByteVectorDirect(), distinct(new ByteVectorDirect(new byte[]{})));
        assertEquals(new ByteVectorDirect(), distinct(new ByteVectorDirect(new byte[]{NULL_BYTE})));
        assertEquals(new ByteVectorDirect(new byte[]{1}), distinct(new ByteVectorDirect(new byte[]{1})));
        assertEquals(new ByteVectorDirect(new byte[]{1,2}), distinct(new ByteVectorDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE})));
        assertEquals(new ByteVectorDirect(new byte[]{1,2}), distinct(new ByteVectorDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), false, false));
        assertEquals(new ByteVectorDirect(new byte[]{1,2,NULL_BYTE}), distinct(new ByteVectorDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), true, false));
        assertEquals(new ByteVectorDirect(new byte[]{1,2,3}), distinct(new ByteVectorDirect(new byte[]{3,1,2,1,NULL_BYTE,NULL_BYTE}), false, true));
        assertEquals(new ByteVectorDirect(new byte[]{1,2,3,4}), distinct(new ByteVectorDirect(new byte[]{3,1,2,4,1,NULL_BYTE,NULL_BYTE}), false, true));
        assertEquals(new ByteVectorDirect(new byte[]{NULL_BYTE,1,2,3,4}), distinct(new ByteVectorDirect(new byte[]{3,1,2,4,1,NULL_BYTE,NULL_BYTE}), true, true));

        assertEquals(null, distinct((byte[])null));
        assertEquals(null, distinct((byte[])null, true, true));
        assertEquals(new byte[]{}, distinct(new byte[]{}));
        assertEquals(new byte[]{}, distinct(new byte[]{NULL_BYTE}));
        assertEquals(new byte[]{1}, distinct(new byte[]{1}));
        assertEquals(new byte[]{1,2}, distinct(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}));
        assertEquals(new byte[]{1,2}, distinct(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}, false, false));
        assertEquals(new byte[]{1,2,NULL_BYTE}, distinct(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}, true, false));
        assertEquals(new byte[]{1,2,3}, distinct(new byte[]{3,1,2,1,NULL_BYTE,NULL_BYTE}, false, true));
        assertEquals(new byte[]{1,2,3,4}, distinct(new byte[]{3,1,2,4,1,NULL_BYTE,NULL_BYTE}, false, true));
        assertEquals(new byte[]{NULL_BYTE,1,2,3,4}, distinct(new byte[]{3,1,2,4,1,NULL_BYTE,NULL_BYTE}, true, true));
    }

    public void testVec(){
        assertEquals(new byte[]{(byte)1,(byte)3,(byte)5}, vec(new ByteVectorDirect((byte)1,(byte)3,(byte)5)));
    }

    public void testArray(){
        assertEquals(new ByteVectorDirect((byte)1,(byte)3,(byte)5), array(new byte[]{(byte)1,(byte)3,(byte)5}));
    }

    public void testIn(){
        assertTrue(in((byte)1,(byte)1,(byte)2,(byte)3));
        assertFalse(in((byte)5,(byte)1,(byte)2,(byte)3));
        assertFalse(in(NULL_BYTE,(byte)1,(byte)2,(byte)3));
        assertTrue(in(NULL_BYTE,(byte)1,(byte)2,NULL_BYTE,(byte)3));
    }

    public void testInRange(){
        assertTrue(inRange((byte)2,(byte)1,(byte)3));
        assertTrue(inRange((byte)1,(byte)1,(byte)3));
        assertFalse(inRange(NULL_BYTE,(byte)1,(byte)3));
        assertTrue(inRange((byte)3,(byte)1,(byte)3));
        assertFalse(inRange((byte)4,(byte)1,(byte)3));
    }

    public void testRepeat() {
        assertEquals(new byte[]{5,5,5}, repeat((byte) 5, 3));
        assertEquals(new byte[]{}, repeat((byte) 5, -3));
    }

    public void testEnlist() {
        assertEquals(new byte[]{1, 11, 6}, enlist((byte)1, (byte)11, (byte)6));
        assertEquals(new byte[]{}, enlist((byte[])(null)));
    }

    public void testConcat() {
        assertEquals(new byte[]{}, concat((byte[][])null));
        assertEquals(new byte[]{1,2,3,4,5,6}, concat(new byte[]{1,2}, new byte[]{3}, new byte[]{4,5,6}));
        assertEquals(new byte[]{}, concat((byte[])(null)));

        assertEquals(new byte[]{}, concat((ByteVector[])null));
        assertEquals(new byte[]{1,2,3,4,5,6}, concat(new ByteVectorDirect(new byte[]{1,2}), new ByteVectorDirect(new byte[]{3}), new ByteVectorDirect(new byte[]{4,5,6})));
        assertEquals(new byte[]{}, concat((ByteVector) (null)));
    }

    public void testReverse() {
        assertEquals(new byte[]{3,2,1}, reverse((byte)1,(byte)2,(byte)3));
        assertEquals(null, reverse((byte[])(null)));

        assertEquals(new byte[]{3,2,1}, reverse(new ByteVectorDirect(new byte[]{1,2,3})));
        assertEquals(null, reverse((ByteVector) (null)));
    }
}

