/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.structures.vector.DbByteArray;
import io.deephaven.engine.structures.vector.DbByteArrayDirect;

import static io.deephaven.libs.primitives.BytePrimitives.*;
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
        assertEquals(new byte[]{(byte) 3, (byte) 7, (byte) 11}, nullToValue(new DbByteArrayDirect(new byte[]{(byte) 3, NULL_BYTE, (byte) 11}), (byte) 7));

        assertEquals(new byte[]{(byte) 3, (byte) 7, (byte) 11}, nullToValue(new byte[]{(byte) 3, NULL_BYTE, (byte) 11}, (byte) 7));
    }

    public void testCount(){
        assertEquals(0, count((DbByteArray)null));
        assertEquals(3,count(new DbByteArrayDirect(new byte[]{40,50,60})));
        assertEquals(0,count(new DbByteArrayDirect()));
        assertEquals(0,count(new DbByteArrayDirect(NULL_BYTE)));
        assertEquals(2,count(new DbByteArrayDirect(new byte[]{5,NULL_BYTE,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60-last(new DbByteArrayDirect(new byte[]{40,50,60})))==0.0);
        assertEquals(NULL_BYTE,last(new DbByteArrayDirect()));
        assertEquals(NULL_BYTE,last(new DbByteArrayDirect(NULL_BYTE)));
        assertTrue(Math.abs(15-last(new DbByteArrayDirect(new byte[]{5,NULL_BYTE,15})))==0.0);
        assertTrue(Math.abs(40-last(new DbByteArrayDirect((byte)40)))==0.0);

        assertTrue(Math.abs(60-last(new byte[]{40,50,60}))==0.0);
        assertEquals(NULL_BYTE,last(new byte[]{}));
        assertEquals(NULL_BYTE,last(new byte[]{NULL_BYTE}));
        assertTrue(Math.abs(15-last(new byte[]{5,NULL_BYTE,15}))==0.0);
        assertTrue(Math.abs(40-last(new byte[]{(byte)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40-first(new DbByteArrayDirect(new byte[]{40,50,60})))==0.0);
        assertEquals(NULL_BYTE,first(new DbByteArrayDirect()));
        assertEquals(NULL_BYTE,first(new DbByteArrayDirect(NULL_BYTE)));
        assertTrue(Math.abs(5-first(new DbByteArrayDirect(new byte[]{5,NULL_BYTE,15})))==0.0);
        assertTrue(Math.abs(40-first(new DbByteArrayDirect((byte)40)))==0.0);

        assertTrue(Math.abs(40-first(new byte[]{40,50,60}))==0.0);
        assertEquals(NULL_BYTE,first(new byte[]{}));
        assertEquals(NULL_BYTE,first(new byte[]{NULL_BYTE}));
        assertTrue(Math.abs(5-first(new byte[]{5,NULL_BYTE,15}))==0.0);
        assertTrue(Math.abs(40-first(new byte[]{(byte)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_BYTE, nth(-1,new DbByteArrayDirect(new byte[]{40,50,60})));
        assertEquals((byte)40, nth(0,new DbByteArrayDirect(new byte[]{40,50,60})));
        assertEquals((byte)50, nth(1,new DbByteArrayDirect(new byte[]{40,50,60})));
        assertEquals((byte)60, nth(2,new DbByteArrayDirect(new byte[]{40,50,60})));
        assertEquals(NULL_BYTE, nth(10,new DbByteArrayDirect(new byte[]{40,50,60})));

        assertEquals(NULL_BYTE, nth(-1,new byte[]{40,50,60}));
        assertEquals((byte)40, nth(0,new byte[]{40,50,60}));
        assertEquals((byte)50, nth(1,new byte[]{40,50,60}));
        assertEquals((byte)60, nth(2,new byte[]{40,50,60}));
        assertEquals(NULL_BYTE, nth(10,new byte[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((DbByteArrayDirect)null));
        assertEquals(NULL_LONG, countDistinct((DbByteArrayDirect)null,true));
        assertEquals(0, countDistinct(new DbByteArrayDirect(new byte[]{})));
        assertEquals(0, countDistinct(new DbByteArrayDirect(new byte[]{NULL_BYTE})));
        assertEquals(1, countDistinct(new DbByteArrayDirect(new byte[]{1})));
        assertEquals(2, countDistinct(new DbByteArrayDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE})));
        assertEquals(2, countDistinct(new DbByteArrayDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), false));
        assertEquals(3, countDistinct(new DbByteArrayDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), true));

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
        assertEquals(null, distinct((DbByteArrayDirect)null));
        assertEquals(null, distinct((DbByteArrayDirect)null, true, true));
        assertEquals(new DbByteArrayDirect(), distinct(new DbByteArrayDirect(new byte[]{})));
        assertEquals(new DbByteArrayDirect(), distinct(new DbByteArrayDirect(new byte[]{NULL_BYTE})));
        assertEquals(new DbByteArrayDirect(new byte[]{1}), distinct(new DbByteArrayDirect(new byte[]{1})));
        assertEquals(new DbByteArrayDirect(new byte[]{1,2}), distinct(new DbByteArrayDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE})));
        assertEquals(new DbByteArrayDirect(new byte[]{1,2}), distinct(new DbByteArrayDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), false, false));
        assertEquals(new DbByteArrayDirect(new byte[]{1,2,NULL_BYTE}), distinct(new DbByteArrayDirect(new byte[]{1,2,1,NULL_BYTE,NULL_BYTE}), true, false));
        assertEquals(new DbByteArrayDirect(new byte[]{1,2,3}), distinct(new DbByteArrayDirect(new byte[]{3,1,2,1,NULL_BYTE,NULL_BYTE}), false, true));
        assertEquals(new DbByteArrayDirect(new byte[]{1,2,3,4}), distinct(new DbByteArrayDirect(new byte[]{3,1,2,4,1,NULL_BYTE,NULL_BYTE}), false, true));
        assertEquals(new DbByteArrayDirect(new byte[]{NULL_BYTE,1,2,3,4}), distinct(new DbByteArrayDirect(new byte[]{3,1,2,4,1,NULL_BYTE,NULL_BYTE}), true, true));

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
        assertEquals(new byte[]{(byte)1,(byte)3,(byte)5}, vec(new DbByteArrayDirect((byte)1,(byte)3,(byte)5)));
    }

    public void testArray(){
        assertEquals(new DbByteArrayDirect((byte)1,(byte)3,(byte)5), array(new byte[]{(byte)1,(byte)3,(byte)5}));
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

        assertEquals(new byte[]{}, concat((DbByteArray[])null));
        assertEquals(new byte[]{1,2,3,4,5,6}, concat(new DbByteArrayDirect(new byte[]{1,2}), new DbByteArrayDirect(new byte[]{3}), new DbByteArrayDirect(new byte[]{4,5,6})));
        assertEquals(new byte[]{}, concat((DbByteArray) (null)));
    }

    public void testReverse() {
        assertEquals(new byte[]{3,2,1}, reverse((byte)1,(byte)2,(byte)3));
        assertEquals(null, reverse((byte[])(null)));

        assertEquals(new byte[]{3,2,1}, reverse(new DbByteArrayDirect(new byte[]{1,2,3})));
        assertEquals(null, reverse((DbByteArray) (null)));
    }
}

