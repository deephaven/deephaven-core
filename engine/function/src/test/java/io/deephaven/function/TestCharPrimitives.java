/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;

import static io.deephaven.function.CharacterPrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestCharPrimitives extends BaseArrayTestCase {

    public void testUnbox(){
        assertNull(unbox((Character[])null));
        assertEquals(new char[]{1, NULL_CHAR, 3, NULL_CHAR}, unbox((char)1, null, (char)3, NULL_CHAR));
    }

    public void testIsNull(){
        assertFalse(isNull((char)3));
        assertTrue(isNull(NULL_CHAR));
    }

    public void testNullToValueScalar() {
        assertEquals((char) 3, nullToValue((char) 3, (char) 7));
        assertEquals((char) 7, nullToValue(NULL_CHAR, (char) 7));
    }

    public void testNullToValueArray() {
        assertEquals(new char[]{(char) 3, (char) 7, (char) 11}, nullToValue(new CharVectorDirect(new char[]{(char) 3, NULL_CHAR, (char) 11}), (char) 7));

        assertEquals(new char[]{(char) 3, (char) 7, (char) 11}, nullToValue(new char[]{(char) 3, NULL_CHAR, (char) 11}, (char) 7));
    }

    public void testCount(){
        assertEquals(0, count((CharVector)null));
        assertEquals(3,count(new CharVectorDirect(new char[]{40,50,60})));
        assertEquals(0,count(new CharVectorDirect()));
        assertEquals(0,count(new CharVectorDirect(NULL_CHAR)));
        assertEquals(2,count(new CharVectorDirect(new char[]{5,NULL_CHAR,15})));
    }

    public void testLast(){
        assertTrue(Math.abs(60-last(new CharVectorDirect(new char[]{40,50,60})))==0.0);
        assertEquals(NULL_CHAR,last(new CharVectorDirect()));
        assertEquals(NULL_CHAR,last(new CharVectorDirect(NULL_CHAR)));
        assertTrue(Math.abs(15-last(new CharVectorDirect(new char[]{5,NULL_CHAR,15})))==0.0);
        assertTrue(Math.abs(40-last(new CharVectorDirect((char)40)))==0.0);

        assertTrue(Math.abs(60-last(new char[]{40,50,60}))==0.0);
        assertEquals(NULL_CHAR,last(new char[]{}));
        assertEquals(NULL_CHAR,last(new char[]{NULL_CHAR}));
        assertTrue(Math.abs(15-last(new char[]{5,NULL_CHAR,15}))==0.0);
        assertTrue(Math.abs(40-last(new char[]{(char)40}))==0.0);
    }

    public void testFirst(){
        assertTrue(Math.abs(40-first(new CharVectorDirect(new char[]{40,50,60})))==0.0);
        assertEquals(NULL_CHAR,first(new CharVectorDirect()));
        assertEquals(NULL_CHAR,first(new CharVectorDirect(NULL_CHAR)));
        assertTrue(Math.abs(5-first(new CharVectorDirect(new char[]{5,NULL_CHAR,15})))==0.0);
        assertTrue(Math.abs(40-first(new CharVectorDirect((char)40)))==0.0);

        assertTrue(Math.abs(40-first(new char[]{40,50,60}))==0.0);
        assertEquals(NULL_CHAR,first(new char[]{}));
        assertEquals(NULL_CHAR,first(new char[]{NULL_CHAR}));
        assertTrue(Math.abs(5-first(new char[]{5,NULL_CHAR,15}))==0.0);
        assertTrue(Math.abs(40-first(new char[]{(char)40}))==0.0);
    }

    public void testNth(){
        assertEquals(NULL_CHAR, nth(-1,new CharVectorDirect(new char[]{40,50,60})));
        assertEquals((char)40, nth(0,new CharVectorDirect(new char[]{40,50,60})));
        assertEquals((char)50, nth(1,new CharVectorDirect(new char[]{40,50,60})));
        assertEquals((char)60, nth(2,new CharVectorDirect(new char[]{40,50,60})));
        assertEquals(NULL_CHAR, nth(10,new CharVectorDirect(new char[]{40,50,60})));

        assertEquals(NULL_CHAR, nth(-1,new char[]{40,50,60}));
        assertEquals((char)40, nth(0,new char[]{40,50,60}));
        assertEquals((char)50, nth(1,new char[]{40,50,60}));
        assertEquals((char)60, nth(2,new char[]{40,50,60}));
        assertEquals(NULL_CHAR, nth(10,new char[]{40,50,60}));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((CharVectorDirect)null));
        assertEquals(NULL_LONG, countDistinct((CharVectorDirect)null,true));
        assertEquals(0, countDistinct(new CharVectorDirect(new char[]{})));
        assertEquals(0, countDistinct(new CharVectorDirect(new char[]{NULL_CHAR})));
        assertEquals(1, countDistinct(new CharVectorDirect(new char[]{1})));
        assertEquals(2, countDistinct(new CharVectorDirect(new char[]{1,2,1,NULL_CHAR,NULL_CHAR})));
        assertEquals(2, countDistinct(new CharVectorDirect(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}), false));
        assertEquals(3, countDistinct(new CharVectorDirect(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}), true));

        assertEquals(NULL_LONG, countDistinct((char[])null));
        assertEquals(NULL_LONG, countDistinct((char[])null,true));
        assertEquals(0, countDistinct(new char[]{}));
        assertEquals(0, countDistinct(new char[]{NULL_CHAR}));
        assertEquals(1, countDistinct(new char[]{1}));
        assertEquals(2, countDistinct(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}));
        assertEquals(2, countDistinct(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}, false));
        assertEquals(3, countDistinct(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}, true));
    }

    public void testDistinct() {
        assertEquals(null, distinct((CharVectorDirect)null));
        assertEquals(null, distinct((CharVectorDirect)null, true, true));
        assertEquals(new CharVectorDirect(), distinct(new CharVectorDirect(new char[]{})));
        assertEquals(new CharVectorDirect(), distinct(new CharVectorDirect(new char[]{NULL_CHAR})));
        assertEquals(new CharVectorDirect(new char[]{1}), distinct(new CharVectorDirect(new char[]{1})));
        assertEquals(new CharVectorDirect(new char[]{1,2}), distinct(new CharVectorDirect(new char[]{1,2,1,NULL_CHAR,NULL_CHAR})));
        assertEquals(new CharVectorDirect(new char[]{1,2}), distinct(new CharVectorDirect(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}), false, false));
        assertEquals(new CharVectorDirect(new char[]{1,2,NULL_CHAR}), distinct(new CharVectorDirect(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}), true, false));
        assertEquals(new CharVectorDirect(new char[]{1,2,3}), distinct(new CharVectorDirect(new char[]{3,1,2,1,NULL_CHAR,NULL_CHAR}), false, true));
        assertEquals(new CharVectorDirect(new char[]{1,2,3,4}), distinct(new CharVectorDirect(new char[]{3,1,2,4,1,NULL_CHAR,NULL_CHAR}), false, true));
        assertEquals(new CharVectorDirect(new char[]{NULL_CHAR,1,2,3,4}), distinct(new CharVectorDirect(new char[]{3,1,2,4,1,NULL_CHAR,NULL_CHAR}), true, true));

        assertEquals(null, distinct((char[])null));
        assertEquals(null, distinct((char[])null, true, true));
        assertEquals(new char[]{}, distinct(new char[]{}));
        assertEquals(new char[]{}, distinct(new char[]{NULL_CHAR}));
        assertEquals(new char[]{1}, distinct(new char[]{1}));
        assertEquals(new char[]{1,2}, distinct(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}));
        assertEquals(new char[]{1,2}, distinct(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}, false, false));
        assertEquals(new char[]{1,2,NULL_CHAR}, distinct(new char[]{1,2,1,NULL_CHAR,NULL_CHAR}, true, false));
        assertEquals(new char[]{1,2,3}, distinct(new char[]{3,1,2,1,NULL_CHAR,NULL_CHAR}, false, true));
        assertEquals(new char[]{1,2,3,4}, distinct(new char[]{3,1,2,4,1,NULL_CHAR,NULL_CHAR}, false, true));
        assertEquals(new char[]{NULL_CHAR,1,2,3,4}, distinct(new char[]{3,1,2,4,1,NULL_CHAR,NULL_CHAR}, true, true));
    }

    public void testVec(){
        assertEquals(new char[]{(char)1,(char)3,(char)5}, vec(new CharVectorDirect((char)1,(char)3,(char)5)));
    }

    public void testArray(){
        assertEquals(new CharVectorDirect((char)1,(char)3,(char)5), array(new char[]{(char)1,(char)3,(char)5}));
    }

    public void testIn(){
        assertTrue(in((char)1,(char)1,(char)2,(char)3));
        assertFalse(in((char)5,(char)1,(char)2,(char)3));
        assertFalse(in(NULL_CHAR,(char)1,(char)2,(char)3));
        assertTrue(in(NULL_CHAR,(char)1,(char)2,NULL_CHAR,(char)3));
    }

    public void testInRange(){
        assertTrue(inRange((char)2,(char)1,(char)3));
        assertTrue(inRange((char)1,(char)1,(char)3));
        assertFalse(inRange(NULL_CHAR,(char)1,(char)3));
        assertTrue(inRange((char)3,(char)1,(char)3));
        assertFalse(inRange((char)4,(char)1,(char)3));
    }

    public void testRepeat() {
        assertEquals(new char[]{5,5,5}, repeat((char) 5, 3));
        assertEquals(new char[]{}, repeat((char) 5, -3));
    }

    public void testEnlist() {
        assertEquals(new char[]{1, 11, 6}, enlist((char)1, (char)11, (char)6));
        assertEquals(new char[]{}, enlist((char[])(null)));
    }

    public void testConcat() {
        assertEquals(new char[]{}, concat((char[][])null));
        assertEquals(new char[]{1,2,3,4,5,6}, concat(new char[]{1,2}, new char[]{3}, new char[]{4,5,6}));
        assertEquals(new char[]{}, concat((char[])(null)));

        assertEquals(new char[]{}, concat((CharVector[])null));
        assertEquals(new char[]{1,2,3,4,5,6}, concat(new CharVectorDirect(new char[]{1,2}), new CharVectorDirect(new char[]{3}), new CharVectorDirect(new char[]{4,5,6})));
        assertEquals(new char[]{}, concat((CharVector) (null)));
    }

    public void testReverse() {
        assertEquals(new char[]{3,2,1}, reverse((char)1,(char)2,(char)3));
        assertEquals(null, reverse((char[])(null)));

        assertEquals(new char[]{3,2,1}, reverse(new CharVectorDirect(new char[]{1,2,3})));
        assertEquals(null, reverse((CharVector) (null)));
    }
}

