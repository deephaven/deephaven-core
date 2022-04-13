/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;
import io.deephaven.util.*;
import junit.framework.TestCase;

import static io.deephaven.function.Basic.*;
import static io.deephaven.util.QueryConstants.*;

/**
 * Test Basic.
 */
public class TestBasic extends BaseArrayTestCase {

    //////////////////////////// Object ////////////////////////////

    public void testGenericNullValueFor() {
        assertNull(nullValueFor(Object.class));
        assertEquals(NULL_BOOLEAN, nullValueFor(Boolean.class));
        assertEquals(NULL_CHAR_BOXED, nullValueFor(Character.class));
        assertEquals(NULL_BYTE_BOXED, nullValueFor(Byte.class));
        assertEquals(NULL_SHORT_BOXED, nullValueFor(Short.class));
        assertEquals(NULL_INT_BOXED, nullValueFor(Integer.class));
        assertEquals(NULL_LONG_BOXED, nullValueFor(Long.class));
        assertEquals(NULL_FLOAT_BOXED, nullValueFor(Float.class));
        assertEquals(NULL_DOUBLE_BOXED, nullValueFor(Double.class));
    }

    public void testGenericIsNull() {
        assertFalse(isNull(new Object()));
        assertTrue(isNull(null));

        assertFalse(isNull(true));
        assertTrue(isNull(NULL_BOOLEAN));

        assertFalse(isNull((char) 1));
        assertTrue(isNull(NULL_CHAR));

        assertFalse(isNull((byte) 1));
        assertTrue(isNull(NULL_BYTE));

        assertFalse(isNull((short) 1));
        assertTrue(isNull(NULL_SHORT));

        assertFalse(isNull((int) 1));
        assertTrue(isNull(NULL_INT));

        assertFalse(isNull((long) 1));
        assertTrue(isNull(NULL_LONG));

        assertFalse(isNull((float) 1));
        assertTrue(isNull(NULL_FLOAT));

        assertFalse(isNull((double) 1));
        assertTrue(isNull(NULL_DOUBLE));
    }

    public void testGenericReplaceIfNullScalar() {
        assertEquals(new Integer(7), replaceIfNull(new Integer(7), new Integer(3)));
        assertEquals(new Integer(3), replaceIfNull((Integer) null, new Integer(3)));
    }

    public void testGenericReplaceIfNullArray() {
        assertEquals(new Integer[]{new Integer(7), new Integer(3), new Integer(-5)},
                replaceIfNull(new ObjectVectorDirect<>(new Integer[]{new Integer(7), null, new Integer(-5)}), new Integer(3)));
    }

    public void testGenericInRange() {
        assertTrue(inRange(2, 1, 3));
        assertTrue(inRange(1, 1, 3));
        assertFalse(inRange(null, 1, 3));
        assertTrue(inRange(3, 1, 3));
        assertFalse(inRange(4, 1, 3));
    }

    public void testGenericCount() {
        assertEquals(NULL_LONG, count((ObjectVector)null));
        assertEquals(3, count(new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(0, count(new ObjectVectorDirect<Integer>()));
        assertEquals(0, count(new ObjectVectorDirect<Integer>(new Integer[]{null})));
        assertEquals(2, count(new ObjectVectorDirect<Integer>(5, null, 15)));
        assertEquals(2, count(new ObjectVectorDirect<Integer>(5, null, 15, NULL_INT)));

        assertEquals(NULL_LONG, count((Integer[])null));
        assertEquals(3, count(new Integer[]{40, 50, 60}));
        assertEquals(0, count(new Integer[]{}));
        assertEquals(0, count(new Integer[]{null}));
        assertEquals(2, count(new Integer[]{5, null, 15}));
        assertEquals(2, count(new Integer[]{5, null, 15, NULL_INT}));
    }

    public void testGenericCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((ObjectVector<Short>)null));
        assertEquals(NULL_LONG, countDistinct((ObjectVector<Short>)null,true));
        assertEquals(0, countDistinct(new ObjectVectorDirect<Short>(new Short[]{})));
        assertEquals(0, countDistinct(new ObjectVectorDirect<Short>(new Short[]{NULL_SHORT})));
        assertEquals(1, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1})));
        assertEquals(2, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT})));
        assertEquals(2, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(3, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), true));

        assertEquals(NULL_LONG, countDistinct((Short[])null));
        assertEquals(NULL_LONG, countDistinct((Short[])null,true));
        assertEquals(0, countDistinct(new Short[]{}));
        assertEquals(0, countDistinct(new Short[]{NULL_SHORT}));
        assertEquals(1, countDistinct(new Short[]{1}));
        assertEquals(2, countDistinct(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}));
        assertEquals(2, countDistinct(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(3, countDistinct(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, true));
    }

    public void testGenericDistinct() {
        assertEquals(null, distinct((ObjectVectorDirect<Short>)null));
        assertEquals(null, distinct((ObjectVectorDirect<Short>)null, true));
        assertEquals(new Short[]{}, distinct(new Short[]{}));
        assertEquals(new Short[]{}, distinct(new ObjectVectorDirect<Short>(new Short[]{NULL_SHORT})));
        assertEquals(new Short[]{1}, distinct(new ObjectVectorDirect<Short>(new Short[]{1})));
        assertEquals(new Short[]{1,2}, distinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT})));
        assertEquals(new Short[]{1,2}, distinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(new Short[]{1,2,NULL_SHORT}, distinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), true));
        assertEquals(new Short[]{3,1,2}, distinct(new ObjectVectorDirect<Short>(new Short[]{3,1,2,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(new Short[]{3,1,2,4}, distinct(new ObjectVectorDirect<Short>(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(new Short[]{3,1,2,4,NULL_SHORT}, distinct(new ObjectVectorDirect<Short>(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}), true));

        assertEquals(null, distinct((Short[])null));
        assertEquals(null, distinct((Short[])null, true));
        assertEquals(new Short[]{}, distinct(new Short[]{}));
        assertEquals(new Short[]{}, distinct(new Short[]{NULL_SHORT}));
        assertEquals(new Short[]{1}, distinct(new Short[]{1}));
        assertEquals(new Short[]{1,2}, distinct(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}));
        assertEquals(new Short[]{1,2}, distinct(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(new Short[]{1,2,NULL_SHORT}, distinct(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, true));
        assertEquals(new Short[]{3,1,2}, distinct(new Short[]{3,1,2,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(new Short[]{3,1,2,4}, distinct(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(new Short[]{3,1,2,4,NULL_SHORT}, distinct(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}, true));
    }

    public void testGenericRepeat() {
        try {
            repeat(new Character('a'),-3);
            fail("Should have failed on invalid arguments.");
        } catch(IllegalArgumentException e){
            //pass
        }

        assertEquals(new Character[]{'a','a','a'}, repeat(new Character('a'),3));
    }

    public void testGenericConcat() {
        assertEquals(new Character[0], concat(new ObjectVectorDirect<Character>()));
        assertEquals(new Character[]{'a','b','c','x','y','z'}, concat(new ObjectVectorDirect<>(new Character[]{'a','b','c'}), new ObjectVectorDirect<>(new Character[]{'x'}), new ObjectVectorDirect<>(new Character[]{'y','z'})));

        assertEquals(new Character[0], concat(new Character[]{}));
        assertEquals(new Character[]{'a','b','c','x','y','z'}, concat(new Character[]{'a','b','c'}, new Character[]{'x'}, new Character[]{'y','z'}));
    }

    public void testGenericReverse() {
        assertEquals(null, reverse((ObjectVector[])null));
        assertEquals(new Object[]{}, reverse(new ObjectVectorDirect()));
        assertEquals(new Character[]{'c','b','a'}, reverse(new ObjectVectorDirect<>(new Character[]{'a','b','c'})));

        assertEquals(null, reverse((Character[])null));
        assertEquals(new Character[]{}, reverse(new Character[]{}));
        assertEquals(new Character[]{'c','b','a'}, reverse(new Character[]{'a','b','c'}));
    }

    public void testGenericFirstIndexOf() {
        assertEquals(1, firstIndexOf(new Integer(40), new Integer[]{0, 40, null, 40, 60, 40, 0}));
        assertEquals(4, firstIndexOf(new Integer(60), new Integer[]{0, 40, null, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOf(new Integer(1), new Integer[]{0, 40, null, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOf(new Integer(40), (Integer[])null));

        assertEquals(1, firstIndexOf(new Integer(40), new ObjectVectorDirect<>(new Integer[]{0, 40, null, 40, 60, 40, 0})));
        assertEquals(4, firstIndexOf(new Integer(60), new ObjectVectorDirect<>(new Integer[]{0, 40, null, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOf(new Integer(1), new ObjectVectorDirect<>(new Integer[]{0, 40, null, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOf(new Integer(40), (ObjectVector) null));
    }

    public void testGenericLast() {
        assertEquals(10, last(new ObjectVectorDirect<Object>(10)));
        assertEquals(3, last(new ObjectVectorDirect<Object>(1, 2, 3)));
        assertEquals(null, last(new ObjectVectorDirect<Object>(1, 2, null)));

        assertEquals(new Integer(10), last(new Integer[]{10}));
        assertEquals(new Integer(3), last(new Integer[]{1, 2, 3}));
        assertEquals(null, last(new Integer[]{1, 2, null}));
    }

    public void testGenericFirst() {
        assertEquals(10, first(new ObjectVectorDirect<Object>(10)));
        assertEquals(3, first(new ObjectVectorDirect<Object>(3, 2, 1)));
        assertEquals(null, first(new ObjectVectorDirect<Object>(null, 1, 2)));

        assertEquals(new Integer(10), first(new Integer[]{10}));
        assertEquals(new Integer(3), first(new Integer[]{3, 2, 1}));
        assertEquals(null, first(new Integer[]{null, 1, 2}));
    }

    public void testGenericNth() {
        assertEquals(null, nth(-1, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(new Integer(40), nth(0, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(new Integer(50), nth(1, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(new Integer(60), nth(2, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(null, nth(10, new ObjectVectorDirect<Integer>(40, 50, 60)));

        assertEquals(null, nth(-1, new Integer[]{40, 50, 60}));
        assertEquals(new Integer(40), nth(0, new Integer[]{40, 50, 60}));
        assertEquals(new Integer(50), nth(1, new Integer[]{40, 50, 60}));
        assertEquals(new Integer(60), nth(2, new Integer[]{40, 50, 60}));
        assertEquals(null, nth(10, new Integer[]{40,50,60}));
    }

    public void testGenericVec() {
        assertEquals(new Character[]{new Character('1'), new Character('3'), new Character('5')}, vec(new ObjectVectorDirect<Character>(new Character('1'), new Character('3'), new Character('5'))));
    }

    public void testGenericArray() {
        assertEquals(new ObjectVectorDirect<>(new Character[]{new Character('1'), new Character('3'), new Character('5')}), array(new Character('1'), new Character('3'), new Character('5')));
    }

    public void testGenericIn() {
        assertTrue(in(1000000L, new Long[]{1000000L, 2000000L, 3000000L}));
        assertFalse(in(5000000L, new Long[]{1000000L, 2000000L, 3000000L}));
        assertFalse(in((Integer)null, new Integer[]{1, 2, 3}));
        assertTrue(in((Integer)null, new Integer[]{1, 2, null, 3}));
    }


    //////////////////////////// boolean ////////////////////////////


    public void testBooleanIsNull() {
        assertFalse(isNull(Boolean.TRUE));
        assertFalse(isNull(Boolean.FALSE));
        assertTrue(isNull((Boolean) null));
    }

    public void testBooleanReplaceIfNullScalar() {
        assertEquals(Boolean.TRUE, replaceIfNull(Boolean.TRUE, false));
        assertEquals(Boolean.FALSE, replaceIfNull((Boolean) null, false));
    }

    public void testBooleanReplaceIfNullArray() {
        assertEquals(new Boolean[]{true, false, false}, replaceIfNull(new Boolean[]{Boolean.TRUE, null, Boolean.FALSE}, false));
        assertEquals(new Boolean[]{true, false, false}, replaceIfNull(new Boolean[]{Boolean.TRUE, null, Boolean.FALSE}, false));
    }

    public void testBooleanCount(){
        assertEquals(3,count(new Boolean[]{true, false, true}));
        assertEquals(0,count(new Boolean[]{}));
        assertEquals(0,count(new Boolean[]{QueryConstants.NULL_BOOLEAN}));
        assertEquals(2,count(new Boolean[]{true, QueryConstants.NULL_BOOLEAN,true}));
        assertEquals(NULL_LONG, count((Boolean[])null));

        assertEquals(3,count(new ObjectVectorDirect<>(new Boolean[]{true, false, true})));
        assertEquals(0,count(new ObjectVectorDirect<>()));
        assertEquals(0,count(new ObjectVectorDirect<>(QueryConstants.NULL_BOOLEAN)));
        assertEquals(2,count(new ObjectVectorDirect<>(new Boolean[]{true, QueryConstants.NULL_BOOLEAN,true})));
        assertEquals(NULL_LONG, count((ObjectVector)null));
    }

    public void testBooleanLast(){
        assertFalse(last(new Boolean[]{true,true,false}));
        assertEquals(QueryConstants.NULL_BOOLEAN,last((Boolean[])null));
        assertEquals(QueryConstants.NULL_BOOLEAN,last(new Boolean[]{}));
        assertEquals(QueryConstants.NULL_BOOLEAN,last(new Boolean[]{QueryConstants.NULL_BOOLEAN}));
        assertTrue(last(new Boolean[]{false, QueryConstants.NULL_BOOLEAN,true}));
        assertTrue(last(new Boolean[]{true}));
    }

    public void testBooleanFirst(){
        assertTrue(first(new Boolean[]{true,false,false}));
        assertEquals(QueryConstants.NULL_BOOLEAN,first((Boolean[])null));
        assertEquals(QueryConstants.NULL_BOOLEAN,first(new Boolean[]{}));
        assertEquals(QueryConstants.NULL_BOOLEAN,first(new Boolean[]{QueryConstants.NULL_BOOLEAN}));
        assertTrue(first(new Boolean[]{true, QueryConstants.NULL_BOOLEAN,false}));
        assertTrue(first(new Boolean[]{true}));
    }

    public void testBooleanNth(){
        assertEquals(QueryConstants.NULL_BOOLEAN, nth(-1,new ObjectVectorDirect<>(new Boolean[]{true, false, true})));
        assertEquals((Boolean)true, nth(0,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));
        assertEquals((Boolean)false, nth(1,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));
        assertEquals((Boolean)true, nth(2,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));
        assertEquals(QueryConstants.NULL_BOOLEAN, nth(10,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));

        assertEquals(QueryConstants.NULL_BOOLEAN, nth(-1,new Boolean[]{true, false, true}));
        assertEquals((Boolean)true, nth(0,new Boolean[]{true,false,true}));
        assertEquals((Boolean)false, nth(1,new Boolean[]{true,false,true}));
        assertEquals((Boolean)true, nth(2,new Boolean[]{true,false,true}));
        assertEquals(QueryConstants.NULL_BOOLEAN, nth(10,new Boolean[]{true,false,true}));
    }

    public void testBooleanVec(){
        assertEquals(null, vec((ObjectVector)null));
        assertEquals(new Boolean[]{true,false,true}, vec(new ObjectVectorDirect<>(true,false,true)));
    }

    public void testBooleanArray(){
        assertEquals(new ObjectVectorDirect<Boolean>(true,false,true), array(new Boolean[]{true,false,true}));
    }

    public void testBooleanIn(){
        assertTrue(in(true,new Boolean[]{true,false}));
        assertFalse(in(false,new Boolean[]{true,true}));
        assertFalse(in((Boolean)null,new Boolean[]{true,false}));
        assertTrue(in(null,new Boolean[]{(Boolean)null,true,false}));
    }


    public void testBooleanCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((ObjectVector)null));
        assertEquals(0, countDistinct(new ObjectVectorDirect<Boolean>((Boolean)null)));
        assertEquals(1, countDistinct(new ObjectVectorDirect<Boolean>((Boolean)null), true));
        assertEquals(0, countDistinct(new ObjectVectorDirect<>(new Boolean[]{})));
        assertEquals(0, countDistinct(new ObjectVectorDirect<>(new Boolean[]{NULL_BOOLEAN})));
        assertEquals(1, countDistinct(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN})));
        assertEquals(1, countDistinct(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN})));
        assertEquals(2, countDistinct(new ObjectVectorDirect<>(new Boolean[]{true,false,true,false,NULL_BOOLEAN})));
        assertEquals(2, countDistinct(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN}),true));
        assertEquals(2, countDistinct(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN}),true));
        assertEquals(3, countDistinct(new ObjectVectorDirect<>(new Boolean[]{false,true,false,true,NULL_BOOLEAN}),true));

        assertEquals(NULL_LONG, countDistinct((Boolean[])null));
        assertEquals(0, countDistinct(new Boolean[]{null}));
        assertEquals(1, countDistinct(new Boolean[]{null}, true));
        assertEquals(0, countDistinct(new Boolean[]{}));
        assertEquals(0, countDistinct(new Boolean[]{NULL_BOOLEAN}));
        assertEquals(1, countDistinct(new Boolean[]{true,true,NULL_BOOLEAN}));
        assertEquals(1, countDistinct(new Boolean[]{false,false,NULL_BOOLEAN}));
        assertEquals(2, countDistinct(new Boolean[]{true,false,true,false,NULL_BOOLEAN}));
        assertEquals(2, countDistinct(new Boolean[]{true,true,NULL_BOOLEAN},true));
        assertEquals(2, countDistinct(new Boolean[]{false,false,NULL_BOOLEAN},true));
        assertEquals(3, countDistinct(new Boolean[]{false,true,false,true,NULL_BOOLEAN},true));
    }

    public void testBooleanDistinct() {
        assertEquals(null, distinct((ObjectVector)null));
        assertEquals(null, distinct((ObjectVector)null, true));
        assertEquals(new Boolean[]{}, distinct(new ObjectVectorDirect<>(new Boolean[]{})));
        assertEquals(new Boolean[]{}, distinct(new ObjectVectorDirect<>(new Boolean[]{NULL_BOOLEAN})));
        assertEquals(new Boolean[]{true}, distinct(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN})));
        assertEquals(new Boolean[]{false}, distinct(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN})));
        assertEquals(new Boolean[]{true,false}, distinct(new ObjectVectorDirect<>(new Boolean[]{true,false,true,false,NULL_BOOLEAN})));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinct(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinct(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{true,false,NULL_BOOLEAN}, distinct(new ObjectVectorDirect<>(new Boolean[]{true,false,false,true,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinct(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinct(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{false,true,NULL_BOOLEAN}, distinct(new ObjectVectorDirect<>(new Boolean[]{false,true,false,true,NULL_BOOLEAN}),true));

        assertEquals(null, distinct((Boolean[])null));
        assertEquals(null, distinct((Boolean[])null, true));
        assertEquals(new Boolean[]{}, distinct(new Boolean[]{}));
        assertEquals(new Boolean[]{}, distinct(new Boolean[]{NULL_BOOLEAN}));
        assertEquals(new Boolean[]{true}, distinct(new Boolean[]{true,true,NULL_BOOLEAN}));
        assertEquals(new Boolean[]{false}, distinct(new Boolean[]{false,false,NULL_BOOLEAN}));
        assertEquals(new Boolean[]{true,false}, distinct(new Boolean[]{true,false,true,false,NULL_BOOLEAN}));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinct(new Boolean[]{true,true,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinct(new Boolean[]{false,false,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{true,false,NULL_BOOLEAN}, distinct(new Boolean[]{true,false,false,true,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinct(new Boolean[]{true,true,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinct(new Boolean[]{false,false,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{false,true,NULL_BOOLEAN}, distinct(new Boolean[]{false,true,false,true,NULL_BOOLEAN},true));

        assertEquals(new Boolean[]{false,true}, distinct(false,true,false,true,NULL_BOOLEAN));
    }

    public void testBooleanRepeat() {
        try {
            repeat(true, -3);
            fail("Should have failed on invalid arguments.");
        } catch(IllegalArgumentException e){
            //pass
        }

        assertEquals(new Boolean[]{true,true,true}, repeat(true, 3));
    }

    public void testBooleanConcat() {
        assertEquals(new Boolean[]{true,false,false,false,true,true}, concat(new Boolean[]{true,false}, new Boolean[]{false}, new Boolean[]{false,true,true}));
        assertEquals(new Boolean[]{}, concat((Boolean[])(null)));

        assertEquals(new Boolean[]{true,false,false,false,true,true}, concat(new ObjectVectorDirect<>(new Boolean[]{true,false}), new ObjectVectorDirect<>(new Boolean[]{false}), new ObjectVectorDirect<>(new Boolean[]{false,true,true})));
        assertEquals(new Boolean[]{}, concat((ObjectVector) (null)));
    }

    public void testBooleanReverse() {
        assertEquals(new Boolean[]{false,true,true}, reverse(new Boolean[]{true, true, false}));
        assertEquals(null, reverse((Boolean[])(null)));

        assertEquals(new Boolean[]{false,true,true}, reverse(new ObjectVectorDirect<>(new Boolean[]{true,true,false})));
        assertEquals(null, reverse((ObjectVector) (null)));
    }

    public void testBooleanFirstIndexOf() {
        assertEquals(0, firstIndexOf(true, new Boolean[]{true, NULL_BOOLEAN, false}));
        assertEquals(2, firstIndexOf(false, new Boolean[]{true, NULL_BOOLEAN, false}));
        assertEquals(1, firstIndexOf(NULL_BOOLEAN, new Boolean[]{true, NULL_BOOLEAN, false}));
        assertEquals(NULL_LONG, firstIndexOf(true, new Boolean[]{false, NULL_BOOLEAN, false}));
        assertEquals(NULL_LONG, firstIndexOf(true, (Boolean[])null));

        assertEquals(0, firstIndexOf(true, new ObjectVectorDirect<Boolean>(new Boolean[]{true, NULL_BOOLEAN, false})));
        assertEquals(2, firstIndexOf(false, new ObjectVectorDirect<Boolean>(new Boolean[]{true, NULL_BOOLEAN, false})));
        assertEquals(1, firstIndexOf(NULL_BOOLEAN, new ObjectVectorDirect<Boolean>(new Boolean[]{true, NULL_BOOLEAN, false})));
        assertEquals(NULL_LONG, firstIndexOf(true, new ObjectVectorDirect<Boolean>(new Boolean[]{false, NULL_BOOLEAN, false})));
        assertEquals(NULL_LONG, firstIndexOf(true, (ObjectVector<Boolean>)null));
    }


    <#list primitiveTypes as pt>
    <#if !pt.valueType.isBoolean >


    //////////////////////////// ${pt.primitive} ////////////////////////////

    public void test${pt.boxed}Unbox(){
        assertNull(unbox((${pt.boxed}[])null));
        assertEquals(new ${pt.primitive}[]{1, ${pt.null}, 3, ${pt.null}}, unbox((${pt.primitive})1, null, (${pt.primitive})3, ${pt.null}));
    }

    public void test${pt.boxed}IsNull(){
        assertFalse(isNull((${pt.primitive})3));
        assertTrue(isNull(${pt.null}));
    }

    public void test${pt.boxed}ReplaceIfNullScalar() {
        assertEquals((${pt.primitive}) 3, replaceIfNull((${pt.primitive}) 3, (${pt.primitive}) 7));
        assertEquals((${pt.primitive}) 7, replaceIfNull(${pt.null}, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}ReplaceIfNullArray() {
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNull(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.null}, (${pt.primitive}) 11}), (${pt.primitive}) 7));

        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNull(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.null}, (${pt.primitive}) 11}, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}Count(){
        assertEquals(NULL_LONG,count((${pt.primitive}[])null));
        assertEquals(3,count(new ${pt.primitive}[]{40,50,60}));
        assertEquals(0,count(new ${pt.primitive}[]{}));
        assertEquals(0,count(${pt.null}));
        assertEquals(2,count(new ${pt.primitive}[]{5, ${pt.null},15}));

        assertEquals(NULL_LONG,count((${pt.dbArray})null));
        assertEquals(3,count(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals(0,count(new ${pt.dbArrayDirect}()));
        assertEquals(0,count(new ${pt.dbArrayDirect}(${pt.null})));
        assertEquals(2,count(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{5, ${pt.null},15})));
    }

    public void test${pt.boxed}Last(){
        assertTrue(Math.abs(60-last(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})))==0.0);
        assertEquals(${pt.null},last((${pt.dbArray})null));
        assertEquals(${pt.null},last(new ${pt.dbArrayDirect}()));
        assertEquals(${pt.null},last(new ${pt.dbArrayDirect}(${pt.null})));
        assertTrue(Math.abs(15-last(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{5, ${pt.null},15})))==0.0);
        assertTrue(Math.abs(40-last(new ${pt.dbArrayDirect}((${pt.primitive})40)))==0.0);

        assertTrue(Math.abs(60-last(new ${pt.primitive}[]{40,50,60}))==0.0);
        assertEquals(${pt.null},last((${pt.primitive}[])null));
        assertEquals(${pt.null},last(new ${pt.primitive}[]{}));
        assertEquals(${pt.null},last(new ${pt.primitive}[]{${pt.null}}));
        assertTrue(Math.abs(15-last(new ${pt.primitive}[]{5, ${pt.null},15}))==0.0);
        assertTrue(Math.abs(40-last(new ${pt.primitive}[]{(${pt.primitive})40}))==0.0);
    }

    public void test${pt.boxed}First(){
        assertTrue(Math.abs(40-first(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})))==0.0);
        assertEquals(${pt.null},first((${pt.dbArray})null));
        assertEquals(${pt.null},first(new ${pt.dbArrayDirect}()));
        assertEquals(${pt.null},first(new ${pt.dbArrayDirect}(${pt.null})));
        assertTrue(Math.abs(5-first(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{5, ${pt.null},15})))==0.0);
        assertTrue(Math.abs(40-first(new ${pt.dbArrayDirect}((${pt.primitive})40)))==0.0);

        assertTrue(Math.abs(40-first(new ${pt.primitive}[]{40,50,60}))==0.0);
        assertEquals(${pt.null},first((${pt.primitive}[])null));
        assertEquals(${pt.null},first(new ${pt.primitive}[]{}));
        assertEquals(${pt.null},first(new ${pt.primitive}[]{${pt.null}}));
        assertTrue(Math.abs(5-first(new ${pt.primitive}[]{5, ${pt.null},15}))==0.0);
        assertTrue(Math.abs(40-first(new ${pt.primitive}[]{(${pt.primitive})40}))==0.0);
    }

    public void test${pt.boxed}Nth(){
        assertEquals(${pt.null}, nth(-1,new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals((${pt.primitive})40, nth(0,new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals((${pt.primitive})50, nth(1,new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals((${pt.primitive})60, nth(2,new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals(${pt.null}, nth(10,new ${pt.dbArrayDirect}(new ${pt.primitive}[]{40,50,60})));

        assertEquals(${pt.null}, nth(-1,new ${pt.primitive}[]{40,50,60}));
        assertEquals((${pt.primitive})40, nth(0,new ${pt.primitive}[]{40,50,60}));
        assertEquals((${pt.primitive})50, nth(1,new ${pt.primitive}[]{40,50,60}));
        assertEquals((${pt.primitive})60, nth(2,new ${pt.primitive}[]{40,50,60}));
        assertEquals(${pt.null}, nth(10,new ${pt.primitive}[]{40,50,60}));
    }

    public void test${pt.boxed}CountDistinct() {
        assertEquals(NULL_LONG, countDistinct((${pt.dbArrayDirect})null));
        assertEquals(NULL_LONG, countDistinct((${pt.dbArrayDirect})null,true));
        assertEquals(0, countDistinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{})));
        assertEquals(0, countDistinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{${pt.null}})));
        assertEquals(1, countDistinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1})));
        assertEquals(2, countDistinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}})));
        assertEquals(2, countDistinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(3, countDistinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), true));

        assertEquals(NULL_LONG, countDistinct((${pt.primitive}[])null));
        assertEquals(NULL_LONG, countDistinct((${pt.primitive}[])null,true));
        assertEquals(0, countDistinct(new ${pt.primitive}[]{}));
        assertEquals(0, countDistinct(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(1, countDistinct(new ${pt.primitive}[]{1}));
        assertEquals(2, countDistinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}));
        assertEquals(2, countDistinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}, false));
        assertEquals(3, countDistinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}, true));

        assertEquals(2, countDistinct((${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})1, ${pt.null}, ${pt.null}));
    }

    public void test${pt.boxed}Distinct() {
        assertEquals(null, distinct((${pt.dbArrayDirect})null));
        assertEquals(null, distinct((${pt.dbArrayDirect})null, true));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{})));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{${pt.null}})));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1})));
        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}})));
        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(new ${pt.primitive}[]{1,2, ${pt.null}}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), true));
        assertEquals(new ${pt.primitive}[]{3,1,2}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{3,1,2,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4,${pt.null}}, distinct(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}), true));

        assertEquals(null, distinct((${pt.primitive}[])null));
        assertEquals(null, distinct((${pt.primitive}[])null, true));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.primitive}[]{}));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.primitive}[]{1}));
        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}));
        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}, false));
        assertEquals(new ${pt.primitive}[]{1,2, ${pt.null}}, distinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}, true));
        assertEquals(new ${pt.primitive}[]{3,1,2}, distinct(new ${pt.primitive}[]{3,1,2,1, ${pt.null}, ${pt.null}}, false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4}, distinct(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}, false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4,${pt.null}}, distinct(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}, true));
    }

    public void test${pt.boxed}Vec(){
        assertEquals(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5}, vec(new ${pt.dbArrayDirect}((${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5)));
        assertEquals(null, vec((${pt.dbArray})null));
    }

    public void test${pt.boxed}Array(){
        assertEquals(new ${pt.dbArrayDirect}((${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5), array(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5}));
    }

    public void test${pt.boxed}In(){
        assertTrue(in((${pt.primitive})1,(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3));
        assertFalse(in((${pt.primitive})5,(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3));
        assertFalse(in(${pt.null},(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3));
        assertTrue(in(${pt.null},(${pt.primitive})1,(${pt.primitive})2, ${pt.null},(${pt.primitive})3));
    }

    public void test${pt.boxed}InRange(){
        assertTrue(inRange((${pt.primitive})2,(${pt.primitive})1,(${pt.primitive})3));
        assertTrue(inRange((${pt.primitive})1,(${pt.primitive})1,(${pt.primitive})3));
        assertFalse(inRange(${pt.null},(${pt.primitive})1,(${pt.primitive})3));
        assertTrue(inRange((${pt.primitive})3,(${pt.primitive})1,(${pt.primitive})3));
        assertFalse(inRange((${pt.primitive})4,(${pt.primitive})1,(${pt.primitive})3));
    }

    public void test${pt.boxed}Repeat() {
        assertEquals(new ${pt.primitive}[]{5,5,5}, repeat((${pt.primitive}) 5, 3));
        assertEquals(new ${pt.primitive}[]{}, repeat((${pt.primitive}) 5, -3));
    }

    public void test${pt.boxed}Enlist() {
        assertEquals(new ${pt.primitive}[]{1, 11, 6}, enlist((${pt.primitive})1, (${pt.primitive})11, (${pt.primitive})6));
        assertEquals(new ${pt.primitive}[]{}, enlist((${pt.primitive}[])(null)));
    }

    public void test${pt.boxed}Concat() {
        assertEquals(new ${pt.primitive}[]{}, concat((${pt.primitive}[][])null));
        assertEquals(new ${pt.primitive}[]{1,2,3,4,5,6}, concat(new ${pt.primitive}[]{1,2}, new ${pt.primitive}[]{3}, new ${pt.primitive}[]{4,5,6}));
        assertEquals(new ${pt.primitive}[]{}, concat((${pt.primitive}[])(null)));

        assertEquals(new ${pt.primitive}[]{}, concat((${pt.dbArray}[])null));
        assertEquals(new ${pt.primitive}[]{1,2,3,4,5,6}, concat(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2}), new ${pt.dbArrayDirect}(new ${pt.primitive}[]{3}), new ${pt.dbArrayDirect}(new ${pt.primitive}[]{4,5,6})));
        assertEquals(new ${pt.primitive}[]{}, concat((${pt.dbArray}) (null)));
    }

    public void test${pt.boxed}Reverse() {
        assertEquals(new ${pt.primitive}[]{3,2,1}, reverse((${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3));
        assertEquals(null, reverse((${pt.primitive}[])(null)));

        assertEquals(new ${pt.primitive}[]{3,2,1}, reverse(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{1,2,3})));
        assertEquals(null, reverse((${pt.dbArray}) (null)));
    }

    public void test${pt.boxed}FirstIndexOf() {
        assertEquals(1, firstIndexOf((${pt.primitive})40, new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0}));
        assertEquals(4, firstIndexOf((${pt.primitive})60, new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})1, new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})40, (${pt.primitive}[])null));

        assertEquals(1, firstIndexOf((${pt.primitive})40, new ${pt.dbArrayDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0})));
        assertEquals(4, firstIndexOf((${pt.primitive})60, new ${pt.dbArrayDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})1, new ${pt.dbArrayDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})40, (${pt.dbArray}) null));
    }

    </#if>
    </#list>
}

