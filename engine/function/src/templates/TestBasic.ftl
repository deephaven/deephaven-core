<#--
  Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
-->

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;
import io.deephaven.util.*;

import static io.deephaven.function.Basic.*;
import static io.deephaven.util.QueryConstants.*;

/**
 * Test Basic.
 */
@SuppressWarnings({"RedundantArrayCreation", "UnnecessaryBoxing", "RedundantCast", "SimplifiableAssertion", "ConstantConditions", "unchecked", "deprecation", "rawtypes"})
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

        assertFalse(isNull(Boolean.TRUE));
        assertTrue(isNull(NULL_BOOLEAN));

        assertFalse(isNull((Character) 'a'));
        assertTrue(isNull(NULL_CHAR_BOXED));

        assertFalse(isNull((Byte) (byte) 1));
        assertTrue(isNull(NULL_BYTE_BOXED));

        assertFalse(isNull((Short) (short) 1));
        assertTrue(isNull(NULL_SHORT_BOXED));

        assertFalse(isNull((Integer) 1));
        assertTrue(isNull(NULL_INT_BOXED));

        assertFalse(isNull((Long) 1L));
        assertTrue(isNull(NULL_LONG_BOXED));

        assertFalse(isNull((Float) 1.0f));
        assertTrue(isNull(NULL_FLOAT_BOXED));

        assertFalse(isNull((Double) 1.0));
        assertTrue(isNull(NULL_DOUBLE_BOXED));
    }

    public void testGenericReplaceIfNullScalar() {
        assertEquals(Integer.valueOf(7), replaceIfNull(Integer.valueOf(7), Integer.valueOf(3)));
        assertEquals(Integer.valueOf(3), replaceIfNull((Integer) null, Integer.valueOf(3)));
    }

    public void testGenericReplaceIfNullArray() {
        assertEquals(new Integer[]{Integer.valueOf(7), Integer.valueOf(3), Integer.valueOf(-5)},
                replaceIfNull(new ObjectVectorDirect<>(new Integer[]{Integer.valueOf(7), null, Integer.valueOf(-5)}), Integer.valueOf(3)));
    }

    public void testGenericInRange() {
        assertTrue(inRange(Integer.valueOf(2), Integer.valueOf(1), Integer.valueOf(3)));
        assertTrue(inRange(Integer.valueOf(1), Integer.valueOf(1), Integer.valueOf(3)));
        assertFalse(inRange(null, Integer.valueOf(1), Integer.valueOf(3)));
        assertTrue(inRange(Integer.valueOf(3), Integer.valueOf(1), Integer.valueOf(3)));
        assertFalse(inRange(Integer.valueOf(4), Integer.valueOf(1), Integer.valueOf(3)));
    }

    public void testObjLen() {
        assertEquals(NULL_LONG, len((ObjectVector)null));
        assertEquals(3, len(new ObjectVectorDirect<>(40, 50, 60)));
        assertEquals(0, len(new ObjectVectorDirect<Integer>()));
        assertEquals(1, len(new ObjectVectorDirect<>(new Integer[]{null})));
        assertEquals(3, len(new ObjectVectorDirect<>(5, null, 15)));
        assertEquals(4, len(new ObjectVectorDirect<>(5, null, 15, NULL_INT)));

        assertEquals(NULL_LONG, len((Integer[])null));
        assertEquals(3, len(new Integer[]{40, 50, 60}));
        assertEquals(0, len(new Integer[]{}));
        assertEquals(1, len(new Integer[]{null}));
        assertEquals(3, len(new Integer[]{5, null, 15}));
        assertEquals(4, len(new Integer[]{5, null, 15, NULL_INT}));
    }

    public void testObjCount() {
        assertEquals(NULL_LONG, countObj((ObjectVector)null));
        assertEquals(3, countObj(new ObjectVectorDirect<>(40, 50, 60)));
        assertEquals(0, countObj(new ObjectVectorDirect<Integer>()));
        assertEquals(0, countObj(new ObjectVectorDirect<>(new Integer[]{null})));
        assertEquals(2, countObj(new ObjectVectorDirect<>(5, null, 15)));
        assertEquals(2, countObj(new ObjectVectorDirect<>(5, null, 15, NULL_INT)));

        assertEquals(NULL_LONG, countObj((Integer[])null));
        assertEquals(3, countObj(new Integer[]{40, 50, 60}));
        assertEquals(0, countObj(new Integer[]{}));
        assertEquals(0, countObj(new Integer[]{null}));
        assertEquals(2, countObj(new Integer[]{5, null, 15}));
        assertEquals(2, countObj(new Integer[]{5, null, 15, NULL_INT}));

        // check that functions can be resolved with varargs
        assertEquals(3, countObj(40, 50, 60));
    }

    public void testObjCountDistinct() {
        assertEquals(NULL_LONG, countDistinctObj((ObjectVector<Short>)null));
        assertEquals(NULL_LONG, countDistinctObj((ObjectVector<Short>)null,true));
        assertEquals(0, countDistinctObj(new ObjectVectorDirect<>(new Short[]{})));
        assertEquals(0, countDistinctObj(new ObjectVectorDirect<>(new Short[]{NULL_SHORT})));
        assertEquals(1, countDistinctObj(new ObjectVectorDirect<>(new Short[]{1})));
        assertEquals(2, countDistinctObj(new ObjectVectorDirect<>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT})));
        assertEquals(2, countDistinctObj(new ObjectVectorDirect<>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(3, countDistinctObj(new ObjectVectorDirect<>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), true));

        assertEquals(NULL_LONG, countDistinctObj((Short[])null));
        assertEquals(NULL_LONG, countDistinctObj((Short[])null,true));
        assertEquals(0, countDistinctObj(new Short[]{}));
        assertEquals(0, countDistinctObj(new Short[]{NULL_SHORT}));
        assertEquals(1, countDistinctObj(new Short[]{1}));
        assertEquals(2, countDistinctObj(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}));
        assertEquals(2, countDistinctObj(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(3, countDistinctObj(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, true));

        // check that functions can be resolved with varargs
        assertEquals(2, countDistinctObj((short)1,(short)2,(short)1,NULL_SHORT,NULL_SHORT));
    }

    public void testObjDistinct() {
        assertEquals(null, distinctObj((ObjectVectorDirect<Short>)null));
        assertEquals(null, distinctObj((ObjectVectorDirect<Short>)null, true));
        assertEquals(new Short[]{}, distinctObj(new Short[]{}));
        assertEquals(new Short[]{}, distinctObj(new ObjectVectorDirect<>(new Short[]{NULL_SHORT})));
        assertEquals(new Short[]{1}, distinctObj(new ObjectVectorDirect<>(new Short[]{1})));
        assertEquals(new Short[]{1,2}, distinctObj(new ObjectVectorDirect<>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT})));
        assertEquals(new Short[]{1,2}, distinctObj(new ObjectVectorDirect<>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(new Short[]{1,2,NULL_SHORT}, distinctObj(new ObjectVectorDirect<>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), true));
        assertEquals(new Short[]{3,1,2}, distinctObj(new ObjectVectorDirect<>(new Short[]{3,1,2,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(new Short[]{3,1,2,4}, distinctObj(new ObjectVectorDirect<>(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(new Short[]{3,1,2,4,NULL_SHORT}, distinctObj(new ObjectVectorDirect<>(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}), true));

        assertEquals(null, distinctObj((Short[])null));
        assertEquals(null, distinctObj((Short[])null, true));
        assertEquals(new Short[]{}, distinctObj(new Short[]{}));
        assertEquals(new Short[]{}, distinctObj(new Short[]{NULL_SHORT}));
        assertEquals(new Short[]{1}, distinctObj(new Short[]{1}));
        assertEquals(new Short[]{1,2}, distinctObj(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}));
        assertEquals(new Short[]{1,2}, distinctObj(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(new Short[]{1,2,NULL_SHORT}, distinctObj(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}, true));
        assertEquals(new Short[]{3,1,2}, distinctObj(new Short[]{3,1,2,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(new Short[]{3,1,2,4}, distinctObj(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}, false));
        assertEquals(new Short[]{3,1,2,4,NULL_SHORT}, distinctObj(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}, true));

        // check that functions can be resolved with varargs
        assertEquals(new Short[]{1,2}, distinctObj((short)1,(short)2,(short)1,NULL_SHORT,NULL_SHORT));
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

    public void testObjReverse() {
        assertEquals(null, reverseObj((ObjectVector[])null));
        assertEquals(new Object[]{}, reverseObj(new ObjectVectorDirect()));
        assertEquals(new Character[]{'c','b','a'}, reverseObj(new ObjectVectorDirect<>(new Character[]{'a','b','c'})));

        assertEquals(null, reverseObj((Character[])null));
        assertEquals(new Character[]{}, reverseObj(new Character[]{}));
        assertEquals(new Character[]{'c','b','a'}, reverseObj(new Character[]{'a','b','c'}));

        // check that functions can be resolved with varargs
        assertEquals(new Character[]{'c','b','a'}, reverseObj('a','b','c'));
    }

    public void testObjFirstIndexOf() {
        assertEquals(1, firstIndexOfObj(Integer.valueOf(40), new Integer[]{0, 40, null, 40, 60, 40, 0}));
        assertEquals(4, firstIndexOfObj(Integer.valueOf(60), new Integer[]{0, 40, null, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOfObj(Integer.valueOf(1), new Integer[]{0, 40, null, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOfObj(Integer.valueOf(40), (Integer[])null));

        assertEquals(1, firstIndexOfObj(Integer.valueOf(40), new ObjectVectorDirect<>(new Integer[]{0, 40, null, 40, 60, 40, 0})));
        assertEquals(4, firstIndexOfObj(Integer.valueOf(60), new ObjectVectorDirect<>(new Integer[]{0, 40, null, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOfObj(Integer.valueOf(1), new ObjectVectorDirect<>(new Integer[]{0, 40, null, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOfObj(Integer.valueOf(40), (ObjectVector) null));

        // check that functions can be resolved with varargs
        assertEquals(1, firstIndexOfObj(40, 0, 40, 40, 60, 40, 0));
    }

    public void testObjLast() {
        assertEquals(null, lastObj((ObjectVectorDirect<Object>)null));
        assertEquals(10, lastObj(new ObjectVectorDirect<Object>(10)));
        assertEquals(3, lastObj(new ObjectVectorDirect<Object>(1, 2, 3)));
        assertEquals(null, lastObj(new ObjectVectorDirect<Object>(1, 2, null)));

        assertEquals(null, lastObj((Integer[])null));
        assertEquals(Integer.valueOf(10), lastObj(new Integer[]{10}));
        assertEquals(Integer.valueOf(3), lastObj(new Integer[]{1, 2, 3}));
        assertEquals(null, lastObj(new Integer[]{1, 2, null}));

        // check that functions can be resolved with varargs
        assertEquals(Integer.valueOf(3), lastObj(1, 2, 3));
    }

    public void testObjFirst() {
        assertEquals(null, firstObj((ObjectVectorDirect<Object>)null));
        assertEquals(10, firstObj(new ObjectVectorDirect<Object>(10)));
        assertEquals(3, firstObj(new ObjectVectorDirect<Object>(3, 2, 1)));
        assertEquals(null, firstObj(new ObjectVectorDirect<Object>(null, 1, 2)));

        assertEquals(null, firstObj((Integer[])null));
        assertEquals(Integer.valueOf(10), firstObj(new Integer[]{10}));
        assertEquals(Integer.valueOf(3), firstObj(new Integer[]{3, 2, 1}));
        assertEquals(null, firstObj(new Integer[]{null, 1, 2}));

        // check that functions can be resolved with varargs
        assertEquals(Integer.valueOf(3), firstObj(3, 2, 1));
    }

    public void testObjNth() {
        assertEquals(null, nthObj(-1, new ObjectVectorDirect<>(40, 50, 60)));
        assertEquals(Integer.valueOf(40), nthObj(0, new ObjectVectorDirect<>(40, 50, 60)));
        assertEquals(Integer.valueOf(50), nthObj(1, new ObjectVectorDirect<>(40, 50, 60)));
        assertEquals(Integer.valueOf(60), nthObj(2, new ObjectVectorDirect<>(40, 50, 60)));
        assertEquals(null, nthObj(10, new ObjectVectorDirect<>(40, 50, 60)));

        assertEquals(null, nthObj(-1, new Integer[]{40, 50, 60}));
        assertEquals(Integer.valueOf(40), nthObj(0, new Integer[]{40, 50, 60}));
        assertEquals(Integer.valueOf(50), nthObj(1, new Integer[]{40, 50, 60}));
        assertEquals(Integer.valueOf(60), nthObj(2, new Integer[]{40, 50, 60}));
        assertEquals(null, nthObj(10, new Integer[]{40,50,60}));

        // check that functions can be resolved with varargs
        assertEquals(Integer.valueOf(40), nthObj(0, 40, 50, 60));
    }

    public void testObjArray() {
        assertEquals(new Character[]{new Character('1'), new Character('3'), new Character('5')}, arrayObj(new ObjectVectorDirect<>(new Character('1'), new Character('3'), new Character('5'))));
    }

    public void testObjVec() {
        assertEquals(null, vecObj((Character[])null));
        assertEquals(new ObjectVectorDirect<>(new Character[]{new Character('1'), new Character('3'), new Character('5')}), vecObj(new Character('1'), new Character('3'), new Character('5')));
    }

    public void testObjIn() {
        assertTrue(inObj(1000000L, new Long[]{1000000L, 2000000L, 3000000L}));
        assertFalse(inObj(5000000L, new Long[]{1000000L, 2000000L, 3000000L}));
        assertFalse(inObj((Integer)null, new Integer[]{1, 2, 3}));
        assertTrue(inObj((Integer)null, new Integer[]{1, 2, null, 3}));

        // check that functions can be resolved with varargs
        assertTrue(inObj(1000000L, 1000000L, 2000000L, 3000000L));
    }

    public void testObjIfelseScalar() {
        final Integer i1 = Integer.valueOf(1);
        final Integer i2 = Integer.valueOf(2);
        assertEquals(null, ifelseObj((Boolean)null, i1, i2));
        assertEquals(i1, ifelseObj(true, i1, i2));
        assertEquals(i2, ifelseObj(false, i1, i2));
    }

    public void testObjIfelseVec() {
        final ObjectVector<Boolean> bv = new ObjectVectorDirect<>(new Boolean[]{null, true, false});
        final ObjectVector<Integer> iv1 = new ObjectVectorDirect<>(new Integer[]{1, 2, 3});
        final ObjectVector<Integer> iv2 = new ObjectVectorDirect<>(new Integer[]{11, 12, 13});
        assertEquals(new Integer[]{null, 2, 13}, ifelseObj(bv, iv1, iv2));
        assertEquals(null, ifelseObj((ObjectVector<Boolean>) null, iv1, iv2));
        assertEquals(null, ifelseObj(bv, null, iv2));
        assertEquals(null, ifelseObj(bv, iv1, null));

        try {
            ifelseObj(new ObjectVectorDirect<>(new Boolean[]{null, true, false, false}), iv1, iv2);
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }

        assertEquals(new Integer[]{null, 1, 2}, ifelseObj(bv, Integer.valueOf(1), Integer.valueOf(2)));
        assertEquals(null, ifelseObj((ObjectVector<Boolean>) null, Integer.valueOf(1), Integer.valueOf(2)));
        assertEquals(new Integer[]{null, null, 2}, ifelseObj(bv, null, Integer.valueOf(2)));
        assertEquals(new Integer[]{null, 1, null}, ifelseObj(bv, Integer.valueOf(1), null));

        try {
            ifelseObj(bv, (Integer)null, (Integer)null);
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }

        try {
            ifelseObj(bv, Integer.valueOf(1), Double.valueOf(2));
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }
    }

    public void testObjIfelseArray() {
        assertEquals(new Integer[]{null, 2, 13}, ifelseObj(new Boolean[]{null, true, false}, new Integer[]{1, 2, 3}, new Integer[]{11, 12, 13}));
        assertEquals(null, ifelseObj((Boolean[]) null, new Integer[]{1, 2, 3}, new Integer[]{11, 12, 13}));
        assertEquals(null, ifelseObj(new Boolean[]{null, true, false}, null, new Integer[]{11, 12, 13}));
        assertEquals(null, ifelseObj(new Boolean[]{null, true, false}, new Integer[]{1, 2, 3}, null));

        try {
            ifelseObj(new Boolean[]{null, true, false, false}, new Integer[]{1, 2, 3}, new Integer[]{11, 12, 13});
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }

        assertEquals(new Integer[]{null, 1, 2}, ifelseObj(new Boolean[]{null, true, false}, Integer.valueOf(1), Integer.valueOf(2)));
        assertEquals(null, ifelseObj((Boolean[]) null, Integer.valueOf(1), Integer.valueOf(2)));
        assertEquals(new Integer[]{null, null, 2}, ifelseObj(new Boolean[]{null, true, false}, null, Integer.valueOf(2)));
        assertEquals(new Integer[]{null, 1, null}, ifelseObj(new Boolean[]{null, true, false}, Integer.valueOf(1), null));

        try {
            ifelseObj(new Boolean[]{null, true, false}, (Integer)null, (Integer)null);
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }

        try {
            ifelseObj(new Boolean[]{null, true, false}, Integer.valueOf(1), Double.valueOf(2));
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }
    }

    public void testObjForwardFillVec() {
        assertEquals(null, forwardFillObj((ObjectVectorDirect<Integer>)null));
        assertEquals(null, forwardFillObj((ObjectVectorDirect<Boolean>)null));

        assertEquals(new Integer[]{0, 0, 1, 2, 2, 3}, forwardFillObj(new ObjectVectorDirect<>(new Integer[]{0, null, 1, 2, null, 3})));
        assertEquals(new Long[]{0L, 0L, 1L, 2L, 2L, 3L}, forwardFillObj(new ObjectVectorDirect<>(new Long[]{0L, null, 1L, 2L, null, 3L})));
        assertEquals(new Double[]{0.0, 0.0, 1.0, 2.0, 2.0, 3.0}, forwardFillObj(new ObjectVectorDirect<>(new Double[]{0.0, null, 1.0, 2.0, null, 3.0})));
        assertEquals(new Boolean[]{true, true, false, true, true, false}, forwardFillObj(new ObjectVectorDirect<>(new Boolean[]{true, null, false, true, null, false})));
    }

    public void testObjForwardFillArray() {
        assertEquals(null, forwardFillObj((Boolean[])null));

        assertEquals(new Integer[]{0, 0, 1, 2, 2, 3}, forwardFillObj(new Integer[]{0, null, 1, 2, null, 3}));
        assertEquals(new Long[]{0L, 0L, 1L, 2L, 2L, 3L}, forwardFillObj(new Long[]{0L, null, 1L, 2L, null, 3L}));
        assertEquals(new Double[]{0.0, 0.0, 1.0, 2.0, 2.0, 3.0}, forwardFillObj(new Double[]{0.0, null, 1.0, 2.0, null, 3.0}));
        assertEquals(new Boolean[]{true, true, false, true, true, false}, forwardFillObj(new Boolean[]{true, null, false, true, null, false}));
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

    public void testBooleanLen() {
        assertEquals(3, len(new Boolean[]{true, false, true}));
        assertEquals(0, len(new Boolean[]{}));
        assertEquals(1, len(new Boolean[]{QueryConstants.NULL_BOOLEAN}));
        assertEquals(3, len(new Boolean[]{true, QueryConstants.NULL_BOOLEAN, true}));
        assertEquals(NULL_LONG, len((Boolean[])null));

        assertEquals(3, len(new ObjectVectorDirect<>(new Boolean[]{true, false, true})));
        assertEquals(0, len(new ObjectVectorDirect<>()));
        assertEquals(1, len(new ObjectVectorDirect<>(QueryConstants.NULL_BOOLEAN)));
        assertEquals(3, len(new ObjectVectorDirect<>(new Boolean[]{true, QueryConstants.NULL_BOOLEAN, true})));
        assertEquals(NULL_LONG, len((ObjectVector)null));
    }

    public void testBooleanCount(){
        assertEquals(3,countObj(new Boolean[]{true, false, true}));
        assertEquals(0,countObj(new Boolean[]{}));
        assertEquals(0,countObj(new Boolean[]{QueryConstants.NULL_BOOLEAN}));
        assertEquals(2,countObj(new Boolean[]{true, QueryConstants.NULL_BOOLEAN,true}));
        assertEquals(NULL_LONG, countObj((Boolean[])null));

        assertEquals(3,countObj(new ObjectVectorDirect<>(new Boolean[]{true, false, true})));
        assertEquals(0,countObj(new ObjectVectorDirect<>()));
        assertEquals(0,countObj(new ObjectVectorDirect<>(QueryConstants.NULL_BOOLEAN)));
        assertEquals(2,countObj(new ObjectVectorDirect<>(new Boolean[]{true, QueryConstants.NULL_BOOLEAN,true})));
        assertEquals(NULL_LONG, countObj((ObjectVector)null));
    }

    public void testBooleanLast(){
        assertFalse(lastObj(new Boolean[]{true,true,false}));
        assertEquals(QueryConstants.NULL_BOOLEAN,lastObj((Boolean[])null));
        assertEquals(QueryConstants.NULL_BOOLEAN,lastObj(new Boolean[]{}));
        assertEquals(QueryConstants.NULL_BOOLEAN,lastObj(new Boolean[]{QueryConstants.NULL_BOOLEAN}));
        assertTrue(lastObj(new Boolean[]{false, QueryConstants.NULL_BOOLEAN,true}));
        assertTrue(lastObj(new Boolean[]{true}));
    }

    public void testBooleanFirst(){
        assertTrue(firstObj(new Boolean[]{true,false,false}));
        assertEquals(QueryConstants.NULL_BOOLEAN,firstObj((Boolean[])null));
        assertEquals(QueryConstants.NULL_BOOLEAN,firstObj(new Boolean[]{}));
        assertEquals(QueryConstants.NULL_BOOLEAN,firstObj(new Boolean[]{QueryConstants.NULL_BOOLEAN}));
        assertTrue(firstObj(new Boolean[]{true, QueryConstants.NULL_BOOLEAN,false}));
        assertTrue(firstObj(new Boolean[]{true}));
    }

    public void testBooleanNth(){
        assertEquals(QueryConstants.NULL_BOOLEAN, nthObj(-1,new ObjectVectorDirect<>(new Boolean[]{true, false, true})));
        assertEquals((Boolean)true, nthObj(0,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));
        assertEquals((Boolean)false, nthObj(1,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));
        assertEquals((Boolean)true, nthObj(2,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));
        assertEquals(QueryConstants.NULL_BOOLEAN, nthObj(10,new ObjectVectorDirect<>(new Boolean[]{true,false,true})));

        assertEquals(QueryConstants.NULL_BOOLEAN, nthObj(-1,new Boolean[]{true, false, true}));
        assertEquals((Boolean)true, nthObj(0,new Boolean[]{true,false,true}));
        assertEquals((Boolean)false, nthObj(1,new Boolean[]{true,false,true}));
        assertEquals((Boolean)true, nthObj(2,new Boolean[]{true,false,true}));
        assertEquals(QueryConstants.NULL_BOOLEAN, nthObj(10,new Boolean[]{true,false,true}));
    }

    public void testBooleanArray(){
        assertEquals(null, arrayObj((ObjectVector)null));
        assertEquals(new Boolean[]{true,false,true}, arrayObj(new ObjectVectorDirect<>(true,false,true)));
    }

    public void testBooleanVec(){
        assertEquals(new ObjectVectorDirect<>(true,false,true), vecObj(new Boolean[]{true,false,true}));
    }

    public void testBooleanIn(){
        assertTrue(inObj(true,new Boolean[]{true,false}));
        assertFalse(inObj(false,new Boolean[]{true,true}));
        assertFalse(inObj((Boolean)null,new Boolean[]{true,false}));
        assertTrue(inObj(null,new Boolean[]{(Boolean)null,true,false}));
    }


    public void testBooleanCountDistinct() {
        assertEquals(NULL_LONG, countDistinctObj((ObjectVector)null));
        assertEquals(0, countDistinctObj(new ObjectVectorDirect<>((Boolean)null)));
        assertEquals(1, countDistinctObj(new ObjectVectorDirect<>((Boolean)null), true));
        assertEquals(0, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{})));
        assertEquals(0, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{NULL_BOOLEAN})));
        assertEquals(1, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN})));
        assertEquals(1, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN})));
        assertEquals(2, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{true,false,true,false,NULL_BOOLEAN})));
        assertEquals(2, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN}),true));
        assertEquals(2, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN}),true));
        assertEquals(3, countDistinctObj(new ObjectVectorDirect<>(new Boolean[]{false,true,false,true,NULL_BOOLEAN}),true));

        assertEquals(NULL_LONG, countDistinctObj((Boolean[])null));
        assertEquals(0, countDistinctObj(new Boolean[]{null}));
        assertEquals(1, countDistinctObj(new Boolean[]{null}, true));
        assertEquals(0, countDistinctObj(new Boolean[]{}));
        assertEquals(0, countDistinctObj(new Boolean[]{NULL_BOOLEAN}));
        assertEquals(1, countDistinctObj(new Boolean[]{true,true,NULL_BOOLEAN}));
        assertEquals(1, countDistinctObj(new Boolean[]{false,false,NULL_BOOLEAN}));
        assertEquals(2, countDistinctObj(new Boolean[]{true,false,true,false,NULL_BOOLEAN}));
        assertEquals(2, countDistinctObj(new Boolean[]{true,true,NULL_BOOLEAN},true));
        assertEquals(2, countDistinctObj(new Boolean[]{false,false,NULL_BOOLEAN},true));
        assertEquals(3, countDistinctObj(new Boolean[]{false,true,false,true,NULL_BOOLEAN},true));
    }

    public void testBooleanDistinct() {
        assertEquals(null, distinctObj((ObjectVector)null));
        assertEquals(null, distinctObj((ObjectVector)null, true));
        assertEquals(new Boolean[]{}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{})));
        assertEquals(new Boolean[]{}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{NULL_BOOLEAN})));
        assertEquals(new Boolean[]{true}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN})));
        assertEquals(new Boolean[]{false}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN})));
        assertEquals(new Boolean[]{true,false}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{true,false,true,false,NULL_BOOLEAN})));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{true,false,NULL_BOOLEAN}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{true,false,false,true,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{true,true,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{false,false,NULL_BOOLEAN}),true));
        assertEquals(new Boolean[]{false,true,NULL_BOOLEAN}, distinctObj(new ObjectVectorDirect<>(new Boolean[]{false,true,false,true,NULL_BOOLEAN}),true));

        assertEquals(null, distinctObj((Boolean[])null));
        assertEquals(null, distinctObj((Boolean[])null, true));
        assertEquals(new Boolean[]{}, distinctObj(new Boolean[]{}));
        assertEquals(new Boolean[]{}, distinctObj(new Boolean[]{NULL_BOOLEAN}));
        assertEquals(new Boolean[]{true}, distinctObj(new Boolean[]{true,true,NULL_BOOLEAN}));
        assertEquals(new Boolean[]{false}, distinctObj(new Boolean[]{false,false,NULL_BOOLEAN}));
        assertEquals(new Boolean[]{true,false}, distinctObj(new Boolean[]{true,false,true,false,NULL_BOOLEAN}));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinctObj(new Boolean[]{true,true,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinctObj(new Boolean[]{false,false,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{true,false,NULL_BOOLEAN}, distinctObj(new Boolean[]{true,false,false,true,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{true,NULL_BOOLEAN}, distinctObj(new Boolean[]{true,true,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{false,NULL_BOOLEAN}, distinctObj(new Boolean[]{false,false,NULL_BOOLEAN},true));
        assertEquals(new Boolean[]{false,true,NULL_BOOLEAN}, distinctObj(new Boolean[]{false,true,false,true,NULL_BOOLEAN},true));

        assertEquals(new Boolean[]{false,true}, distinctObj(false,true,false,true,NULL_BOOLEAN));
    }

    public void testBooleanRepeat() {
        try {
            repeat(true, -3);
            fail("Should have failed on invalid arguments.");
        } catch(IllegalArgumentException expected){
            //pass
        }

        assertEquals(new Boolean[]{true,true,true}, repeat(true, 3));
    }

    public void testBooleanConcat() {
        assertEquals(new Boolean[]{true,false,false,false,true,true}, concat(new Boolean[]{true,false}, new Boolean[]{false}, new Boolean[]{false,true,true}));
        assertEquals(new Boolean[]{}, concat((Boolean[])(null)));

        assertEquals(new Boolean[]{true,false,false,false,true,true}, concat(new ObjectVectorDirect<>(new Boolean[]{true,false}), new ObjectVectorDirect<>(new Boolean[]{false}), new ObjectVectorDirect<>(new Boolean[]{false,true,true})));
        assertEquals(new Boolean[]{}, concat((ObjectVector) (null)));
        assertEquals(new Boolean[]{}, concat(new ObjectVector[]{}));
    }

    public void testBooleanReverse() {
        assertEquals(new Boolean[]{false,true,true}, reverseObj(new Boolean[]{true, true, false}));
        assertEquals(null, reverseObj((Boolean[])(null)));

        assertEquals(new Boolean[]{false,true,true}, reverseObj(new ObjectVectorDirect<>(new Boolean[]{true,true,false})));
        assertEquals(null, reverseObj((ObjectVector) (null)));
    }

    public void testBooleanFirstIndexOf() {
        assertEquals(0, firstIndexOfObj(true, new Boolean[]{true, NULL_BOOLEAN, false}));
        assertEquals(2, firstIndexOfObj(false, new Boolean[]{true, NULL_BOOLEAN, false}));
        assertEquals(1, firstIndexOfObj(NULL_BOOLEAN, new Boolean[]{true, NULL_BOOLEAN, false}));
        assertEquals(NULL_LONG, firstIndexOfObj(true, new Boolean[]{false, NULL_BOOLEAN, false}));
        assertEquals(NULL_LONG, firstIndexOfObj(true, (Boolean[])null));

        assertEquals(0, firstIndexOfObj(true, new ObjectVectorDirect<>(new Boolean[]{true, NULL_BOOLEAN, false})));
        assertEquals(2, firstIndexOfObj(false, new ObjectVectorDirect<>(new Boolean[]{true, NULL_BOOLEAN, false})));
        assertEquals(1, firstIndexOfObj(NULL_BOOLEAN, new ObjectVectorDirect<>(new Boolean[]{true, NULL_BOOLEAN, false})));
        assertEquals(NULL_LONG, firstIndexOfObj(true, new ObjectVectorDirect<>(new Boolean[]{false, NULL_BOOLEAN, false})));
        assertEquals(NULL_LONG, firstIndexOfObj(true, (ObjectVector<Boolean>)null));
    }


    <#list primitiveTypes as pt>

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
        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNull(new ${pt.vectorDirect}(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.null}, (${pt.primitive}) 11}), (${pt.primitive}) 7));

        assertEquals(new ${pt.primitive}[]{(${pt.primitive}) 3, (${pt.primitive}) 7, (${pt.primitive}) 11}, replaceIfNull(new ${pt.primitive}[]{(${pt.primitive}) 3, ${pt.null}, (${pt.primitive}) 11}, (${pt.primitive}) 7));
    }

    public void test${pt.boxed}Len() {
        assertEquals(NULL_LONG, len((${pt.primitive}[])null));
        assertEquals(3, len(new ${pt.primitive}[]{40,50,60}));
        assertEquals(0, len(new ${pt.primitive}[]{}));
        assertEquals(1, len(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(3, len(new ${pt.primitive}[]{5, ${pt.null},15}));

        assertEquals(NULL_LONG, len((${pt.vector})null));
        assertEquals(3, len(new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals(0, len(new ${pt.vectorDirect}()));
        assertEquals(1, len(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(3, len(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null},15})));
    }

    public void test${pt.boxed}Count(){
        assertEquals(NULL_LONG,count((${pt.primitive}[])null));
        assertEquals(3,count(new ${pt.primitive}[]{40,50,60}));
        assertEquals(0,count(new ${pt.primitive}[]{}));
        assertEquals(0,count(${pt.null}));
        assertEquals(2,count(new ${pt.primitive}[]{5, ${pt.null},15}));

        assertEquals(NULL_LONG,count((${pt.vector})null));
        assertEquals(3,count(new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals(0,count(new ${pt.vectorDirect}()));
        assertEquals(0,count(new ${pt.vectorDirect}(${pt.null})));
        assertEquals(2,count(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null},15})));
    }

    public void test${pt.boxed}Last(){
        assertTrue(Math.abs(60-last(new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})))==0.0);
        assertEquals(${pt.null},last((${pt.vector})null));
        assertEquals(${pt.null},last(new ${pt.vectorDirect}()));
        assertEquals(${pt.null},last(new ${pt.vectorDirect}(${pt.null})));
        assertTrue(Math.abs(15-last(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null},15})))==0.0);
        assertTrue(Math.abs(40-last(new ${pt.vectorDirect}((${pt.primitive})40)))==0.0);

        assertTrue(Math.abs(60-last(new ${pt.primitive}[]{40,50,60}))==0.0);
        assertEquals(${pt.null},last((${pt.primitive}[])null));
        assertEquals(${pt.null},last(new ${pt.primitive}[]{}));
        assertEquals(${pt.null},last(new ${pt.primitive}[]{${pt.null}}));
        assertTrue(Math.abs(15-last(new ${pt.primitive}[]{5, ${pt.null},15}))==0.0);
        assertTrue(Math.abs(40-last(new ${pt.primitive}[]{(${pt.primitive})40}))==0.0);
    }

    public void test${pt.boxed}First(){
        assertTrue(Math.abs(40-first(new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})))==0.0);
        assertEquals(${pt.null},first((${pt.vector})null));
        assertEquals(${pt.null},first(new ${pt.vectorDirect}()));
        assertEquals(${pt.null},first(new ${pt.vectorDirect}(${pt.null})));
        assertTrue(Math.abs(5-first(new ${pt.vectorDirect}(new ${pt.primitive}[]{5, ${pt.null},15})))==0.0);
        assertTrue(Math.abs(40-first(new ${pt.vectorDirect}((${pt.primitive})40)))==0.0);

        assertTrue(Math.abs(40-first(new ${pt.primitive}[]{40,50,60}))==0.0);
        assertEquals(${pt.null},first((${pt.primitive}[])null));
        assertEquals(${pt.null},first(new ${pt.primitive}[]{}));
        assertEquals(${pt.null},first(new ${pt.primitive}[]{${pt.null}}));
        assertTrue(Math.abs(5-first(new ${pt.primitive}[]{5, ${pt.null},15}))==0.0);
        assertTrue(Math.abs(40-first(new ${pt.primitive}[]{(${pt.primitive})40}))==0.0);
    }

    public void test${pt.boxed}Nth(){
        assertEquals(${pt.null}, nth(-1,new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals((${pt.primitive})40, nth(0,new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals((${pt.primitive})50, nth(1,new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals((${pt.primitive})60, nth(2,new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})));
        assertEquals(${pt.null}, nth(10,new ${pt.vectorDirect}(new ${pt.primitive}[]{40,50,60})));

        assertEquals(${pt.null}, nth(-1,new ${pt.primitive}[]{40,50,60}));
        assertEquals((${pt.primitive})40, nth(0,new ${pt.primitive}[]{40,50,60}));
        assertEquals((${pt.primitive})50, nth(1,new ${pt.primitive}[]{40,50,60}));
        assertEquals((${pt.primitive})60, nth(2,new ${pt.primitive}[]{40,50,60}));
        assertEquals(${pt.null}, nth(10,new ${pt.primitive}[]{40,50,60}));
    }

    public void test${pt.boxed}CountDistinct() {
        assertEquals(NULL_LONG, countDistinct((${pt.vectorDirect})null));
        assertEquals(NULL_LONG, countDistinct((${pt.vectorDirect})null,true));
        assertEquals(0, countDistinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{})));
        assertEquals(0, countDistinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}})));
        assertEquals(1, countDistinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1})));
        assertEquals(2, countDistinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}})));
        assertEquals(2, countDistinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(3, countDistinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), true));

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
        assertEquals(null, distinct((${pt.vectorDirect})null));
        assertEquals(null, distinct((${pt.vectorDirect})null, true));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{})));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}})));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1})));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{}),true));
        assertEquals(new ${pt.primitive}[]{${pt.null}}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}}),true));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1}),true));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{}),false));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{${pt.null}}),false));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1}),false));


        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}})));
        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(new ${pt.primitive}[]{1,2, ${pt.null}}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}), true));
        assertEquals(new ${pt.primitive}[]{3,1,2}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{3,1,2,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}), false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4,${pt.null}}, distinct(new ${pt.vectorDirect}(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}), true));

        assertEquals(null, distinct((${pt.primitive}[])null));
        assertEquals(null, distinct((${pt.primitive}[])null, true));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.primitive}[]{}));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.primitive}[]{${pt.null}}));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.primitive}[]{1}));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.primitive}[]{}, true));
        assertEquals(new ${pt.primitive}[]{${pt.null}}, distinct(new ${pt.primitive}[]{${pt.null}}, true));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.primitive}[]{1}, true));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.primitive}[]{}, false));
        assertEquals(new ${pt.primitive}[]{}, distinct(new ${pt.primitive}[]{${pt.null}}, false));
        assertEquals(new ${pt.primitive}[]{1}, distinct(new ${pt.primitive}[]{1}, false));
        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}));
        assertEquals(new ${pt.primitive}[]{1,2}, distinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}, false));
        assertEquals(new ${pt.primitive}[]{1,2, ${pt.null}}, distinct(new ${pt.primitive}[]{1,2,1, ${pt.null}, ${pt.null}}, true));
        assertEquals(new ${pt.primitive}[]{3,1,2}, distinct(new ${pt.primitive}[]{3,1,2,1, ${pt.null}, ${pt.null}}, false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4}, distinct(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}, false));
        assertEquals(new ${pt.primitive}[]{3,1,2,4,${pt.null}}, distinct(new ${pt.primitive}[]{3,1,2,4,1, ${pt.null}, ${pt.null}}, true));
    }

    public void test${pt.boxed}Array(){
        assertEquals(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5}, array(new ${pt.vectorDirect}((${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5)));
        assertEquals(null, array((${pt.vector})null));
    }

    public void test${pt.boxed}Vec(){
        assertEquals(new ${pt.vectorDirect}((${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5), vec(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})3,(${pt.primitive})5}));
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

        assertEquals(new ${pt.primitive}[]{}, concat((${pt.vector}[])null));
        assertEquals(new ${pt.primitive}[]{1,2,3,4,5,6}, concat(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2}), new ${pt.vectorDirect}(new ${pt.primitive}[]{3}), new ${pt.vectorDirect}(new ${pt.primitive}[]{4,5,6})));
        assertEquals(new ${pt.primitive}[]{}, concat((${pt.vector}) (null)));
    }

    public void test${pt.boxed}Reverse() {
        assertEquals(new ${pt.primitive}[]{3,2,1}, reverse((${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3));
        assertEquals(null, reverse((${pt.primitive}[])(null)));

        assertEquals(new ${pt.primitive}[]{3,2,1}, reverse(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,2,3})));
        assertEquals(null, reverse((${pt.vector}) (null)));
    }

    public void test${pt.boxed}FirstIndexOf() {
        assertEquals(1, firstIndexOf((${pt.primitive})40, new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0}));
        assertEquals(4, firstIndexOf((${pt.primitive})60, new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})1, new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0}));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})40, (${pt.primitive}[])null));

        assertEquals(1, firstIndexOf((${pt.primitive})40, new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0})));
        assertEquals(4, firstIndexOf((${pt.primitive})60, new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})1, new ${pt.vectorDirect}(new ${pt.primitive}[]{0, 40, ${pt.null}, 40, 60, 40, 0})));
        assertEquals(NULL_LONG, firstIndexOf((${pt.primitive})40, (${pt.vector}) null));
    }

    public void test${pt.boxed}IfelseScalar() {
        assertEquals(${pt.null}, ifelse((Boolean)null, (${pt.primitive})1, (${pt.primitive})2));
        assertEquals((${pt.primitive})1, ifelse(true, (${pt.primitive})1, (${pt.primitive})2));
        assertEquals((${pt.primitive})2, ifelse(false, (${pt.primitive})1, (${pt.primitive})2));
    }

    public void test${pt.boxed}IfelseVec() {
        final ObjectVector<Boolean> bv = new ObjectVectorDirect<>(new Boolean[]{null, true, false});
        final ${pt.vector} iv1 = new ${pt.vectorDirect}(new ${pt.primitive}[]{1, 2, 3});
        final ${pt.vector} iv2 = new ${pt.vectorDirect}(new ${pt.primitive}[]{11, 12, 13});
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 13}, ifelse(bv, iv1, iv2));
        assertEquals(null, ifelse((ObjectVector<Boolean>) null, iv1, iv2));
        assertEquals(null, ifelse(bv, null, iv2));
        assertEquals(null, ifelse(bv, iv1, null));

        try {
            ifelse(new ObjectVectorDirect<>(new Boolean[]{null, true, false, false}), iv1, iv2);
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }

        assertEquals(new ${pt.primitive}[]{${pt.null}, 1, 2}, ifelse(bv, (${pt.primitive})1, (${pt.primitive})2));
        assertEquals(null, ifelse((ObjectVector<Boolean>) null, (${pt.primitive})1, (${pt.primitive})2));
    }

    public void test${pt.boxed}IfelseArray() {
        assertEquals(new ${pt.primitive}[]{${pt.null}, 2, 13}, ifelse(new Boolean[]{null, true, false}, new ${pt.primitive}[]{1, 2, 3}, new ${pt.primitive}[]{11, 12, 13}));
        assertEquals(null, ifelse((Boolean[]) null, new ${pt.primitive}[]{1, 2, 3}, new ${pt.primitive}[]{11, 12, 13}));
        assertEquals(null, ifelse(new Boolean[]{null, true, false}, null, new ${pt.primitive}[]{11, 12, 13}));
        assertEquals(null, ifelse(new Boolean[]{null, true, false}, new ${pt.primitive}[]{1, 2, 3}, null));

        try {
            ifelse(new Boolean[]{null, true, false, false}, new ${pt.primitive}[]{1, 2, 3}, new ${pt.primitive}[]{11, 12, 13});
            fail("Should have raised an IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
        }

        assertEquals(new ${pt.primitive}[]{${pt.null}, 1, 2}, ifelse(new Boolean[]{null, true, false}, (${pt.primitive})1, (${pt.primitive})2));
        assertEquals(null, ifelse((Boolean[]) null, (${pt.primitive})1, (${pt.primitive})2));
    }

    public void test${pt.boxed}ForwardFillVec() {
        assertEquals(null, forwardFill((${pt.vectorDirect})null));
        assertEquals(new ${pt.primitive}[]{0, 0, 1, 2, 2, 3}, forwardFill(new ${pt.vectorDirect}(new ${pt.primitive}[]{0, ${pt.null}, 1, 2, ${pt.null}, 3})));
    }

    public void test${pt.boxed}ForwardFillArray() {
        assertEquals(null, forwardFill((${pt.primitive}[])null));
        assertEquals(new ${pt.primitive}[]{0, 0, 1, 2, 2, 3}, forwardFill(new ${pt.primitive}[]{0, ${pt.null}, 1, 2, ${pt.null}, 3}));
    }

    </#list>
}
