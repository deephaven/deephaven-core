/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.BooleanVectorDirect;
import io.deephaven.vector.ObjectVectorDirect;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

public class TestBooleanPrimitives extends BaseArrayTestCase {

    public void testIsNull() {
        assertFalse(BooleanPrimitives.isNull(Boolean.TRUE));
        assertFalse(BooleanPrimitives.isNull(Boolean.FALSE));
        assertTrue(BooleanPrimitives.isNull((Boolean) null));
    }

    public void testNullToValueScalar() {
        assertEquals(Boolean.TRUE, BooleanPrimitives.nullToValue(Boolean.TRUE, false));
        assertEquals(Boolean.FALSE, BooleanPrimitives.nullToValue((Boolean) null, false));
    }

    public void testNullToValueArray() {
        assertEquals(new Boolean[]{true, false, false}, BooleanPrimitives.nullToValue(new BooleanVectorDirect(Boolean.TRUE, null, Boolean.FALSE), false));
    }

    public void testCount(){
        assertEquals(3, BooleanPrimitives.count(new BooleanVectorDirect(new Boolean[]{true, false, true})));
        assertEquals(0, BooleanPrimitives.count(new BooleanVectorDirect()));
        assertEquals(0, BooleanPrimitives.count(new BooleanVectorDirect(NULL_BOOLEAN)));
        assertEquals(2, BooleanPrimitives.count(new BooleanVectorDirect(new Boolean[]{true,NULL_BOOLEAN,true})));
        assertEquals(0, BooleanPrimitives.count((BooleanVectorDirect)null));
    }

    public void testLast(){
        assertFalse(BooleanPrimitives.last(new Boolean[]{true,true,false}));
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.last(new Boolean[]{}));
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.last(new Boolean[]{NULL_BOOLEAN}));
        assertTrue(BooleanPrimitives.last(new Boolean[]{false,NULL_BOOLEAN,true}));
        assertTrue(BooleanPrimitives.last(new Boolean[]{true}));

        assertEquals(NULL_BOOLEAN, BooleanPrimitives.last(new boolean[]{}));
        assertFalse(BooleanPrimitives.last(new boolean[]{true,true,false}));
        assertTrue(BooleanPrimitives.last(new boolean[]{false,false,true}));
    }

    public void testFirst(){
        assertTrue(BooleanPrimitives.first(new Boolean[]{true,false,false}));
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.first(new Boolean[]{}));
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.first(new Boolean[]{NULL_BOOLEAN}));
        assertTrue(BooleanPrimitives.first(new Boolean[]{true,NULL_BOOLEAN,false}));
        assertTrue(BooleanPrimitives.first(new Boolean[]{true}));

        assertTrue(BooleanPrimitives.first(new boolean[]{true,false,false}));
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.first(new boolean[]{}));
        assertTrue(BooleanPrimitives.first(new boolean[]{true,false,false}));
    }

    public void testNth(){
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.nth(-1,new BooleanVectorDirect(new Boolean[]{true, false, true})));
        assertEquals((Boolean)true, BooleanPrimitives.nth(0,new BooleanVectorDirect(new Boolean[]{true,false,true})));
        assertEquals((Boolean)false, BooleanPrimitives.nth(1,new BooleanVectorDirect(new Boolean[]{true,false,true})));
        assertEquals((Boolean)true, BooleanPrimitives.nth(2,new BooleanVectorDirect(new Boolean[]{true,false,true})));
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.nth(10,new BooleanVectorDirect(new Boolean[]{true,false,true})));

        assertEquals(NULL_BOOLEAN, BooleanPrimitives.nth(-1,new Boolean[]{true, false, true}));
        assertEquals((Boolean)true, BooleanPrimitives.nth(0,new Boolean[]{true,false,true}));
        assertEquals((Boolean)false, BooleanPrimitives.nth(1,new Boolean[]{true,false,true}));
        assertEquals((Boolean)true, BooleanPrimitives.nth(2,new Boolean[]{true,false,true}));
        assertEquals(NULL_BOOLEAN, BooleanPrimitives.nth(10,new Boolean[]{true,false,true}));
    }

    public void testVec(){
        assertEquals(new Boolean[]{true,false,true}, BooleanPrimitives.vec(new BooleanVectorDirect(true,false,true)));
    }

    public void testArray(){
        assertEquals(new BooleanVectorDirect(true,false,true), BooleanPrimitives.array(new Boolean[]{true,false,true}));
    }

    public void testAnd() {
        assertTrue(BooleanPrimitives.and(new Boolean[]{true, true, true}));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, true, true}));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.and(new Boolean[]{true, false, false}));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, false, false}));

        assertTrue(BooleanPrimitives.and(new Boolean[]{true, true, null}, true));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, true, null}, true));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, false, null}, true));
        assertFalse(BooleanPrimitives.and(new Boolean[]{true, false, null}, false));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, false, null}, true));
        assertFalse(BooleanPrimitives.and(new Boolean[]{false, false, null}, false));

        assertTrue(BooleanPrimitives.and(new boolean[]{true, true, true}));
        assertFalse(BooleanPrimitives.and(new boolean[]{false, true, true}));
        assertFalse(BooleanPrimitives.and(new boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.and(new boolean[]{true, false, false}));
        assertFalse(BooleanPrimitives.and(new boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.and(new boolean[]{false, false, false}));

        assertTrue(BooleanPrimitives.and(new ObjectVectorDirect<>(true, true, true)));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, true, true)));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, false, true)));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(true, false, false)));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, false, true)));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, false, false)));

        assertTrue(BooleanPrimitives.and(new ObjectVectorDirect<>(true, true, null), true));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, true, null), true));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, false, null), true));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(true, false, null), false));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, false, null), true));
        assertFalse(BooleanPrimitives.and(new ObjectVectorDirect<>(false, false, null), false));
    }

    public void testSum() {
        assertTrue(BooleanPrimitives.sum(new Boolean[]{true, true, true}));
        assertTrue(BooleanPrimitives.sum(new Boolean[]{false, true, true}));
        assertTrue(BooleanPrimitives.sum(new Boolean[]{false, false, true}));
        assertTrue(BooleanPrimitives.sum(new Boolean[]{true, false, false}));
        assertTrue(BooleanPrimitives.sum(new Boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.sum(new Boolean[]{false, false, false}));

        assertTrue(BooleanPrimitives.sum(new boolean[]{true, true, true}));
        assertTrue(BooleanPrimitives.sum(new boolean[]{false, true, true}));
        assertTrue(BooleanPrimitives.sum(new boolean[]{false, false, true}));
        assertTrue(BooleanPrimitives.sum(new boolean[]{true, false, false}));
        assertTrue(BooleanPrimitives.sum(new boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.sum(new boolean[]{false, false, false}));
    }

    public void testOr() {
        assertTrue(BooleanPrimitives.or(new Boolean[]{true, true, true}));
        assertTrue(BooleanPrimitives.or(new Boolean[]{false, true, true}));
        assertTrue(BooleanPrimitives.or(new Boolean[]{false, false, true}));
        assertTrue(BooleanPrimitives.or(new Boolean[]{true, false, false}));
        assertTrue(BooleanPrimitives.or(new Boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.or(new Boolean[]{false, false, false}));

        assertTrue(BooleanPrimitives.or(new Boolean[]{true, true, null}, false));
        assertTrue(BooleanPrimitives.or(new Boolean[]{false, true, null}, true));
        assertTrue(BooleanPrimitives.or(new Boolean[]{false, false, null}, true));
        assertTrue(BooleanPrimitives.or(new Boolean[]{true, false, null}, false));
        assertTrue(BooleanPrimitives.or(new Boolean[]{false, false, null}, true));
        assertFalse(BooleanPrimitives.or(new Boolean[]{false, false, null}, false));

        assertTrue(BooleanPrimitives.or(new boolean[]{true, true, true}));
        assertTrue(BooleanPrimitives.or(new boolean[]{false, true, true}));
        assertTrue(BooleanPrimitives.or(new boolean[]{false, false, true}));
        assertTrue(BooleanPrimitives.or(new boolean[]{true, false, false}));
        assertTrue(BooleanPrimitives.or(new boolean[]{false, false, true}));
        assertFalse(BooleanPrimitives.or(new boolean[]{false, false, false}));
    }

    public void testNot() {
        assertNull(BooleanPrimitives.not((boolean[])null));
        assertNull(BooleanPrimitives.not((Boolean[])null));

        assertEquals(new Boolean[]{false, false, false}, BooleanPrimitives.not(new Boolean[]{true, true, true}));
        assertEquals(new Boolean[]{true, false, false}, BooleanPrimitives.not(new Boolean[]{false, true, true}));
        assertEquals(new Boolean[]{true, true, false}, BooleanPrimitives.not(new Boolean[]{false, false, true}));
        assertEquals(new Boolean[]{false, true, true}, BooleanPrimitives.not(new Boolean[]{true, false, false}));
        assertEquals(new Boolean[]{true, true, false}, BooleanPrimitives.not(new Boolean[]{false, false, true}));
        assertEquals(new Boolean[]{true, true, true}, BooleanPrimitives.not(new Boolean[]{false, false, false}));
        assertEquals(new Boolean[]{true, null, true}, BooleanPrimitives.not(new Boolean[]{false, null, false}));

        assertEquals(new Boolean[]{false, false, false}, BooleanPrimitives.not(new boolean[]{true, true, true}));
        assertEquals(new Boolean[]{true, false, false}, BooleanPrimitives.not(new boolean[]{false, true, true}));
        assertEquals(new Boolean[]{true, true, false}, BooleanPrimitives.not(new boolean[]{false, false, true}));
        assertEquals(new Boolean[]{false, true, true}, BooleanPrimitives.not(new boolean[]{true, false, false}));
        assertEquals(new Boolean[]{true, true, false}, BooleanPrimitives.not(new boolean[]{false, false, true}));
        assertEquals(new Boolean[]{true, true, true}, BooleanPrimitives.not(new boolean[]{false, false, false}));
    }
}
