/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.structures.vector.DbArrayDirect;
import io.deephaven.engine.structures.vector.DbBooleanArrayDirect;

import static io.deephaven.libs.primitives.BooleanPrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

public class TestBooleanPrimitives extends BaseArrayTestCase {

    public void testIsNull() {
        assertFalse(isNull(Boolean.TRUE));
        assertFalse(isNull(Boolean.FALSE));
        assertTrue(isNull((Boolean) null));
    }

    public void testNullToValueScalar() {
        assertEquals(Boolean.TRUE, nullToValue(Boolean.TRUE, false));
        assertEquals(Boolean.FALSE, nullToValue((Boolean) null, false));
    }

    public void testNullToValueArray() {
        assertEquals(new Boolean[]{true, false, false}, nullToValue(new DbBooleanArrayDirect(Boolean.TRUE, null, Boolean.FALSE), false));
    }

    public void testCount(){
        assertEquals(3,count(new DbBooleanArrayDirect(new Boolean[]{true, false, true})));
        assertEquals(0,count(new DbBooleanArrayDirect()));
        assertEquals(0,count(new DbBooleanArrayDirect(NULL_BOOLEAN)));
        assertEquals(2,count(new DbBooleanArrayDirect(new Boolean[]{true,NULL_BOOLEAN,true})));
        assertEquals(0, count((DbBooleanArrayDirect)null));
    }

    public void testLast(){
        assertFalse(last(new Boolean[]{true,true,false}));
        assertEquals(NULL_BOOLEAN,last(new Boolean[]{}));
        assertEquals(NULL_BOOLEAN,last(new Boolean[]{NULL_BOOLEAN}));
        assertTrue(last(new Boolean[]{false,NULL_BOOLEAN,true}));
        assertTrue(last(new Boolean[]{true}));

        assertEquals(NULL_BOOLEAN,last(new boolean[]{}));
        assertFalse(last(new boolean[]{true,true,false}));
        assertTrue(last(new boolean[]{false,false,true}));
    }

    public void testFirst(){
        assertTrue(first(new Boolean[]{true,false,false}));
        assertEquals(NULL_BOOLEAN,first(new Boolean[]{}));
        assertEquals(NULL_BOOLEAN,first(new Boolean[]{NULL_BOOLEAN}));
        assertTrue(first(new Boolean[]{true,NULL_BOOLEAN,false}));
        assertTrue(first(new Boolean[]{true}));

        assertTrue(first(new boolean[]{true,false,false}));
        assertEquals(NULL_BOOLEAN,first(new boolean[]{}));
        assertTrue(first(new boolean[]{true,false,false}));
    }

    public void testNth(){
        assertEquals(NULL_BOOLEAN, nth(-1,new DbBooleanArrayDirect(new Boolean[]{true, false, true})));
        assertEquals((Boolean)true, nth(0,new DbBooleanArrayDirect(new Boolean[]{true,false,true})));
        assertEquals((Boolean)false, nth(1,new DbBooleanArrayDirect(new Boolean[]{true,false,true})));
        assertEquals((Boolean)true, nth(2,new DbBooleanArrayDirect(new Boolean[]{true,false,true})));
        assertEquals(NULL_BOOLEAN, nth(10,new DbBooleanArrayDirect(new Boolean[]{true,false,true})));

        assertEquals(NULL_BOOLEAN, nth(-1,new Boolean[]{true, false, true}));
        assertEquals((Boolean)true, nth(0,new Boolean[]{true,false,true}));
        assertEquals((Boolean)false, nth(1,new Boolean[]{true,false,true}));
        assertEquals((Boolean)true, nth(2,new Boolean[]{true,false,true}));
        assertEquals(NULL_BOOLEAN, nth(10,new Boolean[]{true,false,true}));
    }

    public void testVec(){
        assertEquals(new Boolean[]{true,false,true}, vec(new DbBooleanArrayDirect(true,false,true)));
    }

    public void testArray(){
        assertEquals(new DbBooleanArrayDirect(true,false,true), array(new Boolean[]{true,false,true}));
    }

    public void testAnd() {
        assertTrue(and(new Boolean[]{true, true, true}));
        assertFalse(and(new Boolean[]{false, true, true}));
        assertFalse(and(new Boolean[]{false, false, true}));
        assertFalse(and(new Boolean[]{true, false, false}));
        assertFalse(and(new Boolean[]{false, false, true}));
        assertFalse(and(new Boolean[]{false, false, false}));

        assertTrue(and(new Boolean[]{true, true, null}, true));
        assertFalse(and(new Boolean[]{false, true, null}, true));
        assertFalse(and(new Boolean[]{false, false, null}, true));
        assertFalse(and(new Boolean[]{true, false, null}, false));
        assertFalse(and(new Boolean[]{false, false, null}, true));
        assertFalse(and(new Boolean[]{false, false, null}, false));

        assertTrue(and(new boolean[]{true, true, true}));
        assertFalse(and(new boolean[]{false, true, true}));
        assertFalse(and(new boolean[]{false, false, true}));
        assertFalse(and(new boolean[]{true, false, false}));
        assertFalse(and(new boolean[]{false, false, true}));
        assertFalse(and(new boolean[]{false, false, false}));

        assertTrue(and(new DbArrayDirect<>(true, true, true)));
        assertFalse(and(new DbArrayDirect<>(false, true, true)));
        assertFalse(and(new DbArrayDirect<>(false, false, true)));
        assertFalse(and(new DbArrayDirect<>(true, false, false)));
        assertFalse(and(new DbArrayDirect<>(false, false, true)));
        assertFalse(and(new DbArrayDirect<>(false, false, false)));

        assertTrue(and(new DbArrayDirect<>(true, true, null), true));
        assertFalse(and(new DbArrayDirect<>(false, true, null), true));
        assertFalse(and(new DbArrayDirect<>(false, false, null), true));
        assertFalse(and(new DbArrayDirect<>(true, false, null), false));
        assertFalse(and(new DbArrayDirect<>(false, false, null), true));
        assertFalse(and(new DbArrayDirect<>(false, false, null), false));
    }

    public void testSum() {
        assertTrue(sum(new Boolean[]{true, true, true}));
        assertTrue(sum(new Boolean[]{false, true, true}));
        assertTrue(sum(new Boolean[]{false, false, true}));
        assertTrue(sum(new Boolean[]{true, false, false}));
        assertTrue(sum(new Boolean[]{false, false, true}));
        assertFalse(sum(new Boolean[]{false, false, false}));

        assertTrue(sum(new boolean[]{true, true, true}));
        assertTrue(sum(new boolean[]{false, true, true}));
        assertTrue(sum(new boolean[]{false, false, true}));
        assertTrue(sum(new boolean[]{true, false, false}));
        assertTrue(sum(new boolean[]{false, false, true}));
        assertFalse(sum(new boolean[]{false, false, false}));
    }

    public void testOr() {
        assertTrue(or(new Boolean[]{true, true, true}));
        assertTrue(or(new Boolean[]{false, true, true}));
        assertTrue(or(new Boolean[]{false, false, true}));
        assertTrue(or(new Boolean[]{true, false, false}));
        assertTrue(or(new Boolean[]{false, false, true}));
        assertFalse(or(new Boolean[]{false, false, false}));

        assertTrue(or(new Boolean[]{true, true, null}, false));
        assertTrue(or(new Boolean[]{false, true, null}, true));
        assertTrue(or(new Boolean[]{false, false, null}, true));
        assertTrue(or(new Boolean[]{true, false, null}, false));
        assertTrue(or(new Boolean[]{false, false, null}, true));
        assertFalse(or(new Boolean[]{false, false, null}, false));

        assertTrue(or(new boolean[]{true, true, true}));
        assertTrue(or(new boolean[]{false, true, true}));
        assertTrue(or(new boolean[]{false, false, true}));
        assertTrue(or(new boolean[]{true, false, false}));
        assertTrue(or(new boolean[]{false, false, true}));
        assertFalse(or(new boolean[]{false, false, false}));
    }

    public void testNot() {
        assertNull(not((boolean[])null));
        assertNull(not((Boolean[])null));

        assertEquals(new Boolean[]{false, false, false}, not(new Boolean[]{true, true, true}));
        assertEquals(new Boolean[]{true, false, false}, not(new Boolean[]{false, true, true}));
        assertEquals(new Boolean[]{true, true, false}, not(new Boolean[]{false, false, true}));
        assertEquals(new Boolean[]{false, true, true}, not(new Boolean[]{true, false, false}));
        assertEquals(new Boolean[]{true, true, false}, not(new Boolean[]{false, false, true}));
        assertEquals(new Boolean[]{true, true, true}, not(new Boolean[]{false, false, false}));
        assertEquals(new Boolean[]{true, null, true}, not(new Boolean[]{false, null, false}));

        assertEquals(new Boolean[]{false, false, false}, not(new boolean[]{true, true, true}));
        assertEquals(new Boolean[]{true, false, false}, not(new boolean[]{false, true, true}));
        assertEquals(new Boolean[]{true, true, false}, not(new boolean[]{false, false, true}));
        assertEquals(new Boolean[]{false, true, true}, not(new boolean[]{true, false, false}));
        assertEquals(new Boolean[]{true, true, false}, not(new boolean[]{false, false, true}));
        assertEquals(new Boolean[]{true, true, true}, not(new boolean[]{false, false, false}));
    }
}
