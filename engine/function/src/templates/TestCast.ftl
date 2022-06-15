/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;

import static io.deephaven.function.Cast.*;
import static io.deephaven.util.QueryConstants.*;

/**
 * Test Cast.
 */
@SuppressWarnings({"RedundantArrayCreation", "ConstantConditions"})
public class TestCast extends BaseArrayTestCase {

    public void testCastIntOverflow() {

        assertEquals(3, castInt(3L));

        assertEquals(Integer.MAX_VALUE-1, castInt(Integer.MAX_VALUE-1L));
        assertEquals(Integer.MIN_VALUE+1, castInt(Integer.MIN_VALUE+1L));

        try{
            castInt(Integer.MAX_VALUE+1L);
            fail("Should throw an exception");
        }catch(CastDoesNotPreserveValue ignored){
        }
        assertEquals((int)(Integer.MAX_VALUE+1L), castInt(Integer.MAX_VALUE+1L, false));

        try{
            castInt(Integer.MIN_VALUE-1L);
            fail("Should throw an exception");
        }catch(CastDoesNotPreserveValue ignored){
        }
        assertEquals((int)(Integer.MIN_VALUE-1L), castInt(Integer.MIN_VALUE-1L, false));
    }

    public void testCastDoubleOverflow() {

        assertEquals(3.0, castDouble(3.0));

        assertEquals((double) 9007199254740992L, castDouble(9007199254740992L));
        assertEquals((double) -9007199254740992L, castDouble(-9007199254740992L));

        try{
            castDouble(9007199254740993L);
            fail("Should throw an exception");
        }catch(CastDoesNotPreserveValue ignored){
        }
        assertEquals((double)9007199254740993L, castDouble(9007199254740993L, false));

        try{
            castDouble(Long.MAX_VALUE-1);
            fail("Should throw an exception");
        }catch(CastDoesNotPreserveValue ignored){
        }
        assertEquals((double)Long.MAX_VALUE, castDouble(Long.MAX_VALUE, false));

        try{
            castDouble(-9007199254740993L);
            fail("Should throw an exception");
        }catch(CastDoesNotPreserveValue ignored){
        }
        assertEquals((double)-9007199254740993L, castDouble(-9007199254740993L, false));

        try{
            castDouble(Long.MIN_VALUE+1);
            fail("Should throw an exception");
        }catch(CastDoesNotPreserveValue ignored){
        }
        assertEquals((double)(Long.MIN_VALUE+1), castDouble(Long.MIN_VALUE+1, false));
    }


    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    <#if pt.valueType.isInteger >

    public void testCastInt${pt.boxed}() {
        assertEquals(3, castInt((${pt.primitive})3));

        assertNull(castInt((${pt.primitive}[])null));
        assertEquals(new int[]{1,2,3,NULL_INT}, castInt(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}));
        assertEquals(new int[]{1,2,3,NULL_INT}, castInt(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}, true));

        assertNull(castInt((${pt.dbArray}) null));
        assertEquals(new int[]{1,2,3,NULL_INT}, castInt(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}})));
        assertEquals(new int[]{1,2,3,NULL_INT}, castInt(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}), true));
    }

    public void testCastLong${pt.boxed}() {
        assertEquals(3L, castLong((${pt.primitive})3));

        assertNull(castLong((${pt.primitive}[])null));
        assertEquals(new long[]{1,2,3,NULL_LONG}, castLong(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}));
        assertEquals(new long[]{1,2,3,NULL_LONG}, castLong(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}, true));

        assertNull(castLong((${pt.dbArray}) null));
        assertEquals(new long[]{1,2,3,NULL_LONG}, castLong(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}})));
        assertEquals(new long[]{1,2,3,NULL_LONG}, castLong(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}), true));
    }

    </#if>


    public void testCastDouble${pt.boxed}() {

        assertEquals(3.0, castDouble((${pt.primitive})3));

        assertNull(castDouble((${pt.primitive}[])null));
        assertEquals(new double[]{1,2,3,NULL_DOUBLE}, castDouble(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}));
        assertEquals(new double[]{1,2,3,NULL_DOUBLE}, castDouble(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}, true));

        assertNull(castDouble((${pt.dbArray}) null));
        assertEquals(new double[]{1,2,3,NULL_DOUBLE}, castDouble(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}})));
        assertEquals(new double[]{1,2,3,NULL_DOUBLE}, castDouble(new ${pt.dbArrayDirect}(new ${pt.primitive}[]{(${pt.primitive})1,(${pt.primitive})2,(${pt.primitive})3,${pt.null}}), true));
    }


    </#if>
    </#list>


}