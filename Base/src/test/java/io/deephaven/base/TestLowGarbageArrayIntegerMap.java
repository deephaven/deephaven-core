/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.verify.RequirementFailure;
import junit.framework.TestCase;

import java.util.HashMap;

// --------------------------------------------------------------------
/**
 * Tests for {@link LowGarbageArrayIntegerMap}
 */
public class TestLowGarbageArrayIntegerMap extends TestCase {

    // ----------------------------------------------------------------
    public void testLowGarbageArrayIntegerMap() {
        HashMap integerToStringMap = new LowGarbageArrayIntegerMap<String>();

        // check empty get
        assertNull(integerToStringMap.get(5));
        // check 'invalid' get
        assertNull(integerToStringMap.get(-1));
        assertNull(integerToStringMap.get(600));
        assertNull(integerToStringMap.get("foo"));

        // check put
        assertNull(integerToStringMap.put(5, "five"));
        assertNull(integerToStringMap.put(50, "fifty"));
        assertEquals("five", integerToStringMap.get(5));
        assertNull(integerToStringMap.get(6));
        // check replace
        assertEquals("five", integerToStringMap.put(5, "cinco"));
        assertEquals("cinco", integerToStringMap.get(5));

        // check low index
        assertNull(integerToStringMap.put(0, "zero"));
        assertEquals("zero", integerToStringMap.get(0));

        // check remove
        assertEquals("fifty", integerToStringMap.get(50));
        assertEquals("fifty", integerToStringMap.remove(50));
        assertNull(integerToStringMap.remove(50));

        // check 'invalid' remove
        assertNull(integerToStringMap.remove(-1));
        assertNull(integerToStringMap.remove(600));
        assertNull(integerToStringMap.remove("foo"));

        // check expand
        assertNull(integerToStringMap.put(600, "six hundred"));
        assertEquals("cinco", integerToStringMap.get(5));
        assertEquals("cinco", integerToStringMap.put(5, "five"));
        assertEquals("five", integerToStringMap.get(5));
        assertEquals("six hundred", integerToStringMap.get(600));

        // check invalid put
        try {
            integerToStringMap.put(-1, "negative one");
            fail("expected bad index to fail");
        } catch (RequirementFailure requirementFailure) {
            assertTrue(requirementFailure.isThisStackFrameCulprit(-1));
        }

        try {
            integerToStringMap.put("foo", "bar");
            fail("expected bad index to fail");
        } catch (ClassCastException c) {
        }
    }
}
