//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCompileSimpleFunction extends RefreshingTableTestCase {

    @Test
    public void testString() {
        String res = DynamicCompileUtils.compileSimpleFunction(String.class, "return \"Hello, world\"").get();
        assertEquals("Hello, world", res);
    }

    @Test
    public void testNotString() {
        try {
            DynamicCompileUtils.compileSimpleFunction(String.class, "return 7");
            fail("Should never have reached this statement.");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("int cannot be converted to java.lang.String"));
        }
    }
}
