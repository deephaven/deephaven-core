//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import junit.framework.TestCase;

public class TestCompileSimpleFunction extends RefreshingTableTestCase {

    public void testString() {
        String res = DynamicCompileUtils.compileSimpleFunction(String.class, "return \"Hello, world\"").get();
        TestCase.assertEquals("Hello, world", res);
    }

    public void testNotString() {
        try {
            DynamicCompileUtils.compileSimpleFunction(String.class, "return 7");
            TestCase.fail("Should never have reached this statement.");
        } catch (RuntimeException e) {
            TestCase.assertTrue(e.getMessage().contains("int cannot be converted to java.lang.String"));
        }
    }
}
