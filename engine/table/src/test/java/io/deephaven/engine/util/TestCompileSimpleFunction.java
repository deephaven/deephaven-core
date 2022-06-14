/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.time.DateTimeUtils;
import junit.framework.TestCase;

import java.util.Collections;

public class TestCompileSimpleFunction extends TestCase {
    public void testString() {
        String res = DynamicCompileUtils.compileSimpleFunction(String.class, "return \"Hello, world\"").get();
        TestCase.assertEquals("Hello, world", res);
    }

    public void testImport() {
        String res = DynamicCompileUtils.compileSimpleFunction(String.class, "return currentDateNy()",
                Collections.emptyList(), Collections.singleton(DateTimeUtils.class)).get();
        TestCase.assertEquals(DateTimeUtils.currentDateNy(), res);
    }

    public void testNotString() {
        try {
            DynamicCompileUtils.compileSimpleFunction(String.class, "return 7");
            TestCase.fail("Should never have reached this statement.");
        } catch (RuntimeException e) {
            TestCase.assertTrue(e.getMessage().contains("int cannot be converted to String"));
        }
    }
}
