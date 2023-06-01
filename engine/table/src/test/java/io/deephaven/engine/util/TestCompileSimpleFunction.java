/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;

public class TestCompileSimpleFunction extends TestCase {
    private SafeCloseable executionContext;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        executionContext.close();
    }

    public void testString() {
        String res = DynamicCompileUtils.compileSimpleFunction(String.class, "return \"Hello, world\"").get();
        TestCase.assertEquals("Hello, world", res);
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
