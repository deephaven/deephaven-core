/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class QueryLibraryTest {
    private SafeCloseable executionContext;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @After
    public void tearDown() {
        executionContext.close();
    }

    @Test
    public void testImportClass() {
        TestCase.assertFalse(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
        ExecutionContext.getContext().getQueryLibrary().importClass(ConcurrentLinkedDeque.class);
        TestCase.assertTrue(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
    }

    @Test
    public void testPackageClass() {
        TestCase.assertFalse(ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                .contains("import java.util.concurrent.*;"));
        ExecutionContext.getContext().getQueryLibrary().importPackage(Package.getPackage("java.util.concurrent"));
        TestCase.assertTrue(ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                .contains("import java.util.concurrent.*;"));
    }

    @Test
    public void testImportStatic() {
        TestCase.assertFalse(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
        ExecutionContext.getContext().getQueryLibrary().importStatic(ConcurrentHashMap.class);
        TestCase.assertTrue(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
    }
}
