/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.lang;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class QueryLibraryTest {
    @Before
    public void setUp() throws Exception {
        QueryLibrary.resetLibrary();
    }

    @After
    public void tearDown() throws Exception {
        QueryLibrary.resetLibrary();
    }

    @Test
    public void testImportClass() {
        TestCase.assertFalse(
                QueryLibrary.getImportStrings().contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
        QueryLibrary.importClass(ConcurrentLinkedDeque.class);
        TestCase.assertTrue(
                QueryLibrary.getImportStrings().contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
    }

    @Test
    public void testPackageClass() {
        TestCase.assertFalse(QueryLibrary.getImportStrings().contains("import java.util.concurrent.*;"));
        QueryLibrary.importPackage(Package.getPackage("java.util.concurrent"));
        TestCase.assertTrue(QueryLibrary.getImportStrings().contains("import java.util.concurrent.*;"));
    }

    @Test
    public void testImportStatic() {
        TestCase.assertFalse(
                QueryLibrary.getImportStrings().contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
        QueryLibrary.importStatic(ConcurrentHashMap.class);
        TestCase.assertTrue(
                QueryLibrary.getImportStrings().contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
    }
}
