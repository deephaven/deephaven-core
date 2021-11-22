/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.lang;

import junit.framework.TestCase;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class QueryLibraryTest extends TestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        QueryLibrary.resetLibrary();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        QueryLibrary.resetLibrary();
    }

    public void testImportClass() {
        TestCase.assertFalse(QueryLibrary.getImportStrings().contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
        QueryLibrary.importClass(ConcurrentLinkedDeque.class);
        TestCase.assertTrue(QueryLibrary.getImportStrings().contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
    }

    public void testPackageClass() {
        TestCase.assertFalse(QueryLibrary.getImportStrings().contains("import java.util.concurrent.*;"));
        QueryLibrary.importPackage(Package.getPackage("java.util.concurrent"));
        TestCase.assertTrue(QueryLibrary.getImportStrings().contains("import java.util.concurrent.*;"));
    }

    public void testImportStatic() {
        TestCase.assertFalse(
                QueryLibrary.getImportStrings().contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
        QueryLibrary.importStatic(ConcurrentHashMap.class);
        TestCase.assertTrue(QueryLibrary.getImportStrings().contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
    }
}
