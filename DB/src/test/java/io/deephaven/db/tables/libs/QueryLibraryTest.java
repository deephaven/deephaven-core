/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.libs;

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
        assertFalse(QueryLibrary.getImportStatement().build()
            .contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
        QueryLibrary.importClass(ConcurrentLinkedDeque.class);
        assertTrue(QueryLibrary.getImportStatement().build()
            .contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
    }

    public void testPackageClass() {
        assertFalse(
            QueryLibrary.getImportStatement().build().contains("import java.util.concurrent.*;"));
        QueryLibrary.importPackage(Package.getPackage("java.util.concurrent"));
        assertTrue(
            QueryLibrary.getImportStatement().build().contains("import java.util.concurrent.*;"));
    }

    public void testImportStatic() {
        assertFalse(QueryLibrary.getImportStatement().build()
            .contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
        QueryLibrary.importStatic(ConcurrentHashMap.class);
        assertTrue(QueryLibrary.getImportStatement().build()
            .contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
    }
}
