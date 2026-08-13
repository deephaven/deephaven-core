//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.junit.DoNotRunWith;
import com.google.gwt.junit.Platform;

@DoNotRunWith(Platform.HtmlUnitBug)
public class DoPutTestGwt extends AbstractAsyncGwtTestCase {

    private final TableSourceBuilder tables = new TableSourceBuilder();

    /**
     * Creates large arrays of ints, doubles, longs, strings, and string arrays (arrays of one element), pushes to
     * server via DoPut (WorkerConnection.newTable), and sets a viewport to confirm the row count is correct.
     */
    public void testLargeTable() {
        final int rowCount = 100_000;

        // Build column-oriented data (Object[][] for WorkerConnection.newTable)
        Object[] intCol = new Object[rowCount];
        Object[] doubleCol = new Object[rowCount];
        Object[] longCol = new Object[rowCount];
        Object[] stringCol = new Object[rowCount];
        Object[] stringArrayCol = new Object[rowCount];
        for (int i = 0; i < rowCount; i++) {
            intCol[i] = (double) i; // JS numbers are doubles
            doubleCol[i] = (double) i * 1.5;
            longCol[i] = LongWrapper.of((long) i * 1000L);
            stringCol[i] = "str_" + i;
            stringArrayCol[i] = new String[] {"item_" + i};
        }

        String[] columnNames = {"MyInt", "MyDouble", "MyLong", "MyString", "MyStringArray"};
        String[] types = {"int", "double", "long", "java.lang.String", "java.lang.String[]"};
        Object[][] data = {intCol, doubleCol, longCol, stringCol, stringArrayCol};

        connect(tables)
                .then(session -> {
                    delayTestFinish(30_000);
                    // Use emptyTable to obtain the WorkerConnection so we can use the underlying newTable api
                    return session.emptyTable(1);
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    table.close();
                    return connection.newTable(columnNames, types, data, null);
                })
                .then(table -> {
                    assertEquals(rowCount, table.getSize(), 0);

                    // Set a viewport covering the full table and verify we get the expected row count
                    table.setViewport(0, rowCount - 1, null);
                    return assertUpdateReceived(table, rowCount, 10_000);
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
