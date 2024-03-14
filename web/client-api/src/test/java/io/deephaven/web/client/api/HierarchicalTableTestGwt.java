//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.dom.CustomEvent;
import io.deephaven.web.client.api.tree.JsTreeTable;

public class HierarchicalTableTestGwt extends AbstractAsyncGwtTestCase {
    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, time_table")
            .script("static_tree",
                    "empty_table(1000).update(['ID=i', 'Parent=i == 0 ? null : (int)(i/10)']).tree('ID', 'Parent')")
            .script("ticking_tree",
                    "time_table('PT0.1s').update(['ID=i', 'Parent=i == 0 ? null : (int)(i/10)']).tree('ID', 'Parent')")
            .script("import time")
            .script("time.sleep(2)");

    public void testStaticTreeTable() {
        connect(tables)
                .then(treeTable("static_tree"))
                .then(treeTable -> {
                    delayTestFinish(1500);
                    assertFalse(treeTable.isRefreshing());
                    assertFalse(treeTable.isClosed());
                    assertFalse(treeTable.isIncludeConstituents());

                    assertEquals(2, treeTable.getColumns().length);
                    assertEquals("ID", treeTable.getColumns().getAt(0).getName());
                    assertEquals("Parent", treeTable.getColumns().getAt(1).getName());

                    treeTable.setViewport(0, 99, treeTable.getColumns(), null);
                    return treeTable.getViewportData().then(data -> {
                        assertEquals(1d, data.getTreeSize());

                        treeTable.expand(JsTreeTable.RowReferenceUnion.of(0), null);
                        return treeTable.<JsTreeTable.TreeViewportData>nextEvent(JsTreeTable.EVENT_UPDATED, 2001d);
                    }).then(event -> {
                        assertEquals(10d, event.detail.getTreeSize());

                        treeTable.close();

                        assertTrue(treeTable.isClosed());
                        return null;
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testRefreshingTreeTable() {
        connect(tables)
                .then(treeTable("ticking_tree"))
                .then(treeTable -> {
                    // Very large timeout, 3.5s is enough that we see failures on this regularly
                    delayTestFinish(20_000);
                    assertTrue(treeTable.isRefreshing());
                    assertFalse(treeTable.isClosed());
                    assertFalse(treeTable.isIncludeConstituents());

                    assertEquals(3, treeTable.getColumns().length);
                    assertEquals("Timestamp", treeTable.getColumns().getAt(0).getName());
                    assertEquals("ID", treeTable.getColumns().getAt(1).getName());
                    assertEquals("Parent", treeTable.getColumns().getAt(2).getName());

                    treeTable.setViewport(0, 99, treeTable.getColumns(), null);
                    return treeTable.getViewportData().then(data -> {
                        assertEquals(1d, data.getTreeSize());

                        treeTable.expand(JsTreeTable.RowReferenceUnion.of(0), null);
                        // This call effectively asserts that there are 10 rows after expand, so we don't need
                        // to worry about an update from the underlying table racing the expand
                        return waitForEventWhere(treeTable, JsTreeTable.EVENT_UPDATED,
                                (CustomEvent<JsTreeTable.TreeViewportData> d) -> d.detail.getTreeSize() == 1, 20004);
                    }).then(event -> {
                        treeTable.close();

                        assertTrue(treeTable.isClosed());
                        return null;
                    });
                })
                .then(this::finish).catch_(this::report);
    }
}
