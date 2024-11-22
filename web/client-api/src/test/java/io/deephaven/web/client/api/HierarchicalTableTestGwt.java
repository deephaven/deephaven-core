//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.dom.CustomEvent;
import io.deephaven.web.client.api.tree.JsTreeTable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.tree.JsRollupConfig;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class HierarchicalTableTestGwt extends AbstractAsyncGwtTestCase {
    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, time_table, agg")
            .script("static_tree",
                    "empty_table(1000).update(['ID=i', 'Parent=i == 0 ? null : (int)(i/10)']).tree('ID', 'Parent')")
            .script("ticking_tree",
                    "time_table('PT0.1s').update(['ID=i', 'Parent=i == 0 ? null : (int)(i/10)']).format_columns(['ID=ID>0 ? GREEN : RED']).tree('ID', 'Parent')")
            .script("table_to_rollup",
                    "time_table('PT0.1s').update(['Y=Math.sin(i/3)', 'X=i%3']).format_columns(['Y=Y>0 ? GREEN : RED'])")
            .script("ticking_rollup",
                    "table_to_rollup.rollup(aggs=[agg.first('Y')],by=['X'],include_constituents=True)");

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

                    // Wait for the table to tick such that the first row has children
                    return waitForEventWhere(treeTable, JsTreeTable.EVENT_UPDATED,
                            (CustomEvent<JsTreeTable.TreeViewportData> d) -> d.detail.getTreeSize() == 1
                                    && d.detail.getRows().getAt(0).hasChildren(),
                            10001).then(data -> {
                                treeTable.expand(JsTreeTable.RowReferenceUnion.of(0), null);

                                // Wait for the expand to occur and table to show all 10 rows
                                return waitForEventWhere(treeTable, JsTreeTable.EVENT_UPDATED,
                                        (CustomEvent<JsTreeTable.TreeViewportData> d) -> d.detail.getTreeSize() == 10,
                                        14004);
                            }).then(event -> {
                                treeTable.close();
                                assertTrue(treeTable.isClosed());
                                return null;
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCreateRollup() {
        connect(tables)
                .then(table("table_to_rollup"))
                .then(table -> {
                    List<Supplier<Promise<JsTreeTable>>> tests = new ArrayList<>();
                    // For each supported operation, apply it to the numeric column
                    // Then expand to verify data can load
                    String[] count = new String[] {
                            JsAggregationOperation.COUNT,
                            JsAggregationOperation.COUNT_DISTINCT,
                            // TODO(deephaven-core#6201) re-enable this line when fixed
                            // JsAggregationOperation.DISTINCT,
                            JsAggregationOperation.MIN,
                            JsAggregationOperation.MAX,
                            JsAggregationOperation.SUM,
                            JsAggregationOperation.ABS_SUM,
                            JsAggregationOperation.VAR,
                            JsAggregationOperation.AVG,
                            JsAggregationOperation.STD,
                            JsAggregationOperation.FIRST,
                            JsAggregationOperation.LAST,
                            JsAggregationOperation.UNIQUE
                    };
                    for (int i = 0; i < count.length; i++) {
                        final int step = i;
                        String operation = count[i];
                        JsRollupConfig cfg = new JsRollupConfig();
                        cfg.groupingColumns = Js.uncheckedCast(JsArray.of("X"));
                        cfg.includeConstituents = true;
                        cfg.aggregations = (JsPropertyMap) JsPropertyMap.of(operation, JsArray.of("Y"));
                        tests.add(() -> table.rollup(cfg).then(r -> {
                            r.setViewport(0, 99, null, null);
                            delayTestFinish(15000 + step);

                            return waitForEventWhere(r, JsTreeTable.EVENT_UPDATED,
                                    (CustomEvent<JsTreeTable.TreeViewportData> d) -> r.getSize() == 4, 13000 + step)
                                    .then(event -> Promise.resolve(r));
                        }));
                    }

                    return tests.stream().reduce((p1, p2) -> () -> p1.get().then(result -> p2.get())).get().get();
                })
                .then(this::finish).catch_(this::report);
    }

}
