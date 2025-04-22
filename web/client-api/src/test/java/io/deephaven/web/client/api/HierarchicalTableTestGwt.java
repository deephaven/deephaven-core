//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.tree.JsRollupConfig;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.tree.TreeViewportData;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class HierarchicalTableTestGwt extends AbstractAsyncGwtTestCase {
    private static final Format red = new Format(0x1ff000001e0e0e0L, 0, null, null);
    private static final Format green = new Format(0x100800001e0e0e0L, 0, null, null);

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, time_table, agg")
            .script("static_tree",
                    "empty_table(1000).update(['ID=i', 'Parent=i == 0 ? null : (int)(i/10)']).format_columns(['ID=ID>0 ? GREEN : RED']).tree('ID', 'Parent')")
            .script("ticking_tree",
                    "time_table('PT0.1s').update(['ID=i', 'Parent=i == 0 ? null : (int)(i/10)']).format_columns(['ID=ID>0 ? GREEN : RED']).tree('ID', 'Parent')")
            .script("static_table_to_rollup",
                    "empty_table(20).update(['Y=i%5', 'X=i%3'])")
            .script("table_to_rollup",
                    "time_table('PT0.1s').update(['Y=Math.sin(i/3)', 'X=i%3', 'Z=`abc` + i']).format_columns(['Y=Y>0 ? GREEN : RED', 'Timestamp=RED'])")
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
                    return treeTable.getViewportData()
                            .then(data -> Promise.resolve((TreeViewportData) data))
                            .then(data -> {
                                assertEquals(1d, data.getTreeSize());

                                TreeViewportData.TreeRow row0 = (TreeViewportData.TreeRow) data.getRows().getAt(0);
                                assertFalse(row0.isExpanded());
                                assertFalse(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(0)));
                                assertFalse(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(row0)));
                                assertTrue(row0.hasChildren());
                                assertEquals(1, row0.depth());

                                treeTable.expand(JsTreeTable.RowReferenceUnion.of(0), null);
                                return treeTable.<TreeViewportData>nextEvent(
                                        JsTreeTable.EVENT_UPDATED, 2001d);
                            }).then(event -> {
                                assertEquals(10d, event.getDetail().getTreeSize());
                                JsArray<TableData.Row> rows = event.getDetail().getRows();
                                assertEquals(10, rows.length);
                                TreeViewportData.TreeRow row0 = (TreeViewportData.TreeRow) rows.getAt(0);
                                assertTrue(row0.isExpanded());
                                assertTrue(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(0)));
                                assertTrue(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(row0)));
                                assertTrue(row0.hasChildren());
                                assertEquals(1, row0.depth());

                                // Next 9 are collapsed, have children, are children of row0
                                for (int i = 1; i < 10; i++) {
                                    TreeViewportData.TreeRow row = (TreeViewportData.TreeRow) rows.getAt(i);
                                    assertFalse(row.isExpanded());
                                    assertFalse(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(i)));
                                    assertTrue(row.hasChildren());
                                    assertEquals(2, row.depth());
                                }

                                // move the viewport and try again
                                treeTable.setViewport(5, 50, treeTable.getColumns(), null);
                                return treeTable.<TreeViewportData>nextEvent(JsTreeTable.EVENT_UPDATED, 2002d);
                            }).then(event -> {
                                assertEquals(10d, event.getDetail().getTreeSize());
                                JsArray<TableData.Row> rows = event.getDetail().getRows();
                                assertEquals(5, rows.length);

                                // Row 0 is already expanded and is the parent of rows 1-9, so expand row 5
                                treeTable.expand(JsTreeTable.RowReferenceUnion.of(5), null);

                                return treeTable.<TreeViewportData>nextEvent(JsTreeTable.EVENT_UPDATED, 2003d);
                            }).then(event -> {
                                assertEquals(20d, event.getDetail().getTreeSize());
                                JsArray<TableData.Row> rows = event.getDetail().getRows();
                                assertEquals(15, rows.length);
                                TreeViewportData.TreeRow row5 = (TreeViewportData.TreeRow) rows.getAt(0);
                                assertTrue(row5.isExpanded());
                                assertTrue(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(5)));
                                assertTrue(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(row5)));
                                assertTrue(row5.hasChildren());
                                assertEquals(2, row5.depth());

                                // Next 10 are collapsed, have children, are children of row5
                                for (int i = 1; i < 11; i++) {
                                    TreeViewportData.TreeRow row = (TreeViewportData.TreeRow) rows.getAt(i);
                                    assertFalse(row.isExpanded());
                                    assertFalse(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(i + 5)));
                                    assertTrue(row.hasChildren());
                                    assertEquals(3, row.depth());
                                }
                                TreeViewportData.TreeRow row16 = (TreeViewportData.TreeRow) rows.getAt(11);
                                assertFalse(row16.isExpanded());
                                assertFalse(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(16)));
                                assertTrue(row16.hasChildren());
                                assertEquals(2, row16.depth());

                                // Expand row 6 by row reference
                                treeTable.expand((JsTreeTable.RowReferenceUnion) rows.getAt(1), null);

                                return treeTable.<TreeViewportData>nextEvent(JsTreeTable.EVENT_UPDATED, 2004d);
                            }).then(event -> {
                                assertEquals(30d, event.getDetail().getTreeSize());
                                JsArray<TableData.Row> rows = event.getDetail().getRows();
                                assertEquals(25, rows.length);
                                TreeViewportData.TreeRow row6 = (TreeViewportData.TreeRow) rows.getAt(1);
                                assertTrue(row6.isExpanded());
                                assertTrue(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(5)));
                                assertTrue(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(row6)));
                                assertTrue(row6.hasChildren());
                                assertEquals(3, row6.depth());

                                // Next 10 are collapsed, are leaf nodes, are children of row5
                                for (int i = 2; i < 12; i++) {
                                    TreeViewportData.TreeRow row = (TreeViewportData.TreeRow) rows.getAt(i);
                                    assertFalse(row.isExpanded());
                                    assertFalse(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(i + 5)));
                                    assertFalse(row.hasChildren());
                                    assertEquals(4, row.depth());
                                }
                                TreeViewportData.TreeRow row17 = (TreeViewportData.TreeRow) rows.getAt(12);
                                assertFalse(row17.isExpanded());
                                assertFalse(treeTable.isExpanded(JsTreeTable.RowReferenceUnion.of(17)));
                                assertTrue(row17.hasChildren());
                                assertEquals(3, row17.depth());

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
                    // Read values from the one returned row
                    return waitForEventWhere(treeTable, JsTreeTable.EVENT_UPDATED,
                            (Event<TreeViewportData> d) -> d.getDetail()
                                    .getTreeSize() == 1
                                    && d.getDetail().getRows().getAtAsAny(0).<TreeViewportData.TreeRow>cast()
                                            .hasChildren(),
                            10001)
                            .then(JsTreeTable::getViewportData)
                            .then(data -> Promise.resolve((TreeViewportData) data))
                            .then(data -> {
                                assertEquals(1.0, data.getTreeSize());
                                TreeViewportData.TreeRow row1 = (TreeViewportData.TreeRow) data.getRows().getAt(0);
                                Column timestampCol = treeTable.findColumn("Timestamp");
                                assertEquals(Format.EMPTY, data.getFormat(0, timestampCol));
                                assertEquals(Format.EMPTY, row1.getFormat(timestampCol));
                                assertEquals(Format.EMPTY, timestampCol.getFormat(row1));

                                Column idCol = treeTable.findColumn("ID");
                                assertEquals(0, data.getData(0, idCol).asInt());
                                assertEquals(0, row1.get(idCol).asInt());
                                assertEquals(0, idCol.get(row1).asInt());

                                assertEquals(red, data.getFormat(0, idCol));
                                assertEquals(red, row1.getFormat(idCol));
                                assertEquals(red, idCol.getFormat(row1));

                                assertNotNull(data.getData(0, timestampCol));
                                assertNotNull(row1.get(timestampCol));
                                assertNotNull(timestampCol.get(row1));

                                treeTable.expand(JsTreeTable.RowReferenceUnion.of(0), null);

                                // Wait for the expand to occur and table to show all 10 rows
                                return waitForEventWhere(treeTable, JsTreeTable.EVENT_UPDATED,
                                        (Event<TreeViewportData> d) -> d.getDetail().getTreeSize() == 10,
                                        14004);
                            })
                            .then(JsTreeTable::getViewportData)
                            .then(data -> Promise.resolve((TreeViewportData) data))
                            .then(data -> {
                                TreeViewportData.TreeRow row2 = (TreeViewportData.TreeRow) data.getRows().getAt(1);

                                Column timestampCol = treeTable.findColumn("Timestamp");
                                assertEquals(Format.EMPTY, data.getFormat(1, timestampCol));
                                assertEquals(Format.EMPTY, row2.getFormat(timestampCol));
                                assertEquals(Format.EMPTY, timestampCol.getFormat(row2));

                                Column idCol = treeTable.findColumn("ID");
                                assertEquals(1, data.getData(1, idCol).asInt());
                                assertEquals(1, row2.get(idCol).asInt());
                                assertEquals(1, idCol.get(row2).asInt());

                                assertEquals(green, data.getFormat(1, idCol));
                                assertEquals(green, row2.getFormat(idCol));
                                assertEquals(green, idCol.getFormat(row2));

                                // Move the viewport and make sure we get the correct data
                                treeTable.setViewport(5, 49, treeTable.getColumns(), null);
                                return treeTable.<TreeViewportData>nextEvent(
                                        JsTreeTable.EVENT_UPDATED, 2002d);
                            }).then(event -> {
                                assertEquals(10d, event.getDetail().getTreeSize());
                                assertEquals(5, event.getDetail().getRows().length);

                                return Promise.resolve(treeTable);
                            })
                            .then(event -> {
                                treeTable.close();
                                assertTrue(treeTable.isClosed());
                                return null;
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testTreeCustomColumnException() {
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

                    CustomColumnOptions col0Options = new CustomColumnOptions();
                    col0Options.rollupNodeType = "aggregated";
                    CustomColumn col0 = new CustomColumn("ID_new", CustomColumn.TYPE_NEW, "`ID-` + ID", col0Options);

                    JsArray<JsTable.CustomColumnArgUnionType> columns = new JsArray<>(
                            JsTable.CustomColumnArgUnionType.of(col0));

                    // This call should produce a UOE.
                    treeTable.applyCustomColumns(columns);
                    treeTable.setViewport(0, 99, treeTable.getColumns(), null);
                    return treeTable.getViewportData();
                })
                // This should result in an error, report if we do not get one.
                .then(this::report, this::finish);
    }


    public void testTickingRollup() {
        connect(tables)
                .then(treeTable("ticking_rollup"))
                .then(rollup -> {
                    // Very large timeout, 3.5s is enough that we see failures on this regularly
                    delayTestFinish(20_001);
                    assertTrue(rollup.isRefreshing());
                    assertFalse(rollup.isClosed());
                    assertTrue(rollup.isIncludeConstituents());

                    assertEquals(4, rollup.getColumns().length);
                    assertEquals("X", rollup.getColumns().getAt(0).getName());
                    assertEquals("Y", rollup.getColumns().getAt(1).getName());
                    assertEquals("Timestamp", rollup.getColumns().getAt(2).getName());
                    assertEquals("Z", rollup.getColumns().getAt(3).getName());

                    rollup.setViewport(0, 99, rollup.getColumns(), null);

                    Column xCol = rollup.findColumn("X");
                    Column yCol = rollup.findColumn("Y");
                    Column timestampCol = rollup.findColumn("Timestamp");
                    Column zCol = rollup.findColumn("Z");

                    // Wait for the table to tick such that we have at least 4 rows (root, three children)
                    return waitForEventWhere(rollup, JsTreeTable.EVENT_UPDATED,
                            (Event<TreeViewportData> d) -> d.getDetail().getTreeSize() == 4,
                            10002)
                            .then(JsTreeTable::getViewportData)
                            .then(data -> Promise.resolve((TreeViewportData) data))
                            .then(data -> {
                                TreeViewportData.TreeRow row1 = (TreeViewportData.TreeRow) data.getRows().getAt(0);

                                assertEquals(Format.EMPTY, data.getFormat(0, xCol));
                                assertEquals(Format.EMPTY, row1.getFormat(xCol));
                                assertEquals(Format.EMPTY, xCol.getFormat(row1));

                                assertNull(data.getData(0, xCol));
                                assertNull(row1.get(xCol));
                                assertNull(xCol.get(row1));

                                assertEquals(Format.EMPTY, data.getFormat(0, yCol));
                                assertEquals(Format.EMPTY, row1.getFormat(yCol));
                                assertEquals(Format.EMPTY, yCol.getFormat(row1));

                                assertEquals(0d, data.getData(0, yCol).asDouble());
                                assertEquals(0d, row1.get(yCol).asDouble());
                                assertEquals(0d, yCol.get(row1).asDouble());

                                assertNull(data.getData(0, zCol));
                                assertNull(data.getData(0, timestampCol));
                                assertNull(row1.get(zCol));
                                assertNull(row1.get(timestampCol));
                                assertNull(zCol.get(row1));
                                assertNull(timestampCol.get(row1));

                                assertEquals(Format.EMPTY, data.getFormat(0, zCol));
                                assertEquals(Format.EMPTY, data.getFormat(0, timestampCol));
                                assertEquals(Format.EMPTY, row1.getFormat(zCol));
                                assertEquals(Format.EMPTY, row1.getFormat(timestampCol));
                                assertEquals(Format.EMPTY, zCol.getFormat(row1));
                                assertEquals(Format.EMPTY, timestampCol.getFormat(row1));


                                TreeViewportData.TreeRow row2 = (TreeViewportData.TreeRow) data.getRows().getAt(1);
                                assertEquals(Format.EMPTY, data.getFormat(1, xCol));
                                assertEquals(Format.EMPTY, row2.getFormat(xCol));
                                assertEquals(Format.EMPTY, xCol.getFormat(row2));

                                assertEquals(0d, data.getData(1, xCol).asDouble());
                                assertEquals(0d, row2.get(xCol).asDouble());
                                assertEquals(0d, xCol.get(row2).asDouble());

                                assertEquals(Format.EMPTY, data.getFormat(1, yCol));
                                assertEquals(Format.EMPTY, row2.getFormat(yCol));
                                assertEquals(Format.EMPTY, yCol.getFormat(row2));

                                assertEquals(0d, data.getData(1, yCol).asDouble());
                                assertEquals(0d, row2.get(yCol).asDouble());
                                assertEquals(0d, yCol.get(row2).asDouble());

                                assertNull(data.getData(1, zCol));
                                assertNull(data.getData(1, timestampCol));
                                assertNull(row2.get(zCol));
                                assertNull(row2.get(timestampCol));
                                assertNull(zCol.get(row1));
                                assertNull(timestampCol.get(row1));

                                assertEquals(Format.EMPTY, data.getFormat(1, zCol));
                                assertEquals(Format.EMPTY, data.getFormat(1, timestampCol));
                                assertEquals(Format.EMPTY, row2.getFormat(zCol));
                                assertEquals(Format.EMPTY, row2.getFormat(timestampCol));
                                assertEquals(Format.EMPTY, zCol.getFormat(row1));
                                assertEquals(Format.EMPTY, timestampCol.getFormat(row1));

                                // Expand row 2
                                rollup.expand(JsTreeTable.RowReferenceUnion.of(1), null);

                                // Wait for the expand to occur and table to show all 10 rows
                                return waitForEventWhere(rollup, JsTreeTable.EVENT_UPDATED,
                                        (Event<TreeViewportData> d) -> d.getDetail().getTreeSize() > 4,
                                        14008);
                            })
                            .then(JsTreeTable::getViewportData)
                            .then(data -> Promise.resolve((TreeViewportData) data))
                            .then(data -> {
                                TreeViewportData.TreeRow row3 = (TreeViewportData.TreeRow) data.getRows().getAt(2);

                                assertEquals(Format.EMPTY, data.getFormat(2, xCol));
                                assertEquals(Format.EMPTY, row3.getFormat(xCol));
                                assertEquals(Format.EMPTY, xCol.getFormat(row3));

                                assertEquals(0d, data.getData(2, yCol).asDouble());
                                assertEquals(0d, row3.get(yCol).asDouble());
                                assertEquals(0d, yCol.get(row3).asDouble());

                                assertEquals(red, data.getFormat(2, yCol));
                                assertEquals(red, row3.getFormat(yCol));
                                assertEquals(red, yCol.getFormat(row3));

                                assertEquals(0d, data.getData(2, yCol).asDouble());
                                assertEquals(0d, row3.get(yCol).asDouble());
                                assertEquals(0d, yCol.get(row3).asDouble());

                                assertEquals("abc0", data.getData(2, zCol).asString());
                                assertNotNull(data.getData(2, timestampCol));
                                assertEquals("abc0", row3.get(zCol).asString());
                                assertNotNull(row3.get(timestampCol));
                                assertEquals("abc0", zCol.get(row3).asString());
                                assertNotNull(timestampCol.get(row3));

                                assertEquals(Format.EMPTY, data.getFormat(2, zCol));
                                assertEquals(red, data.getFormat(2, timestampCol));
                                assertEquals(Format.EMPTY, row3.getFormat(zCol));
                                assertEquals(red, row3.getFormat(timestampCol));
                                assertEquals(Format.EMPTY, zCol.getFormat(row3));
                                assertEquals(red, timestampCol.getFormat(row3));

                                // Collapse row 2, wait until back to 4 rows
                                rollup.collapse(JsTreeTable.RowReferenceUnion.of(1));
                                return waitForEventWhere(rollup, JsTreeTable.EVENT_UPDATED,
                                        (Event<TreeViewportData> d) -> d.getDetail().getTreeSize() == 4,
                                        14009);
                            })
                            .then(event -> {
                                rollup.close();
                                assertTrue(rollup.isClosed());
                                return null;
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCreateRollupAggTypes() {
        connect(tables)
                .then(table("table_to_rollup"))
                .then(table -> {
                    List<Supplier<Promise<JsTreeTable>>> tests = new ArrayList<>();
                    // For each supported operation, apply it to the numeric column
                    // Then expand to verify data can load
                    List<String> aggs = List.of(
                            JsAggregationOperation.COUNT,
                            JsAggregationOperation.COUNT_DISTINCT,
                            JsAggregationOperation.DISTINCT,
                            JsAggregationOperation.MIN,
                            JsAggregationOperation.MAX,
                            JsAggregationOperation.SUM,
                            JsAggregationOperation.ABS_SUM,
                            JsAggregationOperation.VAR,
                            JsAggregationOperation.AVG,
                            JsAggregationOperation.STD,
                            JsAggregationOperation.FIRST,
                            JsAggregationOperation.LAST,
                            JsAggregationOperation.UNIQUE);
                    for (int i = 0; i < aggs.size(); i++) {
                        final int step = i;
                        String operation = aggs.get(i);
                        JsRollupConfig cfg = new JsRollupConfig();
                        cfg.groupingColumns = Js.uncheckedCast(JsArray.of("X"));
                        cfg.includeConstituents = true;
                        cfg.aggregations = JsPropertyMap.of(operation, JsArray.of("Y"));
                        tests.add(() -> table.rollup(cfg).then(r -> {
                            r.setViewport(0, 99, null, null);
                            delayTestFinish(15000 + step);

                            return waitForEventWhere(r, JsTreeTable.EVENT_UPDATED,
                                    (Event<TreeViewportData> d) -> r.getSize() == 4, 13000 + step)
                                    .then(event -> Promise.resolve(r));
                        }));
                    }

                    return tests.stream().reduce((p1, p2) -> () -> p1.get().then(result -> p2.get())).get().get();
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCreateRollupsConstituents() {
        connect(tables)
                .then(table("table_to_rollup"))
                .then(table -> {
                    List<Supplier<Promise<JsTreeTable>>> tests = new ArrayList<>();

                    JsRollupConfig cfg = new JsRollupConfig();
                    cfg.groupingColumns = Js.uncheckedCast(JsArray.of("X"));
                    cfg.aggregations = JsPropertyMap.of("Count", JsArray.of("Y"));
                    Stream.of(true, false).forEach(includeConstituents -> {
                        cfg.includeConstituents = includeConstituents;
                        JsRollupConfig copy = new JsRollupConfig((JsPropertyMap<Object>) cfg);
                        tests.add(() -> table.rollup(copy).then(r -> {
                            r.setViewport(0, 99, null, null);
                            r.expandAll();
                            delayTestFinish(15000 + (includeConstituents ? 1 : 0));

                            return waitForEventWhere(r, JsTreeTable.EVENT_UPDATED,
                                    (Event<TreeViewportData> d) -> r.getSize() > 3,
                                    13000 + (includeConstituents ? 1 : 0))
                                    .then(JsTreeTable::getViewportData)
                                    .then(result -> {
                                        TreeViewportData data = (TreeViewportData) result;

                                        // Check if the 2nd row has constituent data as expected (or doesn't)
                                        TreeViewportData.TreeRow row =
                                                (TreeViewportData.TreeRow) data.getRows().getAt(2);
                                        if (includeConstituents) {
                                            assertEquals(3, row.depth());
                                            assertFalse(row.hasChildren());
                                            assertNotNull(row.get(r.findColumn("Z")));
                                        } else {
                                            // Instead of finding the constituent row, we find the next parent
                                            assertEquals(2, row.depth());
                                            assertFalse(row.hasChildren());

                                            // Expect to fail if trying to ready the constituent-only column
                                            try {
                                                r.findColumn("Z");
                                                fail("Expected to fail finding constituent-only column");
                                            } catch (Exception ignore) {
                                                // Expected
                                            }
                                        }

                                        return Promise.resolve(r);
                                    });
                        }));
                    });

                    return tests.stream().reduce((p1, p2) -> () -> p1.get().then(result -> p2.get())).get().get();
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCreateRollupUpdateView() {
        connect(tables)
                .then(table("static_table_to_rollup"))
                .then(table -> {
                    JsRollupConfig cfg = new JsRollupConfig();
                    cfg.groupingColumns = Js.uncheckedCast(JsArray.of("X"));
                    cfg.includeConstituents = true;
                    cfg.aggregations = JsPropertyMap.of(JsAggregationOperation.SUM, JsArray.of("Y"));
                    return table.rollup(cfg).then(rollupTable -> {

                        JsPropertyMap<Object> col0 =
                                JsPropertyMap.of("name", "YPlusAgg", "expression", "Y + 1", "type",
                                        CustomColumn.TYPE_NEW);
                        col0.set("options", JsPropertyMap.of("rollupNodeType", "aggregated"));

                        CustomColumnOptions col1Options = new CustomColumnOptions();
                        col1Options.rollupNodeType = "constituent";
                        CustomColumn col1 = new CustomColumn("YPlusConst", CustomColumn.TYPE_NEW, "Y + 1", col1Options);

                        JsArray<JsTable.CustomColumnArgUnionType> columns = new JsArray<>(
                                JsTable.CustomColumnArgUnionType.of(col0),
                                JsTable.CustomColumnArgUnionType.of(col1));

                        assertEquals(2, rollupTable.getColumns().length);

                        rollupTable.applyCustomColumns(columns);

                        rollupTable.setViewport(0, 99, null, null);
                        return rollupTable.getViewportData()
                                .then(data -> {
                                    // After the refresh, we should have the new columns in the rollup table
                                    rollupTable.setViewport(0, 99, rollupTable.getColumns(), null);
                                    return rollupTable.getViewportData();
                                })
                                .then(data -> Promise.resolve((TreeViewportData) data))
                                .then(data -> {
                                    assertEquals(4, rollupTable.getColumns().length);
                                    assertEquals("X", rollupTable.getColumns().getAt(0).getName());
                                    assertEquals("Y", rollupTable.getColumns().getAt(1).getName());
                                    assertEquals("YPlusAgg", rollupTable.getColumns().getAt(2).getName());
                                    assertEquals("YPlusConst", rollupTable.getColumns().getAt(3).getName());

                                    assertEquals(4d, data.getTreeSize());

                                    TreeViewportData.TreeRow row0 = (TreeViewportData.TreeRow) data.getRows().getAt(0);
                                    assertTrue(row0.isExpanded());
                                    assertTrue(row0.hasChildren());
                                    assertEquals(1, row0.depth());

                                    TreeViewportData.TreeRow row1 = (TreeViewportData.TreeRow) data.getRows().getAt(1);
                                    assertFalse(row1.isExpanded());
                                    assertTrue(row1.hasChildren());
                                    assertEquals(2, row1.depth());

                                    TreeViewportData.TreeRow row2 = (TreeViewportData.TreeRow) data.getRows().getAt(2);
                                    assertFalse(row2.isExpanded());
                                    assertTrue(row2.hasChildren());
                                    assertEquals(2, row2.depth());

                                    TreeViewportData.TreeRow row3 = (TreeViewportData.TreeRow) data.getRows().getAt(3);
                                    assertFalse(row3.isExpanded());
                                    assertTrue(row3.hasChildren());
                                    assertEquals(2, row3.depth());

                                    Column xCol = rollupTable.findColumn("X");
                                    Column yCol = rollupTable.findColumn("Y");

                                    assertEquals(40L, ((LongWrapper) yCol.get(row0)).getWrapped());

                                    assertEquals(0, xCol.get(row1).asInt());
                                    assertEquals(13L, ((LongWrapper) yCol.get(row1)).getWrapped());

                                    assertEquals(1, xCol.get(row2).asInt());
                                    assertEquals(15L, ((LongWrapper) yCol.get(row2)).getWrapped());

                                    assertEquals(2, xCol.get(row3).asInt());
                                    assertEquals(12L, ((LongWrapper) yCol.get(row3)).getWrapped());

                                    // Check the update view column for aggregated rows

                                    Column yPlusAgg = rollupTable.findColumn("YPlusAgg");

                                    assertEquals(41L, ((LongWrapper) yPlusAgg.get(row0)).getWrapped());
                                    assertEquals(14L, ((LongWrapper) yPlusAgg.get(row1)).getWrapped());
                                    assertEquals(16L, ((LongWrapper) yPlusAgg.get(row2)).getWrapped());
                                    assertEquals(13L, ((LongWrapper) yPlusAgg.get(row3)).getWrapped());

                                    // Expand row 1, exposing constituent rows
                                    rollupTable.expand(JsTreeTable.RowReferenceUnion.of(1), null);
                                    return rollupTable.<TreeViewportData>nextEvent(
                                            JsTreeTable.EVENT_UPDATED, 2001d);
                                }).then(event -> {
                                    TreeViewportData data = event.getDetail();

                                    Column yCol = rollupTable.findColumn("Y");
                                    Column yPlusConst = rollupTable.findColumn("YPlusConst");

                                    // Row 2 is the first constituent of row 1, with 7 constituent rows
                                    for (int i = 2; i < 2 + 7; i++) {
                                        TreeViewportData.TreeRow row =
                                                (TreeViewportData.TreeRow) data.getRows().getAt(i);
                                        assertFalse(row.isExpanded());
                                        assertFalse(row.hasChildren());
                                        assertEquals(3, row.depth());

                                        // get the raw Y value and test against the plus 1
                                        int yVal = yCol.get(row).asInt();
                                        assertEquals(yVal + 1, yPlusConst.get(row).asInt());
                                    }

                                    rollupTable.close();
                                    assertTrue(rollupTable.isClosed());
                                    return null;
                                });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCreateRollupUpdateViewUnsafe() {
        connect(tables)
                .then(table("static_table_to_rollup"))
                .then(table -> {
                    JsRollupConfig cfg = new JsRollupConfig();
                    cfg.groupingColumns = Js.uncheckedCast(JsArray.of("X"));
                    cfg.includeConstituents = true;
                    cfg.aggregations = JsPropertyMap.of(JsAggregationOperation.SUM, JsArray.of("Y"));
                    return table.rollup(cfg).then(rollupTable -> {

                        JsPropertyMap<Object> col0 =
                                JsPropertyMap.of("name", "UnsafeOp",
                                        "expression", "Runtime.getRuntime().exec(`ls`)",
                                        "type", CustomColumn.TYPE_NEW);
                        col0.set("options", JsPropertyMap.of("rollupNodeType", "aggregated"));

                        JsArray<JsTable.CustomColumnArgUnionType> columns = new JsArray<>(
                                JsTable.CustomColumnArgUnionType.of(col0));

                        assertEquals(2, rollupTable.getColumns().length);

                        rollupTable.applyCustomColumns(columns);

                        rollupTable.setViewport(0, 99, null, null);
                        return rollupTable.getViewportData()
                                .then(data -> {
                                    // After the refresh, we should have the new columns in the rollup table
                                    rollupTable.setViewport(0, 99, rollupTable.getColumns(), null);
                                    return rollupTable.getViewportData();
                                })
                                .then(data -> Promise.resolve((TreeViewportData) data));
                    });
                })
                .then(this::report, this::finish);
    }
}
