//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.ide.IdeSession;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Set;

public class InputTableTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, input_table")
            .script("source",
                    "empty_table(5).update([\"A=`` + i\", \"B=`` + i * 2\", \"C=i\", \"D=i * 2\", \"E=`` + i\", \"F=`` + i * 2\",])")
            .script("result1", "input_table(init_table=source, key_cols=[])")
            .script("result2", "input_table(init_table=source, key_cols=[\"A\" , \"B\" ])")
            .script("result3", "input_table(init_table=source, key_cols=[\"C\"])")
            .script("result4", "input_table(init_table=source, key_cols=[\"E\" , \"F\" ])")
            // and a bad input table, that previously crashed the server
            .script("result5", "source.with_attributes({'InputTable': 'oops'})")
            .script("result6", "result4.natural_join(empty_table(1).update([\"Dummy=1\", \"Dumber=3\"]), \"\")")
            .script("from deephaven import dtypes")
            .script("editable", "input_table(col_defs={'K': dtypes.int32, 'V': dtypes.int32}, key_cols=['K'])");

    private static Promise<Double> sizeOf(IdeSession session, String name) {
        return session.getTable(name, null).asPromise().then(table -> {
            double size = table.getSize();
            table.close();
            return Promise.resolve(size);
        });
    }

    public void testAddSeveralTablesThenDelete() {
        // Several tables are merged by a request sent with the add, not awaited first
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_007);
                    return session.getTable("editable", null).asPromise().then(editable -> editable.inputTable()
                            .then(input -> input.addTables(new JsTableReference[] {
                                    Js.uncheckedCast(session.emptyTable(2).update(arr("K = i", "V = i * 10"))),
                                    Js.uncheckedCast(session.emptyTable(1).update(arr("K = 5", "V = 50")))}))
                            .then(input -> sizeOf(session, "editable").then(size -> {
                                assertEquals(3., size, 0);
                                return input.deleteTable(
                                        Js.uncheckedCast(session.emptyTable(1).update(arr("K = 5"))));
                            }))
                            .then(input -> sizeOf(session, "editable"))
                            .then(size -> {
                                assertEquals(2., size, 0);
                                editable.close();
                                return Promise.resolve(session);
                            }));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testNoKeyCols() {
        connect(tables)
                .then(table("result1"))
                .then(JsTable::inputTable)
                .then(inputTable -> {
                    JsArray<Column> keyColumns = Js.uncheckedCast(inputTable.getKeyColumns());
                    assertEquals(0, keyColumns.length);
                    return null;
                })
                .then(this::finish).catch_(this::report);
    }

    public void testFirstColsAreKeyCols() {
        connect(tables)
                .then(table("result2"))
                .then(JsTable::inputTable)
                .then(inputTable -> {
                    JsArray<Column> keyColumns = Js.uncheckedCast(inputTable.getKeyColumns());
                    assertEquals(2,
                            keyColumns.filter((col, idx) -> Set.of("A", "B").contains(col.getName())).length);
                    JsArray<Column> valueColumns = Js.uncheckedCast(inputTable.getValueColumns());
                    assertEquals(4,
                            valueColumns
                                    .filter((col, idx) -> Set.of("C", "D", "E", "F").contains(col.getName())).length);
                    return null;
                })
                .then(this::finish).catch_(this::report);
    }

    public void testOneKeyCol() {
        connect(tables)
                .then(table("result3"))
                .then(JsTable::inputTable)
                .then(inputTable -> {
                    JsArray<Column> keyColumns = Js.uncheckedCast(inputTable.getKeyColumns());
                    assertEquals(1, keyColumns.filter((col, idx) -> col.getName().equals("C")).length);
                    JsArray<Column> valueColumns = Js.uncheckedCast(inputTable.getValueColumns());
                    assertEquals(5, valueColumns
                            .filter((col, idx) -> Set.of("A", "B", "D", "E", "F").contains(col.getName())).length);
                    return null;
                })
                .then(this::finish).catch_(this::report);
    }

    public void testLaterColsAreKeyCols() {
        connect(tables)
                .then(table("result4"))
                .then(JsTable::inputTable)
                .then(inputTable -> {
                    JsArray<Column> keyColumns = Js.uncheckedCast(inputTable.getKeyColumns());
                    assertEquals(2,
                            keyColumns.filter((col, idx) -> Set.of("E", "F").contains(col.getName())).length);
                    JsArray<Column> valueColumns = Js.uncheckedCast(inputTable.getValueColumns());
                    assertEquals(4, valueColumns
                            .filter((col, idx) -> Set.of("A", "B", "C", "D").contains(col.getName())).length);
                    return null;
                })
                .then(this::finish).catch_(this::report);
    }

    public void testBadInputTable() {
        connect(tables)
                .then(table("result5"))
                .then(y -> Promise
                        .reject("Should not have been able to retrieve result5, with a bad InputTable attribute."),
                        x -> {
                            if (x != null && x.toString().startsWith("Error: Details Logged w/ID '")) {
                                // good enough
                                return finish(null);
                            }
                            // we are expecting an error message
                            return report(x);
                        });
    }

    public void testExtraColumns() {
        connect(tables)
                .then(table("result6"))
                .then(JsTable::inputTable)
                .then(inputTable -> {
                    JsArray<Column> keyColumns = Js.uncheckedCast(inputTable.getKeyColumns());
                    assertEquals(2,
                            keyColumns.filter((col, idx) -> Set.of("E", "F").contains(col.getName())).length);
                    JsArray<Column> valueColumns = Js.uncheckedCast(inputTable.getValueColumns());
                    assertEquals(4, valueColumns
                            .filter((col, idx) -> Set.of("A", "B", "C", "D").contains(col.getName())).length);
                    final JsArray<Column> allColumns = Js.uncheckedCast(inputTable.getTable().getColumns());
                    assertEquals(2, allColumns
                            .filter((col, idx) -> Set.of("Dummy", "Dumber").contains(col.getName())).length);
                    return null;
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAddRowsExtraColumns() {
        connect(tables)
                .then(table("result6"))
                .then(JsTable::inputTable)
                .then(inputTable -> {
                    JsPropertyMap<Object> row = JsPropertyMap.of();
                    row.set("E", "5");
                    row.set("F", "10");
                    return inputTable.addRows(new JsPropertyMap[] {row}, null);
                })
                .then(inputTable -> {
                    delayTestFinish(8000);
                    JsTable table = inputTable.getTable();
                    table.setViewport(0, 100, null);
                    return waitForEvent(table, JsTable.EVENT_ROWADDED, 2001).onInvoke(table);
                })
                .then(table -> {
                    assertEquals(6.0, table.getSize());
                    return null;
                })
                .then(this::finish).catch_(this::report);
    }


    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
