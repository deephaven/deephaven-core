//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.ide.IdeSession;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

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

    /** Resolves once {@code call} has rejected naming the connection; fails naming {@code what} otherwise. */
    private static Promise<Object> rejectsForConnection(String what, Promise<?> call) {
        Promise<Object> settled = call.then(
                ignore -> Promise.reject(what + " resolved; expected it to reject its foreign operand"),
                err -> String.valueOf(err).contains("connection") ? Promise.resolve((Object) null)
                        : Promise.reject(what + " rejected for another reason: " + err));
        Promise<Object> timeout = delay(5_000).then(ignore -> Promise.reject(what + " did not settle within 5s"));
        return Promise.race(settled, timeout);
    }

    public void testOperandFromAnotherConnectionRejects() {
        // A ticket from another connection can name a different table on this one, so it must be refused
        connect(tables).then(first -> connect(tables).then(second -> {
            delayTestFinish(20_008);
            return second.getTable("source", null).asPromise().then(foreign -> first.getTable("source", null)
                    .asPromise().then(local -> first.getTable("editable", null).asPromise()
                            .then(editable -> editable.inputTable().then(input -> {
                                JsTableReference operand = JsTableReference.of(foreign);
                                List<String> failures = new ArrayList<>();
                                Promise<Object> checks = Promise.resolve((Object) null);
                                for (Supplier<Promise<Object>> check : List.<Supplier<Promise<Object>>>of(
                                        () -> rejectsForConnection("snapshot",
                                                local.snapshot(operand, true, null)),
                                        () -> rejectsForConnection("addTable", input.addTable(operand)),
                                        () -> rejectsForConnection("deleteTable", input.deleteTable(operand)),
                                        () -> rejectsForConnection("addTables",
                                                input.addTables(arr(JsTableReference.of(local), operand))),
                                        () -> rejectsForConnection("bindTableToVariable",
                                                first.bindTableToVariable(foreign, "foreign")))) {
                                    checks = checks.then(ignore -> check.get().then(Promise::resolve, err -> {
                                        failures.add(String.valueOf(err));
                                        return Promise.resolve((Object) null);
                                    }));
                                }
                                return checks.then(ignore -> {
                                    foreign.close();
                                    local.close();
                                    editable.close();
                                    return failures.isEmpty() ? Promise.resolve(first)
                                            : Promise.reject(failures.size() + " failed: " + failures);
                                });
                            }))));
        })).then(this::finish).catch_(this::report);
    }

    public void testDeleteThatThrowsKeepsNothing() {
        // Deleting through an input table whose table was closed throws, as it always has; it must not keep holding
        // the table it was given
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_009);
                    return session.getTable("source", null).resolve().then(operand -> session
                            .getTable("editable", null).asPromise()
                            .then(editable -> editable.inputTable().then(input -> {
                                ClientTableState state = operand.getConnection().getCache()
                                        .get(new TableTicket(operand.typedTicket().getTicket())).get();
                                editable.close();
                                try {
                                    input.deleteTable(JsTableReference.of(operand));
                                    return Promise.reject(
                                            "deleteTable through a closed table returned instead of throwing");
                                } catch (Throwable expected) {
                                    // As on main
                                }
                                operand.close();
                                return afterReleaseFlush().then(ignore -> {
                                    assertEquals("the delete that threw still held its operand",
                                            ClientTableState.ResolutionState.RELEASED, state.getResolution());
                                    return Promise.resolve(session);
                                });
                            })));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAddSeveralTablesThenDelete() {
        // Pending tables added together all land, and one can then be deleted
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_007);
                    return session.getTable("editable", null).asPromise().then(editable -> editable.inputTable()
                            .then(input -> input.addTables(arr(
                                    session.emptyTable(2).update(arr("K = i", "V = i * 10")),
                                    session.emptyTable(1).update(arr("K = 5", "V = 50")))))
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
