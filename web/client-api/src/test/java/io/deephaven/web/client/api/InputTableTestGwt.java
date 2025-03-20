//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import jsinterop.base.Js;

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
            .script("result5", "source.with_attributes({'InputTable': 'oops'})");

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
                            keyColumns.filter((col, idx) -> col.getName() == "A" || col.getName() == "B").length);
                    JsArray<Column> valueColumns = Js.uncheckedCast(inputTable.getValueColumns());
                    assertEquals(4, valueColumns.filter((col, idx) -> col.getName() == "C" || col.getName() == "D"
                            || col.getName() == "E" || col.getName() == "F").length);
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
                    assertEquals(1, keyColumns.filter((col, idx) -> col.getName() == "C").length);
                    JsArray<Column> valueColumns = Js.uncheckedCast(inputTable.getValueColumns());
                    assertEquals(5, valueColumns.filter((col, idx) -> col.getName() == "A" || col.getName() == "B"
                            || col.getName() == "D" || col.getName() == "E" || col.getName() == "F").length);
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
                            keyColumns.filter((col, idx) -> col.getName() == "E" || col.getName() == "F").length);
                    JsArray<Column> valueColumns = Js.uncheckedCast(inputTable.getValueColumns());
                    assertEquals(4, valueColumns.filter((col, idx) -> col.getName() == "A" || col.getName() == "B"
                            || col.getName() == "C" || col.getName() == "D").length);
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
                            return Promise.reject(x);
                        });
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
