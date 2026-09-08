//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.junit.DoNotRunWith;
import com.google.gwt.junit.Platform;
import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableSubscription;
import jsinterop.base.Js;

public class DictionaryEncodedTestGwt extends AbstractAsyncGwtTestCase {

    // JPY scripts that build two Arrow-dictionary-encoded tables and attach them via BarrageSchema.
    // dict_table_annotated has two columns:
    // DictStr: alternating "hello" / "world"
    // DictInt: alternating 10 / 20
    // dict_null_table_annotated has one column:
    // DictNullStr: null for rows 0-2, "foo" for rows 3-5
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table\nimport jpy")
            .script("_flat_dict",
                    "empty_table(6).update(["
                            + "\"DictStr=(String)(i%2==0?`hello`:`world`)\","
                            + "\"DictInt=(int)(i%2==0?10:20)\"])")
            .script("_flat_null",
                    "empty_table(6).update([\"DictNullStr=i<3?null:(String)`foo`\"])")
            .script(
                    "_JIntCls = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Int')",
                    "_JUtf8Cls = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Utf8')",
                    "_JDictEncCls = jpy.get_type('org.apache.arrow.vector.types.pojo.DictionaryEncoding')",
                    "_JField = jpy.get_type('org.apache.arrow.vector.types.pojo.Field')",
                    "_JFieldType = jpy.get_type('org.apache.arrow.vector.types.pojo.FieldType')",
                    "_JSchema = jpy.get_type('org.apache.arrow.vector.types.pojo.Schema')",
                    "_JHashMap = jpy.get_type('java.util.HashMap')",
                    "_JArrayList = jpy.get_type('java.util.ArrayList')",
                    "_JInt32 = _JIntCls(32, True)",
                    "_JUtf8 = _JUtf8Cls()")
            // helper: build one dictionary-encoded Arrow field
            .script("def _make_dict_field(col_name, val_type, dh_type_str, dict_id):\n"
                    + "    attrs = _JHashMap()\n"
                    + "    attrs.put('deephaven:type', dh_type_str)\n"
                    + "    dict_enc = _JDictEncCls(dict_id, False, _JInt32)\n"
                    + "    field_type = _JFieldType(True, val_type, dict_enc, attrs)\n"
                    + "    return _JField(col_name, field_type, _JArrayList())")
            // schema for the two-column table (dict ids 0 and 1)
            .script("_dict_fields = _JArrayList()\n"
                    + "_dict_fields.add(_make_dict_field('DictStr', _JUtf8, 'java.lang.String', 0))\n"
                    + "_dict_fields.add(_make_dict_field('DictInt', _JInt32, 'int', 1))\n"
                    + "_dict_schema = _JSchema(_dict_fields)")
            // schema for the null table (dict id 2)
            .script("_null_fields = _JArrayList()\n"
                    + "_null_fields.add(_make_dict_field('DictNullStr', _JUtf8, 'java.lang.String', 2))\n"
                    + "_null_schema = _JSchema(_null_fields)")
            // annotated tables: BarrageSchema attribute triggers dictionary encoding on the server
            .script("dict_table_annotated = _flat_dict.with_attributes({'BarrageSchema': _dict_schema})")
            .script("dict_null_table_annotated = _flat_null.with_attributes({'BarrageSchema': _null_schema})");

    public void testDictColumnTypes() {
        connect(tables)
                .then(table("dict_table_annotated"))
                .then(table -> {
                    delayTestFinish(5000);
                    assertEquals("java.lang.String", table.findColumn("DictStr").getType());
                    assertEquals("int", table.findColumn("DictInt").getType());
                    assertEquals(6., table.getSize(), 0);
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testDictSubscription() {
        connect(tables)
                .then(table("dict_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    DataOptions.SubscriptionOptions options = new DataOptions.SubscriptionOptions();
                    options.columns = table.getColumns();
                    TableSubscription sub = table.createSubscription(options);
                    return assertUpdateReceived(sub, data -> validateDictValues(table, data), 5000)
                            .then(ignore -> Promise.resolve(table));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testDictSnapshot() {
        connect(tables)
                .then(table("dict_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    DataOptions.SnapshotOptions opts = new DataOptions.SnapshotOptions();
                    opts.columns = table.getColumns();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 5));
                    return table.createSnapshot(opts).then(data -> {
                        validateDictValues(table, data);
                        return Promise.resolve(table);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testDictViewport() {
        connect(tables)
                .then(table("dict_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    table.setViewport(0, 5, null);
                    return assertUpdateReceived(table, data -> validateDictValues(table, data), 5000);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testDictNulls() {
        connect(tables)
                .then(table("dict_null_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    table.setViewport(0, 5, null);
                    return assertUpdateReceived(table, data -> {
                        JsArray<? extends TableData.Row> rows = data.getRows();
                        Column dictNullStr = table.findColumn("DictNullStr");
                        for (int i = 0; i < 3; i++) {
                            assertNull("Expected null at row " + i, rows.getAt(i).get(dictNullStr));
                        }
                        for (int i = 3; i < 6; i++) {
                            assertEquals("Expected 'foo' at row " + i, "foo",
                                    rows.getAt(i).get(dictNullStr).asString());
                        }
                    }, 5000);
                })
                .then(this::finish).catch_(this::report);
    }

    private static void validateDictValues(JsTable table, TableData data) {
        JsArray<? extends TableData.Row> rows = data.getRows();
        assertEquals(6, rows.length);

        Column dictStr = table.findColumn("DictStr");
        Column dictInt = table.findColumn("DictInt");

        String[] expectedStrs = {"hello", "world", "hello", "world", "hello", "world"};
        int[] expectedInts = {10, 20, 10, 20, 10, 20};

        for (int i = 0; i < 6; i++) {
            TableData.Row row = rows.getAt(i);
            assertEquals("DictStr[" + i + "]", expectedStrs[i], row.get(dictStr).asString());
            assertEquals("DictInt[" + i + "]", expectedInts[i], row.get(dictInt).asInt());
        }
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
