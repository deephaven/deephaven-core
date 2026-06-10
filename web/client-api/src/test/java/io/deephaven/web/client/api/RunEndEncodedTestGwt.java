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

@DoNotRunWith(Platform.HtmlUnitBug)
public class RunEndEncodedTestGwt extends AbstractAsyncGwtTestCase {

    // Complicated JPY mess that creates a table and assignes the BARRAGE_SCHEMA_ATTRIBUTE to trigger REE encoding on
    // the server. The table has two columns:
    // 1) RepInt: runs of 0, 1, and 2
    // 2) RepStr: runs of "hello" and "world"
    private final TableSourceBuilder tables = new TableSourceBuilder()
            // imports
            .script("from deephaven import empty_table\nimport jpy")
            // flat source tables
            .script("_flat_ree",
                    "empty_table(6).update(["
                            + "\"RepInt=(int)(i<2?0:i<4?1:2)\","
                            + "\"RepStr=(String)(i<3?`hello`:`world`)\"])")
            .script("_flat_null",
                    "empty_table(6).update([\"RepNullInt=i<3?null:42\"])")
            // jpy Arrow type handles
            .script("_JIntCls = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Int')",
                    "_JUtf8Cls = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Utf8')",
                    "_JREECls = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded')",
                    "_JField = jpy.get_type('org.apache.arrow.vector.types.pojo.Field')",
                    "_JFieldType = jpy.get_type('org.apache.arrow.vector.types.pojo.FieldType')",
                    "_JSchema = jpy.get_type('org.apache.arrow.vector.types.pojo.Schema')",
                    "_JHashMap = jpy.get_type('java.util.HashMap')",
                    "_JArrayList = jpy.get_type('java.util.ArrayList')",
                    "_JInt32 = _JIntCls(32, True)",
                    "_JUtf8 = _JUtf8Cls()",
                    "_JREE = _JREECls.INSTANCE")
            // helper: build one REE-typed Arrow field
            .script("def _make_ree_field(col_name, val_type, dh_type_str):\n"
                    + "    run_ends = _JField.notNullable('run_ends', _JInt32)\n"
                    + "    attrs = _JHashMap()\n"
                    + "    attrs.put('deephaven:type', dh_type_str)\n"
                    + "    val_children = _JArrayList()\n"
                    + "    val_f = _JField('values', _JFieldType(True, val_type, None, attrs), val_children)\n"
                    + "    children = _JArrayList()\n"
                    + "    children.add(run_ends)\n"
                    + "    children.add(val_f)\n"
                    + "    return _JField(col_name, _JFieldType(True, _JREE, None, None), children)")
            // REE schema for the int+str table
            .script("_ree_fields = _JArrayList()\n"
                    + "_ree_fields.add(_make_ree_field('RepInt', _JInt32, 'int'))\n"
                    + "_ree_fields.add(_make_ree_field('RepStr', _JUtf8, 'java.lang.String'))\n"
                    + "_ree_schema = _JSchema(_ree_fields)")
            // REE schema for the null table
            .script("_null_fields = _JArrayList()\n"
                    + "_null_fields.add(_make_ree_field('RepNullInt', _JInt32, 'int'))\n"
                    + "_null_schema = _JSchema(_null_fields)")
            // annotated tables: attach BARRAGE_SCHEMA_ATTRIBUTE so server encodes as REE
            .script("ree_table_annotated = _flat_ree.with_attributes({'BarrageSchema': _ree_schema})")
            .script("ree_null_table_annotated = _flat_null.with_attributes({'BarrageSchema': _null_schema})");

    public void testReeColumnTypes() {
        connect(tables)
                .then(table("ree_table_annotated"))
                .then(table -> {
                    delayTestFinish(5000);
                    assertEquals("int", table.findColumn("RepInt").getType());
                    assertEquals("java.lang.String", table.findColumn("RepStr").getType());
                    assertEquals(6., table.getSize(), 0);
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testReeSubscription() {
        connect(tables)
                .then(table("ree_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    DataOptions.SubscriptionOptions options = new DataOptions.SubscriptionOptions();
                    options.columns = table.getColumns();
                    TableSubscription sub = table.createSubscription(options);
                    return assertUpdateReceived(sub, data -> validateReeValues(table, data), 5000)
                            .then(ignore -> Promise.resolve(table));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testReeSnapshot() {
        connect(tables)
                .then(table("ree_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    DataOptions.SnapshotOptions opts = new DataOptions.SnapshotOptions();
                    opts.columns = table.getColumns();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 5));
                    return table.createSnapshot(opts).then(data -> {
                        validateReeValues(table, data);
                        return Promise.resolve(table);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testReeViewport() {
        connect(tables)
                .then(table("ree_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    table.setViewport(0, 5, null);
                    return assertUpdateReceived(table, data -> validateReeValues(table, data), 5000);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testReeNullsInRuns() {
        connect(tables)
                .then(table("ree_null_table_annotated"))
                .then(table -> {
                    delayTestFinish(7000);
                    table.setViewport(0, 5, null);
                    return assertUpdateReceived(table, data -> {
                        JsArray<? extends TableData.Row> rows = data.getRows();
                        Column repNullInt = table.findColumn("RepNullInt");
                        for (int i = 0; i < 3; i++) {
                            assertNull("Expected null at row " + i, rows.getAt(i).get(repNullInt));
                        }
                        for (int i = 3; i < 6; i++) {
                            assertEquals("Expected 42 at row " + i, 42,
                                    rows.getAt(i).get(repNullInt).asInt());
                        }
                    }, 5000);
                })
                .then(this::finish).catch_(this::report);
    }

    private static void validateReeValues(JsTable table, TableData data) {
        JsArray<? extends TableData.Row> rows = data.getRows();
        assertEquals(6, rows.length);

        Column repInt = table.findColumn("RepInt");
        Column repStr = table.findColumn("RepStr");

        int[] expectedInts = {0, 0, 1, 1, 2, 2};
        String[] expectedStrs = {"hello", "hello", "hello", "world", "world", "world"};

        for (int i = 0; i < 6; i++) {
            TableData.Row row = rows.getAt(i);
            assertEquals("RepInt[" + i + "]", expectedInts[i], row.get(repInt).asInt());
            assertEquals("RepStr[" + i + "]", expectedStrs[i], row.get(repStr).asString());
        }
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
