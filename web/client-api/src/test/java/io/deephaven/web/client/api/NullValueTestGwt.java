package io.deephaven.web.client.api;

import com.google.gwt.junit.DoNotRunWith;
import com.google.gwt.junit.Platform;
import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.subscription.ViewportRow;

@DoNotRunWith(Platform.HtmlUnitBug)
public class NullValueTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("nulltable", "empty_table(2).update([\n" +
                    "   \"MyInt=i==0?null:i\",\n" +
                    "   \"MyLong=i==0?null:(long)i\",\n" +
                    "   \"MyDouble=i==0?null:(double)i\",\n" +
                    "   \"MyShort=i==0?null:(short)i\",\n" +
                    "   \"MyFloat=i==0?null:(float)i\",\n" +
                    "   \"MyChar=i==0?null:(char)i\",\n" +
                    "   \"MyByte=i==0?null:(byte)i\",\n" +
                    "   \"MyBoolean=i==0?null:true\",\n" +
                    "   \"MyDate=i==0?null:epochNanosToInstant(i)\"\n" +
                    "])");
    public void testNullTable() {
        connect(tables)
                .then(table("nulltable"))
                .then(table -> {
                    delayTestFinish(5000);

                    assertEquals(2., table.getSize(), 0);
                    assertEquals(2., table.getTotalSize(), 0);

                    return Promise.resolve(table);
                })
                .then(table -> {
                    assertEquals("int", table.findColumn("MyInt").getType());
                    assertEquals("long", table.findColumn("MyLong").getType());
                    assertEquals("double", table.findColumn("MyDouble").getType());
                    assertEquals("short", table.findColumn("MyShort").getType());
                    assertEquals("float", table.findColumn("MyFloat").getType());
                    assertEquals("char", table.findColumn("MyChar").getType());
                    assertEquals("byte", table.findColumn("MyByte").getType());
                    assertEquals("java.lang.Boolean", table.findColumn("MyBoolean").getType());
                    assertEquals("java.time.Instant", table.findColumn("MyDate").getType());

                    return Promise.resolve(table);
                })
                .then(table -> {
                    table.setViewport(0, 1, null);
                    return assertUpdateReceived(table, viewport -> {
                        JsArray<ViewportRow> rows = viewport.getRows();
                        ViewportRow nullRow = rows.getAt(0);

                        JsArray<Column> columns = table.getColumns();
                        for (int i = 0; i < columns.length; i++) {
                            assertEquals(null, nullRow.get(columns.getAt(i)));
                        }

                        ViewportRow valueRow = rows.getAt(1);
                        assertEquals(1, valueRow.get(table.findColumn("MyInt")).asInt());
                        assertEquals((long)1, valueRow.get(table.findColumn("MyLong")).<LongWrapper>cast().getWrapped());
                        assertEquals((double)1, valueRow.get(table.findColumn("MyDouble")).asDouble());
                        assertEquals((short)1, valueRow.get(table.findColumn("MyShort")).asShort());
                        assertEquals((float)1., valueRow.get(table.findColumn("MyFloat")).asFloat());
                        assertEquals((char)1, valueRow.get(table.findColumn("MyChar")).asChar());
                        assertEquals((byte)1, valueRow.get(table.findColumn("MyByte")).asByte());
                        assertEquals(true, valueRow.get(table.findColumn("MyBoolean")).asBoolean());
                        assertEquals((long)1, valueRow.get(table.findColumn("MyDate")).<DateWrapper>cast().getWrapped());
                    }, 1000);
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}