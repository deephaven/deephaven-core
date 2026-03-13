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
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import jsinterop.base.Js;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

@DoNotRunWith(Platform.HtmlUnitBug)
public class NullValueTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("some_null_table", "empty_table(2).update([\n" +
                    "   \"MyInt=i==0?null:i\",\n" +
                    "   \"MyLong=i==0?null:(long)i\",\n" +
                    "   \"MyDouble=i==0?null:(double)i\",\n" +
                    "   \"MyShort=i==0?null:(short)i\",\n" +
                    "   \"MyFloat=i==0?null:(float)i\",\n" +
                    "   \"MyChar=i==0?null:(char)i\",\n" +
                    "   \"MyByte=i==0?null:(byte)i\",\n" +
                    "   \"MyBoolean=i==0?null:true\",\n" +
                    "   \"MyString=i==0?null:``+i\",\n" +
                    "   \"MyDate=i==0?null:epochNanosToInstant(i)\",\n" +
                    "   \"MyBigInteger=i==0?null:java.math.BigInteger.valueOf(i)\",\n" +
                    "   \"MyBigDecimal=i==0?null:java.math.BigDecimal.valueOf(i, 4)\",\n" +
                    "   \"MyStringArray1=i==0?null:new String[] {`A`, `B`, `C`}\"\n" +
                    "])")
            .script("all_null_table", "empty_table(20).update([\n" +
                    "   \"MyInt=(Integer)null\",\n" +
                    "   \"MyLong=(Long)null\",\n" +
                    "   \"MyDouble=(Double)null\",\n" +
                    "   \"MyShort=(Short)null\",\n" +
                    "   \"MyFloat=(Float)null\",\n" +
                    "   \"MyChar=(Character)null\",\n" +
                    "   \"MyByte=(Byte)null\",\n" +
                    "   \"MyBoolean=(Boolean)null\",\n" +
                    "   \"MyString=(String)null\",\n" +
                    "   \"MyDate=(java.time.Instant) null\",\n" +
                    "   \"MyBigInteger=(java.math.BigInteger)null\",\n" +
                    "   \"MyBigDecimal=(java.math.BigDecimal)null\",\n" +
                    "   \"MyStringArray1=(String[])null\"\n" +
                    "])");

    public void testTableWithSomeNulls() {
        connect(tables)
                .then(table("some_null_table"))
                .then(table -> {
                    delayTestFinish(5000);

                    assertFalse(table.isRefreshing());
                    assertFalse(table.isClosed());
                    assertFalse(table.isBlinkTable());
                    assertFalse(table.hasInputTable());
                    assertFalse((table.isUncoalesced()));

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
                    assertEquals("java.lang.String", table.findColumn("MyString").getType());
                    assertEquals("java.time.Instant", table.findColumn("MyDate").getType());
                    assertEquals("java.math.BigInteger", table.findColumn("MyBigInteger").getType());
                    assertEquals("java.math.BigDecimal", table.findColumn("MyBigDecimal").getType());
                    assertEquals("java.lang.String[]", table.findColumn("MyStringArray1").getType());

                    return Promise.resolve(table);
                })
                // test previews with new subscription
                .then(table -> {
                    DataOptions.SubscriptionOptions options = new DataOptions.SubscriptionOptions();
                    options.columns = table.getColumns();
                    TableSubscription sub = table.createSubscription(options);
                    return assertUpdateReceived(sub, data -> {
                        JsArray<? extends TableData.Row> rows = data.getRows();
                        TableData.Row nullRow = rows.getAt(0);

                        JsArray<Column> columns = table.getColumns();
                        for (int i = 0; i < columns.length; i++) {
                            assertFalse(Js.isTripleEqual(Js.undefined(), nullRow.get(columns.getAt(i))));
                            assertNull(nullRow.get(columns.getAt(i)));
                        }

                        TableData.Row valueRow = rows.getAt(1);
                        assertEquals(1, valueRow.get(table.findColumn("MyInt")).asInt());
                        assertEquals((long) 1,
                                valueRow.get(table.findColumn("MyLong")).<LongWrapper>cast().getWrapped());
                        assertEquals((double) 1, valueRow.get(table.findColumn("MyDouble")).asDouble());
                        assertEquals((short) 1, valueRow.get(table.findColumn("MyShort")).asShort());
                        assertEquals((float) 1., valueRow.get(table.findColumn("MyFloat")).asFloat());
                        assertEquals((char) 1, valueRow.get(table.findColumn("MyChar")).asChar());
                        assertEquals((byte) 1, valueRow.get(table.findColumn("MyByte")).asByte());
                        assertEquals(true, valueRow.get(table.findColumn("MyBoolean")).asBoolean());
                        assertEquals("1", valueRow.get(table.findColumn("MyString")).asString());
                        assertEquals(1, valueRow.get(table.findColumn("MyDate")).<DateWrapper>cast().getWrapped());
                        assertEquals(BigInteger.ONE,
                                valueRow.get(table.findColumn("MyBigInteger")).<BigIntegerWrapper>cast().getWrapped());
                        assertEquals(BigDecimal.valueOf(1, 4),
                                valueRow.get(table.findColumn("MyBigDecimal")).<BigDecimalWrapper>cast().getWrapped());
                        assertArrayEquals(new String[] {"A", "B", "C"},
                                valueRow.get(table.findColumn("MyStringArray1")).<String[]>cast());
                    }, 1000).then(ignore -> Promise.resolve(table));
                })

                // Test previews with new snapshot
                .then(table -> {
                    DataOptions.SnapshotOptions snapshotOptions = new DataOptions.SnapshotOptions();
                    snapshotOptions.columns = table.getColumns();
                    snapshotOptions.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 1));
                    return table.createSnapshot(snapshotOptions).then(data -> {
                        JsArray<? extends TableData.Row> rows = data.getRows();
                        TableData.Row nullRow = rows.getAt(0);

                        JsArray<Column> columns = table.getColumns();
                        for (int i = 0; i < columns.length; i++) {
                            assertFalse(Js.isTripleEqual(Js.undefined(), nullRow.get(columns.getAt(i))));
                            assertNull(nullRow.get(columns.getAt(i)));
                        }

                        TableData.Row valueRow = rows.getAt(1);
                        assertEquals(1, valueRow.get(table.findColumn("MyInt")).asInt());
                        assertEquals((long) 1,
                                valueRow.get(table.findColumn("MyLong")).<LongWrapper>cast().getWrapped());
                        assertEquals((double) 1, valueRow.get(table.findColumn("MyDouble")).asDouble());
                        assertEquals((short) 1, valueRow.get(table.findColumn("MyShort")).asShort());
                        assertEquals((float) 1., valueRow.get(table.findColumn("MyFloat")).asFloat());
                        assertEquals((char) 1, valueRow.get(table.findColumn("MyChar")).asChar());
                        assertEquals((byte) 1, valueRow.get(table.findColumn("MyByte")).asByte());
                        assertEquals(true, valueRow.get(table.findColumn("MyBoolean")).asBoolean());
                        assertEquals("1", valueRow.get(table.findColumn("MyString")).asString());
                        assertEquals(1, valueRow.get(table.findColumn("MyDate")).<DateWrapper>cast().getWrapped());
                        assertEquals(BigInteger.ONE,
                                valueRow.get(table.findColumn("MyBigInteger")).<BigIntegerWrapper>cast().getWrapped());
                        assertEquals(BigDecimal.valueOf(1, 4),
                                valueRow.get(table.findColumn("MyBigDecimal")).<BigDecimalWrapper>cast().getWrapped());
                        assertArrayEquals(new String[] {"A", "B", "C"},
                                valueRow.get(table.findColumn("MyStringArray1")).<String[]>cast());

                        return Promise.resolve(table);
                    });
                })
                // Test previews with old setViewport
                .then(table -> {
                    table.setViewport(0, 1, null);
                    return assertUpdateReceived(table, viewport -> {
                        JsArray<? extends TableData.Row> rows = viewport.getRows();
                        TableData.Row nullRow = rows.getAt(0);

                        JsArray<Column> columns = table.getColumns();
                        for (int i = 0; i < columns.length; i++) {
                            assertFalse(Js.isTripleEqual(Js.undefined(), nullRow.get(columns.getAt(i))));
                            assertNull(nullRow.get(columns.getAt(i)));
                        }

                        TableData.Row valueRow = rows.getAt(1);
                        assertEquals(1, valueRow.get(table.findColumn("MyInt")).asInt());
                        assertEquals((long) 1,
                                valueRow.get(table.findColumn("MyLong")).<LongWrapper>cast().getWrapped());
                        assertEquals((double) 1, valueRow.get(table.findColumn("MyDouble")).asDouble());
                        assertEquals((short) 1, valueRow.get(table.findColumn("MyShort")).asShort());
                        assertEquals((float) 1., valueRow.get(table.findColumn("MyFloat")).asFloat());
                        assertEquals((char) 1, valueRow.get(table.findColumn("MyChar")).asChar());
                        assertEquals((byte) 1, valueRow.get(table.findColumn("MyByte")).asByte());
                        assertEquals(true, valueRow.get(table.findColumn("MyBoolean")).asBoolean());
                        assertEquals("1", valueRow.get(table.findColumn("MyString")).asString());
                        assertEquals(1, valueRow.get(table.findColumn("MyDate")).<DateWrapper>cast().getWrapped());
                        assertEquals(BigInteger.ONE,
                                valueRow.get(table.findColumn("MyBigInteger")).<BigIntegerWrapper>cast().getWrapped());
                        assertEquals(BigDecimal.valueOf(1, 4),
                                valueRow.get(table.findColumn("MyBigDecimal")).<BigDecimalWrapper>cast().getWrapped());
                        assertEquals("[A,B,C]", valueRow.get(table.findColumn("MyStringArray1")).<String[]>cast());
                    }, 1000).then(ignore -> Promise.resolve(table));
                })
                // Test previews with new viewport subscription
                .then(table -> {
                    DataOptions.ViewportSubscriptionOptions options = new DataOptions.ViewportSubscriptionOptions();
                    options.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 1));
                    options.columns = table.getColumns();
                    TableViewportSubscription sub = table.createViewportSubscription(options);
                    return assertUpdateReceived(sub, viewport -> {
                        JsArray<? extends TableData.Row> rows = viewport.getRows();
                        TableData.Row nullRow = rows.getAt(0);

                        JsArray<Column> columns = table.getColumns();
                        for (int i = 0; i < columns.length; i++) {
                            assertFalse(Js.isTripleEqual(Js.undefined(), nullRow.get(columns.getAt(i))));
                            assertNull(nullRow.get(columns.getAt(i)));
                        }

                        TableData.Row valueRow = rows.getAt(1);
                        assertEquals(1, valueRow.get(table.findColumn("MyInt")).asInt());
                        assertEquals((long) 1,
                                valueRow.get(table.findColumn("MyLong")).<LongWrapper>cast().getWrapped());
                        assertEquals((double) 1, valueRow.get(table.findColumn("MyDouble")).asDouble());
                        assertEquals((short) 1, valueRow.get(table.findColumn("MyShort")).asShort());
                        assertEquals((float) 1., valueRow.get(table.findColumn("MyFloat")).asFloat());
                        assertEquals((char) 1, valueRow.get(table.findColumn("MyChar")).asChar());
                        assertEquals((byte) 1, valueRow.get(table.findColumn("MyByte")).asByte());
                        assertEquals(true, valueRow.get(table.findColumn("MyBoolean")).asBoolean());
                        assertEquals("1", valueRow.get(table.findColumn("MyString")).asString());
                        assertEquals(1, valueRow.get(table.findColumn("MyDate")).<DateWrapper>cast().getWrapped());
                        assertEquals(BigInteger.ONE,
                                valueRow.get(table.findColumn("MyBigInteger")).<BigIntegerWrapper>cast().getWrapped());
                        assertEquals(BigDecimal.valueOf(1, 4),
                                valueRow.get(table.findColumn("MyBigDecimal")).<BigDecimalWrapper>cast().getWrapped());
                        assertArrayEquals(new String[] {"A", "B", "C"},
                                valueRow.get(table.findColumn("MyStringArray1")).<String[]>cast());
                    }, 1000);
                })
                .then(this::finish).catch_(this::report);
    }

    private static void assertArrayEquals(Object[] expected, Object[] actual) {
        if (expected == actual) {
            return;
        }
        if (expected == null || actual == null) {
            fail("One of the arrays is null");
        }
        assertEquals("Array lengths differ", expected.length, actual.length);
        String expectedString = Arrays.toString(expected);
        String actualString = Arrays.toString(actual);
        for (int i = 0; i < expected.length; i++) {
            assertEquals("Expected " + expectedString + " but got " + actualString, expected[i], actual[i]);
        }
    }

    public void testTableWithAllNulls() {
        connect(tables)
                .then(table("all_null_table"))
                .then(table -> {
                    delayTestFinish(5000);

                    assertFalse(table.isRefreshing());
                    assertFalse(table.isClosed());
                    assertFalse(table.isBlinkTable());
                    assertFalse(table.hasInputTable());
                    assertFalse((table.isUncoalesced()));

                    assertEquals(20., table.getSize(), 0);
                    assertEquals(20., table.getTotalSize(), 0);

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
                    assertEquals("java.lang.String", table.findColumn("MyString").getType());
                    assertEquals("java.time.Instant", table.findColumn("MyDate").getType());
                    assertEquals("java.math.BigInteger", table.findColumn("MyBigInteger").getType());
                    assertEquals("java.math.BigDecimal", table.findColumn("MyBigDecimal").getType());
                    assertEquals("java.lang.String[]", table.findColumn("MyStringArray1").getType());

                    return Promise.resolve(table);
                })
                .then(table -> {
                    table.setViewport(0, 1, null);
                    return assertUpdateReceived(table, viewport -> {
                        JsArray<? extends TableData.Row> rows = viewport.getRows();

                        JsArray<Column> columns = table.getColumns();
                        for (int i = 0; i < columns.length; i++) {
                            for (int j = 0; j < rows.length; j++) {
                                TableData.Row row = rows.getAt(j);
                                assertFalse(Js.isTripleEqual(Js.undefined(), row.get(columns.getAt(i))));
                                assertNull(columns.getAt(i).getName(), row.get(columns.getAt(i)));
                            }
                        }
                    }, 1000);
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
