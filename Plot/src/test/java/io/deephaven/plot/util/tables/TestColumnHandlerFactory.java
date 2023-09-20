/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.util.tables;

import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.plot.errors.PlotIllegalArgumentException;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTimeUtils;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.util.Date;

import static io.deephaven.util.QueryConstants.*;
import static junit.framework.TestCase.*;

public class TestColumnHandlerFactory {

    @Rule
    final public EngineCleanup framework = new EngineCleanup();

    private final ZoneId tz = DateTimeUtils.timeZone();
    private final int[] ints = {NULL_INT, 2, 3};
    private final float[] floats = {NULL_FLOAT, 2, 3};
    private final long[] longs = {NULL_LONG, 2, 3};
    private final double[] doubles = {NULL_DOUBLE, 2, 3};
    private final short[] shorts = {NULL_SHORT, 2, 3};
    private final Short[] Shorts = {null, 2, 3};
    private final Integer[] Integers = {null, 2, 3};
    private final Long[] Longs = {null, 2L, 3L};
    private final Float[] Floats = {null, 2f, 3f};
    private final Double[] Doubles = {null, 2d, 3d};
    private final Number[] Numbers = {null, 2, 3};
    private final Date[] dates = {null, new Date(1), new Date(2)};
    private final Instant[] instants = {
            null, DateTimeUtils.epochNanosToInstant(1), DateTimeUtils.epochNanosToInstant(2)};
    private final ZonedDateTime[] zdts = {
            null, DateTimeUtils.epochNanosToZonedDateTime(1, tz), DateTimeUtils.epochNanosToZonedDateTime(2, tz)};
    private final Paint[] paints = {null, new Color(100, 0, 0), new Color(0, 100, 0)};
    private final String[] strings = {"A", "B", "C"};

    private Table table;
    private TableHandle tableHandle;

    @Before
    public void setUp() {
        table = TableTools.newTable(
                TableTools.intCol("ints", ints),
                TableTools.floatCol("floats", floats),
                TableTools.longCol("longs", longs),
                TableTools.doubleCol("doubles", doubles),
                TableTools.shortCol("shorts", shorts),
                TableTools.col("Shorts", Shorts),
                TableTools.col("Integers", Integers),
                TableTools.col("Longs", Longs),
                TableTools.col("Floats", Floats),
                TableTools.col("Doubles", Doubles),
                TableTools.col("Numbers", Numbers),
                TableTools.col("Dates", dates),
                TableTools.col("Instants", instants),
                TableTools.col("Paints", paints),
                TableTools.col("Strings", strings)).ungroup();
        tableHandle = new TableHandle(table,
                "ints", "floats", "longs", "doubles", "shorts", "Shorts", "Integers", "Longs", "Floats", "Doubles",
                "Numbers", "Dates", "Instants", "Paints", "Strings");
    }

    @Test
    public void testTypeClassification() {
        assertTrue(ColumnHandlerFactory.TypeClassification.INTEGER.isNumeric());
        assertTrue(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT.isNumeric());
        assertTrue(ColumnHandlerFactory.TypeClassification.TIME.isNumeric());
        assertFalse(ColumnHandlerFactory.TypeClassification.PAINT.isNumeric());
        assertFalse(ColumnHandlerFactory.TypeClassification.COMPARABLE.isNumeric());
        assertFalse(ColumnHandlerFactory.TypeClassification.OBJECT.isNumeric());
    }

    @Test
    public void testNumericColumnHandlerHandle() {
        try {
            ColumnHandlerFactory.newNumericHandler(tableHandle, null, null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newNumericHandler((TableHandle) null, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        ColumnHandlerFactory.ColumnHandler handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "ints", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "ints", int.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "doubles", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "doubles", double.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "longs", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "longs", long.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "floats", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "floats", float.class, handler);


        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Integers", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "Integers", int.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Doubles", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "Doubles", double.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Floats", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "Floats", float.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Shorts", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "Shorts", short.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Longs", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "Longs", long.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Numbers", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "Numbers", Number.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Dates", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.TIME, "Dates", Date.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Instants", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.TIME, "Instants", Instant.class, handler);

        handler.getTableHandle();
        handler = ColumnHandlerFactory.newNumericHandler(tableHandle, "Paints", null);
        assertEquals(ColumnHandlerFactory.TypeClassification.PAINT, handler.typeClassification());
        assertEquals(paints.length, handler.size());
        assertEquals("Paints", handler.getColumnName());
        assertEquals(Paint.class, handler.type());

        for (int i = 0; i < paints.length; i++) {
            assertEquals(paints[i], handler.get(i));
        }

        try {
            handler.getDouble(0);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("conversion"));
        }



        try {
            ColumnHandlerFactory.newNumericHandler(tableHandle, "Strings", null);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }
    }

    @Test
    public void testNumericColumnHandlerTable() {
        try {
            ColumnHandlerFactory.newNumericHandler(table, null, null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newNumericHandler((Table) null, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        ColumnHandlerFactory.ColumnHandler handler = ColumnHandlerFactory.newNumericHandler(table, "ints", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "ints", int.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "doubles", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "doubles", double.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "longs", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "longs", long.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "floats", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "floats", float.class, handler);


        handler = ColumnHandlerFactory.newNumericHandler(table, "Integers", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "Integers", int.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "Doubles", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "Doubles", double.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "Floats", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "Floats", float.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "Shorts", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "Shorts", short.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "Longs", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.INTEGER, "Longs", long.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "Numbers", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.FLOATINGPOINT, "Numbers", Number.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "Dates", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.TIME, "Dates", Date.class, handler);

        handler = ColumnHandlerFactory.newNumericHandler(table, "Instants", null);
        columnHandlerTest(ColumnHandlerFactory.TypeClassification.TIME, "Instants", Instant.class, handler);

        try {
            handler.getTableHandle();
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("does not support table handles"));
        }

        handler = ColumnHandlerFactory.newNumericHandler(table, "Paints", null);
        assertEquals(ColumnHandlerFactory.TypeClassification.PAINT, handler.typeClassification());
        assertEquals(paints.length, handler.size());
        assertEquals("Paints", handler.getColumnName());
        assertEquals(Paint.class, handler.type());

        for (int i = 0; i < paints.length; i++) {
            assertEquals(paints[i], handler.get(i));
        }

        try {
            handler.getDouble(0);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("conversion"));
        }



        try {
            ColumnHandlerFactory.newNumericHandler(table, "Strings", null);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }
    }

    @Test
    public void testComparableHandlerHandle() {
        try {
            ColumnHandlerFactory.newComparableHandler(tableHandle, null, null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newComparableHandler((TableHandle) null, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newComparableHandler(tableHandle, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }


        ColumnHandlerFactory.ColumnHandler handler =
                ColumnHandlerFactory.newComparableHandler(tableHandle, "Strings", null);
        assertEquals(ColumnHandlerFactory.TypeClassification.COMPARABLE, handler.typeClassification());
        assertEquals(strings.length, handler.size());
        assertEquals("Strings", handler.getColumnName());
        assertEquals(String.class, handler.type());
        for (int i = 0; i < strings.length; i++) {
            assertEquals(strings[i], handler.get(i));
        }

        try {
            handler.getDouble(0);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("conversion"));
        }
    }

    @Test
    public void testComparableHandlerTable() {
        try {
            ColumnHandlerFactory.newComparableHandler(table, null, null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newComparableHandler((Table) null, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newComparableHandler(table, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }


        ColumnHandlerFactory.ColumnHandler handler = ColumnHandlerFactory.newComparableHandler(table, "Strings", null);
        assertEquals(ColumnHandlerFactory.TypeClassification.COMPARABLE, handler.typeClassification());
        assertEquals(strings.length, handler.size());
        assertEquals("Strings", handler.getColumnName());
        assertEquals(String.class, handler.type());
        for (int i = 0; i < strings.length; i++) {
            assertEquals(strings[i], handler.get(i));
        }

        try {
            handler.getDouble(0);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("conversion"));
        }
    }

    @Test
    public void testObjectHandlerHandle() {
        try {
            ColumnHandlerFactory.newObjectHandler(tableHandle, null, null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newObjectHandler((TableHandle) null, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }


        ColumnHandlerFactory.ColumnHandler handler =
                ColumnHandlerFactory.newObjectHandler(tableHandle, "Strings", null);
        assertEquals(ColumnHandlerFactory.TypeClassification.OBJECT, handler.typeClassification());
        assertEquals(strings.length, handler.size());
        assertEquals("Strings", handler.getColumnName());
        assertEquals(String.class, handler.type());
        for (int i = 0; i < strings.length; i++) {
            assertEquals(strings[i], handler.get(i));
        }

        try {
            handler.getDouble(0);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("conversion"));
        }
    }

    @Test
    public void testObjectHandlerTable() {
        try {
            ColumnHandlerFactory.newObjectHandler(table, null, null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            ColumnHandlerFactory.newObjectHandler((Table) null, "ints", null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }


        ColumnHandlerFactory.ColumnHandler handler = ColumnHandlerFactory.newObjectHandler(table, "Strings", null);
        assertEquals(ColumnHandlerFactory.TypeClassification.OBJECT, handler.typeClassification());
        assertEquals(strings.length, handler.size());
        assertEquals("Strings", handler.getColumnName());
        assertEquals(String.class, handler.type());
        for (int i = 0; i < strings.length; i++) {
            assertEquals(strings[i], handler.get(i));
        }

        try {
            handler.getDouble(0);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("conversion"));
        }
    }

    private void columnHandlerTest(ColumnHandlerFactory.TypeClassification type, String name, Class clazz,
            ColumnHandlerFactory.ColumnHandler handler) {

        assertEquals(type, handler.typeClassification());
        assertEquals(doubles.length, handler.size());
        assertEquals(name, handler.getColumnName());
        assertEquals(clazz, handler.type());


        assertEquals(Double.NaN, handler.getDouble(0));
        assertNull(handler.get(0));
        for (int i = 1; i < doubles.length; i++) {
            if (clazz.equals(Date.class)) {
                assertEquals(dates[i].getTime(), ((Date) handler.get(i)).getTime());
                assertEquals((double) dates[i].getTime() * 1000000, handler.getDouble(i));
            } else if (clazz.equals(Instant.class)) {
                assertEquals(instants[i], handler.get(i));
                assertEquals((double) DateTimeUtils.epochNanos(instants[i]), handler.getDouble(i));
            } else if (clazz.equals(ZonedDateTime.class)) {
                assertEquals(zdts[i], handler.get(i));
                assertEquals((double) DateTimeUtils.epochNanos(zdts[i]), handler.getDouble(i));
            } else {
                assertEquals(doubles[i], handler.getDouble(i));
                if (Number.class.isAssignableFrom(handler.get(i).getClass())) {
                    assertEquals(doubles[i], ((Number) handler.get(i)).doubleValue());
                } else {
                    assertEquals(doubles[i], (double) handler.get(i));
                }
            }
        }
    }
}
