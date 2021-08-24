/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util;


import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.gui.color.Color;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataArrayInt;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.TableTools;
import junit.framework.TestCase;

import java.util.Date;

public class TestArgumentValidations extends BaseArrayTestCase {

    public void testArgumentValidations() {
        final String NON_NULL = "TEST";
        final String message = "message";
        final String stringColumn = "StringColumn";
        final String intColumn = "Ints";
        final String dateColumn = "Dates";
        final String colorColumn = "Colors";
        final String INVALID = "INVALID";
        final int[] ints = {1};
        final IndexableNumericData intData = new IndexableNumericDataArrayInt(ints, null);
        final IndexableNumericData intData2 =
            new IndexableNumericDataArrayInt(new int[] {2, 3}, null);
        final DBDateTime[] dates = {new DBDateTime(1)};
        final Color[] colors = {new Color(1)};
        final Table table = TableTools.newTable(
            TableTools.col(stringColumn, NON_NULL),
            TableTools.col(dateColumn, dates),
            TableTools.col(colorColumn, colors),
            TableTools.intCol(intColumn, ints)).ungroup();

        ArgumentValidations.assertInstance(String.class, String.class, message, null);
        try {
            ArgumentValidations.assertInstance(String.class, Number.class, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }

        ArgumentValidations.assertInstance(table, stringColumn, String.class, message, null);
        try {
            ArgumentValidations.assertInstance(table, INVALID, String.class, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }
        try {
            ArgumentValidations.assertInstance(table, intColumn, String.class, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }

        ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(table, intColumn,
            message, null);
        ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(table, dateColumn,
            message, null);
        ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(table, stringColumn,
            message, null);
        try {
            ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(table, INVALID,
                message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }
        try {
            ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(table, INVALID,
                message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        ArgumentValidations.assertSameSize(new IndexableNumericData[] {intData},
            new String[] {"TEST"}, null);
        try {
            ArgumentValidations.assertSameSize(new IndexableNumericData[] {null},
                new String[] {message}, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertSameSize(new IndexableNumericData[] {intData, intData2},
                new String[] {"A", "B"}, null);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Input data"));
        }

        assertEquals(int.class, ArgumentValidations.getColumnType(table, intColumn, null));
        try {
            ArgumentValidations.getColumnType(table, INVALID, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        assertTrue(ArgumentValidations.isTime(DBDateTime.class, null));
        assertTrue(ArgumentValidations.isTime(Date.class, null));
        assertFalse(ArgumentValidations.isTime(int.class, null));
        assertFalse(ArgumentValidations.isTime(Double.class, null));

        assertTrue(ArgumentValidations.isNumericOrTime(DBDateTime.class, null));
        assertTrue(ArgumentValidations.isNumericOrTime(Date.class, null));
        assertTrue(ArgumentValidations.isNumericOrTime(int.class, null));
        assertTrue(ArgumentValidations.isNumericOrTime(Double.class, null));

        assertTrue(ArgumentValidations.isTime(table, dateColumn, null));
        assertFalse(ArgumentValidations.isTime(table, intColumn, null));
        assertFalse(ArgumentValidations.isTime(table, stringColumn, null));

        assertFalse(ArgumentValidations.isPrimitiveNumeric(table, dateColumn, null));
        assertTrue(ArgumentValidations.isPrimitiveNumeric(table, intColumn, null));
        assertFalse(ArgumentValidations.isPrimitiveNumeric(table, stringColumn, null));

        assertFalse(ArgumentValidations.isBoxedNumeric(table, dateColumn, null));
        assertFalse(ArgumentValidations.isBoxedNumeric(table, intColumn, null));
        assertFalse(ArgumentValidations.isBoxedNumeric(table, stringColumn, null));

        assertFalse(ArgumentValidations.isNumeric(table, dateColumn, null));
        assertTrue(ArgumentValidations.isNumeric(table, intColumn, null));
        assertFalse(ArgumentValidations.isNumeric(table, stringColumn, null));

        assertTrue(ArgumentValidations.isNumericOrTime(table, dateColumn, null));
        assertTrue(ArgumentValidations.isNumericOrTime(table, intColumn, null));
        assertFalse(ArgumentValidations.isNumericOrTime(table, stringColumn, null));

        ArgumentValidations.assertIsTime(table, dateColumn, message, null);
        try {
            ArgumentValidations.assertIsTime(table, intColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsTime(table, stringColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsTime(table, INVALID, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        ArgumentValidations.assertIsPrimitiveNumeric(table, intColumn, message, null);
        try {
            ArgumentValidations.assertIsPrimitiveNumeric(table, dateColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsPrimitiveNumeric(table, stringColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsPrimitiveNumeric(table, INVALID, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        try {
            ArgumentValidations.assertIsBoxedNumeric(table, intColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsBoxedNumeric(table, dateColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsBoxedNumeric(table, stringColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsBoxedNumeric(table, INVALID, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        ArgumentValidations.assertIsNumeric(table, intColumn, message, null);
        try {
            ArgumentValidations.assertIsNumeric(table, dateColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsNumeric(table, stringColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsNumeric(table, INVALID, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        ArgumentValidations.assertIsNumericOrTime(table, intColumn, message, null);
        ArgumentValidations.assertIsNumericOrTime(table, dateColumn, message, null);
        try {
            ArgumentValidations.assertIsNumericOrTime(table, stringColumn, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(message));
        }
        try {
            ArgumentValidations.assertIsNumericOrTime(table, INVALID, message, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        ArgumentValidations.assertColumnsInTable(table, null, dateColumn, intColumn, stringColumn);
        try {
            ArgumentValidations.assertIsNumericOrTime(table, stringColumn, INVALID, null);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }

        final TableHandle handle =
            new TableHandle(table, intColumn, stringColumn, dateColumn);
        ArgumentValidations.assertColumnsInTable(handle, null, dateColumn, intColumn, stringColumn);
        try {
            ArgumentValidations.assertColumnsInTable(handle, null, stringColumn, INVALID);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(INVALID));
        }
    }

    public void testNaNSafeEquals() {
        assertTrue(ArgumentValidations.nanSafeEquals(5, 5));
        assertFalse(ArgumentValidations.nanSafeEquals(4, 5));
        assertTrue(ArgumentValidations.nanSafeEquals(Double.NaN, Double.NaN));
        assertFalse(ArgumentValidations.nanSafeEquals(Double.NaN, 5));
    }
}
