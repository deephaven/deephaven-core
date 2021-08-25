package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.LiveQueryTable;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.test.junit4.JUnit4LiveTableTestCase;
import io.deephaven.util.QueryConstants;
import io.deephaven.tablelogger.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;

import static io.deephaven.db.tables.utils.TableTools.*;

public class TestDynamicTableWriter {
    final JUnit4LiveTableTestCase ltc = new JUnit4LiveTableTestCase();

    @Before
    public void setUp() throws Exception {
        ltc.setUp();
    }

    @After
    public void tearDown() throws Exception {
        ltc.tearDown();
    }

    @Test
    public void testTypes() throws IOException {
        final String[] names = new String[] {"BC", "CC", "SC", "IC", "LC", "FC", "DC", "StrC", "BLC", "DTC", "BIC"};
        final Class[] types = new Class[] {byte.class, char.class, short.class, int.class, long.class, float.class,
                double.class, String.class, Boolean.class, DBDateTime.class, BigInteger.class};
        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        writer.getSetter("BC").setByte((byte) 1);
        writer.getSetter("CC").setChar('A');
        writer.getSetter("SC").setShort((short) 2);
        writer.getSetter("IC").setInt(3);
        writer.getSetter("LC").setLong(4);
        writer.getSetter("FC").setFloat(5.5f);
        writer.getSetter("DC").setDouble(6.6);
        writer.getSetter("StrC", String.class).set("Seven");
        writer.getSetter("BLC", Boolean.class).setBoolean(true);
        writer.getSetter("DTC", DBDateTime.class).set(DBTimeUtils.convertDateTime("2020-09-16T07:55:00 NY"));
        writer.getSetter("BIC", BigInteger.class).set(BigInteger.valueOf(8));
        writer.writeRow();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final DynamicTable expected1 = newTable(byteCol("BC", (byte) 1),
                charCol("CC", 'A'),
                shortCol("SC", (short) 2),
                intCol("IC", 3),
                longCol("LC", 4),
                floatCol("FC", 5.5f),
                doubleCol("DC", 6.6),
                stringCol("StrC", "Seven"),
                col("BLC", true),
                col("DTC", DBTimeUtils.convertDateTime("2020-09-16T07:55:00 NY")),
                col("BIC", BigInteger.valueOf(8)));
        TstUtils.assertTableEquals(expected1, result);

        final Row row = writer.getRowWriter();

        row.getSetter("BC").setByte((byte) 9);
        row.getSetter("CC").setChar('B');
        row.getSetter("SC").setShort((short) 10);
        row.getSetter("IC").setInt(11);
        row.getSetter("LC").setLong(12);
        row.getSetter("FC").setFloat(13.13f);
        row.getSetter("DC").setDouble(14.14);
        row.getSetter("StrC", String.class).set("Fifteen");
        row.getSetter("BLC", Boolean.class).setBoolean(true);
        row.getSetter("DTC", DBDateTime.class).set(DBTimeUtils.convertDateTime("2020-09-16T08:55:00 NY"));
        row.getSetter("BIC", BigInteger.class).set(BigInteger.valueOf(16));
        row.setFlags(Row.Flags.StartTransaction);
        row.writeRow();

        final Row row2 = writer.getRowWriter();
        row2.getSetter("BC").setByte((byte) 17);
        row2.getSetter("CC").setChar('C');
        row2.getSetter("SC").setShort((short) 18);
        row2.getSetter("IC").setInt(19);
        row2.getSetter("LC").setLong(20);
        row2.getSetter("FC").setFloat(21.21f);
        row2.getSetter("DC").setDouble(22.22);
        row2.getSetter("StrC", String.class).set("Twenty Three");
        row2.getSetter("BLC", Boolean.class).setBoolean(false);
        row2.getSetter("DTC", DBDateTime.class).set(DBTimeUtils.convertDateTime("2020-09-16T09:55:00 NY"));
        row2.getSetter("BIC", BigInteger.class).set(BigInteger.valueOf(24));
        row2.setFlags(Row.Flags.StartTransaction);
        row2.writeRow();

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);
        TstUtils.assertTableEquals(expected1, result);

        final Row row3 = writer.getRowWriter();
        row3.getSetter("BC", byte.class).set((byte) 25);
        row3.getSetter("CC", char.class).set('D');
        row3.getSetter("SC", short.class).set((short) 26);
        row3.getSetter("IC", int.class).set(27);
        row3.getSetter("LC", long.class).set((long) 28);
        row3.getSetter("FC", float.class).set(29.29f);
        row3.getSetter("DC", double.class).set(30.30);
        row3.getSetter("StrC", String.class).set("Thirty One");
        row3.getSetter("BLC", Boolean.class).set(null);
        row3.getSetter("DTC", DBDateTime.class).set(DBTimeUtils.convertDateTime("2020-09-16T10:55:00 NY"));
        row3.getSetter("BIC", BigInteger.class).set(BigInteger.valueOf(32));
        row3.setFlags(Row.Flags.EndTransaction);
        row3.writeRow();

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final DynamicTable expected2 = newTable(byteCol("BC", (byte) 1, (byte) 17, (byte) 25),
                charCol("CC", 'A', 'C', 'D'),
                shortCol("SC", (short) 2, (short) 18, (short) 26),
                intCol("IC", 3, 19, 27),
                longCol("LC", 4, 20, 28),
                floatCol("FC", 5.5f, 21.21f, 29.29f),
                doubleCol("DC", 6.6, 22.22, 30.30),
                stringCol("StrC", "Seven", "Twenty Three", "Thirty One"),
                col("BLC", true, false, null),
                col("DTC", DBTimeUtils.convertDateTime("2020-09-16T07:55:00 NY"),
                        DBTimeUtils.convertDateTime("2020-09-16T09:55:00 NY"),
                        DBTimeUtils.convertDateTime("2020-09-16T10:55:00 NY")),
                col("BIC", BigInteger.valueOf(8), BigInteger.valueOf(24), BigInteger.valueOf(32)));
        TstUtils.assertTableEquals(expected2, result);

    }

    @Test
    public void testNulls() throws IOException {
        final String[] names = new String[] {"BC", "CC", "SC", "IC", "LC", "FC", "DC", "StrC", "BLC", "DTC", "BIC"};
        final Class[] types = new Class[] {byte.class, char.class, short.class, int.class, long.class, float.class,
                double.class, String.class, Boolean.class, DBDateTime.class, BigInteger.class};
        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        writer.getSetter("BC").setByte((byte) 1);
        writer.getSetter("CC").setChar('A');
        writer.getSetter("SC").setShort((short) 2);
        writer.getSetter("IC").setInt(3);
        writer.getSetter("LC").setLong(4);
        writer.getSetter("FC").setFloat(5.5f);
        writer.writeRow();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected1 = newTable(byteCol("BC", (byte) 1),
                charCol("CC", 'A'),
                shortCol("SC", (short) 2),
                intCol("IC", 3),
                longCol("LC", 4),
                floatCol("FC", 5.5f),
                doubleCol("DC", QueryConstants.NULL_DOUBLE))
                        .updateView("StrC=(String)null", "BLC=(Boolean)null", "DTC=(DBDateTime)null",
                                "BIC=(java.math.BigInteger)null");
        TstUtils.assertTableEquals(expected1, result);

        final Row row = writer.getRowWriter();

        row.getSetter("DC").setDouble(14.14);
        row.getSetter("StrC", String.class).set("Fifteen");
        row.getSetter("BLC", Boolean.class).setBoolean(true);
        row.getSetter("DTC", DBDateTime.class).set(DBTimeUtils.convertDateTime("2020-09-16T08:55:00 NY"));
        row.getSetter("BIC", BigInteger.class).set(BigInteger.valueOf(16));
        row.setFlags(Row.Flags.SingleRow);
        row.writeRow();

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final DynamicTable expected2 = newTable(byteCol("BC", QueryConstants.NULL_BYTE),
                charCol("CC", QueryConstants.NULL_CHAR),
                shortCol("SC", QueryConstants.NULL_SHORT),
                intCol("IC", QueryConstants.NULL_INT),
                longCol("LC", QueryConstants.NULL_LONG),
                floatCol("FC", QueryConstants.NULL_FLOAT),
                doubleCol("DC", 14.14),
                stringCol("StrC", "Fifteen"),
                col("BLC", true),
                col("DTC", DBTimeUtils.convertDateTime("2020-09-16T08:55:00 NY")),
                col("BIC", BigInteger.valueOf(16)));
        TstUtils.assertTableEquals(merge(expected1, expected2), result);

    }

    @Test
    public void testTransactions() throws IOException {
        final String[] columnNames = new String[] {"A", "B"};
        final Class[] columnTypes = new Class[] {String.class, int.class};
        final DynamicTableWriter writer = new DynamicTableWriter(columnNames, columnTypes);
        final LiveQueryTable result = writer.getTable();
        TstUtils.assertTableEquals(TableTools.newTable(TableTools.stringCol("A"), TableTools.intCol("B")), result);

        addRow(writer, Row.Flags.SingleRow, "Fred", 1);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final DynamicTable lonelyFred =
                TableTools.newTable(TableTools.stringCol("A", "Fred"), TableTools.intCol("B", 1));
        TstUtils.assertTableEquals(lonelyFred, result);

        addRow(writer, Row.Flags.StartTransaction, "Barney", 2);
        addRow(writer, Row.Flags.None, "Betty", 3);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        TstUtils.assertTableEquals(lonelyFred, result);

        addRow(writer, Row.Flags.EndTransaction, "Bam-Bam", 4);

        TstUtils.assertTableEquals(lonelyFred, result);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final DynamicTable withRubbles = TableTools.newTable(
                TableTools.stringCol("A", "Fred", "Barney", "Betty", "Bam-Bam"), TableTools.intCol("B", 1, 2, 3, 4));
        TstUtils.assertTableEquals(withRubbles, result);

        addRow(writer, Row.Flags.StartTransaction, "Wilma", 5);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);
        TstUtils.assertTableEquals(withRubbles, result);

        addRow(writer, Row.Flags.StartTransaction, "Pebbles", 6);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);
        TstUtils.assertTableEquals(withRubbles, result);

        addRow(writer, Row.Flags.EndTransaction, "Wilma", 7);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);
        final DynamicTable allTogether =
                TableTools.newTable(TableTools.stringCol("A", "Fred", "Barney", "Betty", "Bam-Bam", "Pebbles", "Wilma"),
                        TableTools.intCol("B", 1, 2, 3, 4, 6, 7));
        TstUtils.assertTableEquals(allTogether, result);
    }

    private void addRow(DynamicTableWriter writer, Row.Flags startTransaction, String barney, int i)
            throws IOException {
        final Row rw = writer.getRowWriter();
        rw.setFlags(startTransaction);
        rw.getSetter("A", String.class).set(barney);
        rw.getSetter("B", int.class).setInt(i);
        rw.writeRow();
    }
}
