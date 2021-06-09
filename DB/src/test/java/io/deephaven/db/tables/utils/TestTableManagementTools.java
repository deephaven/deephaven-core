/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.FileUtils;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.StringSetWrapper;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.internal.log.LoggerFactory;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;

import static io.deephaven.db.tables.utils.TableTools.*;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TableManagementTools}.
 */
public class TestTableManagementTools {
    private static final TableManagementTools.StorageFormat storageFormat = TableManagementTools.StorageFormat.Parquet;

    private final static String testRoot = Configuration.getInstance().getWorkspacePath() + File.separator + "TestTableManagementTools";
    private final static File testRootFile = new File(testRoot);
    private static final Logger log = LoggerFactory.getLogger(TestTableManagementTools.class);

    private static Table table1;
    private static Table table2;
    private static Table emptyTable;
    private static Table brokenTable;
    private static Table table3;


    @BeforeClass
    public static void setUpFirst() {
        table1 = new InMemoryTable(
                new String[]{"StringKeys", "GroupedInts"},
                new Object[]{
                        new String[]{"key1", "key1", "key1", "key1", "key2", "key2", "key2", "key2", "key2"},
                        new int[]{1, 1, 2, 2, 2, 3, 3, 3, 3}
                });
        table2 = new InMemoryTable(
                new String[]{"StringKeys", "GroupedInts"},
                new Object[]{
                        new String[]{"key1", "key1", "key1", "key1", "key2", "key2", "key2", "key2", "key2"},
                        new byte[]{1, 1, 2, 2, 2, 3, 3, 3, 3}
                });
        table3 = new InMemoryTable(
                new String[]{"StringKeys", "GroupedInts"},
                new Object[]{
                        new String[]{"key11", "key11", "key11", "key11", "key21", "key21", "key22"},
                        new int[]{1, 1, 2, 2, 2, 3, 3}
                });
        emptyTable = new InMemoryTable(
                new String[]{"Column1", "Column2"},
                new Object[]{
                        new String[]{},
                        new byte[]{}
                });
        brokenTable = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class[]{Table.class}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                throw new UnsupportedOperationException("This table is broken!");
            }
        });
    }

    @Before
    public void setUp() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);

        testRootFile.mkdirs();
    }

    @After
    public void tearDown() {
        try {
            if (testRootFile.exists()) {
                int tries = 0;
                boolean success = false;
                do {
                    try {
                        FileUtils.deleteRecursively(testRootFile);
                        success = true;
                    } catch (Exception e) {
                        System.gc();
                        tries++;
                    }
                } while (!success && tries < 10);
                TestCase.assertTrue(success);
            }
        } finally {
            LiveTableMonitor.DEFAULT.resetForUnitTests(true);
        }
    }

    public static enum TestEnum {
        a, b, s, d, f, e, tt, re, tr, ed, te;
    }

    private String[] toString(Enum[] enums) {
        String[] result = new String[enums.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (enums[i] != null ? enums[i].name() : null);
        }
        return result;
    }

    public static StringSet newSet(String... values) {
        return new StringSetWrapper(values);
    }

    public static String toS(Object o) {
        if (o == null) {
            return null;
        } else {
            return "" + o;
        }
    }

    @Test
    public void testWriteTable() throws IOException {
        TableManagementTools.writeTable(table1, new File(testRoot + File.separator + "Table1"), storageFormat);
        Table result = TableManagementTools.readTable(new File(testRoot + File.separator + "Table1"), table1.getDefinition());
        TableTools.show(result);
        TableTools.show(table1);
        TestTableTools.tableRangesAreEqual(table1, result, 0, 0, table1.size());
        result.close();

        QueryLibrary.importClass(TestEnum.class);
        QueryLibrary.importClass(StringSetWrapper.class);
        QueryLibrary.importStatic(this.getClass());
        Table test = TableTools.emptyTable(10).select("enumC=TestEnum.values()[i]", "enumSet=newSet(" +
                "toS(enumC_[(i + 9) % 10])," +
                "toS(enumC_[i])," +
                "toS(enumC_[(i+1)% 10]))");
        TableManagementTools.writeTable(test, new File(testRoot + File.separator + "Table2"), storageFormat);
        Table test2 = TableManagementTools.readTable(new File(testRoot + File.separator + "Table2"), test.getDefinition());
        assertEquals(10, test2.size());
        assertEquals(2, test2.getColumns().length);
        assertEquals(Arrays.asList(toString((Enum[]) test.getColumn("enumC").get(0, 10))), Arrays.asList(toString((Enum[]) test2.getColumn("enumC").get(0, 10))));
        StringSet[] objects = (StringSet[]) test.getColumn("enumSet").get(0, 10);
        StringSet[] objects1 = (StringSet[]) test2.getColumn("enumSet").get(0, 10);
        for (int i = 0; i < objects1.length; i++) {
            assertEquals(new HashSet(Arrays.asList(objects[i].values())), new HashSet(Arrays.asList(objects1[i].values())));
        }
        test2.close();


        test = TableTools.emptyTable(10).select("enumC=TestEnum.values()[i]", "enumSet=EnumSet.of((TestEnum)enumC_[(i + 9) % 10],(TestEnum)enumC_[i],(TestEnum)enumC_[(i+1)% 10])");
        TableManagementTools.writeTable(test, new File(testRoot + File.separator + "Table3"), storageFormat);
        test2 = TableManagementTools.readTable(new File(testRoot + File.separator + "Table3"), test.getDefinition());
        assertEquals(10, test2.size());
        assertEquals(2, test2.getColumns().length);
        assertEquals(Arrays.asList(test.getColumn("enumC").get(0, 10)), Arrays.asList(test2.getColumn("enumC").get(0, 10)));
        assertEquals(Arrays.asList(test.getColumn("enumSet").get(0, 10)), Arrays.asList(test2.getColumn("enumSet").get(0, 10)));
        test2.close();

        test = TableTools.newTable(TableDefinition.of(
            ColumnDefinition.ofInt("anInt"),
            ColumnDefinition.ofString("aString").withGrouping()),
            col("anInt", 1, 2, 3),
            col("aString", "ab", "ab", "bc"));
        TableManagementTools.writeTable(test, new File(testRoot + File.separator + "Table4"), storageFormat);
        test2 = TableManagementTools.readTable(new File(testRoot + File.separator + "Table4"), test.getDefinition());
        assertNotNull(test2.getColumnSource("aString").getGroupToRange());
        test2.close();
    }


    @Test
    public void testWriteTableEmpty() throws IOException {
        TableManagementTools.writeTable(emptyTable, new File(testRoot + File.separator + "Empty"), storageFormat);
        Table result = TableManagementTools.readTable(new File(testRoot + File.separator + "Empty"), emptyTable.getDefinition());
        TestTableTools.tableRangesAreEqual(emptyTable, result, 0, 0, emptyTable.size());
        result.close();
    }

    @Test
    public void testWriteTableMissingColumns() {
        // TODO (deephaven/deephaven-core/issues/321): Fix the apparent bug in the parquet table writer.
//        final Table nullTable = TableTools.emptyTable(10_000L).updateView(
//                "B    = NULL_BYTE",
//                "C    = NULL_CHAR",
//                "S    = NULL_SHORT",
//                "I    = NULL_INT",
//                "L    = NULL_LONG",
//                "F    = NULL_FLOAT",
//                "D    = NULL_DOUBLE",
//                "Bl   = (Boolean) null",
//                "Str  = (String) null",
//                "DT   = (DBDateTime) null"
//        );
//        TableManagementTools.writeTables(new Table[]{TableTools.emptyTable(10_000L)}, nullTable.getDefinition(), new File[]{new File(testRoot + File.separator + "Null")});
//        final Table result = TableManagementTools.readTable(new File(testRoot + File.separator + "Null"), nullTable.getDefinition());
//        TstUtils.assertTableEquals(nullTable, result);
//        result.close();
    }

    @Test
    public void testWriteTableExceptions() throws IOException {
        new File(testRoot + File.separator + "unexpectedFile").createNewFile();
        try {
            TableManagementTools.writeTable(table1, new File(testRoot + File.separator + "unexpectedFile" + File.separator + "Table1"), storageFormat);
            TestCase.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            //Expected
        }

        new File(testRoot + File.separator + "Table1").mkdirs();
        new File(testRoot + File.separator + "Table1" + File.separator + "extraFile").createNewFile();
        TableManagementTools.writeTable(table1, new File(testRoot + File.separator + "Table1"), storageFormat);
        TestCase.assertFalse(new File(testRoot + File.separator + "Table1" + File.separator + "extraFile").exists());

        new File(testRoot + File.separator + "Nested").mkdirs();
        try {
            TableManagementTools.writeTable(brokenTable, new File(testRoot + File.separator + "Nested" + File.separator + "Broken"), storageFormat);
            TestCase.fail("Expected exception");
        } catch (UnsupportedOperationException e) {
            //Expected exception
        }
        TestCase.assertFalse(new File(testRoot + File.separator + "Nested" + File.separator + "Broken").exists());
        TestCase.assertTrue(new File(testRoot + File.separator + "Nested").isDirectory());

        new File(testRoot + File.separator + "Nested").setReadOnly();
        try {
            TableManagementTools.writeTable(brokenTable, new File(testRoot + File.separator + "Nested" + File.separator + "Broken"), storageFormat);
            TestCase.fail("Expected exception");
        } catch (RuntimeException e) {
            //Expected exception
        }
        new File(testRoot + File.separator + "Nested").setWritable(true);
    }

    @Test
    public void testDeleteTable() throws IOException {
        if (System.getProperty("os.name").startsWith("Windows")) {
            //TODO: Remove when come up with a workaround for Windows file handling issues.
            return;
        }
        File path = new File(testRoot + File.separator + "Table1");
        TableManagementTools.writeTable(table1, path, storageFormat);
        Table result = TableManagementTools.readTable(new File(testRoot + File.separator + "Table1"), table1.getDefinition());
        TestTableTools.tableRangesAreEqual(table1, result, 0, 0, table1.size());
        result.close();
        TableManagementTools.deleteTable(path);
        TestCase.assertFalse(path.exists());
    }

    private Table getAggregatedResultTable() {
        final int size = 40;
        final String [] symbol = new String[size];
        final double [] bid = new double[size];
        final double [] bidSize = new double[size];
        for (int ii = 0; ii < size; ++ii) {
            symbol[ii] =  (ii < 8) ? "ABC" : "XYZ";
            bid[ii] =  (ii < 15) ? 98 : 99;
            bidSize[ii] =  ii;
        }
        final Table baseTable = newTable(stringCol("USym", symbol), doubleCol("Bid", bid), doubleCol("BidSize", bidSize));
        return baseTable.by("USym", "Bid").by("USym");
    }
    @Test
    public void testWriteAggregatedTable() {
        String path = testRoot + File.separator + "testWriteAggregatedTable";
        final Table table = getAggregatedResultTable();
        final TableDefinition def = table.getDefinition();
        TableManagementTools.writeTable(table, def, new File(path), TableManagementTools.StorageFormat.Parquet);
        Table readBackTable = TableManagementTools.readTable(new File(path), def);
        TableTools.show(readBackTable);
        TableTools.show(table);
        final long sz = table.size();
        TestTableTools.tableRangesAreEqual(table, readBackTable,0, 0, sz);
        readBackTable.close();
    }
}
