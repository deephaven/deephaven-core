/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.DataAccessHelpers;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout;
import io.deephaven.stringset.HashStringSet;
import io.deephaven.stringset.StringSet;
import io.deephaven.vector.*;
import junit.framework.TestCase;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.tableRangesAreEqual;
import static io.deephaven.engine.util.TableTools.*;

/**
 * Tests for {@link ParquetTools}.
 */
public class TestParquetTools {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private Table table1;
    private Table emptyTable;
    private Table brokenTable;

    private String testRoot;
    private File testRootFile;

    @Before
    public void setUp() throws IOException {
        table1 = new InMemoryTable(
                new String[] {"StringKeys", "GroupedInts"},
                new Object[] {
                        new String[] {"key1", "key1", "key1", "key1", "key2", "key2", "key2", "key2", "key2"},
                        new short[] {1, 1, 2, 2, 2, 3, 3, 3, 3}
                });
        emptyTable = new InMemoryTable(
                new String[] {"Column1", "Column2"},
                new Object[] {
                        new String[] {},
                        new byte[] {}
                });
        brokenTable = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class[] {Table.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException("This table is broken!");
                });
        testRootFile = Files.createTempDirectory(TestParquetTools.class.getName()).toFile();
        testRoot = testRootFile.toString();
    }

    @After
    public void tearDown() {
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
    }

    public enum TestEnum {
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
        return new HashStringSet(values);
    }

    public static String toS(Object o) {
        if (o == null) {
            return null;
        } else {
            return "" + o;
        }
    }

    @Test
    public void testWriteTable() {
        String path = testRoot + File.separator + "Table1.parquet";
        ParquetTools.writeTable(table1, path);
        Table result = ParquetTools.readTable(new File(path));
        TableTools.show(result);
        TableTools.show(table1);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());
        result.close();

        ExecutionContext.getContext().getQueryLibrary().importClass(TestEnum.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(HashStringSet.class);
        ExecutionContext.getContext().getQueryLibrary().importStatic(this.getClass());
        Table test = TableTools.emptyTable(10).select("enumC=TestEnum.values()[i]", "enumSet=newSet(" +
                "toS(enumC_[(i + 9) % 10])," +
                "toS(enumC_[i])," +
                "toS(enumC_[(i+1)% 10]))");
        path = testRoot + File.separator + "Table2.parquet";
        ParquetTools.writeTable(test, path);
        Table test2 = ParquetTools.readTable(path);
        Assert.assertEquals(10, test2.size());
        Assert.assertEquals(2, test2.numColumns());
        Assert.assertEquals(Arrays.asList(toString((Enum[]) DataAccessHelpers.getColumn(test, "enumC").get(0, 10))),
                Arrays.asList(toString((Enum[]) DataAccessHelpers.getColumn(test2, "enumC").get(0, 10))));
        StringSet[] objects = (StringSet[]) DataAccessHelpers.getColumn(test, "enumSet").get(0, 10);
        StringSet[] objects1 = (StringSet[]) DataAccessHelpers.getColumn(test2, "enumSet").get(0, 10);
        for (int i = 0; i < objects1.length; i++) {
            Assert.assertEquals(new HashSet<>(Arrays.asList(objects[i].values())),
                    new HashSet<>(Arrays.asList(objects1[i].values())));
        }
        test2.close();

        test = TableTools.emptyTable(10).select("enumC=TestEnum.values()[i]",
                "enumSet=EnumSet.of((TestEnum)enumC_[(i + 9) % 10],(TestEnum)enumC_[i],(TestEnum)enumC_[(i+1)% 10])");
        path = testRoot + File.separator + "Table3.parquet";
        ParquetTools.writeTable(test, path);
        test2 = ParquetTools.readTable(path);
        Assert.assertEquals(10, test2.size());
        Assert.assertEquals(2, test2.numColumns());
        Assert.assertEquals(Arrays.asList(DataAccessHelpers.getColumn(test, "enumC").get(0, 10)),
                Arrays.asList(DataAccessHelpers.getColumn(test2, "enumC").get(0, 10)));
        Assert.assertEquals(Arrays.asList(DataAccessHelpers.getColumn(test, "enumSet").get(0, 10)),
                Arrays.asList(DataAccessHelpers.getColumn(test2, "enumSet").get(0, 10)));
        test2.close();

        test = TableTools.newTable(TableDefinition.of(
                ColumnDefinition.ofInt("anInt"),
                ColumnDefinition.ofString("aString").withGrouping()),
                col("anInt", 1, 2, 3),
                col("aString", "ab", "ab", "bc"));
        path = testRoot + File.separator + "Table4.parquet";
        ParquetTools.writeTable(test, path);
        test2 = ParquetTools.readTable(new File(path));
        TestCase.assertNotNull(test2.getColumnSource("aString").getGroupToRange());
        test2.close();
    }

    @Test
    public void testWriteTableRenames() {
        final String path = testRoot + File.separator + "Table_W_Renames.parquet";
        final File pathFile = new File(path);
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addColumnNameMapping("X", "StringKeys")
                .addColumnNameMapping("Y", "GroupedInts")
                .build();
        ParquetTools.writeTable(table1, pathFile, instructions);

        final Table resultDefault = ParquetTools.readTable(pathFile);
        TableTools.show(table1);
        TableTools.show(resultDefault);
        assertTableEquals(table1.view("X=StringKeys", "Y=GroupedInts"), resultDefault);
        resultDefault.close();

        final Table resultRenamed = ParquetTools.readTable(pathFile, instructions);
        TableTools.show(table1);
        TableTools.show(resultRenamed);
        assertTableEquals(table1, resultRenamed);
        resultRenamed.close();
    }

    @Test
    public void testWriteTableEmpty() {
        final File dest = new File(testRoot + File.separator + "Empty.parquet");
        ParquetTools.writeTable(emptyTable, dest);
        Table result = ParquetTools.readTable(dest);
        tableRangesAreEqual(emptyTable, result, 0, 0, emptyTable.size());
        result.close();
    }

    @Test
    public void testWriteTableNoColumns() {
        final Table source = TableTools.emptyTable(100);
        final File dest = new File(testRoot + File.separator + "NoColumns.parquet");
        try {
            ParquetTools.writeTable(source, dest);
            TestCase.fail("Expected exception");
        } catch (TableDataException expected) {
        }
        try {
            ParquetTools.writeTables(new Table[] {source}, source.getDefinition(), new File[] {dest});
            TestCase.fail("Expected exception");
        } catch (TableDataException expected) {
        }
    }

    @Test
    public void testWriteTableMissingColumns() {
        // TODO (deephaven/deephaven-core/issues/321): Fix the apparent bug in the parquet table writer.
        final Table nullTable = TableTools.emptyTable(10_000L).updateView(
                "B    = NULL_BYTE",
                "C    = NULL_CHAR",
                "S    = NULL_SHORT",
                "I    = NULL_INT",
                "L    = NULL_LONG",
                "F    = NULL_FLOAT",
                "D    = NULL_DOUBLE",
                "Bl   = (Boolean) null",
                "Str  = (String) null",
                "DT   = (Instant) null");
        final File dest = new File(testRoot + File.separator + "Null.parquet");
        ParquetTools.writeTables(new Table[] {TableTools.emptyTable(10_000L)}, nullTable.getDefinition(),
                new File[] {dest});
        final Table result = ParquetTools.readTable(dest);
        assertTableEquals(nullTable, result);
        result.close();
    }

    @Test
    public void testWriteTableExceptions() throws IOException {
        new File(testRoot + File.separator + "unexpectedFile").createNewFile();
        try {
            ParquetTools.writeTable(table1,
                    new File(testRoot + File.separator + "unexpectedFile" + File.separator + "Table1"));
            TestCase.fail("Expected exception");
        } catch (UncheckedDeephavenException e) {
            // Expected
        }

        new File(testRoot + File.separator + "Table1").mkdirs();
        new File(testRoot + File.separator + "Table1" + File.separator + "extraFile").createNewFile();
        try {
            ParquetTools.writeTable(table1, new File(testRoot + File.separator + "Table1"));
            TestCase.fail("Expected exception");
        } catch (UncheckedDeephavenException e) {
            // Expected
        }
        new File(testRoot + File.separator + "Nested").mkdirs();
        try {
            ParquetTools.writeTable(brokenTable,
                    new File(testRoot + File.separator + "Nested" + File.separator + "Broken"));
            TestCase.fail("Expected exception");
        } catch (UnsupportedOperationException e) {
            // Expected exception
        }
        TestCase.assertFalse(new File(testRoot + File.separator + "Nested" + File.separator + "Broken").exists());
        TestCase.assertTrue(new File(testRoot + File.separator + "Nested").isDirectory());

        new File(testRoot + File.separator + "Nested").setReadOnly();
        try {
            ParquetTools.writeTable(brokenTable,
                    new File(testRoot + File.separator + "Nested" + File.separator + "Broken"));
            TestCase.fail("Expected exception");
        } catch (RuntimeException e) {
            // Expected exception
        }
        new File(testRoot + File.separator + "Nested").setWritable(true);
    }

    @Test
    public void testDeleteTable() {
        if (System.getProperty("os.name").startsWith("Windows")) {
            // TODO: Remove when come up with a workaround for Windows file handling issues.
            return;
        }
        File dest = new File(testRoot + File.separator + "Table1.parquet");
        ParquetTools.writeTable(table1, dest);
        Table result = ParquetTools.readTable(dest);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());
        result.close();
        ParquetTools.deleteTable(dest);
        TestCase.assertFalse(dest.exists());
    }

    private Table getAggregatedResultTable() {
        final int size = 40;
        final String[] symbol = new String[size];
        final double[] bid = new double[size];
        final double[] bidSize = new double[size];
        for (int ii = 0; ii < size; ++ii) {
            symbol[ii] = (ii < 8) ? "Num" : "XYZ";
            bid[ii] = (ii < 15) ? 98 : 99;
            bidSize[ii] = ii;
        }
        final Table baseTable =
                newTable(stringCol("USym", symbol), doubleCol("Bid", bid), doubleCol("BidSize", bidSize));
        return baseTable.groupBy("USym", "Bid").groupBy("USym");
    }

    @Test
    public void testWriteAggregatedTable() {
        String path = testRoot + File.separator + "testWriteAggregatedTable.parquet";
        final Table table = getAggregatedResultTable();
        final TableDefinition def = table.getDefinition();
        ParquetTools.writeTable(table, new File(path), def);
        Table readBackTable = ParquetTools.readTable(new File(path));
        TableTools.show(readBackTable);
        TableTools.show(table);
        final long sz = table.size();
        tableRangesAreEqual(table, readBackTable, 0, 0, sz);
        readBackTable.close();
    }

    @Test
    public void testPartitionedRead() {
        ParquetTools.writeTable(table1, new File(testRootFile,
                "Date=2021-07-20" + File.separator + "Num=200" + File.separator + "file1.parquet"));
        ParquetTools.writeTable(table1, new File(testRootFile,
                "Date=2021-07-20" + File.separator + "Num=100" + File.separator + "file2.parquet"));
        ParquetTools.writeTable(table1, new File(testRootFile,
                "Date=2021-07-21" + File.separator + "Num=300" + File.separator + "file3.parquet"));

        final List<ColumnDefinition<?>> allColumns = new ArrayList<>();
        allColumns.add(
                ColumnDefinition.fromGenericType("Date", String.class, null, ColumnDefinition.ColumnType.Partitioning));
        allColumns.add(
                ColumnDefinition.fromGenericType("Num", int.class, null, ColumnDefinition.ColumnType.Partitioning));
        allColumns.addAll(table1.getDefinition().getColumns());
        final TableDefinition partitionedDefinition = TableDefinition.of(allColumns);

        final Table result = ParquetTools.readPartitionedTableInferSchema(
                new ParquetKeyValuePartitionedLayout(testRootFile, 2), ParquetInstructions.EMPTY);
        TestCase.assertEquals(partitionedDefinition, result.getDefinition());
        final Table expected = TableTools.merge(
                table1.updateView("Date=`2021-07-20`", "Num=100"),
                table1.updateView("Date=`2021-07-20`", "Num=200"),
                table1.updateView("Date=`2021-07-21`", "Num=300")).moveColumnsUp("Date", "Num");
        assertTableEquals(expected, result);
    }

    @Test
    public void testBigArrays() {
        ExecutionContext.getContext().getQueryLibrary().importClass(LongVectorDirect.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(LongVector.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(LongStream.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(IntVectorDirect.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(IntVector.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(IntStream.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(TestParquetTools.class);

        final Table stuff = emptyTable(10)
                .update("Biggie = (int)(500_000/(k+1))",
                        "Doobles = (LongVector)new LongVectorDirect(LongStream.range(0, Biggie).toArray())",
                        "Woobles = TestParquetTools.generateDoubles(Biggie)",
                        "Goobles = (IntVector)new IntVectorDirect(IntStream.range(0, 2*Biggie).toArray())",
                        "Toobles = TestParquetTools.generateDoubles(2*Biggie)",
                        "Noobles = TestParquetTools.makeSillyStringArray(Biggie)");

        final File f2w = new File(testRoot, "bigArray.parquet");
        ParquetTools.writeTable(stuff, f2w);

        final Table readBack = ParquetTools.readTable(f2w);
        assertTableEquals(stuff, readBack);
    }

    @Test
    public void testColumnSwapping() {
        testWriteRead(emptyTable(10).update("X = ii * 2", "Y = ii * 2 + 1"),
                t -> t.updateView("T = X", "X = Y", "Y = T"));
    }

    @Test
    public void testColumnRedefineArrayDep() {
        testWriteRead(emptyTable(10).update("X = ii * 2", "Y = ii * 2 + 1"),
                t -> t.view("T = X_[i-1]"));
    }

    @Test
    public void testMultipleIdenticalRenames() {
        testWriteRead(emptyTable(10).update("X = `` + ii"),
                t -> t.updateView("A = X", "B = X").where("(A + B).length() > 0"));
        testWriteRead(emptyTable(10).update("X = `` + ii"),
                t -> t.updateView("A = X", "B = X").where("(A_[ii] + B_[ii]).length() > 0"));
    }

    @Test
    public void testChangedThenRenamed() {
        testWriteRead(emptyTable(10).update("X = ii", "Y = ii"),
                t -> t.updateView("Y = ii * 2", "X = Y").where("X % 2 == 0"));
        testWriteRead(emptyTable(10).update("X = ii", "Y = ii"),
                t -> t.updateView("Y = ii * 2", "X = Y").where("X_[ii] % 2 == 0"));
    }

    @Test
    public void testOverloadAsRename() {
        testWriteRead(emptyTable(10).update("X = ii"),
                t -> t.updateView("Y = X").where("(X + Y) % 2 == 0"));
        testWriteRead(emptyTable(10).update("X = ii"),
                t -> t.updateView("Y = X").where("(X_[ii] + Y_[ii]) % 2 == 0"));
    }

    @Test
    public void testMultipleRenamesWithSameOuterName() {
        testWriteRead(emptyTable(10).update("X = ii", "Y = ii", "Z = ii % 3"),
                t -> t.updateView("Y = Z", "Y = X").where("Y % 2 == 0"));
    }

    @Test
    public void e0() {
        final Table uncompressed =
                ParquetTools.readTable(TestParquetTools.class.getResource("/e0/uncompressed.parquet").getFile());

        final Table gzip = ParquetTools.readTable(TestParquetTools.class.getResource("/e0/gzip.parquet").getFile());
        assertTableEquals(uncompressed, gzip);

        final Table lz4 = ParquetTools.readTable(TestParquetTools.class.getResource("/e0/lz4.parquet").getFile());
        assertTableEquals(uncompressed, lz4);

        final Table snappy = ParquetTools.readTable(TestParquetTools.class.getResource("/e0/snappy.parquet").getFile());
        assertTableEquals(uncompressed, snappy);

        final Table zstd = ParquetTools.readTable(TestParquetTools.class.getResource("/e0/zstd.parquet").getFile());
        assertTableEquals(uncompressed, zstd);
    }

    @Test
    public void e1() {
        final Table uncompressed =
                ParquetTools.readTable(TestParquetTools.class.getResource("/e1/uncompressed.parquet").getFile());

        final Table gzip = ParquetTools.readTable(TestParquetTools.class.getResource("/e1/gzip.parquet").getFile());
        assertTableEquals(uncompressed, gzip);

        final Table lz4 = ParquetTools.readTable(TestParquetTools.class.getResource("/e1/lz4.parquet").getFile());
        assertTableEquals(uncompressed, lz4);

        final Table snappy = ParquetTools.readTable(TestParquetTools.class.getResource("/e1/snappy.parquet").getFile());
        assertTableEquals(uncompressed, snappy);

        final Table zstd = ParquetTools.readTable(TestParquetTools.class.getResource("/e1/zstd.parquet").getFile());
        assertTableEquals(uncompressed, zstd);
    }

    @Test
    public void e2() {
        final Table uncompressed =
                ParquetTools.readTable(TestParquetTools.class.getResource("/e2/uncompressed.parquet").getFile());

        final Table gzip = ParquetTools.readTable(TestParquetTools.class.getResource("/e2/gzip.parquet").getFile());
        assertTableEquals(uncompressed, gzip);

        final Table lz4 = ParquetTools.readTable(TestParquetTools.class.getResource("/e2/lz4.parquet").getFile());
        assertTableEquals(uncompressed, lz4);

        final Table snappy = ParquetTools.readTable(TestParquetTools.class.getResource("/e2/snappy.parquet").getFile());
        assertTableEquals(uncompressed, snappy);

        final Table zstd = ParquetTools.readTable(TestParquetTools.class.getResource("/e2/zstd.parquet").getFile());
        assertTableEquals(uncompressed, zstd);
    }

    private void testWriteRead(Table source, Function<Table, Table> transform) {
        final File f2w = new File(testRoot, "testWriteRead.parquet");
        ParquetTools.writeTable(source, f2w);
        final Table readBack = ParquetTools.readTable(f2w);
        assertTableEquals(transform.apply(source), transform.apply(readBack));
    }

    public static DoubleVector generateDoubles(int howMany) {
        final double[] yarr = new double[howMany];
        for (int ii = 0; ii < howMany; ii++) {
            yarr[ii] = ii;
        }
        return new DoubleVectorDirect(yarr);
    }

    public static FloatVector generateFloats(int howMany) {
        final float[] yarr = new float[howMany];
        for (int ii = 0; ii < howMany; ii++) {
            yarr[ii] = ii;
        }
        return new FloatVectorDirect(yarr);
    }

    public static ObjectVector<String> makeSillyStringArray(int howMany) {
        final String[] fireTruck = new String[howMany];
        for (int ii = 0; ii < howMany; ii++) {
            fireTruck[ii] = String.format("%04d", ii);
        }
        return new ObjectVectorDirect<>(fireTruck);
    }
}
