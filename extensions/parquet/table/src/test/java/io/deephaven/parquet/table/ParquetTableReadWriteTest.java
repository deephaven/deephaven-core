/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.Selectable;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.stringset.ArrayStringSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.stringset.StringSet;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class ParquetTableReadWriteTest {

    private static final String ROOT_FILENAME = ParquetTableReadWriteTest.class.getName() + "_root";
    public static final int LARGE_TABLE_SIZE = 2_000_000;

    private static File rootFile;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Before
    public void setUp() {
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        FileUtils.deleteRecursively(rootFile);
    }

    private static Table getTableFlat(int size, boolean includeSerializable, boolean includeBigDecimal) {
        ExecutionContext.getContext().getQueryLibrary().importClass(SomeSillyTest.class);
        ArrayList<String> columns =
                new ArrayList<>(Arrays.asList("someStringColumn = i % 10 == 0?null:(`` + (i % 101))",
                        "nonNullString = `` + (i % 60)",
                        "nullString = (String) null",
                        "nonNullPolyString = `` + (i % 600)",
                        "someIntColumn = i",
                        "someLongColumn = ii",
                        "someDoubleColumn = i*1.1",
                        "someFloatColumn = (float)(i*1.1)",
                        "someBoolColumn = i % 3 == 0?true:i%3 == 1?false:null",
                        "someShortColumn = (short)i",
                        "someByteColumn = (byte)i",
                        "someCharColumn = (char)i",
                        "someTime = DateTimeUtils.now() + i",
                        "someKey = `` + (int)(i /100)",
                        "nullKey = i < -1?`123`:null",
                        "nullIntColumn = (int)null",
                        "nullLongColumn = (long)null",
                        "nullDoubleColumn = (double)null",
                        "nullFloatColumn = (float)null",
                        "nullBoolColumn = (Boolean)null",
                        "nullShortColumn = (short)null",
                        "nullByteColumn = (byte)null",
                        "nullCharColumn = (char)null",
                        "nullTime = (Instant)null",
                        "nullString = (String)null"));
        if (includeBigDecimal) {
            columns.add("bdColumn = java.math.BigDecimal.valueOf(ii).stripTrailingZeros()");
            columns.add("biColumn = java.math.BigInteger.valueOf(ii)");
        }
        if (includeSerializable) {
            columns.add("someSerializable = new SomeSillyTest(i)");
        }
        return TableTools.emptyTable(size).select(
                Selectable.from(columns));
    }

    private static Table getOneColumnTableFlat(int size) {
        ExecutionContext.getContext().getQueryLibrary().importClass(SomeSillyTest.class);
        return TableTools.emptyTable(size).select(
                // "someBoolColumn = i % 3 == 0?true:i%3 == 1?false:null"
                "someIntColumn = i % 3 == 0 ? null:i");
    }

    private static Table getGroupedOneColumnTable(int size) {
        Table t = getOneColumnTableFlat(size);
        ExecutionContext.getContext().getQueryLibrary().importClass(ArrayStringSet.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(StringSet.class);
        Table result = t.updateView("groupKey = i % 100 + (int)(i/10)").groupBy("groupKey");
        result = result.select(result.getDefinition().getColumnNames().stream()
                .map(name -> name.equals("groupKey") ? name
                        : (name + " = i % 5 == 0 ? null:(i%3 == 0?" + name + ".subVector(0,0):" + name
                                + ")"))
                .toArray(String[]::new));
        return result;
    }

    public static class SomeSillyTest implements Serializable {
        private static final long serialVersionUID = 6668727512367188538L;
        final int value;

        public SomeSillyTest(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "SomeSillyTest{" +
                    "value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SomeSillyTest)) {
                return false;
            }
            return value == ((SomeSillyTest) obj).value;
        }
    }

    private static Table getEmptyArray(int size) {
        ExecutionContext.getContext().getQueryLibrary().importClass(SomeSillyTest.class);
        return TableTools.emptyTable(size).select(
                "someEmptyString = new String[0]",
                "someEmptyInt = new int[0]",
                "someEmptyBool = new Boolean[0]",
                "someEmptyObject = new SomeSillyTest[0]");
    }

    private static Table getGroupedTable(int size, boolean includeSerializable) {
        Table t = getTableFlat(size, includeSerializable, true);
        ExecutionContext.getContext().getQueryLibrary().importClass(ArrayStringSet.class);
        ExecutionContext.getContext().getQueryLibrary().importClass(StringSet.class);
        Table result = t.updateView("groupKey = i % 100 + (int)(i/10)").groupBy("groupKey");
        result = result.select(result.getDefinition().getColumnNames().stream()
                .map(name -> name.equals("groupKey") ? name
                        : (name + " = i % 5 == 0 ? null:(i%3 == 0?" + name + ".subVector(0,0):" + name
                                + ")"))
                .toArray(String[]::new));
        result = result.update(
                "someStringSet = (StringSet)new ArrayStringSet( ((Object)nonNullString) == null?new String[0]:(String[])nonNullString.toArray())");
        result = result.update(
                "largeStringSet = (StringSet)new ArrayStringSet(((Object)nonNullPolyString) == null?new String[0]:(String[])nonNullPolyString.toArray())");
        result = result.update(
                "nullStringSet = (StringSet)null");
        result = result.update(
                "someStringColumn = (String[])(((Object)someStringColumn) == null?null:someStringColumn.toArray())",
                "nonNullString = (String[])(((Object)nonNullString) == null?null:nonNullString.toArray())",
                "nonNullPolyString = (String[])(((Object)nonNullPolyString) == null?null:nonNullPolyString.toArray())",
                "someBoolColumn = (Boolean[])(((Object)someBoolColumn) == null?null:someBoolColumn.toArray())",
                "someTime = (Instant[])(((Object)someTime) == null?null:someTime.toArray())");
        return result;
    }

    private void flatTable(String tableName, int size, boolean includeSerializable) {
        final Table tableToSave = getTableFlat(size, includeSerializable, true);
        final File dest = new File(rootFile, "ParquetTest_" + tableName + "_test.parquet");
        ParquetTools.writeTable(tableToSave, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(maybeFixBigDecimal(tableToSave), fromDisk);
    }

    private void groupedTable(String tableName, int size, boolean includeSerializable) {
        final Table tableToSave = getGroupedTable(size, includeSerializable);
        final File dest = new File(rootFile, "ParquetTest_" + tableName + "_test.parquet");
        ParquetTools.writeTable(tableToSave, dest, tableToSave.getDefinition());
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(tableToSave, fromDisk);
    }

    private void groupedOneColumnTable(String tableName, int size) {
        final Table tableToSave = getGroupedOneColumnTable(size);
        TableTools.show(tableToSave, 50);
        final File dest = new File(rootFile, "ParquetTest_" + tableName + "_test.parquet");
        ParquetTools.writeTable(tableToSave, dest, tableToSave.getDefinition());
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(tableToSave, fromDisk);
    }

    private void testEmptyArrayStore(String tableName, int size) {
        final Table tableToSave = getEmptyArray(size);
        final File dest = new File(rootFile, "ParquetTest_" + tableName + "_test.parquet");
        ParquetTools.writeTable(tableToSave, dest, tableToSave.getDefinition());
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(tableToSave, fromDisk);
    }

    @Test
    public void emptyTrivialTable() {
        final Table t = TableTools.emptyTable(0).select("A = i");
        assertEquals(int.class, t.getDefinition().getColumn("A").getDataType());
        final File dest = new File(rootFile, "ParquetTest_emptyTrivialTable.parquet");
        ParquetTools.writeTable(t, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(t, fromDisk);
        assertEquals(t.getDefinition(), fromDisk.getDefinition());
    }

    @Test
    public void flatParquetFormat() {
        flatTable("emptyFlatParquet", 0, true);
        flatTable("smallFlatParquet", 20, true);
        flatTable("largeFlatParquet", LARGE_TABLE_SIZE, false);
    }

    @Test
    public void vectorParquetFormat() {
        testEmptyArrayStore("smallEmpty", 20);
        groupedOneColumnTable("smallAggOneColumn", 20);
        groupedTable("smallAggParquet", 20, true);
        testEmptyArrayStore("largeEmpty", LARGE_TABLE_SIZE);
        groupedOneColumnTable("largeAggOneColumn", LARGE_TABLE_SIZE);
        groupedTable("largeAggParquet", LARGE_TABLE_SIZE, false);
    }

    @Test
    public void groupingByLongKey() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("someInt"),
                ColumnDefinition.ofLong("someLong").withGrouping());
        final Table testTable =
                ((QueryTable) TableTools.emptyTable(10).select("someInt = i", "someLong  = ii % 3")
                        .groupBy("someLong").ungroup("someInt")).withDefinitionUnsafe(definition);
        final File dest = new File(rootFile, "ParquetTest_groupByLong_test.parquet");
        ParquetTools.writeTable(testTable, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(fromDisk, testTable);
        TestCase.assertNotNull(fromDisk.getColumnSource("someLong").getGroupToRange());
    }

    @Test
    public void groupingByStringKey() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("someInt"),
                ColumnDefinition.ofString("someString").withGrouping());
        final Table testTable =
                ((QueryTable) TableTools.emptyTable(10).select("someInt = i", "someString  = `foo`")
                        .where("i % 2 == 0").groupBy("someString").ungroup("someInt"))
                        .withDefinitionUnsafe(definition);
        final File dest = new File(rootFile, "ParquetTest_groupByString_test.parquet");
        ParquetTools.writeTable(testTable, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(fromDisk, testTable);
        TestCase.assertNotNull(fromDisk.getColumnSource("someString").getGroupToRange());
    }

    @Test
    public void groupingByBigInt() {
        ExecutionContext.getContext().getQueryLibrary().importClass(BigInteger.class);
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("someInt"),
                ColumnDefinition.fromGenericType("someBigInt", BigInteger.class).withGrouping());
        final Table testTable = ((QueryTable) TableTools.emptyTable(10)
                .select("someInt = i", "someBigInt  =  BigInteger.valueOf(i % 3)").where("i % 2 == 0")
                .groupBy("someBigInt").ungroup("someInt")).withDefinitionUnsafe(definition);
        final File dest = new File(rootFile, "ParquetTest_groupByBigInt_test.parquet");
        ParquetTools.writeTable(testTable, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(fromDisk, testTable);
        TestCase.assertNotNull(fromDisk.getColumnSource("someBigInt").getGroupToRange());
    }

    private void compressionCodecTestHelper(final String codec) {
        final String currentCodec = ParquetInstructions.getDefaultCompressionCodecName();
        try {
            ParquetInstructions.setDefaultCompressionCodecName(codec);
            String path = rootFile + File.separator + "Table1.parquet";
            final Table table1 = getTableFlat(10000, false, true);
            ParquetTools.writeTable(table1, path);
            assertTrue(new File(path).length() > 0);
            final Table table2 = ParquetTools.readTable(path);
            TstUtils.assertTableEquals(maybeFixBigDecimal(table1), table2);
        } finally {
            ParquetInstructions.setDefaultCompressionCodecName(currentCodec);
        }
    }

    @Test
    public void testParquetLzoCompressionCodec() {
        compressionCodecTestHelper("LZO");
    }

    @Test
    public void testParquetLz4CompressionCodec() {
        compressionCodecTestHelper("LZ4");
    }

    @Ignore("See BrotliParquetReadWriteTest instead")
    @Test
    public void testParquetBrotliCompressionCodec() {
        compressionCodecTestHelper("BROTLI");
    }

    @Test
    public void testParquetZstdCompressionCodec() {
        compressionCodecTestHelper("ZSTD");
    }

    @Test
    public void testParquetGzipCompressionCodec() {
        compressionCodecTestHelper("GZIP");
    }

    @Test
    public void testParquetSnappyCompressionCodec() {
        // while Snappy is covered by other tests, this is a very fast test to quickly confirm that it works in the same
        // way as the other similar codec tests.
        compressionCodecTestHelper("SNAPPY");
    }

    @Test
    public void testBigDecimalPrecisionScale() {
        // https://github.com/deephaven/deephaven-core/issues/3650
        final BigDecimal myBigDecimal = new BigDecimal(".0005");
        assertEquals(1, myBigDecimal.precision());
        assertEquals(4, myBigDecimal.scale());
        final Table table = TableTools
                .newTable(new ColumnHolder<>("MyBigDecimal", BigDecimal.class, null, false, myBigDecimal));
        final File dest = new File(rootFile, "ParquetTest_testBigDecimalPrecisionScale.parquet");
        ParquetTools.writeTable(table, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        try (final CloseableIterator<BigDecimal> it = fromDisk.objectColumnIterator("MyBigDecimal")) {
            assertTrue(it.hasNext());
            final BigDecimal item = it.next();
            assertFalse(it.hasNext());
            assertEquals(myBigDecimal, item);
        }
    }

    @Test
    public void testNullVectorColumns() {
        final Table nullTable = getTableFlat(10, true, false);

        final File dest = new File(rootFile + File.separator + "nullTable.parquet");
        ParquetTools.writeTable(nullTable, dest);
        Table fromDisk = ParquetTools.readTable(dest);
        assertTableEquals(nullTable, fromDisk);

        // Take a groupBy to create vector columns containing null values
        final Table nullVectorTable = nullTable.groupBy();
        ParquetTools.writeTable(nullVectorTable, dest);
        fromDisk = ParquetTools.readTable(dest);
        assertTableEquals(nullVectorTable, fromDisk);
    }

    @Test
    public void testArrayColumns() {
        ArrayList<String> columns =
                new ArrayList<>(Arrays.asList(
                        "someStringArrayColumn = new String[] {i % 10 == 0?null:(`` + (i % 101))}",
                        "someIntArrayColumn = new int[] {i}",
                        "someLongArrayColumn = new long[] {ii}",
                        "someDoubleArrayColumn = new double[] {i*1.1}",
                        "someFloatArrayColumn = new float[] {(float)(i*1.1)}",
                        "someBoolArrayColumn = new Boolean[] {i % 3 == 0?true:i%3 == 1?false:null}",
                        "someShorArrayColumn = new short[] {(short)i}",
                        "someByteArrayColumn = new byte[] {(byte)i}",
                        "someCharArrayColumn = new char[] {(char)i}",
                        "someTimeArrayColumn = new Instant[] {(Instant)DateTimeUtils.now() + i}",
                        "nullStringArrayColumn = new String[] {(String)null}",
                        "nullIntArrayColumn = new int[] {(int)null}",
                        "nullLongArrayColumn = new long[] {(long)null}",
                        "nullDoubleArrayColumn = new double[] {(double)null}",
                        "nullFloatArrayColumn = new float[] {(float)null}",
                        "nullBoolArrayColumn = new Boolean[] {(Boolean)null}",
                        "nullShorArrayColumn = new short[] {(short)null}",
                        "nullByteArrayColumn = new byte[] {(byte)null}",
                        "nullCharArrayColumn = new char[] {(char)null}",
                        "nullTimeArrayColumn = new Instant[] {(Instant)null}"));

        final Table arrayTable = TableTools.emptyTable(20).select(
                Selectable.from(columns));
        final File dest = new File(rootFile + File.separator + "arrayTable.parquet");
        ParquetTools.writeTable(arrayTable, dest);
        Table fromDisk = ParquetTools.readTable(dest);
        assertTableEquals(arrayTable, fromDisk);
    }

    /**
     * Encoding bigDecimal is tricky -- the writer will try to pick the precision and scale automatically. Because of
     * that tableTools.assertTableEquals will fail because, even though the numbers are identical, the representation
     * may not be so we have to coerce the expected values to the same precision and scale value. We know how it should
     * be doing it, so we can use the same pattern of encoding/decoding with the codec.
     *
     * @param toFix
     * @return
     */
    private Table maybeFixBigDecimal(Table toFix) {
        final BigDecimalUtils.PrecisionAndScale pas = BigDecimalUtils.computePrecisionAndScale(toFix, "bdColumn");
        final BigDecimalParquetBytesCodec codec = new BigDecimalParquetBytesCodec(pas.precision, pas.scale, -1);

        ExecutionContext.getContext()
                .getQueryScope()
                .putParam("__codec", codec);
        return toFix
                .updateView("bdColE = __codec.encode(bdColumn)", "bdColumn=__codec.decode(bdColE, 0, bdColE.length)")
                .dropColumns("bdColE");
    }

    // Following is used for testing both writing APIs for parquet tables
    private interface TestParquetTableWriter {
        void writeTable(final Table table, final File destFile);
    }

    TestParquetTableWriter singleWriter = (table, destFile) -> ParquetTools.writeTable(table, destFile);
    TestParquetTableWriter multiWriter = (table, destFile) -> ParquetTools.writeTables(new Table[] {table},
            table.getDefinition(), new File[] {destFile});

    /**
     * These are tests for writing a table to a parquet file and making sure there are no unnecessary files left in the
     * directory after we finish writing.
     */
    @Test
    public void basicWriteTests() {
        basicWriteTestsImpl(singleWriter);
        basicWriteTestsImpl(multiWriter);
    }

    private void basicWriteTestsImpl(TestParquetTableWriter writer) {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();
        assertTrue(parentDir.exists() && parentDir.isDirectory() && parentDir.list().length == 0);

        // There should be just one file in the directory on a successful write and no temporary files
        final Table tableToSave = TableTools.emptyTable(5).update("A=(int)i", "B=(long)i", "C=(double)i");
        final String filename = "basicWriteTests.parquet";
        final File destFile = new File(parentDir, filename);
        writer.writeTable(tableToSave, destFile);
        List filesInDir = Arrays.asList(parentDir.list());
        assertTrue(filesInDir.size() == 1 && filesInDir.contains(filename));
        Table fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, tableToSave);

        // This write should fail
        final Table badTable = TableTools.emptyTable(5)
                .updateView("InputString = ii % 2 == 0 ? Long.toString(ii) : null", "A=InputString.charAt(0)");
        try {
            writer.writeTable(badTable, destFile);
            TestCase.fail("Exception expected for invalid formula");
        } catch (UncheckedDeephavenException e) {
            assertTrue(e.getCause() instanceof FormulaEvaluationException);
        }

        // Make sure that original file is preserved and no temporary files
        filesInDir = Arrays.asList(parentDir.list());
        assertTrue(filesInDir.size() == 1 && filesInDir.contains(filename));
        fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, tableToSave);

        // Write a new table successfully at the same path
        final Table newTableToSave = TableTools.emptyTable(5).update("A=(int)i");
        writer.writeTable(newTableToSave, destFile);
        filesInDir = Arrays.asList(parentDir.list());
        assertTrue(filesInDir.size() == 1 && filesInDir.contains(filename));
        fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, newTableToSave);
        FileUtils.deleteRecursively(parentDir);
    }

    /**
     * These are tests for writing multiple parquet tables in a single call.
     */
    @Test
    public void writeMultiTableBasicTest() {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();

        // Write two tables to parquet file and read them back
        final Table firstTable = TableTools.emptyTable(5)
                .updateView("InputString = Long.toString(ii)", "A=InputString.charAt(0)");
        final String firstFilename = "firstTable.parquet";
        final File firstDestFile = new File(parentDir, firstFilename);

        final Table secondTable = TableTools.emptyTable(5)
                .updateView("InputString = Long.toString(ii*5)", "A=InputString.charAt(0)");
        final String secondFilename = "secondTable.parquet";
        final File secondDestFile = new File(parentDir, secondFilename);

        Table[] tablesToSave = new Table[] {firstTable, secondTable};
        File[] destFiles = new File[] {firstDestFile, secondDestFile};

        ParquetTools.writeTables(tablesToSave, firstTable.getDefinition(), destFiles);

        List filesInDir = Arrays.asList(parentDir.list());
        assertTrue(filesInDir.size() == 2 && filesInDir.contains(firstFilename) && filesInDir.contains(secondFilename));

        TstUtils.assertTableEquals(ParquetTools.readTable(firstDestFile), firstTable);
        TstUtils.assertTableEquals(ParquetTools.readTable(secondDestFile), secondTable);
    }

    /**
     * These are tests for writing multiple parquet tables such that there is an exception in the second write.
     */
    @Test
    public void writeMultiTableExceptionTest() {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();

        // Write two tables to parquet file and read them back
        final Table firstTable = TableTools.emptyTable(5)
                .updateView("InputString = Long.toString(ii)", "A=InputString.charAt(0)");
        final File firstDestFile = new File(parentDir, "firstTable.parquet");

        final Table secondTable = TableTools.emptyTable(5)
                .updateView("InputString = ii % 2 == 0 ? Long.toString(ii*5) : null", "A=InputString.charAt(0)");
        final File secondDestFile = new File(parentDir, "secondTable.parquet");

        Table[] tablesToSave = new Table[] {firstTable, secondTable};
        File[] destFiles = new File[] {firstDestFile, secondDestFile};

        // This write should fail
        try {
            ParquetTools.writeTables(tablesToSave, firstTable.getDefinition(), destFiles);
            TestCase.fail("Exception expected for invalid formula");
        } catch (UncheckedDeephavenException e) {
            assertTrue(e.getCause() instanceof FormulaEvaluationException);
        }

        // All files should be deleted even though first table would be written successfully
        assertTrue(parentDir.list().length == 0);
    }


    /**
     * These are tests for writing to a table with grouping columns to a parquet file and making sure there are no
     * unnecessary files left in the directory after we finish writing.
     */
    @Test
    public void groupingColumnsBasicWriteTests() {
        groupingColumnsBasicWriteTestsImpl(singleWriter);
        groupingColumnsBasicWriteTestsImpl(multiWriter);
    }

    public void groupingColumnsBasicWriteTestsImpl(TestParquetTableWriter writer) {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();
        assertTrue(parentDir.exists() && parentDir.isDirectory() && parentDir.list().length == 0);

        Integer data[] = new Integer[500 * 4];
        for (int i = 0; i < data.length; i++) {
            data[i] = i / 4;
        }
        final TableDefinition tableDefinition = TableDefinition.of(ColumnDefinition.ofInt("vvv").withGrouping());
        final Table tableToSave = TableTools.newTable(tableDefinition, TableTools.col("vvv", data));

        // For a completed write, there should be two parquet files in the directory, the table data and the grouping
        // data
        final String destFilename = "groupingColumnsWriteTests.parquet";
        final File destFile = new File(parentDir, destFilename);
        writer.writeTable(tableToSave, destFile);
        List filesInDir = Arrays.asList(parentDir.list());
        String vvvGroupingFilename = ParquetTools.defaultGroupingFileName(destFilename).apply("vvv");
        assertTrue(filesInDir.size() == 2 && filesInDir.contains(destFilename)
                && filesInDir.contains(vvvGroupingFilename));
        Table fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, tableToSave);

        // Verify that the key-value metadata in the file has the correct name
        ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(destFile, 0, null);
        String metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(vvvGroupingFilename));

        // Write another table but this write should fail
        final TableDefinition badTableDefinition = TableDefinition.of(ColumnDefinition.ofInt("www").withGrouping());
        final Table badTable = TableTools.newTable(badTableDefinition, TableTools.col("www", data))
                .updateView("InputString = ii % 2 == 0 ? Long.toString(ii) : null", "A=InputString.charAt(0)");
        try {
            writer.writeTable(badTable, destFile);
            TestCase.fail("Exception expected for invalid formula");
        } catch (UncheckedDeephavenException e) {
            assertTrue(e.getCause() instanceof FormulaEvaluationException);
        }

        // Make sure that original file is preserved and no temporary files
        filesInDir = Arrays.asList(parentDir.list());
        assertTrue(filesInDir.size() == 2 && filesInDir.contains(destFilename)
                && filesInDir.contains(vvvGroupingFilename));
        fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, tableToSave);
        FileUtils.deleteRecursively(parentDir);
    }


    /**
     * These are tests for writing multiple parquet tables with grouping columns.
     */
    @Test
    public void writeMultiTableGroupingColumnTest() {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();

        Integer data[] = new Integer[500 * 4];
        for (int i = 0; i < data.length; i++) {
            data[i] = i / 4;
        }
        final TableDefinition tableDefinition = TableDefinition.of(ColumnDefinition.ofInt("vvv").withGrouping());
        final Table firstTable = TableTools.newTable(tableDefinition, TableTools.col("vvv", data));
        final String firstFilename = "firstTable.parquet";
        final File firstDestFile = new File(parentDir, firstFilename);

        final Table secondTable = TableTools.newTable(tableDefinition, TableTools.col("vvv", data));
        final String secondFilename = "secondTable.parquet";
        final File secondDestFile = new File(parentDir, secondFilename);

        Table[] tablesToSave = new Table[] {firstTable, secondTable};
        File[] destFiles = new File[] {firstDestFile, secondDestFile};

        ParquetTools.writeTables(tablesToSave, firstTable.getDefinition(), destFiles);

        List<String> filesInDir = Arrays.asList(parentDir.list());
        String firstGroupingFilename = ParquetTools.defaultGroupingFileName(firstFilename).apply("vvv");
        String secondGroupingFilename = ParquetTools.defaultGroupingFileName(secondFilename).apply("vvv");
        assertTrue(filesInDir.size() == 4 && filesInDir.contains(firstFilename)
                && filesInDir.contains(secondFilename) && filesInDir.contains(firstGroupingFilename)
                && filesInDir.contains(secondGroupingFilename));

        // Verify that the key-value metadata in the file has the correct name
        ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(firstDestFile, 0, null);
        String metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(firstGroupingFilename));
        tableLocationKey = new ParquetTableLocationKey(secondDestFile, 0, null);
        metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(secondGroupingFilename));

        // Read back the files and verify contents match
        TstUtils.assertTableEquals(ParquetTools.readTable(firstDestFile), firstTable);
        TstUtils.assertTableEquals(ParquetTools.readTable(secondDestFile), secondTable);
    }

    @Test
    public void groupingColumnsOverwritingTests() {
        groupingColumnsOverwritingTestsImpl(singleWriter);
        groupingColumnsOverwritingTestsImpl(multiWriter);
    }

    public void groupingColumnsOverwritingTestsImpl(TestParquetTableWriter writer) {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();
        assertTrue(parentDir.exists() && parentDir.isDirectory() && parentDir.list().length == 0);

        Integer data[] = new Integer[500 * 4];
        for (int i = 0; i < data.length; i++) {
            data[i] = i / 4;
        }
        final TableDefinition tableDefinition = TableDefinition.of(ColumnDefinition.ofInt("vvv").withGrouping());
        final Table tableToSave = TableTools.newTable(tableDefinition, TableTools.col("vvv", data));

        final String destFilename = "groupingColumnsWriteTests.parquet";
        final File destFile = new File(parentDir, destFilename);
        writer.writeTable(tableToSave, destFile);
        String vvvGroupingFilename = ParquetTools.defaultGroupingFileName(destFilename).apply("vvv");

        // Write a new table successfully at the same position with different grouping columns
        final TableDefinition anotherTableDefinition = TableDefinition.of(ColumnDefinition.ofInt("xxx").withGrouping());
        Table anotherTableToSave = TableTools.newTable(anotherTableDefinition, TableTools.col("xxx", data));
        writer.writeTable(anotherTableToSave, destFile);
        List filesInDir = Arrays.asList(parentDir.list());
        final String xxxGroupingFilename = ParquetTools.defaultGroupingFileName(destFilename).apply("xxx");

        // The directory now should contain the updated table, its grouping file for column xxx, and old grouping file
        // for column vvv
        assertTrue(filesInDir.size() == 3 && filesInDir.contains(destFilename)
                && filesInDir.contains(vvvGroupingFilename)
                && filesInDir.contains(xxxGroupingFilename));
        Table fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, anotherTableToSave);

        ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(destFile, 0, null);
        String metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(xxxGroupingFilename) && !metadataString.contains(vvvGroupingFilename));

        // Overwrite the table
        writer.writeTable(anotherTableToSave, destFile);

        // The directory should still contain the updated table, its grouping file for column xxx, and old grouping file
        // for column vvv
        filesInDir = Arrays.asList(parentDir.list());
        final File xxxGroupingFile = new File(parentDir, xxxGroupingFilename);
        final File backupXXXGroupingFile = ParquetTools.getBackupFile(xxxGroupingFile);
        final String backupXXXGroupingFileName = backupXXXGroupingFile.getName();
        assertTrue(filesInDir.size() == 3 && filesInDir.contains(destFilename)
                && filesInDir.contains(vvvGroupingFilename)
                && filesInDir.contains(xxxGroupingFilename));

        tableLocationKey = new ParquetTableLocationKey(destFile, 0, null);
        metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(xxxGroupingFilename) && !metadataString.contains(vvvGroupingFilename)
                && !metadataString.contains(backupXXXGroupingFileName));
        FileUtils.deleteRecursively(parentDir);
    }

    @Test
    public void readChangedUnderlyingFileTests() {
        readChangedUnderlyingFileTestsImpl(singleWriter);
        readChangedUnderlyingFileTestsImpl(multiWriter);
    }

    public void readChangedUnderlyingFileTestsImpl(TestParquetTableWriter writer) {
        // Write a table to parquet file and read it back
        final Table tableToSave = TableTools.emptyTable(5).update("A=(int)i", "B=(long)i", "C=(double)i");
        final String filename = "readChangedUnderlyingFileTests.parquet";
        final File destFile = new File(rootFile, filename);
        writer.writeTable(tableToSave, destFile);
        Table fromDisk = ParquetTools.readTable(destFile);
        // At this point, fromDisk is not fully materialized in the memory and would be read from the file on demand

        // Change the underlying file
        final Table stringTable = TableTools.emptyTable(5).update("InputString = Long.toString(ii)");
        writer.writeTable(stringTable, destFile);
        Table stringFromDisk = ParquetTools.readTable(destFile).select();
        TstUtils.assertTableEquals(stringTable, stringFromDisk);

        // Close all the file handles so that next time when fromDisk is accessed, we need to reopen the file handle
        TrackedFileHandleFactory.getInstance().closeAll();

        // Read back fromDisk and compare it with original table. Since the underlying file has changed,
        // assertTableEquals will try to read the file and would crash
        try {
            TstUtils.assertTableEquals(tableToSave, fromDisk);
            TestCase.fail();
        } catch (Exception ignored) {
        }
    }

    @Test
    public void readModifyWriteTests() {
        readModifyWriteTestsImpl(singleWriter);
        readModifyWriteTestsImpl(multiWriter);
    }

    public void readModifyWriteTestsImpl(TestParquetTableWriter writer) {
        // Write a table to parquet file and read it back
        final Table tableToSave = TableTools.emptyTable(5).update("A=(int)i", "B=(long)i", "C=(double)i");
        final String filename = "readModifyWriteTests.parquet";
        final File destFile = new File(rootFile, filename);
        writer.writeTable(tableToSave, destFile);
        Table fromDisk = ParquetTools.readTable(destFile);
        // At this point, fromDisk is not fully materialized in the memory and would be read from the file on demand

        // Create a view table on fromDisk which should fail on writing, and try to write at the same location
        // Since we are doing a view() operation and adding a new column and overwriting an existing column, the table
        // won't be materialized in memory or cache.
        final Table badTable =
                fromDisk.view("InputString = ii % 2 == 0 ? Long.toString(ii) : null", "A=InputString.charAt(0)");
        try {
            writer.writeTable(badTable, destFile);
            TestCase.fail();
        } catch (UncheckedDeephavenException e) {
            assertTrue(e.getCause() instanceof FormulaEvaluationException);
        }

        // Close all old file handles so that we read the file path fresh instead of using any old handles
        TrackedFileHandleFactory.getInstance().closeAll();

        // Read back fromDisk and compare it with original table. If the underlying file has not been corrupted or
        // swapped out, then we would not be able to read from the file
        TstUtils.assertTableEquals(tableToSave, fromDisk);
    }
}
