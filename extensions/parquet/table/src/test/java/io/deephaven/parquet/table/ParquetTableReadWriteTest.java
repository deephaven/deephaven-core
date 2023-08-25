/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.Selectable;
import io.deephaven.base.FileUtils;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.function.FloatConsumer;
import io.deephaven.engine.primitive.function.ShortConsumer;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.table.iterators.*;
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
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.vector.*;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.*;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.util.QueryConstants.*;
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

    @Test
    public void readWriteStatisticsTest() {
        // Test simple structured table.
        final ColumnDefinition<byte[]> columnDefinition =
                ColumnDefinition.fromGenericType("VariableWidthByteArrayColumn", byte[].class, byte.class);
        final TableDefinition tableDefinition = TableDefinition.of(columnDefinition);
        final byte[] byteArray = new byte[] {1, 2, 3, 4, NULL_BYTE, 6, 7, 8, 9, NULL_BYTE, 11, 12, 13};
        final Table simpleTable = TableTools.newTable(tableDefinition,
                TableTools.col("VariableWidthByteArrayColumn", null, byteArray, byteArray, byteArray, byteArray,
                        byteArray));
        final File simpleTableDest = new File(rootFile, "ParquetTest_simple_statistics_test.parquet");
        ParquetTools.writeTable(simpleTable, simpleTableDest);

        final Table simpleFromDisk = ParquetTools.readTable(simpleTableDest);
        TstUtils.assertTableEquals(simpleTable, simpleFromDisk);

        assertTableStatistics(simpleTable, simpleTableDest);

        // Test flat columns.
        final Table flatTableToSave = getTableFlat(10_000, true, true);
        final File flatTableDest = new File(rootFile, "ParquetTest_flat_statistics_test.parquet");
        ParquetTools.writeTable(flatTableToSave, flatTableDest);

        final Table flatFromDisk = ParquetTools.readTable(flatTableDest);
        TstUtils.assertTableEquals(maybeFixBigDecimal(flatTableToSave), flatFromDisk);

        assertTableStatistics(flatTableToSave, flatTableDest);

        // Test nested columns.
        final Table groupedTableToSave = getGroupedTable(10_000, true);
        final File groupedTableDest = new File(rootFile, "ParquetTest_grouped_statistics_test.parquet");
        ParquetTools.writeTable(groupedTableToSave, groupedTableDest, groupedTableToSave.getDefinition());

        final Table groupedFromDisk = ParquetTools.readTable(groupedTableDest);
        TstUtils.assertTableEquals(groupedTableToSave, groupedFromDisk);

        assertTableStatistics(groupedTableToSave, groupedTableDest);
    }

    /**
     * Test our manual verification techniques against a file generated by pyarrow. Here is the code to produce the file
     * when/if this file needs to be re-generated or changed.
     *
     * <pre>
     * ###############################################################################
     * import pyarrow.parquet
     *
     * pa_table = pyarrow.table({
     *     'int': [0, None, 100, -100],
     *     'float': [0.0, None, 100.0, -100.0],
     *     'string': ["aaa", None, "111", "ZZZ"],
     *     'intList': [
     *         [0, None, 2],
     *         None,
     *         [3, 4, 6, 7, 8, 9, 10, 100, -100],
     *         [5]
     *     ],
     *     'floatList': [
     *         [0.0, None, 2.0],
     *         None,
     *         [3.0, 4.0, 6.0, 7.0, 8.0, 9.0, 10.0, 100.0, -100.0],
     *         [5.0]
     *     ],
     *     'stringList': [
     *         ["aaa", None, None],
     *         None,
     *         ["111", "zzz", "ZZZ", "AAA"],
     *         ["ccc"]
     *     ]})
     * pyarrow.parquet.write_table(pa_table, './extensions/parquet/table/src/test/resources/e0/pyarrow_stats.parquet')
     * ###############################################################################
     * </pre>
     */
    @Test
    public void verifyPyArrowStatistics() {
        final String path = ParquetTableReadWriteTest.class.getResource("/e0/pyarrow_stats.parquet").getFile();
        final File pyarrowDest = new File(path);
        final Table pyarrowFromDisk = ParquetTools.readTable(pyarrowDest);

        // Verify that our verification code works for a pyarrow generated table.
        assertTableStatistics(pyarrowFromDisk, pyarrowDest);

        // Write the table to disk using our code.
        final File dhDest = new File(rootFile, "ParquetTest_statistics_test.parquet");
        ParquetTools.writeTable(pyarrowFromDisk, dhDest);

        // Read the table back in using our code.
        final Table dhFromDisk = ParquetTools.readTable(dhDest);

        // Verify the two tables loaded from disk are equal.
        TstUtils.assertTableEquals(pyarrowFromDisk, dhFromDisk);

        // Run the verification code against DHC writer stats.
        assertTableStatistics(pyarrowFromDisk, dhDest);
        assertTableStatistics(dhFromDisk, dhDest);
    }

    private void assertTableStatistics(Table inputTable, File dest) {
        // Verify that the columns have the correct statistics.
        final ParquetMetadata metadata = new ParquetTableLocationKey(dest, 0, null).getMetadata();

        final String[] colNames =
                inputTable.getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        for (int colIdx = 0; colIdx < inputTable.numColumns(); ++colIdx) {
            final String colName = colNames[colIdx];

            final ColumnSource<?> columnSource = inputTable.getColumnSource(colName);
            final ColumnChunkMetaData columnChunkMetaData = metadata.getBlocks().get(0).getColumns().get(colIdx);
            final Statistics<?> statistics = columnChunkMetaData.getStatistics();

            final Class<?> csType = columnSource.getType();

            if (csType == boolean.class || csType == Boolean.class) {
                assertBooleanColumnStatistics(
                        new SerialByteColumnIterator(
                                ReinterpretUtils.booleanToByteSource((ColumnSource<Boolean>) columnSource),
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == Boolean[].class) {
                assertBooleanArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<Boolean[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == byte.class || csType == Byte.class) {
                assertByteColumnStatistics(
                        new SerialByteColumnIterator(
                                (ColumnSource<Byte>) columnSource, inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == byte[].class) {
                assertByteArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<byte[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == ByteVector.class) {
                assertByteVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<ByteVector>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == char.class || csType == Character.class) {
                assertCharColumnStatistics(
                        new SerialCharacterColumnIterator(
                                (ColumnSource<Character>) columnSource, inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == char[].class) {
                assertCharArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<char[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == CharVector.class) {
                assertCharVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<CharVector>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == short.class || csType == Short.class) {
                assertShortColumnStatistics(
                        new SerialShortColumnIterator(
                                (ColumnSource<Short>) columnSource, inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == short[].class) {
                assertShortArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<short[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == ShortVector.class) {
                assertShortVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<ShortVector>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == int.class || csType == Integer.class) {
                assertIntColumnStatistics(
                        new SerialIntegerColumnIterator(
                                (ColumnSource<Integer>) columnSource, inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == int[].class) {
                assertIntArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<int[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == IntVector.class) {
                assertIntVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<IntVector>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Integer>) statistics);
            } else if (csType == long.class || csType == Long.class) {
                assertLongColumnStatistics(
                        new SerialLongColumnIterator(
                                (ColumnSource<Long>) columnSource, inputTable.getRowSet()),
                        (Statistics<Long>) statistics);
            } else if (csType == long[].class) {
                assertLongArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<long[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Long>) statistics);
            } else if (csType == LongVector.class) {
                assertLongVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<LongVector>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Long>) statistics);
            } else if (csType == float.class || csType == Float.class) {
                assertFloatColumnStatistics(
                        new SerialFloatColumnIterator(
                                (ColumnSource<Float>) columnSource, inputTable.getRowSet()),
                        (Statistics<Float>) statistics);
            } else if (csType == float[].class) {
                assertFloatArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<float[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Float>) statistics);
            } else if (csType == FloatVector.class) {
                assertFloatVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<FloatVector>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Float>) statistics);
            } else if (csType == double.class || csType == Double.class) {
                assertDoubleColumnStatistics(
                        new SerialDoubleColumnIterator(
                                (ColumnSource<Double>) columnSource, inputTable.getRowSet()),
                        (Statistics<Double>) statistics);
            } else if (csType == double[].class) {
                assertDoubleArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<double[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Double>) statistics);
            } else if (csType == DoubleVector.class) {
                assertDoubleVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<DoubleVector>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Double>) statistics);
            } else if (csType == String.class) {
                assertStringColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<String>) columnSource, inputTable.getRowSet()),
                        (Statistics<Binary>) statistics);
            } else if (csType == String[].class) {
                assertStringArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<String[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Binary>) statistics);
            } else if (csType == ObjectVector.class && columnSource.getComponentType() == String.class) {
                assertStringVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<ObjectVector<String>>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Binary>) statistics);
            } else if (csType == BigInteger.class) {
                assertBigIntegerColumnStatistics(
                        new SerialObjectColumnIterator(
                                (ColumnSource<BigInteger>) columnSource, inputTable.getRowSet()),
                        (Statistics<Binary>) statistics);
            } else if (csType == BigDecimal.class) {
                assertBigDecimalColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<BigDecimal>) columnSource, inputTable.getRowSet()),
                        (Statistics<Binary>) statistics);
            } else if (csType == Instant.class) {
                assertInstantColumnStatistic(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<Instant>) columnSource, inputTable.getRowSet()),
                        (Statistics<Long>) statistics);
            } else if (csType == Instant[].class) {
                assertInstantArrayColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<Instant[]>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Long>) statistics);
            } else if (csType == ObjectVector.class && columnSource.getComponentType() == Instant.class) {
                assertInstantVectorColumnStatistics(
                        new SerialObjectColumnIterator<>(
                                (ColumnSource<ObjectVector<Instant>>) columnSource,
                                inputTable.getRowSet()),
                        (Statistics<Long>) statistics);
            } else {
                // We can't verify statistics for this column type, so just skip it.
                System.out.println("Ignoring column " + colName + " of type " + csType.getName());
            }
        }
    }

    // region Column Statistics Assertions
    private void assertBooleanColumnStatistics(SerialByteColumnIterator iterator, Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_BYTE);
        MutableInt max = new MutableInt(NULL_BYTE);

        iterator.forEachRemaining((ByteConsumer) value -> {
            itemCount.increment();
            if (value == NULL_BYTE) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_BYTE || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_BYTE || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue() == 1, statistics.genericGetMin());
            assertEquals(max.getValue() == 1, statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertBooleanArrayColumnStatistics(SerialObjectColumnIterator<Boolean[]> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_BYTE);
        MutableInt max = new MutableInt(NULL_BYTE);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final Boolean value : values) {
                itemCount.increment();
                if (value == null) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_BYTE || (value ? 1 : 0) < min.getValue()) {
                        min.setValue(value ? 1 : 0);
                    }
                    if (max.getValue() == NULL_BYTE || (value ? 1 : 0) > max.getValue()) {
                        max.setValue(value ? 1 : 0);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue() == 1, statistics.genericGetMin());
            assertEquals(max.getValue() == 1, statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertByteColumnStatistics(SerialByteColumnIterator iterator, Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_BYTE);
        MutableInt max = new MutableInt(NULL_BYTE);

        iterator.forEachRemaining((ByteConsumer) value -> {
            itemCount.increment();
            if (value == NULL_BYTE) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_BYTE || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_BYTE || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertByteArrayColumnStatistics(SerialObjectColumnIterator<byte[]> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_BYTE);
        MutableInt max = new MutableInt(NULL_BYTE);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final byte value : values) {
                itemCount.increment();
                if (value == NULL_BYTE) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_BYTE || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_BYTE || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertByteVectorColumnStatistics(SerialObjectColumnIterator<ByteVector> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_BYTE);
        MutableInt max = new MutableInt(NULL_BYTE);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final byte value : values) {
                itemCount.increment();
                if (value == NULL_BYTE) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_BYTE || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_BYTE || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertCharColumnStatistics(SerialCharacterColumnIterator iterator, Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_CHAR);
        MutableInt max = new MutableInt(NULL_CHAR);

        iterator.forEachRemaining((CharConsumer) value -> {
            itemCount.increment();
            if (value == NULL_CHAR) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_CHAR || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_CHAR || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertCharArrayColumnStatistics(SerialObjectColumnIterator<char[]> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_CHAR);
        MutableInt max = new MutableInt(NULL_CHAR);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final char value : values) {
                itemCount.increment();
                if (value == NULL_CHAR) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_CHAR || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_CHAR || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertCharVectorColumnStatistics(SerialObjectColumnIterator<CharVector> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_CHAR);
        MutableInt max = new MutableInt(NULL_CHAR);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final char value : values) {
                itemCount.increment();
                if (value == NULL_CHAR) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_CHAR || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_CHAR || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertShortColumnStatistics(SerialShortColumnIterator iterator, Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_SHORT);
        MutableInt max = new MutableInt(NULL_SHORT);

        iterator.forEachRemaining((ShortConsumer) value -> {
            itemCount.increment();
            if (value == NULL_SHORT) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_SHORT || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_SHORT || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertShortArrayColumnStatistics(SerialObjectColumnIterator<short[]> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_SHORT);
        MutableInt max = new MutableInt(NULL_SHORT);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final short value : values) {
                itemCount.increment();
                if (value == NULL_SHORT) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_SHORT || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_SHORT || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertShortVectorColumnStatistics(SerialObjectColumnIterator<ShortVector> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_SHORT);
        MutableInt max = new MutableInt(NULL_SHORT);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final short value : values) {
                itemCount.increment();
                if (value == NULL_SHORT) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_SHORT || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_SHORT || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertIntColumnStatistics(SerialIntegerColumnIterator iterator, Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_INT);
        MutableInt max = new MutableInt(NULL_INT);

        iterator.forEachRemaining((IntConsumer) value -> {
            itemCount.increment();
            if (value == NULL_INT) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_INT || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_INT || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertIntArrayColumnStatistics(SerialObjectColumnIterator<int[]> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_INT);
        MutableInt max = new MutableInt(NULL_INT);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final int value : values) {
                itemCount.increment();
                if (value == NULL_INT) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_INT || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_INT || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertIntVectorColumnStatistics(SerialObjectColumnIterator<IntVector> iterator,
            Statistics<Integer> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableInt min = new MutableInt(NULL_INT);
        MutableInt max = new MutableInt(NULL_INT);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final int value : values) {
                itemCount.increment();
                if (value == NULL_INT) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_INT || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_INT || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertLongColumnStatistics(SerialLongColumnIterator iterator, Statistics<Long> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableLong min = new MutableLong(NULL_LONG);
        MutableLong max = new MutableLong(NULL_LONG);

        iterator.forEachRemaining((LongConsumer) value -> {
            itemCount.increment();
            if (value == NULL_LONG) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_LONG || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_LONG || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertLongArrayColumnStatistics(SerialObjectColumnIterator<long[]> iterator,
            Statistics<Long> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableLong min = new MutableLong(NULL_LONG);
        MutableLong max = new MutableLong(NULL_LONG);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final long value : values) {
                itemCount.increment();
                if (value == NULL_LONG) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_LONG || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_LONG || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertLongVectorColumnStatistics(SerialObjectColumnIterator<LongVector> iterator,
            Statistics<Long> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableLong min = new MutableLong(NULL_LONG);
        MutableLong max = new MutableLong(NULL_LONG);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final long value : values) {
                itemCount.increment();
                if (value == NULL_LONG) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_LONG || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_LONG || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertFloatColumnStatistics(SerialFloatColumnIterator iterator, Statistics<Float> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableFloat min = new MutableFloat(NULL_FLOAT);
        MutableFloat max = new MutableFloat(NULL_FLOAT);

        iterator.forEachRemaining((FloatConsumer) value -> {
            itemCount.increment();
            if (value == NULL_FLOAT) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_FLOAT || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_FLOAT || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            // Use FloatComparisons.compare() to handle -0.0f == 0.0f properly
            assertEquals(FloatComparisons.compare(min.getValue(), statistics.genericGetMin()), 0);
            assertEquals(FloatComparisons.compare(max.getValue(), statistics.genericGetMax()), 0);
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertFloatArrayColumnStatistics(SerialObjectColumnIterator<float[]> iterator,
            Statistics<Float> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableFloat min = new MutableFloat(NULL_FLOAT);
        MutableFloat max = new MutableFloat(NULL_FLOAT);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final float value : values) {
                itemCount.increment();
                if (value == NULL_FLOAT) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_FLOAT || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_FLOAT || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            // Use FloatComparisons.compare() to handle -0.0f == 0.0f properly
            assertEquals(FloatComparisons.compare(min.getValue(), statistics.genericGetMin()), 0);
            assertEquals(FloatComparisons.compare(max.getValue(), statistics.genericGetMax()), 0);
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertFloatVectorColumnStatistics(SerialObjectColumnIterator<FloatVector> iterator,
            Statistics<Float> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableFloat min = new MutableFloat(NULL_FLOAT);
        MutableFloat max = new MutableFloat(NULL_FLOAT);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final float value : values) {
                itemCount.increment();
                if (value == NULL_FLOAT) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_FLOAT || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_FLOAT || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            // Use FloatComparisons.compare() to handle -0.0f == 0.0f properly
            assertEquals(FloatComparisons.compare(min.getValue(), statistics.genericGetMin()), 0);
            assertEquals(FloatComparisons.compare(max.getValue(), statistics.genericGetMax()), 0);
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertDoubleColumnStatistics(SerialDoubleColumnIterator iterator, Statistics<Double> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableDouble min = new MutableDouble(NULL_DOUBLE);
        MutableDouble max = new MutableDouble(NULL_DOUBLE);

        iterator.forEachRemaining((DoubleConsumer) value -> {
            itemCount.increment();
            if (value == NULL_DOUBLE) {
                nullCount.increment();
            } else {
                if (min.getValue() == NULL_DOUBLE || value < min.getValue()) {
                    min.setValue(value);
                }
                if (max.getValue() == NULL_DOUBLE || value > max.getValue()) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            // Use DoubleComparisons.compare() to handle -0.0f == 0.0f properly
            assertEquals(DoubleComparisons.compare(min.getValue(), statistics.genericGetMin()), 0);
            assertEquals(DoubleComparisons.compare(max.getValue(), statistics.genericGetMax()), 0);
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertDoubleArrayColumnStatistics(SerialObjectColumnIterator<double[]> iterator,
            Statistics<Double> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableDouble min = new MutableDouble(NULL_DOUBLE);
        MutableDouble max = new MutableDouble(NULL_DOUBLE);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final double value : values) {
                itemCount.increment();
                if (value == NULL_DOUBLE) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_DOUBLE || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_DOUBLE || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            // Use DoubleComparisons.compare() to handle -0.0f == 0.0f properly
            assertEquals(DoubleComparisons.compare(min.getValue(), statistics.genericGetMin()), 0);
            assertEquals(DoubleComparisons.compare(max.getValue(), statistics.genericGetMax()), 0);
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertDoubleVectorColumnStatistics(SerialObjectColumnIterator<DoubleVector> iterator,
            Statistics<Double> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableDouble min = new MutableDouble(NULL_DOUBLE);
        MutableDouble max = new MutableDouble(NULL_DOUBLE);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final double value : values) {
                itemCount.increment();
                if (value == NULL_DOUBLE) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == NULL_DOUBLE || value < min.getValue()) {
                        min.setValue(value);
                    }
                    if (max.getValue() == NULL_DOUBLE || value > max.getValue()) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            // Use DoubleComparisons.compare() to handle -0.0f == 0.0f properly
            assertEquals(DoubleComparisons.compare(min.getValue(), statistics.genericGetMin()), 0);
            assertEquals(DoubleComparisons.compare(max.getValue(), statistics.genericGetMax()), 0);
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertStringColumnStatistics(SerialObjectColumnIterator<String> iterator,
            Statistics<Binary> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableObject<String> min = new MutableObject<>(null);
        MutableObject<String> max = new MutableObject<>(null);

        iterator.forEachRemaining((value) -> {
            itemCount.increment();
            if (value == null) {
                nullCount.increment();
            } else {
                if (min.getValue() == null || value.compareTo(min.getValue()) < 0) {
                    min.setValue(value);
                }
                if (max.getValue() == null || value.compareTo(max.getValue()) > 0) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(Binary.fromString(min.getValue()), statistics.genericGetMin());
            assertEquals(Binary.fromString(max.getValue()), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertStringArrayColumnStatistics(SerialObjectColumnIterator<String[]> iterator,
            Statistics<Binary> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableObject<String> min = new MutableObject<>(null);
        MutableObject<String> max = new MutableObject<>(null);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final String value : values) {
                itemCount.increment();
                if (value == null) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == null || value.compareTo(min.getValue()) < 0) {
                        min.setValue(value);
                    }
                    if (max.getValue() == null || value.compareTo(max.getValue()) > 0) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(Binary.fromString(min.getValue()), statistics.genericGetMin());
            assertEquals(Binary.fromString(max.getValue()), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertStringVectorColumnStatistics(SerialObjectColumnIterator<ObjectVector<String>> iterator,
            Statistics<Binary> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableObject<String> min = new MutableObject<>(null);
        MutableObject<String> max = new MutableObject<>(null);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (String value : values) {
                itemCount.increment();
                if (value == null) {
                    nullCount.increment();
                } else {
                    if (min.getValue() == null || value.compareTo(min.getValue()) < 0) {
                        min.setValue(value);
                    }
                    if (max.getValue() == null || value.compareTo(max.getValue()) > 0) {
                        max.setValue(value);
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(Binary.fromString(min.getValue()), statistics.genericGetMin());
            assertEquals(Binary.fromString(max.getValue()), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertInstantColumnStatistic(SerialObjectColumnIterator<Instant> iterator,
            Statistics<Long> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableLong min = new MutableLong(NULL_LONG);
        MutableLong max = new MutableLong(NULL_LONG);

        iterator.forEachRemaining((value) -> {
            itemCount.increment();
            if (value == null) {
                nullCount.increment();
            } else {
                // DateTimeUtils.epochNanos() is the correct conversion for Instant to long.
                if (min.getValue() == NULL_LONG || DateTimeUtils.epochNanos(value) < min.getValue()) {
                    min.setValue(DateTimeUtils.epochNanos(value));
                }
                if (max.getValue() == NULL_LONG || DateTimeUtils.epochNanos(value) > max.getValue()) {
                    max.setValue(DateTimeUtils.epochNanos(value));
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertInstantArrayColumnStatistics(SerialObjectColumnIterator<Instant[]> iterator,
            Statistics<Long> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableLong min = new MutableLong(NULL_LONG);
        MutableLong max = new MutableLong(NULL_LONG);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (final Instant value : values) {
                itemCount.increment();
                if (value == null) {
                    nullCount.increment();
                } else {
                    // DateTimeUtils.epochNanos() is the correct conversion for Instant to long.
                    if (min.getValue() == NULL_LONG || DateTimeUtils.epochNanos(value) < min.getValue()) {
                        min.setValue(DateTimeUtils.epochNanos(value));
                    }
                    if (max.getValue() == NULL_LONG || DateTimeUtils.epochNanos(value) > max.getValue()) {
                        max.setValue(DateTimeUtils.epochNanos(value));
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertInstantVectorColumnStatistics(SerialObjectColumnIterator<ObjectVector<Instant>> iterator,
            Statistics<Long> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableLong min = new MutableLong(NULL_LONG);
        MutableLong max = new MutableLong(NULL_LONG);

        iterator.forEachRemaining(values -> {
            if (values == null) {
                itemCount.increment();
                nullCount.increment();
                return;
            }
            for (Instant value : values) {
                itemCount.increment();
                if (value == null) {
                    nullCount.increment();
                } else {
                    // DateTimeUtils.epochNanos() is the correct conversion for Instant to long.
                    if (min.getValue() == NULL_LONG || DateTimeUtils.epochNanos(value) < min.getValue()) {
                        min.setValue(DateTimeUtils.epochNanos(value));
                    }
                    if (max.getValue() == NULL_LONG || DateTimeUtils.epochNanos(value) > max.getValue()) {
                        max.setValue(DateTimeUtils.epochNanos(value));
                    }
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed
            // values.
            assertEquals(min.getValue(), statistics.genericGetMin());
            assertEquals(max.getValue(), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertBigDecimalColumnStatistics(SerialObjectColumnIterator<BigDecimal> iterator,
            Statistics<Binary> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableObject<BigDecimal> min = new MutableObject<>(null);
        MutableObject<BigDecimal> max = new MutableObject<>(null);

        iterator.forEachRemaining((value) -> {
            itemCount.increment();
            if (value == null) {
                nullCount.increment();
            } else {
                if (min.getValue() == null || value.compareTo(min.getValue()) < 0) {
                    min.setValue(value);
                }
                if (max.getValue() == null || value.compareTo(max.getValue()) > 0) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(Binary.fromConstantByteArray(min.getValue().unscaledValue().toByteArray()),
                    statistics.genericGetMin());
            assertEquals(Binary.fromConstantByteArray(max.getValue().unscaledValue().toByteArray()),
                    statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    private void assertBigIntegerColumnStatistics(SerialObjectColumnIterator<BigInteger> iterator,
            Statistics<Binary> statistics) {
        MutableLong itemCount = new MutableLong(0);
        MutableLong nullCount = new MutableLong(0);
        MutableObject<BigInteger> min = new MutableObject<>(null);
        MutableObject<BigInteger> max = new MutableObject<>(null);

        iterator.forEachRemaining((value) -> {
            itemCount.increment();
            if (value == null) {
                nullCount.increment();
            } else {
                if (min.getValue() == null || value.compareTo(min.getValue()) < 0) {
                    min.setValue(value);
                }
                if (max.getValue() == null || value.compareTo(max.getValue()) > 0) {
                    max.setValue(value);
                }
            }
        });

        assertEquals(nullCount.intValue(), statistics.getNumNulls());
        if (!itemCount.getValue().equals(nullCount.getValue())) {
            // There are some non-null values, so min and max should be non-null and equal to observed values.
            assertEquals(Binary.fromConstantByteArray(min.getValue().toByteArray()), statistics.genericGetMin());
            assertEquals(Binary.fromConstantByteArray(max.getValue().toByteArray()), statistics.genericGetMax());
        } else {
            // Everything is null, statistics should be empty.
            assertFalse(statistics.hasNonNullValue());
        }
    }

    // endregion Column Statistics Assertions
}
