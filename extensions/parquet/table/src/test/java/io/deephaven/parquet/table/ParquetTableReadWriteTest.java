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
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.table.iterators.*;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.parquet.base.NullStatistics;
import io.deephaven.parquet.base.InvalidParquetFileException;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.parquet.table.transfer.StringDictionary;
import io.deephaven.stringset.ArrayStringSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.stringset.StringSet;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import junit.framework.TestCase;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.vector.*;
import org.apache.commons.lang3.mutable.*;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.junit.experimental.categories.Category;

import javax.annotation.Nullable;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public final class ParquetTableReadWriteTest {

    private static final String ROOT_FILENAME = ParquetTableReadWriteTest.class.getName() + "_root";
    private static final int LARGE_TABLE_SIZE = 2_000_000;

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
                        "someBiColumn = java.math.BigInteger.valueOf(ii)",
                        "someDateColumn = i % 10 == 0 ? null : java.time.LocalDate.ofEpochDay(i)",
                        "someTimeColumn = i % 10 == 0 ? null : java.time.LocalTime.of(i%24, i%60, (i+10)%60)",
                        "someDateTimeColumn = i % 10 == 0 ? null : java.time.LocalDateTime.of(2000+i%10, i%12+1, i%30+1, (i+4)%24, (i+5)%60, (i+6)%60, i)",
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
                        "nullBiColumn = (java.math.BigInteger)null",
                        "nullString = (String)null",
                        "nullDateColumn = (java.time.LocalDate)null",
                        "nullTimeColumn = (java.time.LocalTime)null"));
        if (includeBigDecimal) {
            columns.add("bdColumn = java.math.BigDecimal.valueOf(ii).stripTrailingZeros()");
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

    private void compressionCodecTestHelper(final ParquetInstructions codec) {
        File dest = new File(rootFile + File.separator + "Table1.parquet");
        final Table table1 = getTableFlat(10000, false, true);
        ParquetTools.writeTable(table1, dest, codec);
        assertTrue(dest.length() > 0L);
        final Table table2 = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(maybeFixBigDecimal(table1), table2);
    }

    @Test
    public void testParquetUncompressedCompressionCodec() {
        compressionCodecTestHelper(ParquetTools.UNCOMPRESSED);
    }

    @Test
    public void testParquetLzoCompressionCodec() {
        compressionCodecTestHelper(ParquetTools.LZO);
    }

    @Test
    public void testParquetLz4CompressionCodec() {
        compressionCodecTestHelper(ParquetTools.LZ4);
    }

    @Test
    public void test_lz4_compressed() {
        // Write and read a LZ4 compressed file
        File dest = new File(rootFile + File.separator + "Table.parquet");
        final Table table = getTableFlat(100, false, false);
        ParquetTools.writeTable(table, dest, ParquetTools.LZ4);
        Table fromDisk = ParquetTools.readTable(dest).select();
        TstUtils.assertTableEquals(fromDisk, table);

        try {
            // The following file is tagged as LZ4 compressed based on its metadata, but is actually compressed with
            // LZ4_RAW. We should be able to read it anyway with no exceptions.
            String path = TestParquetTools.class.getResource("/sample_lz4_compressed.parquet").getFile();
            fromDisk = ParquetTools.readTable(path).select();
        } catch (RuntimeException e) {
            TestCase.fail("Failed to read parquet file sample_lz4_compressed.parquet");
        }
        File randomDest = new File(rootFile, "random.parquet");
        ParquetTools.writeTable(fromDisk, randomDest, ParquetTools.LZ4_RAW);

        // Read the LZ4 compressed file again, to make sure we use a new adapter
        fromDisk = ParquetTools.readTable(dest).select();
        TstUtils.assertTableEquals(fromDisk, table);
    }

    @Test
    public void testParquetLz4RawCompressionCodec() {
        compressionCodecTestHelper(ParquetTools.LZ4_RAW);
    }

    @Ignore("See BrotliParquetReadWriteTest instead")
    @Test
    public void testParquetBrotliCompressionCodec() {
        compressionCodecTestHelper(ParquetTools.BROTLI);
    }

    @Test
    public void testParquetZstdCompressionCodec() {
        compressionCodecTestHelper(ParquetTools.ZSTD);
    }

    @Test
    public void testParquetGzipCompressionCodec() {
        compressionCodecTestHelper(ParquetTools.GZIP);
    }

    @Test
    public void testParquetSnappyCompressionCodec() {
        // while Snappy is covered by other tests, this is a very fast test to quickly confirm that it works in the same
        // way as the other similar codec tests.
        compressionCodecTestHelper(ParquetTools.SNAPPY);
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

    private static void writeReadTableTest(final Table table, final File dest) {
        writeReadTableTest(table, dest, ParquetInstructions.EMPTY);
    }

    private static void writeReadTableTest(final Table table, final File dest,
            final ParquetInstructions writeInstructions) {
        ParquetTools.writeTable(table, dest, writeInstructions);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(table, fromDisk);
    }

    @Test
    public void testVectorColumns() {
        final Table table = getTableFlat(20000, true, false);
        // Take a groupBy to create vector columns containing null values
        Table vectorTable = table.groupBy().select();

        final File dest = new File(rootFile + File.separator + "testVectorColumns.parquet");
        writeReadTableTest(vectorTable, dest);

        // Take a join with empty table to repeat the same row multiple times
        vectorTable = vectorTable.join(TableTools.emptyTable(100)).select();
        writeReadTableTest(vectorTable, dest);

        // Convert the table from vector to array column
        final Table arrayTable = vectorTable.updateView(vectorTable.getColumnSourceMap().keySet().stream()
                .map(name -> name + " = " + name + ".toArray()")
                .toArray(String[]::new));
        writeReadTableTest(arrayTable, dest);

        // Enforce a smaller page size to overflow the page
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setTargetPageSize(ParquetInstructions.MIN_TARGET_PAGE_SIZE)
                .build();
        writeReadTableTest(arrayTable, dest, writeInstructions);
        writeReadTableTest(vectorTable, dest, writeInstructions);
    }

    private static Table arrayToVectorTable(final Table table) {
        final TableDefinition tableDefinition = table.getDefinition();
        final Collection<SelectColumn> arrayToVectorFormulas = new ArrayList<>();
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String columnName = columnDefinition.getName();
            final Class<Object> sourceDataType = (Class<Object>) columnDefinition.getDataType();
            if (!sourceDataType.isArray()) {
                continue;
            }
            final Class<?> componentType = Objects.requireNonNull(columnDefinition.getComponentType());
            final VectorFactory vectorFactory = VectorFactory.forElementType(componentType);
            final Class<? extends Vector<?>> destinationDataType = vectorFactory.vectorType();
            final Function<Object, Vector<?>> vectorWrapFunction = vectorFactory::vectorWrap;
            // noinspection unchecked,rawtypes
            arrayToVectorFormulas.add(new FunctionalColumn(
                    columnName, sourceDataType, columnName, destinationDataType, componentType, vectorWrapFunction));
        }
        return arrayToVectorFormulas.isEmpty() ? table : table.updateView(arrayToVectorFormulas);
    }

    @Test
    public void testArrayColumns() {
        ArrayList<String> columns =
                new ArrayList<>(Arrays.asList(
                        "someStringArrayColumn = new String[] {i % 10 == 0 ? null : (`` + (i % 101))}",
                        "someIntArrayColumn = new int[] {i % 10 == 0 ? null : i}",
                        "someLongArrayColumn = new long[] {i % 10 == 0 ? null : i}",
                        "someDoubleArrayColumn = new double[] {i % 10 == 0 ? null : i*1.1}",
                        "someFloatArrayColumn = new float[] {i % 10 == 0 ? null : (float)(i*1.1)}",
                        "someBoolArrayColumn = new Boolean[] {i % 3 == 0 ? true :i % 3 == 1 ? false : null}",
                        "someShorArrayColumn = new short[] {i % 10 == 0 ? null : (short)i}",
                        "someByteArrayColumn = new byte[] {i % 10 == 0 ? null : (byte)i}",
                        "someCharArrayColumn = new char[] {i % 10 == 0 ? null : (char)i}",
                        "someTimeArrayColumn = new Instant[] {i % 10 == 0 ? null : (Instant)DateTimeUtils.now() + i}",
                        "someBiArrayColumn = new java.math.BigInteger[] {i % 10 == 0 ? null : java.math.BigInteger.valueOf(i)}",
                        "someDateArrayColumn = new java.time.LocalDate[] {i % 10 == 0 ? null : java.time.LocalDate.ofEpochDay(i)}",
                        "someTimeArrayColumn = new java.time.LocalTime[] {i % 10 == 0 ? null : java.time.LocalTime.of(i%24, i%60, (i+10)%60)}",
                        "someDateTimeArrayColumn = new java.time.LocalDateTime[] {i % 10 == 0 ? null : java.time.LocalDateTime.of(2000+i%10, i%12+1, i%30+1, (i+4)%24, (i+5)%60, (i+6)%60, i)}",
                        "nullStringArrayColumn = new String[] {(String)null}",
                        "nullIntArrayColumn = new int[] {(int)null}",
                        "nullLongArrayColumn = new long[] {(long)null}",
                        "nullDoubleArrayColumn = new double[] {(double)null}",
                        "nullFloatArrayColumn = new float[] {(float)null}",
                        "nullBoolArrayColumn = new Boolean[] {(Boolean)null}",
                        "nullShorArrayColumn = new short[] {(short)null}",
                        "nullByteArrayColumn = new byte[] {(byte)null}",
                        "nullCharArrayColumn = new char[] {(char)null}",
                        "nullTimeArrayColumn = new Instant[] {(Instant)null}",
                        "nullBiColumn = new java.math.BigInteger[] {(java.math.BigInteger)null}",
                        "nullDateColumn = new java.time.LocalDate[] {(java.time.LocalDate)null}",
                        "nullTimeColumn = new java.time.LocalTime[] {(java.time.LocalTime)null}"));

        Table arrayTable = TableTools.emptyTable(10000).select(Selectable.from(columns));
        final File dest = new File(rootFile + File.separator + "testArrayColumns.parquet");
        writeReadTableTest(arrayTable, dest);

        // Convert array table to vector
        Table vectorTable = arrayToVectorTable(arrayTable);
        writeReadTableTest(vectorTable, dest);

        // Enforce a smaller dictionary size to overflow the dictionary and test plain encoding
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setMaximumDictionarySize(20)
                .build();
        arrayTable = arrayTable.select("someStringArrayColumn", "nullStringArrayColumn");
        writeReadTableTest(arrayTable, dest, writeInstructions);

        // Make sure the column didn't use dictionary encoding
        ParquetMetadata metadata = new ParquetTableLocationKey(dest, 0, null).getMetadata();
        String firstColumnMetadata = metadata.getBlocks().get(0).getColumns().get(0).toString();
        assertTrue(firstColumnMetadata.contains("someStringArrayColumn")
                && !firstColumnMetadata.contains("RLE_DICTIONARY"));

        vectorTable = vectorTable.select("someStringArrayColumn", "nullStringArrayColumn");
        writeReadTableTest(vectorTable, dest, writeInstructions);

        // Make sure the column didn't use dictionary encoding
        metadata = new ParquetTableLocationKey(dest, 0, null).getMetadata();
        firstColumnMetadata = metadata.getBlocks().get(0).getColumns().get(0).toString();
        assertTrue(firstColumnMetadata.contains("someStringArrayColumn")
                && !firstColumnMetadata.contains("RLE_DICTIONARY"));
    }

    @Test
    public void stringDictionaryTest() {
        final int nullPos = -5;
        final int maxKeys = 10;
        final int maxDictSize = 100;
        final Statistics<?> stats = NullStatistics.INSTANCE;
        StringDictionary dict = new StringDictionary(maxKeys, maxDictSize, NullStatistics.INSTANCE, nullPos);
        assertEquals(0, dict.getKeyCount());
        assertEquals(nullPos, dict.add(null));

        final String[] keys = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
        final Map<String, Integer> keyToPos = new HashMap<>();
        for (int ii = 0; ii <= 6 * keys.length; ii += 3) {
            final String key = keys[ii % keys.length];
            final int dictPos = dict.add(key);
            if (keyToPos.containsKey(key)) {
                assertEquals(keyToPos.get(key).intValue(), dictPos);
            } else {
                keyToPos.put(key, dictPos);
                assertEquals(dictPos, dict.getKeyCount() - 1);
            }
        }
        assertEquals(keys.length, dict.getKeyCount());
        assertEquals(keys.length, keyToPos.size());
        final Binary[] encodedKeys = dict.getEncodedKeys();
        for (int i = 0; i < keys.length; i++) {
            final String decodedKey = encodedKeys[i].toStringUsingUTF8();
            final int expectedPos = keyToPos.get(decodedKey).intValue();
            assertEquals(i, expectedPos);
        }
        assertEquals(nullPos, dict.add(null));
        try {
            dict.add("Never before seen key which should take us over the allowed dictionary size");
            TestCase.fail("Exception expected for exceeding dictionary size");
        } catch (DictionarySizeExceededException expected) {
        }
    }

    /**
     * Encoding bigDecimal is tricky -- the writer will try to pick the precision and scale automatically. Because of
     * that tableTools.assertTableEquals will fail because, even though the numbers are identical, the representation
     * may not be, so we have to coerce the expected values to the same precision and scale value. We know how it should
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
     * Verify that the parent directory contains the expected parquet files and index files in the right directory
     * structure.
     */
    private static void verifyFilesInDir(final File parentDir, final String[] expectedDataFiles,
            @Nullable final Map<String, String[]> indexingColumnToFileMap) {
        final List filesInParentDir = Arrays.asList(parentDir.list());
        for (String expectedFile : expectedDataFiles) {
            assertTrue(filesInParentDir.contains(expectedFile));
        }
        if (indexingColumnToFileMap == null) {
            assertTrue(filesInParentDir.size() == expectedDataFiles.length);
            return;
        }
        assertTrue(filesInParentDir.size() == expectedDataFiles.length + 1);
        final File metadataDir = new File(parentDir, ".dh_metadata");
        assertTrue(metadataDir.exists() && metadataDir.isDirectory() && metadataDir.list().length == 1);
        final File indexesDir = new File(metadataDir, "indexes");
        assertTrue(indexesDir.exists() && indexesDir.isDirectory()
                && indexesDir.list().length == indexingColumnToFileMap.size());
        for (Map.Entry<String, String[]> set : indexingColumnToFileMap.entrySet()) {
            final String indexColName = set.getKey();
            final String[] indexFilePaths = set.getValue();
            final File indexColDir = new File(indexesDir, indexColName);
            assertTrue(indexColDir.exists() && indexColDir.isDirectory()
                    && indexColDir.list().length == indexFilePaths.length);
            final List filesInIndexColDir = Arrays.asList(indexColDir.list());
            for (final String indexFilePath : indexFilePaths) {
                final File indexFile = new File(parentDir, indexFilePath);
                assertTrue(indexFile.exists() && indexFile.isFile() && indexFile.length() > 0 &&
                        filesInIndexColDir.contains(indexFile.getName()));
            }
        }
    }

    /**
     * These are tests for writing a table to a parquet file and making sure there are no unnecessary files left in the
     * directory after we finish writing.
     */
    @Test
    public void basicWriteTests() {
        basicWriteTestsImpl(singleWriter);
        basicWriteTestsImpl(multiWriter);
    }

    private static void basicWriteTestsImpl(TestParquetTableWriter writer) {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();
        assertTrue(parentDir.exists() && parentDir.isDirectory() && parentDir.list().length == 0);

        // There should be just one file in the directory on a successful write and no temporary files
        final Table tableToSave = TableTools.emptyTable(5).update("A=(int)i", "B=(long)i", "C=(double)i");
        final String filename = "basicWriteTests.parquet";
        final File destFile = new File(parentDir, filename);
        writer.writeTable(tableToSave, destFile);
        verifyFilesInDir(parentDir, new String[] {filename}, null);
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
        verifyFilesInDir(parentDir, new String[] {filename}, null);
        fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, tableToSave);

        // Write a new table successfully at the same path
        final Table newTableToSave = TableTools.emptyTable(5).update("A=(int)i");
        writer.writeTable(newTableToSave, destFile);
        verifyFilesInDir(parentDir, new String[] {filename}, null);
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

        verifyFilesInDir(parentDir, new String[] {firstFilename, secondFilename}, null);
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

        final String destFilename = "groupingColumnsWriteTests.parquet";
        final File destFile = new File(parentDir, destFilename);
        writer.writeTable(tableToSave, destFile);
        String vvvIndexFilePath = ".dh_metadata/indexes/vvv/index_vvv_groupingColumnsWriteTests.parquet";
        verifyFilesInDir(parentDir, new String[] {destFilename}, Map.of("vvv", new String[] {vvvIndexFilePath}));

        Table fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, tableToSave);

        // Verify that the key-value metadata in the file has the correct name
        ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(destFile, 0, null);
        String metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(vvvIndexFilePath));

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
        verifyFilesInDir(parentDir, new String[] {destFilename}, Map.of("vvv", new String[] {vvvIndexFilePath}));
        fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, tableToSave);
        FileUtils.deleteRecursively(parentDir);
    }

    @Test
    public void legacyGroupingFileReadTest() {
        final String path =
                ParquetTableReadWriteTest.class.getResource("/ParquetDataWithLegacyGroupingInfo.parquet").getFile();
        final File destFile = new File(path);

        // Read the legacy file and verify that grouping column is read correctly
        final Table fromDisk;
        try {
            fromDisk = ParquetTools.readTable(destFile);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof InvalidParquetFileException) {
                final String InvalidParquetFileErrorMsgString = "Invalid parquet file detected, please ensure the " +
                        "file is fetched properly from Git LFS. Run commands 'git lfs install; git lfs pull' inside " +
                        "the repo to pull the files from LFS. Check cause of exception for more details.";
                throw new UncheckedDeephavenException(InvalidParquetFileErrorMsgString, e.getCause());
            }
            throw e;
        }
        final String groupingColName = "gcol";
        assertTrue(fromDisk.getDefinition().getColumn(groupingColName).isGrouping());

        // Verify that the key-value metadata in the file has the correct legacy grouping file name
        final ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(destFile, 0, null);
        final String metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        String groupingFileName = ParquetTools.legacyGroupingFileName(destFile, groupingColName);
        assertTrue(metadataString.contains(groupingFileName));

        // Following is how this file was generated, so verify the table read from disk against this
        Integer data[] = new Integer[500 * 4];
        for (int i = 0; i < data.length; i++) {
            data[i] = i / 4;
        }
        final TableDefinition tableDefinition =
                TableDefinition.of(ColumnDefinition.ofInt(groupingColName).withGrouping());
        final Table table = TableTools.newTable(tableDefinition, TableTools.col(groupingColName, data));
        TstUtils.assertTableEquals(fromDisk, table);
    }

    @Test
    public void parquetDirectoryWithDotFilesTest() throws IOException {
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

        final String destFilename = "data.parquet";
        final File destFile = new File(parentDir, destFilename);
        ParquetTools.writeTable(tableToSave, destFile);
        String vvvIndexFilePath = ".dh_metadata/indexes/vvv/index_vvv_data.parquet";
        verifyFilesInDir(parentDir, new String[] {destFilename}, Map.of("vvv", new String[] {vvvIndexFilePath}));

        // Call readTable on parent directory
        Table fromDisk = ParquetTools.readTable(parentDir);
        TstUtils.assertTableEquals(fromDisk, tableToSave);

        // Add an empty dot file and dot directory (with valid parquet files) in the parent directory
        final File dotFile = new File(parentDir, ".dotFile");
        assertTrue(dotFile.createNewFile());
        final File dotDir = new File(parentDir, ".dotDir");
        assertTrue(dotDir.mkdir());
        final Table someTable = TableTools.emptyTable(5).update("A=(int)i");
        ParquetTools.writeTable(someTable, new File(dotDir, "data.parquet"));
        fromDisk = ParquetTools.readTable(parentDir);
        TstUtils.assertTableEquals(fromDisk, tableToSave);

        // Add a dot parquet in parent directory
        final Table anotherTable = TableTools.emptyTable(5).update("A=(int)i");
        final File pqDotFile = new File(parentDir, ".dotFile.parquet");
        ParquetTools.writeTable(anotherTable, pqDotFile);
        fromDisk = ParquetTools.readTable(parentDir);
        TstUtils.assertTableEquals(fromDisk, tableToSave);
    }

    @Test
    public void partitionedParquetWithDotFilesTest() throws IOException {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "tempDir");
        parentDir.mkdir();
        assertTrue(parentDir.exists() && parentDir.isDirectory() && parentDir.list().length == 0);

        final Table someTable = TableTools.emptyTable(5).update("A=(int)i");
        final File firstPartition = new File(parentDir, "X=A");
        final File firstDataFile = new File(firstPartition, "data.parquet");
        final File secondPartition = new File(parentDir, "X=B");
        final File secondDataFile = new File(secondPartition, "data.parquet");

        ParquetTools.writeTable(someTable, firstDataFile);
        ParquetTools.writeTable(someTable, secondDataFile);

        Table partitionedTable = ParquetTools.readTable(parentDir).select();
        final Set<?> columnsSet = partitionedTable.getColumnSourceMap().keySet();
        assertTrue(columnsSet.size() == 2 && columnsSet.contains("A") && columnsSet.contains("X"));

        // Add an empty dot file and dot directory (with valid parquet files) in one of the partitions
        final File dotFile = new File(firstPartition, ".dotFile");
        assertTrue(dotFile.createNewFile());
        final File dotDir = new File(firstPartition, ".dotDir");
        assertTrue(dotDir.mkdir());
        ParquetTools.writeTable(someTable, new File(dotDir, "data.parquet"));
        Table fromDisk = ParquetTools.readTable(parentDir);
        TstUtils.assertTableEquals(fromDisk, partitionedTable);

        // Add a dot parquet file in one of the partitions directory
        final Table anotherTable = TableTools.emptyTable(5).update("B=(int)i");
        final File pqDotFile = new File(secondPartition, ".dotFile.parquet");
        ParquetTools.writeTable(anotherTable, pqDotFile);
        fromDisk = ParquetTools.readTable(parentDir);
        TstUtils.assertTableEquals(fromDisk, partitionedTable);
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

        String firstIndexFilePath = ".dh_metadata/indexes/vvv/index_vvv_firstTable.parquet";
        String secondIndexFilePath = ".dh_metadata/indexes/vvv/index_vvv_secondTable.parquet";
        verifyFilesInDir(parentDir, new String[] {firstFilename, secondFilename},
                Map.of("vvv", new String[] {firstIndexFilePath, secondIndexFilePath}));

        // Verify that the key-value metadata in the file has the correct name
        ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(firstDestFile, 0, null);
        String metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(firstIndexFilePath));
        tableLocationKey = new ParquetTableLocationKey(secondDestFile, 0, null);
        metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(secondIndexFilePath));

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
        String vvvIndexFilePath = ".dh_metadata/indexes/vvv/index_vvv_groupingColumnsWriteTests.parquet";

        // Write a new table successfully at the same position with different grouping columns
        final TableDefinition anotherTableDefinition = TableDefinition.of(ColumnDefinition.ofInt("xxx").withGrouping());
        Table anotherTableToSave = TableTools.newTable(anotherTableDefinition, TableTools.col("xxx", data));
        writer.writeTable(anotherTableToSave, destFile);
        final String xxxIndexFilePath = ".dh_metadata/indexes/xxx/index_xxx_groupingColumnsWriteTests.parquet";

        // The directory now should contain the updated table, its grouping file for column xxx, and old grouping file
        // for column vvv
        verifyFilesInDir(parentDir, new String[] {destFilename},
                Map.of("vvv", new String[] {vvvIndexFilePath},
                        "xxx", new String[] {xxxIndexFilePath}));

        Table fromDisk = ParquetTools.readTable(destFile);
        TstUtils.assertTableEquals(fromDisk, anotherTableToSave);

        ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(destFile, 0, null);
        String metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(xxxIndexFilePath) && !metadataString.contains(vvvIndexFilePath));

        // Overwrite the table
        writer.writeTable(anotherTableToSave, destFile);

        // The directory should still contain the updated table, its grouping file for column xxx, and old grouping file
        // for column vvv
        final File xxxIndexFile = new File(parentDir, xxxIndexFilePath);
        final File backupXXXIndexFile = ParquetTools.getBackupFile(xxxIndexFile);
        final String backupXXXIndexFileName = backupXXXIndexFile.getName();
        verifyFilesInDir(parentDir, new String[] {destFilename},
                Map.of("vvv", new String[] {vvvIndexFilePath},
                        "xxx", new String[] {xxxIndexFilePath}));

        tableLocationKey = new ParquetTableLocationKey(destFile, 0, null);
        metadataString = tableLocationKey.getMetadata().getFileMetaData().toString();
        assertTrue(metadataString.contains(xxxIndexFilePath) && !metadataString.contains(vvvIndexFilePath)
                && !metadataString.contains(backupXXXIndexFileName));
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
    public void dictionaryEncodingTest() {
        Collection<String> columns = new ArrayList<>(Arrays.asList(
                "shortStringColumn = `Row ` + i",
                "longStringColumn = `This is row ` + i",
                "someIntColumn = i"));
        final int numRows = 10;
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setMaximumDictionarySize(100) // Force "longStringColumn" to use non-dictionary encoding
                .build();
        final Table stringTable = TableTools.emptyTable(numRows).select(Selectable.from(columns));
        final File dest = new File(rootFile + File.separator + "dictEncoding.parquet");
        ParquetTools.writeTable(stringTable, dest, writeInstructions);
        Table fromDisk = ParquetTools.readTable(dest);
        assertTableEquals(stringTable, fromDisk);

        // Verify that string columns are properly dictionary encoded
        final ParquetMetadata metadata = new ParquetTableLocationKey(dest, 0, null).getMetadata();
        final String firstColumnMetadata = metadata.getBlocks().get(0).getColumns().get(0).toString();
        assertTrue(firstColumnMetadata.contains("shortStringColumn") && firstColumnMetadata.contains("RLE_DICTIONARY"));
        final String secondColumnMetadata = metadata.getBlocks().get(0).getColumns().get(1).toString();
        assertTrue(
                secondColumnMetadata.contains("longStringColumn") && !secondColumnMetadata.contains("RLE_DICTIONARY"));
        final String thirdColumnMetadata = metadata.getBlocks().get(0).getColumns().get(2).toString();
        assertTrue(thirdColumnMetadata.contains("someIntColumn") && !thirdColumnMetadata.contains("RLE_DICTIONARY"));
    }

    @Test
    public void overflowingStringsTest() {
        // Test the behavior of writing parquet files if entries exceed the page size limit
        final int pageSize = ParquetInstructions.MIN_TARGET_PAGE_SIZE;
        final char[] data = new char[pageSize / 4];
        String someString = new String(data);
        Collection<String> columns = new ArrayList<>(Arrays.asList(
                "someStringColumn = `" + someString + "` + i%10"));
        final long numRows = 10;
        ColumnChunkMetaData columnMetadata = overflowingStringsTestHelper(columns, numRows, pageSize);
        String metadataStr = columnMetadata.toString();
        assertTrue(metadataStr.contains("someStringColumn") && metadataStr.contains("PLAIN")
                && !metadataStr.contains("RLE_DICTIONARY"));

        // We exceed page size on hitting 4 rows, and we have 10 total rows.
        // Therefore, we should have total 4 pages containing 3, 3, 3, 1 rows respectively.
        assertEquals(columnMetadata.getEncodingStats().getNumDataPagesEncodedAs(Encoding.PLAIN), 4);

        final char[] veryLongData = new char[pageSize];
        someString = new String(veryLongData);
        columns = new ArrayList<>(
                Arrays.asList("someStringColumn =  ii % 2 == 0 ? Long.toString(ii) : `" + someString + "` + ii"));
        columnMetadata = overflowingStringsTestHelper(columns, numRows, pageSize);
        // We will have 10 pages each containing 1 row.
        assertEquals(columnMetadata.getEncodingStats().getNumDataPagesEncodedAs(Encoding.PLAIN), 10);

        // Table with rows of null alternating with strings exceeding the page size
        columns = new ArrayList<>(Arrays.asList("someStringColumn =  ii % 2 == 0 ? null : `" + someString + "` + ii"));
        columnMetadata = overflowingStringsTestHelper(columns, numRows, pageSize);
        // We will have 6 pages containing 1, 2, 2, 2, 2, 1 rows.
        assertEquals(columnMetadata.getEncodingStats().getNumDataPagesEncodedAs(Encoding.PLAIN), 6);
    }

    private static ColumnChunkMetaData overflowingStringsTestHelper(final Collection<String> columns,
            final long numRows, final int pageSize) {
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setTargetPageSize(pageSize) // Force a small page size to cause splitting across pages
                .setMaximumDictionarySize(50) // Force "someStringColumn" to use non-dictionary encoding
                .build();
        Table stringTable = TableTools.emptyTable(numRows).select(Selectable.from(columns));
        final File dest = new File(rootFile + File.separator + "overflowingStringsTest.parquet");
        ParquetTools.writeTable(stringTable, dest, writeInstructions);
        Table fromDisk = ParquetTools.readTable(dest).select();
        assertTableEquals(stringTable, fromDisk);

        ParquetMetadata metadata = new ParquetTableLocationKey(dest, 0, null).getMetadata();
        ColumnChunkMetaData columnMetadata = metadata.getBlocks().get(0).getColumns().get(0);
        return columnMetadata;
    }

    @Test
    public void overflowingCodecsTest() {
        final int pageSize = ParquetInstructions.MIN_TARGET_PAGE_SIZE;
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setTargetPageSize(pageSize) // Force a small page size to cause splitting across pages
                .addColumnCodec("VariableWidthByteArrayColumn", SimpleByteArrayCodec.class.getName())
                .build();

        final ColumnDefinition<byte[]> columnDefinition =
                ColumnDefinition.fromGenericType("VariableWidthByteArrayColumn", byte[].class, byte.class);
        final TableDefinition tableDefinition = TableDefinition.of(columnDefinition);
        final byte[] byteArray = new byte[pageSize / 2];
        final Table table = TableTools.newTable(tableDefinition,
                TableTools.col("VariableWidthByteArrayColumn", byteArray, byteArray, byteArray));

        final File dest = new File(rootFile + File.separator + "overflowingCodecsTest.parquet");
        ParquetTools.writeTable(table, dest, writeInstructions);
        Table fromDisk = ParquetTools.readTable(dest).select();
        assertTableEquals(table, fromDisk);

        final ParquetMetadata metadata = new ParquetTableLocationKey(dest, 0, null).getMetadata();
        final String metadataStr = metadata.getFileMetaData().getKeyValueMetaData().get("deephaven");
        assertTrue(
                metadataStr.contains("VariableWidthByteArrayColumn") && metadataStr.contains("SimpleByteArrayCodec"));
        final ColumnChunkMetaData columnMetadata = metadata.getBlocks().get(0).getColumns().get(0);
        final String columnMetadataStr = columnMetadata.toString();
        assertTrue(columnMetadataStr.contains("VariableWidthByteArrayColumn") && columnMetadataStr.contains("PLAIN"));
        // Each byte array is of half the page size. So we exceed page size on hitting 3 byteArrays.
        // Therefore, we should have total 2 pages containing 2, 1 rows respectively.
        assertEquals(columnMetadata.getEncodingStats().getNumDataPagesEncodedAs(Encoding.PLAIN), 2);
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

    @Test
    public void readWriteDateTimeTest() {
        final int NUM_ROWS = 1000;
        final Table table = TableTools.emptyTable(NUM_ROWS).view(
                "someDateColumn = java.time.LocalDate.ofEpochDay(i)",
                "someTimeColumn = java.time.LocalTime.of(i%24, i%60, (i+10)%60)",
                "someLocalDateTimeColumn = java.time.LocalDateTime.of(2000+i%10, i%12+1, i%30+1, (i+4)%24, (i+5)%60, (i+6)%60, i)",
                "someInstantColumn = DateTimeUtils.now() + i").select();
        final File dest = new File(rootFile, "readWriteDateTimeTest.parquet");
        writeReadTableTest(table, dest);

        // Verify that the types are correct in the schema
        final ParquetMetadata metadata = new ParquetTableLocationKey(dest, 0, null).getMetadata();
        final ColumnChunkMetaData dateColMetadata = metadata.getBlocks().get(0).getColumns().get(0);
        assertTrue(dateColMetadata.toString().contains("someDateColumn"));
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, dateColMetadata.getPrimitiveType().getPrimitiveTypeName());
        assertEquals(LogicalTypeAnnotation.dateType(), dateColMetadata.getPrimitiveType().getLogicalTypeAnnotation());

        final ColumnChunkMetaData timeColMetadata = metadata.getBlocks().get(0).getColumns().get(1);
        assertTrue(timeColMetadata.toString().contains("someTimeColumn"));
        assertEquals(PrimitiveType.PrimitiveTypeName.INT64, timeColMetadata.getPrimitiveType().getPrimitiveTypeName());
        assertEquals(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
                timeColMetadata.getPrimitiveType().getLogicalTypeAnnotation());

        final ColumnChunkMetaData localDateTimeColMetadata = metadata.getBlocks().get(0).getColumns().get(2);
        assertTrue(localDateTimeColMetadata.toString().contains("someLocalDateTimeColumn"));
        assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
                localDateTimeColMetadata.getPrimitiveType().getPrimitiveTypeName());
        assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS),
                localDateTimeColMetadata.getPrimitiveType().getLogicalTypeAnnotation());

        final ColumnChunkMetaData instantColMetadata = metadata.getBlocks().get(0).getColumns().get(3);
        assertTrue(instantColMetadata.toString().contains("someInstantColumn"));
        assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
                instantColMetadata.getPrimitiveType().getPrimitiveTypeName());
        assertEquals(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
                instantColMetadata.getPrimitiveType().getLogicalTypeAnnotation());
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
        final Table pyarrowFromDisk;
        try {
            pyarrowFromDisk = ParquetTools.readTable(pyarrowDest);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof InvalidParquetFileException) {
                final String InvalidParquetFileErrorMsgString = "Invalid parquet file detected, please ensure the " +
                        "file is fetched properly from Git LFS. Run commands 'git lfs install; git lfs pull' inside " +
                        "the repo to pull the files from LFS. Check cause of exception for more details.";
                throw new UncheckedDeephavenException(InvalidParquetFileErrorMsgString, e.getCause());
            }
            throw e;
        }
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

    @Test
    public void inferParquetOrderLastKey() {
        // Create an empty parent directory
        final File parentDir = new File(rootFile, "inferParquetOrder");
        parentDir.mkdir();
        final TableDefinition td1 = TableDefinition.of(ColumnDefinition.ofInt("Foo"));
        final TableDefinition td2 =
                TableDefinition.of(ColumnDefinition.ofInt("Foo"), ColumnDefinition.ofString("Bar"));
        ParquetTools.writeTable(TableTools.newTable(td1), new File(parentDir, "01.parquet"));
        ParquetTools.writeTable(TableTools.newTable(td2), new File(parentDir, "02.parquet"));
        final Table table = ParquetTools.readTable(parentDir);
        assertEquals(td2, table.getDefinition());
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
