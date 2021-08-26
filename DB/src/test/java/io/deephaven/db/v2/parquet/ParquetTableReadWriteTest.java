package io.deephaven.db.v2.parquet;

import io.deephaven.api.Selectable;
import io.deephaven.base.FileUtils;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.StringSetArrayWrapper;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class ParquetTableReadWriteTest {

    private static final String ROOT_FILENAME = ParquetTableReadWriteTest.class.getName() + "_root";

    private static File rootFile;

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

    private static Table getTableFlat(int size, boolean includeSerializable) {
        QueryLibrary.importClass(ParquetTableWriter.SomeSillyTest.class);
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
                        "someTime = DBDateTime.now() + i",
                        "someKey = `` + (int)(i /100)",
                        "nullKey = i < -1?`123`:null"));
        if (includeSerializable) {
            columns.add("someSerializable = new SomeSillyTest(i)");
        }
        return TableTools.emptyTable(size).select(
                Selectable.from(columns));
    }

    private static Table getOneColumnTableFlat(int size) {
        QueryLibrary.importClass(ParquetTableWriter.SomeSillyTest.class);
        return TableTools.emptyTable(size).select(
                // "someBoolColumn = i % 3 == 0?true:i%3 == 1?false:null"
                "someIntColumn = i % 3 == 0 ? null:i");
    }

    private static Table getGroupedOneColumnTable(int size) {
        Table t = getOneColumnTableFlat(size);
        QueryLibrary.importClass(StringSetArrayWrapper.class);
        QueryLibrary.importClass(StringSet.class);
        Table result = t.by("groupKey = i % 100 + (int)(i/10)");
        result = result.select(result.getDefinition().getColumnNames().stream()
                .map(name -> name.equals("groupKey") ? name
                        : (name + " = i % 5 == 0 ? null:(i%3 == 0?" + name + ".subArray(0,0):" + name
                                + ")"))
                .toArray(String[]::new));
        return result;
    }

    private static Table getEmptyArray(int size) {
        QueryLibrary.importClass(ParquetTableWriter.SomeSillyTest.class);
        return TableTools.emptyTable(size).select(
                "someEmptyString = new String[0]",
                "someEmptyInt = new int[0]",
                "someEmptyBool = new Boolean[0]",
                "someEmptyObject = new SomeSillyTest[0]");
    }

    private static Table getGroupedTable(int size, boolean includeSerializable) {
        Table t = getTableFlat(size, includeSerializable);
        QueryLibrary.importClass(StringSetArrayWrapper.class);
        QueryLibrary.importClass(StringSet.class);
        Table result = t.by("groupKey = i % 100 + (int)(i/10)");
        result = result.select(result.getDefinition().getColumnNames().stream()
                .map(name -> name.equals("groupKey") ? name
                        : (name + " = i % 5 == 0 ? null:(i%3 == 0?" + name + ".subArray(0,0):" + name
                                + ")"))
                .toArray(String[]::new));
        result = result.update(
                "someStringSet = (StringSet)new StringSetArrayWrapper( ((Object)nonNullString) == null?new String[0]:(String[])nonNullString.toArray())");
        result = result.update(
                "largeStringSet = (StringSet)new StringSetArrayWrapper(((Object)nonNullPolyString) == null?new String[0]:(String[])nonNullPolyString.toArray())");
        result = result.update(
                "someStringColumn = (String[])(((Object)someStringColumn) == null?null:someStringColumn.toArray())",
                "nonNullString = (String[])(((Object)nonNullString) == null?null:nonNullString.toArray())",
                "nonNullPolyString = (String[])(((Object)nonNullPolyString) == null?null:nonNullPolyString.toArray())",
                "someBoolColumn = (Boolean[])(((Object)someBoolColumn) == null?null:someBoolColumn.toArray())",
                "someTime = (DBDateTime[])(((Object)someTime) == null?null:someTime.toArray())");
        return result;
    }

    private void flatTable(String tableName, int size, boolean includeSerializable) {
        final Table tableToSave = getTableFlat(size, includeSerializable);
        final File dest = new File(rootFile, "ParquetTest_" + tableName + "_test.parquet");
        ParquetTools.writeTable(tableToSave, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(tableToSave, fromDisk);
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
        flatTable("largeFlatParquet", 4000000, false);
    }

    @Test
    public void vectorParquetFormat() {
        testEmptyArrayStore("smallEmpty", 20);
        groupedOneColumnTable("smallAggOneColumn", 20);
        groupedTable("smallAggParquet", 20, true);
        testEmptyArrayStore("largeEmpty", 4000000);
        groupedOneColumnTable("largeAggOneColumn", 4000000);
        groupedTable("largeAggParquet", 4000000, false);
    }

    @Test
    public void groupingByLongKey() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("someInt"),
                ColumnDefinition.ofLong("someLong").withGrouping());
        final Table testTable =
                ((QueryTable) TableTools.emptyTable(10).select("someInt = i", "someLong  = ii % 3")
                        .by("someLong").ungroup("someInt")).withDefinitionUnsafe(definition);
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
                        .where("i % 2 == 0").by("someString").ungroup("someInt"))
                                .withDefinitionUnsafe(definition);
        final File dest = new File(rootFile, "ParquetTest_groupByString_test.parquet");
        ParquetTools.writeTable(testTable, dest);
        final Table fromDisk = ParquetTools.readTable(dest);
        TstUtils.assertTableEquals(fromDisk, testTable);
        TestCase.assertNotNull(fromDisk.getColumnSource("someString").getGroupToRange());
    }

    @Test
    public void groupingByBigInt() {
        QueryLibrary.importClass(BigInteger.class);
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("someInt"),
                ColumnDefinition.fromGenericType("someBigInt", BigInteger.class).withGrouping());
        final Table testTable = ((QueryTable) TableTools.emptyTable(10)
                .select("someInt = i", "someBigInt  =  BigInteger.valueOf(i % 3)").where("i % 2 == 0")
                .by("someBigInt").ungroup("someInt")).withDefinitionUnsafe(definition);
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
            final Table table1 = getTableFlat(10000, false);
            ParquetTools.writeTable(table1, path);
            assertTrue(new File(path).length() > 0);
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
}
