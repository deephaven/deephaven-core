//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import com.google.common.io.BaseEncoding;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.UncoalescedTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.base.InvalidParquetFileException;
import io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.parquet.table.location.ParquetFieldIdColumnResolverFactory;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.qst.type.Type;
import io.deephaven.stringset.HashStringSet;
import io.deephaven.stringset.StringSet;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import io.deephaven.vector.*;
import junit.framework.TestCase;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.Assertions;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.tableRangesAreEqual;
import static io.deephaven.engine.util.TableTools.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
        final Table result = ParquetTools.readTable(path);
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
        assertEquals(10, test2.size());
        assertEquals(2, test2.numColumns());
        assertEquals(ColumnVectors.of(test, "enumC"), ColumnVectors.of(test2, "enumC"));
        assertEquals(ColumnVectors.of(test, "enumSet"), ColumnVectors.of(test2, "enumSet"));
        StringSet[] objects = ColumnVectors.ofObject(test, "enumSet", StringSet.class).toArray();
        StringSet[] objects1 = ColumnVectors.ofObject(test2, "enumSet", StringSet.class).toArray();
        for (int i = 0; i < objects1.length; i++) {
            assertEquals(new HashSet<>(Arrays.asList(objects[i].values())),
                    new HashSet<>(Arrays.asList(objects1[i].values())));
        }
        test2.close();

        test = TableTools.emptyTable(10).select("enumC=TestEnum.values()[i]",
                "enumSet=EnumSet.of((TestEnum)enumC_[(i + 9) % 10],(TestEnum)enumC_[i],(TestEnum)enumC_[(i+1)% 10])");
        path = testRoot + File.separator + "Table3.parquet";
        ParquetTools.writeTable(test, path);
        test2 = ParquetTools.readTable(path);
        assertEquals(10, test2.size());
        assertEquals(2, test2.numColumns());
        assertEquals(ColumnVectors.of(test, "enumC"), ColumnVectors.of(test2, "enumC"));
        assertEquals(ColumnVectors.of(test, "enumSet"), ColumnVectors.of(test2, "enumSet"));
        test2.close();

        test = TableTools.newTable(TableDefinition.of(
                ColumnDefinition.ofInt("anInt"),
                ColumnDefinition.ofString("aString")),
                col("anInt", 1, 2, 3),
                col("aString", "ab", "ab", "bc"));

        DataIndexer.getOrCreateDataIndex(test, "aString");
        path = testRoot + File.separator + "Table4.parquet";
        ParquetTools.writeTable(test, path);

        test2 = ParquetTools.readTable(path);
        assertTrue(DataIndexer.hasDataIndex(test2, "aString"));
        test2.close();
    }

    @Test
    public void testWriteTableRenames() {
        final String path = testRoot + File.separator + "Table_W_Renames.parquet";
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addColumnNameMapping("X", "StringKeys")
                .addColumnNameMapping("Y", "GroupedInts")
                .build();
        ParquetTools.writeTable(table1, path, instructions);

        final Table resultDefault = ParquetTools.readTable(path);
        TableTools.show(table1);
        TableTools.show(resultDefault);
        assertTableEquals(table1.view("X=StringKeys", "Y=GroupedInts"), resultDefault);
        resultDefault.close();

        final Table resultRenamed = ParquetTools.readTable(path, instructions);
        TableTools.show(table1);
        TableTools.show(resultRenamed);
        assertTableEquals(table1, resultRenamed);
        resultRenamed.close();
    }

    @Test
    public void testWriteTableEmpty() {
        final String path = testRoot + File.separator + "Empty.parquet";
        ParquetTools.writeTable(emptyTable, path);
        final Table result = ParquetTools.readTable(path);
        tableRangesAreEqual(emptyTable, result, 0, 0, emptyTable.size());
        result.close();
    }

    @Test
    public void testWriteTableNoColumns() {
        final Table source = TableTools.emptyTable(100);
        final String path = testRoot + File.separator + "NoColumns.parquet";
        try {
            ParquetTools.writeTable(source, path);
            TestCase.fail("Expected exception");
        } catch (TableDataException expected) {
        }
        try {
            ParquetTools.writeTables(new Table[] {source}, new String[] {path},
                    ParquetInstructions.EMPTY.withTableDefinition(source.getDefinition()));
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
        ParquetTools.writeTables(new Table[] {TableTools.emptyTable(10_000L)},
                new String[] {dest.getPath()},
                ParquetInstructions.EMPTY.withTableDefinition(nullTable.getDefinition()));
        final Table result = ParquetTools.readTable(dest.getPath());
        assertTableEquals(nullTable, result);
        result.close();
    }

    @Test
    public void testWriteTableExceptions() throws IOException {
        new File(testRoot + File.separator + "unexpectedFile").createNewFile();
        try {
            ParquetTools.writeTable(table1, testRoot + File.separator + "unexpectedFile" + File.separator + "Table1");
            TestCase.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        new File(testRoot + File.separator + "Table1").mkdirs();
        new File(testRoot + File.separator + "Table1" + File.separator + "extraFile").createNewFile();
        try {
            ParquetTools.writeTable(table1, testRoot + File.separator + "Table1");
            TestCase.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        new File(testRoot + File.separator + "Nested").mkdirs();
        try {
            ParquetTools.writeTable(brokenTable, testRoot + File.separator + "Nested" + File.separator + "Broken");
            TestCase.fail("Expected exception");
        } catch (UnsupportedOperationException e) {
            // Expected exception
        }
        TestCase.assertFalse(new File(testRoot + File.separator + "Nested" + File.separator + "Broken").exists());
        TestCase.assertTrue(new File(testRoot + File.separator + "Nested").isDirectory());

        new File(testRoot + File.separator + "Nested").setReadOnly();
        try {
            ParquetTools.writeTable(brokenTable, testRoot + File.separator + "Nested" + File.separator + "Broken");
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
        final String path = testRoot + File.separator + "Table1.parquet";
        ParquetTools.writeTable(table1, path);
        final Table result = ParquetTools.readTable(path);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());
        result.close();
        ParquetTools.deleteTable(path);
        TestCase.assertFalse(new File(path).exists());
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
        final String path = testRoot + File.separator + "testWriteAggregatedTable.parquet";
        final Table table = getAggregatedResultTable();
        final TableDefinition def = table.getDefinition();
        ParquetTools.writeTable(table, path, ParquetInstructions.EMPTY.withTableDefinition(def));
        final Table readBackTable = ParquetTools.readTable(path);
        TableTools.show(readBackTable);
        TableTools.show(table);
        final long sz = table.size();
        tableRangesAreEqual(table, readBackTable, 0, 0, sz);
        readBackTable.close();
    }

    @Test
    public void testPartitionedRead() {
        ParquetTools.writeTable(table1, new File(testRootFile,
                "Date=2021-07-20" + File.separator + "Num=200" + File.separator + "file1.parquet").getPath());
        ParquetTools.writeTable(table1, new File(testRootFile,
                "Date=2021-07-20" + File.separator + "Num=100" + File.separator + "file2.parquet").getPath());
        ParquetTools.writeTable(table1, new File(testRootFile,
                "Date=2021-07-21" + File.separator + "Num=300" + File.separator + "file3.parquet").getPath());

        final List<ColumnDefinition<?>> allColumns = new ArrayList<>();
        allColumns.add(
                ColumnDefinition.fromGenericType("Date", String.class, null, ColumnDefinition.ColumnType.Partitioning));
        allColumns.add(
                ColumnDefinition.fromGenericType("Num", int.class, null, ColumnDefinition.ColumnType.Partitioning));
        allColumns.addAll(table1.getDefinition().getColumns());
        final TableDefinition partitionedDefinition = TableDefinition.of(allColumns);

        final Table result = ParquetTools.readTable(
                ParquetKeyValuePartitionedLayout.create(testRootFile.toURI(), 2, ParquetInstructions.EMPTY, null),
                ParquetInstructions.EMPTY);
        TestCase.assertEquals(partitionedDefinition, result.getDefinition());
        final Table expected = TableTools.merge(
                table1.updateView("Date=`2021-07-20`", "Num=100"),
                table1.updateView("Date=`2021-07-20`", "Num=200"),
                table1.updateView("Date=`2021-07-21`", "Num=300")).moveColumnsUp("Date", "Num");
        assertTableEquals(expected, result);
    }

    @Test
    public void testBooleanPartition() {
        ParquetTools.writeTable(table1,
                new File(testRootFile, "Active=true" + File.separator + "file1.parquet").getPath());
        ParquetTools.writeTable(table1,
                new File(testRootFile, "Active=false" + File.separator + "file2.parquet").getPath());
        Table table = ParquetTools.readTable(testRootFile.getPath());
        Assert.assertTrue(table instanceof UncoalescedTable);
        final Table expected = TableTools.merge(
                table1.updateView("Active=false"),
                table1.updateView("Active=true")).moveColumnsUp("Active");

        assertTableEquals(expected, table);
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
        ParquetTools.writeTable(stuff, f2w.getPath());

        final Table readBack = ParquetTools.readTable(f2w.getPath());
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

    private static final String InvalidParquetFileErrorMsgString = "Invalid parquet file detected, please ensure " +
            "the file is fetched properly from Git LFS. Run commands 'git lfs install; git lfs pull' inside the repo " +
            "to pull the files from LFS. Check cause of exception for more details.";

    @Test
    public void e0() {
        try {
            final Table uncompressed =
                    ParquetTools.readTable(TestParquetTools.class.getResource("/e0/uncompressed.parquet").getFile());

            final Table gzip = ParquetTools.readTable(TestParquetTools.class.getResource("/e0/gzip.parquet").getFile());
            assertTableEquals(uncompressed, gzip);

            final Table lz4 = ParquetTools.readTable(TestParquetTools.class.getResource("/e0/lz4.parquet").getFile());
            assertTableEquals(uncompressed, lz4);

            final Table snappy =
                    ParquetTools.readTable(TestParquetTools.class.getResource("/e0/snappy.parquet").getFile());
            assertTableEquals(uncompressed, snappy);

            final Table zstd = ParquetTools.readTable(TestParquetTools.class.getResource("/e0/zstd.parquet").getFile());
            assertTableEquals(uncompressed, zstd);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof InvalidParquetFileException) {
                throw new UncheckedDeephavenException(InvalidParquetFileErrorMsgString, e.getCause());
            }
        }
    }

    @Test
    public void e1() {
        try {
            final Table uncompressed =
                    ParquetTools.readTable(TestParquetTools.class.getResource("/e1/uncompressed.parquet").getFile());

            final Table gzip = ParquetTools.readTable(TestParquetTools.class.getResource("/e1/gzip.parquet").getFile());
            assertTableEquals(uncompressed, gzip);

            final Table lz4 = ParquetTools.readTable(TestParquetTools.class.getResource("/e1/lz4.parquet").getFile());
            assertTableEquals(uncompressed, lz4);

            final Table snappy =
                    ParquetTools.readTable(TestParquetTools.class.getResource("/e1/snappy.parquet").getFile());
            assertTableEquals(uncompressed, snappy);

            final Table zstd = ParquetTools.readTable(TestParquetTools.class.getResource("/e1/zstd.parquet").getFile());
            assertTableEquals(uncompressed, zstd);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof InvalidParquetFileException) {
                throw new UncheckedDeephavenException(InvalidParquetFileErrorMsgString, e.getCause());
            }
        }
    }

    @Test
    public void e2() {
        try {
            final Table uncompressed =
                    ParquetTools.readTable(TestParquetTools.class.getResource("/e2/uncompressed.parquet").getFile());

            final Table gzip = ParquetTools.readTable(TestParquetTools.class.getResource("/e2/gzip.parquet").getFile());
            assertTableEquals(uncompressed, gzip);

            final Table lz4 = ParquetTools.readTable(TestParquetTools.class.getResource("/e2/lz4.parquet").getFile());
            assertTableEquals(uncompressed, lz4);

            final Table snappy =
                    ParquetTools.readTable(TestParquetTools.class.getResource("/e2/snappy.parquet").getFile());
            assertTableEquals(uncompressed, snappy);

            final Table zstd = ParquetTools.readTable(TestParquetTools.class.getResource("/e2/zstd.parquet").getFile());
            assertTableEquals(uncompressed, zstd);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof InvalidParquetFileException) {
                throw new UncheckedDeephavenException(InvalidParquetFileErrorMsgString, e.getCause());
            }
        }
    }

    private void testWriteRead(Table source, Function<Table, Table> transform) {
        final File f2w = new File(testRoot, "testWriteRead.parquet");
        ParquetTools.writeTable(source, f2w.getPath());
        final Table readBack = ParquetTools.readTable(f2w.getPath());
        assertTableEquals(transform.apply(source), transform.apply(readBack));
    }

    // This method is used in a formula. Do not remove
    @SuppressWarnings("unused")
    public static DoubleVector generateDoubles(int howMany) {
        final double[] yarr = new double[howMany];
        for (int ii = 0; ii < howMany; ii++) {
            yarr[ii] = ii;
        }
        return new DoubleVectorDirect(yarr);
    }

    // This method is used in a formula. Do not remove
    @SuppressWarnings("unused")
    public static FloatVector generateFloats(int howMany) {
        final float[] yarr = new float[howMany];
        for (int ii = 0; ii < howMany; ii++) {
            yarr[ii] = ii;
        }
        return new FloatVectorDirect(yarr);
    }

    // This method is used in a formula. Do not remove
    @SuppressWarnings("unused")
    public static ObjectVector<String> makeSillyStringArray(int howMany) {
        final String[] fireTruck = new String[howMany];
        for (int ii = 0; ii < howMany; ii++) {
            fireTruck[ii] = String.format("%04d", ii);
        }
        return new ObjectVectorDirect<>(fireTruck);
    }

    /**
     * This test checks that we can read old parquet files that don't properly have dictionary page offset set, and also
     * that we can read int96 columns with null values.
     */
    @Test
    public void testNoDictionaryOffset() {
        final Table withNullsAndMissingOffsets =
                ParquetTools.readTable(TestParquetTools.class.getResource("/dictOffset/dh17395.parquet").getFile());
        final Table clean = ParquetTools
                .readTable(TestParquetTools.class.getResource("/dictOffset/dh17395_clean.parquet").getFile());

        assertEquals("D2442F78-AEFB-49F7-85A0-00506BBE74D4",
                withNullsAndMissingOffsets.getColumnSource("LISTID").get(0));
        assertNull(withNullsAndMissingOffsets.getColumnSource("TIMESTAMP").get(0));
        assertNull(withNullsAndMissingOffsets.getColumnSource("SETTLEMENT_DATE").get(0));
        assertEquals(1698672050703000000L,
                DateTimeUtils.epochNanos((Instant) withNullsAndMissingOffsets.getColumnSource("CREATE_DATE").get(0)));
        assertTableEquals(withNullsAndMissingOffsets, clean);
    }

    @Test
    public void testWriteParquetFieldIds() throws NoSuchAlgorithmException, IOException {
        final int BAZ_ID = 111;
        final int ZAP_ID = 112;
        final String BAZ = "Baz";
        final String ZAP = "Zap";
        final String BAZ_PARQUET_NAME = "Some Random Parquet Column Name";
        final String ZAP_PARQUET_NAME = "ABCDEFG";
        final ColumnDefinition<Long> bazCol = ColumnDefinition.ofLong(BAZ);
        final ColumnDefinition<?> zapCol = ColumnDefinition.of(ZAP, Type.stringType().arrayType());
        final TableDefinition td = TableDefinition.of(bazCol, zapCol);
        final Table expected = newTable(td,
                longCol(BAZ, 99, 101),
                new ColumnHolder<>(ZAP, String[].class, String.class, false, new String[] {"Foo", "Bar"},
                        new String[] {"Hello"}));
        final File file = new File(testRoot, "testWriteParquetFieldIds.parquet");
        {
            final ParquetInstructions writeInstructions = ParquetInstructions.builder()
                    .setFieldId(BAZ, BAZ_ID)
                    .setFieldId(ZAP, ZAP_ID)
                    .addColumnNameMapping(BAZ_PARQUET_NAME, BAZ)
                    .addColumnNameMapping(ZAP_PARQUET_NAME, ZAP)
                    .build();
            ParquetTools.writeTable(expected, file.getPath(), writeInstructions);
        }

        {
            final MessageType expectedSchema = Types.buildMessage()
                    .optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .id(BAZ_ID)
                    .named(BAZ_PARQUET_NAME)
                    .optionalList()
                    .id(ZAP_ID)
                    .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ZAP_PARQUET_NAME)
                    .named(MappedSchema.SCHEMA_NAME);
            final MessageType actualSchema = readSchema(file);
            assertEquals(expectedSchema, actualSchema);
        }

        //
        // This is somewhat fragile, but has been manually verified to contain the field_ids that we expect.
        // We may want to consider more explicit tests that verify our writing logic is consistent, as it would be good
        // to know whenever serialization changes in any way.
        assertEquals("2ea68b0ddaeb432e9c2721f15460b6c42449a479c1960e836f6ebe3b14f33dc1", sha256sum(file.toPath()));

        // This test is a bit circular; but assuming we trust our reading code, we should have relative confidence that
        // we are writing it down correctly if we can read it correctly.
        {
            final ParquetInstructions readInstructions = ParquetInstructions.builder()
                    .setTableDefinition(td)
                    .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(
                            BAZ, BAZ_ID,
                            ZAP, ZAP_ID)))
                    .build();
            {
                final Table actual = ParquetTools.readTable(file.getPath(), readInstructions);
                assertEquals(td, actual.getDefinition());
                assertTableEquals(expected, actual);
            }
        }
    }

    /**
     * This data was generated via the script:
     *
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * field_id = b"PARQUET:field_id"
     * fields = [
     *     pa.field(
     *         "e0cf7927-45dc-4dfc-b4ef-36bf4b6ae463", pa.int64(), metadata={field_id: b"0"}
     *     ),
     *     pa.field(
     *         "53f0de5a-e06f-476e-b82a-a3f9294fcd05", pa.string(), metadata={field_id: b"1"}
     *     ),
     * ]
     * table = pa.table([[99, 101], ["Foo", "Bar"]], schema=pa.schema(fields))
     * pq.write_table(table, "ReferenceSimpleParquetFieldIds.parquet")
     * </pre>
     *
     * @see <a href="https://arrow.apache.org/docs/cpp/parquet.html#parquet-field-id">Arrow Parquet field_id</a>
     */
    @Test
    public void testParquetFieldIds() {
        final String file = TestParquetTools.class.getResource("/ReferenceSimpleParquetFieldIds.parquet").getFile();

        // No instructions; will sanitize the names. Both columns get the "-" removed and the second column gets the
        // "column_" prefix added because it starts with a digit.
        {
            final TableDefinition expectedInferredTD = TableDefinition.of(
                    ColumnDefinition.ofLong("e0cf792745dc4dfcb4ef36bf4b6ae463"),
                    ColumnDefinition.ofString("column_53f0de5ae06f476eb82aa3f9294fcd05"));
            final Table table = ParquetTools.readTable(file);
            assertEquals(expectedInferredTD, table.getDefinition());
            assertTableEquals(newTable(expectedInferredTD,
                    longCol("e0cf792745dc4dfcb4ef36bf4b6ae463", 99, 101),
                    stringCol("column_53f0de5ae06f476eb82aa3f9294fcd05", "Foo", "Bar")), table);
        }

        final int BAZ_ID = 0;
        final int ZAP_ID = 1;
        final String BAZ = "Baz";
        final String ZAP = "Zap";
        final ColumnDefinition<Long> bazCol = ColumnDefinition.ofLong(BAZ);
        final ColumnDefinition<String> zapCol = ColumnDefinition.ofString(ZAP);

        final TableDefinition td = TableDefinition.of(bazCol, zapCol);
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(td)
                .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(BAZ, BAZ_ID, ZAP, ZAP_ID)))
                .build();

        // But, the user can still provide a TableDefinition
        {
            final Table table = ParquetTools.readTable(file, instructions.withTableDefinition(td));
            assertEquals(td, table.getDefinition());
            assertTableEquals(newTable(td,
                    longCol(BAZ, 99, 101),
                    stringCol(ZAP, "Foo", "Bar")), table);
        }

        // The user can provide the full mapping, but still a more limited definition
        {
            final TableDefinition justIdTD = TableDefinition.of(bazCol);
            final Table table = ParquetTools.readTable(file, instructions.withTableDefinition(justIdTD));
            assertEquals(justIdTD, table.getDefinition());
            assertTableEquals(newTable(justIdTD,
                    longCol(BAZ, 99, 101)), table);
        }

        // If only a partial id mapping is provided, only that will be "properly" mapped
        {
            final TableDefinition partialTD = TableDefinition.of(
                    ColumnDefinition.ofLong(BAZ),
                    ColumnDefinition.ofString("column_53f0de5ae06f476eb82aa3f9294fcd05"));
            final ParquetInstructions partialInstructions = ParquetInstructions.builder()
                    .setTableDefinition(partialTD)
                    .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(BAZ, BAZ_ID)))
                    .build();
            final Table table = ParquetTools.readTable(file, partialInstructions);
            assertEquals(partialTD, table.getDefinition());
        }

        // There are no errors if a field ID is configured but not found
        {
            final Table table = ParquetTools.readTable(file, ParquetInstructions.builder()
                    .setTableDefinition(td)
                    .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(
                            BAZ, BAZ_ID,
                            ZAP, ZAP_ID,
                            "Fake", 99)))
                    .build());
            assertEquals(td, table.getDefinition());
            assertTableEquals(newTable(td,
                    longCol(BAZ, 99, 101),
                    stringCol(ZAP, "Foo", "Bar")), table);
        }

        // If it's explicitly asked for, like other columns, it will return an appropriate null value
        {
            final TableDefinition tdWithFake =
                    TableDefinition.of(bazCol, zapCol, ColumnDefinition.ofShort("Fake"));
            final Table table = ParquetTools.readTable(file, ParquetInstructions.builder()
                    .setTableDefinition(tdWithFake)
                    .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(
                            BAZ, BAZ_ID,
                            ZAP, ZAP_ID,
                            "Fake", 99)))
                    .build());
            assertEquals(tdWithFake, table.getDefinition());
            assertTableEquals(newTable(tdWithFake,
                    longCol(BAZ, 99, 101),
                    stringCol(ZAP, "Foo", "Bar"),
                    shortCol("Fake", QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT)), table);
        }

        // You can even re-use IDs to get the same physical column out multiple times
        {
            final String BAZ_DUPE = "BazDupe";
            final TableDefinition dupeTd =
                    TableDefinition.of(bazCol, zapCol, ColumnDefinition.ofLong(BAZ_DUPE));
            final ParquetInstructions dupeInstructions = ParquetInstructions.builder()
                    .setTableDefinition(dupeTd)
                    .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(
                            BAZ, BAZ_ID,
                            ZAP, ZAP_ID,
                            BAZ_DUPE, BAZ_ID)))
                    .build();
            {
                final Table table = ParquetTools.readTable(file, dupeInstructions);
                assertEquals(dupeTd, table.getDefinition());
                assertTableEquals(newTable(dupeTd,
                        longCol(BAZ, 99, 101),
                        stringCol(ZAP, "Foo", "Bar"),
                        longCol(BAZ_DUPE, 99, 101)), table);
            }
        }

        // If both a field id and parquet column name mapping is provided, column resolution wins
        {
            final TableDefinition bazTd = TableDefinition.of(bazCol);
            final ParquetInstructions inconsistent = ParquetInstructions.builder()
                    .setTableDefinition(bazTd)
                    .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(BAZ, BAZ_ID)))
                    .addColumnNameMapping("53f0de5a-e06f-476e-b82a-a3f9294fcd05", BAZ)
                    .build();
            final Table table = ParquetTools.readTable(file, inconsistent);
            assertEquals(bazTd, table.getDefinition());
            assertTableEquals(newTable(bazTd, longCol(BAZ, 99, 101)), table);
        }
    }

    /**
     * This data was generated via the script:
     *
     * <pre>
     * import uuid
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     *
     * def write_to(path: str):
     *     field_id = b"PARQUET:field_id"
     *     fields = [
     *         pa.field(str(uuid.uuid4()), pa.int64(), metadata={field_id: b"42"}),
     *         pa.field(str(uuid.uuid4()), pa.string(), metadata={field_id: b"43"}),
     *     ]
     *     table = pa.table([[] for _ in fields], schema=pa.schema(fields))
     *     pq.write_table(table, path)
     *
     *
     * write_to("/ReferencePartitionedFieldIds/Partition=0/table.parquet")
     * write_to("/ReferencePartitionedFieldIds/Partition=1/table.parquet")
     * </pre>
     *
     * It mimics the case of a higher-level schema management where the physical column names may be random.
     *
     * @see <a href="https://arrow.apache.org/docs/cpp/parquet.html#parquet-field-id">Arrow Parquet field_id</a>
     */
    @Test
    public void testPartitionedParquetFieldIds() {
        final String file = TestParquetTools.class.getResource("/ReferencePartitionedFieldIds").getFile();

        final int BAZ_ID = 42;
        final int ZAP_ID = 43;
        final String BAZ = "Baz";
        final String ZAP = "Zap";
        final String PARTITION = "Partition";
        final ColumnDefinition<Integer> partitionColumn = ColumnDefinition.ofInt(PARTITION).withPartitioning();
        final ColumnDefinition<Long> bazCol = ColumnDefinition.ofLong(BAZ);
        final ColumnDefinition<String> zapCol = ColumnDefinition.ofString(ZAP);
        final TableDefinition expectedTd = TableDefinition.of(partitionColumn, bazCol, zapCol);
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(expectedTd)
                .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(
                        BAZ, BAZ_ID,
                        ZAP, ZAP_ID)))
                .build();
        final Table expected = newTable(expectedTd,
                intCol(PARTITION, 0, 0, 1, 1),
                longCol(BAZ, 99, 101, 99, 101),
                stringCol(ZAP, "Foo", "Bar", "Foo", "Bar"));
        {
            final Table actual = ParquetTools.readTable(file, instructions);
            assertEquals(expectedTd, actual.getDefinition());
            assertTableEquals(expected, actual);
        }
    }

    /**
     * This data was generated via the script:
     *
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * field_id = b"PARQUET:field_id"
     * schema = pa.schema(
     *     [pa.field("some random name", pa.list_(pa.int32()), metadata={field_id: b"999"})]
     * )
     * data = [pa.array([[1, 2, 3], None, [], [42]], type=pa.list_(pa.int32()))]
     * table = pa.Table.from_arrays(data, schema=schema)
     * pq.write_table(table, "ReferenceListParquetFieldIds.parquet")
     * </pre>
     *
     * @see <a href="https://arrow.apache.org/docs/cpp/parquet.html#parquet-field-id">Arrow Parquet field_id</a>
     */
    @Test
    public void testParquetFieldIdsWithListType() {
        final String file = TestParquetTools.class.getResource("/ReferenceListParquetFieldIds.parquet").getFile();
        final String FOO = "Foo";
        final TableDefinition td = TableDefinition.of(ColumnDefinition.of(FOO, Type.intType().arrayType()));
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(td)
                .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(FOO, 999)))
                .build();
        final Table expected = TableTools.newTable(td, new ColumnHolder<>(FOO, int[].class, int.class, false,
                new int[] {1, 2, 3},
                null,
                new int[0],
                new int[] {42}));
        {
            final Table actual = ParquetTools.readTable(file, instructions);
            assertEquals(td, actual.getDefinition());
            assertTableEquals(expected, actual);
        }
    }

    /**
     * This is meant to test a "common" renaming scenario. Originally, a schema might be written down with a column
     * named "Name" where semantically, this was really a first name. Later, the schema might be "corrected" to label
     * this column as "FirstName". Both standalone, and in combination with the newer file, we should be able to read it
     * with the latest schema.
     */
    @Test
    public void testRenamingColumnResolver() {
        final File f1 = new File(testRoot, "testRenamingResolveViaFieldId.00.parquet");
        final File f2 = new File(testRoot, "testRenamingResolveViaFieldId.01.parquet");

        final String NAME = "Name";
        final TableDefinition td1 = TableDefinition.of(ColumnDefinition.ofString(NAME));
        final int NAME_ID = 15;
        final Table t1 = newTable(td1, stringCol(NAME, "Shivam", "Ryan"));
        {
            ParquetTools.writeTable(t1, f1.getPath(), ParquetInstructions.builder()
                    .setFieldId(NAME, NAME_ID)
                    .build());
        }

        final String FIRST_NAME = "FirstName";
        final String LAST_NAME = "LastName";
        final TableDefinition td2 = TableDefinition.of(
                ColumnDefinition.ofString(FIRST_NAME),
                ColumnDefinition.ofString(LAST_NAME));
        // noinspection UnnecessaryLocalVariable
        final int FIRST_NAME_ID = NAME_ID;
        final int LAST_NAME_ID = 16;
        final Table t2 = newTable(td2,
                stringCol(FIRST_NAME, "Pete", "Colin"),
                stringCol(LAST_NAME, "Goddard", "Alworth"));
        {
            ParquetTools.writeTable(t2, f2.getPath(), ParquetInstructions.builder()
                    .setFieldId(FIRST_NAME, FIRST_NAME_ID)
                    .setFieldId(LAST_NAME, LAST_NAME_ID)
                    .build());
        }

        final ParquetColumnResolver.Factory resolver = ParquetFieldIdColumnResolverFactory.of(Map.of(
                NAME, NAME_ID,
                FIRST_NAME, FIRST_NAME_ID,
                LAST_NAME, LAST_NAME_ID));

        // f1 + td1
        {
            final Table actual = ParquetTools.readTable(f1.getPath(), ParquetInstructions.builder()
                    .setTableDefinition(td1)
                    .setColumnResolverFactory(resolver)
                    .build());
            assertEquals(td1, actual.getDefinition());
            assertTableEquals(t1, actual);
        }

        // f1 + td2
        {
            final Table expected = newTable(td2,
                    stringCol(FIRST_NAME, "Shivam", "Ryan"),
                    stringCol(LAST_NAME, null, null));
            final Table actual = ParquetTools.readTable(f1.getPath(), ParquetInstructions.builder()
                    .setTableDefinition(td2)
                    .setColumnResolverFactory(resolver)
                    .build());
            assertEquals(td2, actual.getDefinition());
            assertTableEquals(expected, actual);
        }

        // f2 + td1
        {
            final Table expected = newTable(td1, stringCol(NAME, "Pete", "Colin"));
            final Table actual = ParquetTools.readTable(f2.getPath(), ParquetInstructions.builder()
                    .setTableDefinition(td1)
                    .setColumnResolverFactory(resolver)
                    .build());
            assertEquals(td1, actual.getDefinition());
            assertTableEquals(expected, actual);
        }

        // f2 + td2
        {
            final Table actual = ParquetTools.readTable(f2.getPath(), ParquetInstructions.builder()
                    .setTableDefinition(td2)
                    .setColumnResolverFactory(resolver)
                    .build());
            assertEquals(td2, actual.getDefinition());
            assertTableEquals(t2, actual);
        }

        // (f1, f2) + td1
        {
            final Table expected = newTable(td1,
                    stringCol(NAME, "Shivam", "Ryan", "Pete", "Colin"));
            final Table actual = ParquetTools.readTable(testRoot, ParquetInstructions.builder()
                    .setTableDefinition(td1)
                    .setColumnResolverFactory(resolver)
                    .build());
            assertEquals(td1, actual.getDefinition());
            assertTableEquals(expected, actual);
        }

        // (f1, f2) + td2
        {
            final Table expected = newTable(td2,
                    stringCol(FIRST_NAME, "Shivam", "Ryan", "Pete", "Colin"),
                    stringCol(LAST_NAME, null, null, "Goddard", "Alworth"));
            final Table actual = ParquetTools.readTable(testRoot, ParquetInstructions.builder()
                    .setTableDefinition(td2)
                    .setColumnResolverFactory(resolver)
                    .build());
            assertEquals(td2, actual.getDefinition());
            assertTableEquals(expected, actual);
        }
    }


    @Test
    public void parquetWithNonUniqueFieldIds() {
        final File f = new File(testRoot, "parquetWithNonUniqueFieldIds.parquet");
        final String FOO = "Foo";
        final String BAR = "Bar";
        final int fieldId = 31337;
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setFieldId(FOO, fieldId)
                .setFieldId(BAR, fieldId)
                .build();
        final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(FOO), ColumnDefinition.ofString(BAR));
        final Table expected = newTable(td,
                intCol(FOO, 44, 45),
                stringCol(BAR, "Zip", "Zap"));
        {
            ParquetTools.writeTable(expected, f.getPath(), instructions);
        }

        {
            final MessageType expectedSchema = Types.buildMessage()
                    .optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(32))
                    .id(fieldId)
                    .named(FOO)
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .id(fieldId)
                    .named(BAR)
                    .named(MappedSchema.SCHEMA_NAME);
            final MessageType actualSchema = readSchema(f);
            assertEquals(expectedSchema, actualSchema);
        }

        // normal + no definition
        {
            final Table actual = ParquetTools.readTable(f.getPath());
            assertEquals(td, actual.getDefinition());
            assertTableEquals(expected, actual);
        }

        // normal + definition
        {
            final Table actual = ParquetTools.readTable(f.getPath(), ParquetInstructions.builder()
                    .setTableDefinition(td)
                    .build());
            assertEquals(td, actual.getDefinition());
            assertTableEquals(expected, actual);
        }

        // ParquetColumnResolverFieldIds will not work given the duplicate field IDs in the file
        {
            final Table table = ParquetTools.readTable(f.getPath(), ParquetInstructions.builder()
                    .setTableDefinition(td)
                    .setColumnResolverFactory(ParquetFieldIdColumnResolverFactory.of(Map.of(
                            FOO, fieldId,
                            BAR, fieldId)))
                    .build());

            // Only noticed when we build ParquetTableLocation; if necessary, we could refactor the implementation to
            // notice this earlier on.
            try {
                table.select();
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining(
                        "Parquet columns can't be unambigously mapped. Bar -> 31337 has multiple paths [Foo], [Bar]");
            }
        }
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * fields = [
     *     pa.field("Foo", pa.int64()),
     *     pa.field(
     *         "MyStruct",
     *         pa.struct(
     *             [
     *                 pa.field("Zip", pa.int16()),
     *                 pa.field("Zap", pa.int32()),
     *             ]
     *         ),
     *     ),
     *     pa.field("Bar", pa.string()),
     * ]
     *
     * table = pa.table([[] for _ in fields], schema=pa.schema(fields))
     * pq.write_table(table, "NestedStruct1.parquet", compression="none")
     * </pre>
     */
    @Test
    public void nestedMessageEmpty() {
        final String file = TestParquetTools.class.getResource("/NestedStruct1.parquet").getFile();
        final TableDefinition expectedTd =
                TableDefinition.of(ColumnDefinition.ofLong("Foo"), ColumnDefinition.ofString("Bar"));
        final Table table = TableTools.newTable(expectedTd, longCol("Foo"), stringCol("Bar"));
        // If we use an explicit definition, we can skip over MyStruct and read Foo, Bar
        {
            final Table actual =
                    ParquetTools.readTable(file, ParquetInstructions.EMPTY.withTableDefinition(expectedTd));
            assertEquals(expectedTd, actual.getDefinition());
            assertTableEquals(table, actual);
        }

        // If we try to infer, we currently throw an error.
        // TODO(deephaven-core#871): Parquet: Support repetition level >1 and multi-column fields
        try {
            ParquetTools.readTable(file);
            Assertions.failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            assertThat(e)
                    .hasMessageContaining("Encountered unsupported multi-column field MyStruct");
        }
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * fields = [
     *     pa.field("Foo", pa.int64()),
     *     pa.field(
     *         "MyStruct",
     *         pa.struct(
     *             [
     *                 pa.field("Zip", pa.int16()),
     *                 pa.field("Zap", pa.int32()),
     *             ]
     *         ),
     *     ),
     *     pa.field("Bar", pa.string()),
     * ]
     *
     * table = pa.table([[None] for _ in fields], schema=pa.schema(fields))
     * pq.write_table(table, "NestedStruct2.parquet", compression="none")
     * </pre>
     */
    @Test
    public void nestedMessage1Row() {
        final String file = TestParquetTools.class.getResource("/NestedStruct2.parquet").getFile();
        final TableDefinition expectedTd =
                TableDefinition.of(ColumnDefinition.ofLong("Foo"), ColumnDefinition.ofString("Bar"));
        final Table table = TableTools.newTable(expectedTd, longCol("Foo", QueryConstants.NULL_LONG),
                stringCol("Bar", (String) null));
        // If we use an explicit definition, we can skip over MyStruct and read Foo, Bar
        {
            final Table actual =
                    ParquetTools.readTable(file, ParquetInstructions.EMPTY.withTableDefinition(expectedTd));
            assertEquals(expectedTd, actual.getDefinition());
            assertTableEquals(table, actual);
        }

        // If we try to infer, we currently throw an error.
        // TODO(deephaven-core#871): Parquet: Support repetition level >1 and multi-column fields
        try {
            ParquetTools.readTable(file);
            Assertions.failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            assertThat(e)
                    .hasMessageContaining("Encountered unsupported multi-column field MyStruct");
        }
    }

    @Test
    public void intArray() {
        final File f = new File(testRoot, "intArray.parquet");
        final String FOO = "Foo";
        final TableDefinition td = TableDefinition.of(ColumnDefinition.of(FOO, Type.intType().arrayType()));
        final Table table = TableTools.newTable(td, new ColumnHolder<>(FOO, int[].class, int.class, false,
                new int[] {1, 2, 3},
                null,
                new int[0],
                new int[] {42}));
        {
            ParquetTools.writeTable(table, f.getPath());
        }

        {
            final MessageType expectedSchema = Types.buildMessage()
                    .optionalList()
                    .optionalElement(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(32))
                    .named(FOO)
                    .named(MappedSchema.SCHEMA_NAME);
            assertEquals(readSchema(f), expectedSchema);
        }

        {
            final Table actual = ParquetTools.readTable(f.getPath());
            assertEquals(td, actual.getDefinition());
            assertTableEquals(table, actual);
        }

        {
            final Table actual = ParquetTools.readTable(f.getPath(), ParquetInstructions.builder()
                    .setTableDefinition(td)
                    .build());
            assertEquals(td, actual.getDefinition());
            assertTableEquals(table, actual);
        }
    }

    @Test
    public void stringArray() {
        final File f = new File(testRoot, "stringArray.parquet");
        final String FOO = "Foo";
        final TableDefinition td = TableDefinition.of(ColumnDefinition.of(FOO, Type.stringType().arrayType()));
        final Table expected = TableTools.newTable(td, new ColumnHolder<>(FOO, String[].class, String.class, false,
                new String[] {null, "", "Hello, world!"},
                null,
                new String[0],
                new String[] {"42"}));
        {
            ParquetTools.writeTable(expected, f.getPath());
        }

        {
            final MessageType expectedSchema = Types.buildMessage()
                    .optionalList()
                    .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(FOO)
                    .named(MappedSchema.SCHEMA_NAME);
            assertEquals(readSchema(f), expectedSchema);
        }

        {
            final Table actual = ParquetTools.readTable(f.getPath());
            assertEquals(td, actual.getDefinition());
            assertTableEquals(expected, actual);
        }

        {
            final ParquetInstructions instructions = ParquetInstructions.builder()
                    .setTableDefinition(td)
                    .build();
            final Table actual = ParquetTools.readTable(f.getPath(), instructions);
            assertEquals(td, actual.getDefinition());
            assertTableEquals(expected, actual);
        }
    }

    private static String sha256sum(Path path) throws NoSuchAlgorithmException, IOException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final DigestOutputStream out = new DigestOutputStream(OutputStream.nullOutputStream(), digest);
        Files.copy(path, out);
        return BaseEncoding.base16().lowerCase().encode(digest.digest());
    }

    private static MessageType readSchema(File file) {
        final URI uri = FileUtils.convertToURI(file, false);
        try (final SeekableChannelsProvider channelsProvider =
                SeekableChannelsProviderLoader.getInstance().load(FileUtils.FILE_URI_SCHEME, null)) {
            final ParquetTableLocationKey locationKey = new ParquetTableLocationKey(uri, 0, null, channelsProvider);
            // TODO: which is more appropriate?
            // locationKey.getFileReader().getSchema();
            // locationKey.getMetadata().getFileMetaData().getSchema();
            return locationKey.getFileReader().getSchema();
        }
    }
}
