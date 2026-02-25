//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import com.google.common.collect.Lists;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.testutil.filters.ParallelizedRowSetCapturingFilter;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.location.ParquetColumnResolverMap;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import io.deephaven.stringset.ArrayStringSet;
import io.deephaven.stringset.StringSet;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.deephaven.engine.table.impl.select.WhereFilterFactory.getExpression;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.parquet.table.ParquetTools.*;
import static io.deephaven.time.DateTimeUtils.parseInstant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@Category(OutOfBandTest.class)
public final class ParquetTableFilterTest {

    private static final String ROOT_FILENAME = ParquetTableFilterTest.class.getName() + "_root";
    private static final ParquetInstructions EMPTY = ParquetInstructions.EMPTY;

    private static File rootFile;
    private static boolean pushdownDataIndexDisabled;

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
        pushdownDataIndexDisabled = QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX;
    }

    @After
    public void tearDown() {
        FileUtils.deleteRecursively(rootFile);
        QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX = pushdownDataIndexDisabled;
    }

    private Table[] splitTable(final Table source, int numSplits, boolean randomSizes) {
        final Table[] result = new Table[numSplits];
        final long splitSize = source.size() / numSplits;

        if (randomSizes) {
            // Random sized splits
            final Random random = new Random(1234567890L);
            final long maxSplitSize = splitSize * 2;
            long startPos = 0;
            for (int i = 0; i < numSplits / 2; i++) {
                // Create in pairs, where the two sum to the maxSplitSize
                final long localSplitSize = Math.abs(random.nextLong()) % maxSplitSize;
                result[2 * i] = source.slice(startPos, startPos + localSplitSize);
                result[2 * i + 1] = source.slice(startPos + localSplitSize, startPos + maxSplitSize);
                startPos += maxSplitSize;
            }
            if (numSplits % 2 == 1) {
                // If odd, add the last split
                result[numSplits - 1] = source.slice(startPos, source.size());
            }
            return result;
        }

        // Even sized splits
        for (int i = 0; i < numSplits - 1; i++) {
            result[i] = source.slice(i * splitSize, (i + 1) * splitSize);
        }
        result[numSplits - 1] = source.slice((numSplits - 1) * splitSize, source.size());
        return result;
    }

    private void writeTables(final String destPath, final Table[] tables, final ParquetInstructions instructions) {
        for (int i = 0; i < tables.length; i++) {
            final Table table = tables[i];
            final String tableName = "table_" + String.format("%05d", i) + ".parquet";
            // System.out.println("Writing table " + tableName + " to " + destPath);
            ParquetTools.writeTable(table, Path.of(destPath, tableName).toString(), instructions);
        }
    }

    private static void verifyResults(Table filteredDiskTable, Table filteredMemTable) {
        Assert.assertFalse("filteredMemTable.isEmpty()", filteredMemTable.isEmpty());
        Assert.assertFalse("filteredDiskTable.isEmpty()", filteredDiskTable.isEmpty());
        verifyResultsAllowEmpty(filteredDiskTable, filteredMemTable);
    }

    private static void filterAndVerifyResults(Table diskTable, Table memTable, String... filters) {
        verifyResults(diskTable.where(filters).coalesce(), memTable.where(filters).coalesce());
    }

    private static void filterAndVerifyResults(Table diskTable, Table memTable, Filter filter) {
        verifyResults(diskTable.where(filter).coalesce(), memTable.where(filter).coalesce());
    }

    private static void filterAndVerifyResults(Table diskTable, Table memTable, WhereFilter filter) {
        verifyResults(diskTable.where(filter).coalesce(), memTable.where(filter).coalesce());
    }

    private static void filterAndVerifyResultsAllowEmpty(Table diskTable, Table memTable, String... filters) {
        verifyResultsAllowEmpty(diskTable.where(filters).coalesce(), memTable.where(filters).coalesce());
    }

    private static void filterAndVerifyResultsAllowEmpty(Table diskTable, Table memTable, WhereFilter filter) {
        verifyResultsAllowEmpty(diskTable.where(filter).coalesce(), memTable.where(filter).coalesce());
    }

    private static void verifyResultsAllowEmpty(Table filteredDiskTable, Table filteredMemTable) {
        assertTableEquals(filteredDiskTable, filteredMemTable);
    }

    private static void filterAndVerifyThrowsSame(Table diskTable, Table memTable, String... filters) {
        final Class<? extends Throwable> memEx =
                Assert.assertThrows("memTable should throw", Throwable.class,
                        () -> memTable.where(filters).coalesce()).getClass();
        final Class<? extends Throwable> diskEx =
                Assert.assertThrows("diskTable should throw", memEx,
                        () -> diskTable.where(filters).coalesce()).getClass();
        Assert.assertSame("Exception types not matching", memEx, diskEx);
    }

    @Test
    public void filterDatatypeMismatchTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "sequential_val = i % 117 == 0 ? null : i", // with nulls
                "price = randomInt(0,100000) * 0.01f");
        final int partitionCount = 5;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        // Re-write the schema to have only long and double datatypes
        final TableDefinition readDef = TableDefinition.of(
                ColumnDefinition.ofLong("sequential_val"), // int to long
                ColumnDefinition.ofDouble("price") // float to double
        );

        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(readDef)
                .build();

        final Table fromDisk = ParquetTools.readTable(destPath, instructions);

        // Assert that we have identical row set sizes when we filter on upcast columns
        Assert.assertEquals("Row set sizes do not match",
                fromDisk.where("sequential_val = null").size(),
                largeTable.where("sequential_val = null").size());

        Assert.assertEquals("Row set sizes do not match",
                fromDisk.where("sequential_val != null").size(),
                largeTable.where("sequential_val != null").size());

        Assert.assertEquals("Row set sizes do not match",
                fromDisk.where("sequential_val < 1000").size(),
                largeTable.where("sequential_val < 1000").size());

        Assert.assertEquals("Row set sizes do not match",
                fromDisk.where("sequential_val = 1000").size(),
                largeTable.where("sequential_val = 1000").size());

        Assert.assertEquals("Row set sizes do not match",
                fromDisk.where("price < 500.0").size(),
                largeTable.where("price < 500.0").size());

        Assert.assertEquals("Row set sizes do not match",
                fromDisk.where("price = 500.0").size(),
                largeTable.where("price = 500.0").size());
    }

    /**
     * This test fails because of the mismatch between the data index and the upcast parquet column types. This test
     * will continue to fail until the data index is also upcast to match the parquet column types (DH-19443).
     * <p>
     * When the data index is fixed, this test should be re-enabled.
     */
    @Ignore
    @Test
    public void filterDatatypeMismatchDataIndexTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "symbol = String.format(`%04d`, randomInt(0,10000))",
                "exchange = randomInt(0,100)",
                "price = randomInt(0,10000) * 0.01f");
        final int partitionCount = 5;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);

        final ParquetInstructions writeInstructions = ParquetInstructions.builder()
                .addIndexColumns("symbol")
                .addIndexColumns("exchange")
                .build();

        writeTables(destPath, randomPartitions, writeInstructions);

        // Re-write the schema to have only long and double datatypes
        final TableDefinition readDef = TableDefinition.of(
                ColumnDefinition.ofString("symbol"),
                ColumnDefinition.ofLong("exchange"), // int to long
                ColumnDefinition.ofDouble("price") // float to double
        );

        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setTableDefinition(readDef)
                .build();

        final Table diskTable = ParquetTools.readTable(destPath, readInstructions);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // Assert that we have identical row set sizes when we filter on upcast columns
        Assert.assertEquals("Row set sizes do not match",
                diskTable.where("symbol < `0050`").size(),
                largeTable.where("symbol < `0050`").size());

        Assert.assertEquals("Row set sizes do not match",
                diskTable.where("symbol < `0050`", "symbol >= `0049`").size(),
                largeTable.where("symbol < `0050`", "symbol >= `0049`").size());

        Assert.assertEquals("Row set sizes do not match",
                diskTable.where("exchange <= 10").size(),
                largeTable.where("exchange <= 10").size());

        Assert.assertEquals("Row set sizes do not match",
                diskTable.where("exchange = 10").size(),
                largeTable.where("exchange = 10").size());

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "symbol >= `0049`");
        filterAndVerifyResults(diskTable, memTable, "symbol = `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol.startsWith(`002`)");

        // int range and match filters
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "exchange % 10 == 0");

        // combined filters
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "symbol.startsWith(`002`)", "exchange % 10 == 0");

        // mixed type with complex filters
        Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));
    }

    @Test
    public void flatPartitionsNoDataIndexTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime + i * 1_000_000_000L",
                "boolean_col = ii % 3 == 0 ? null : ii % 2 == 0? true : false", // with nulls
                "sequential_val = ii % 117 == 0 ? null : ii", // with nulls
                "symbol = ii % 119 == 0 ? null : String.format(`%04d`, randomInt(0,10_000))",
                "sequential_bd = java.math.BigDecimal.valueOf(ii * 0.1)",
                "sequential_bi = java.math.BigInteger.valueOf(ii)");
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol = null");
        filterAndVerifyResults(diskTable, memTable, "symbol != null");
        filterAndVerifyResults(diskTable, memTable, "symbol = `1000`");
        filterAndVerifyResults(diskTable, memTable, "symbol < `1000`");
        filterAndVerifyResults(diskTable, memTable, "symbol = `5000`");

        // Timestamp range and match filters
        filterAndVerifyResults(diskTable, memTable, "Timestamp < '2023-01-02T00:00:00 NY'");
        filterAndVerifyResults(diskTable, memTable, "Timestamp = '2023-01-02T00:00:00 NY'");

        // Boolean column filters
        filterAndVerifyResults(diskTable, memTable, "boolean_col = null");
        filterAndVerifyResults(diskTable, memTable, "boolean_col != null");
        filterAndVerifyResults(diskTable, memTable, "boolean_col = true");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "boolean_col = false");

        // BigDecimal range filters (match is complicated with BD, given
        ExecutionContext.getContext().getQueryScope().putParam("bd_500", BigDecimal.valueOf(500.0));
        ExecutionContext.getContext().getQueryScope().putParam("bd_1000", BigDecimal.valueOf(1000.00));

        filterAndVerifyResults(diskTable, memTable, "sequential_bd < bd_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd >= bd_500", "sequential_bd < bd_1000");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bd < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd >= 500.0", "sequential_bd < 1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd < 1000", "sequential_bd >= 500.0");

        // BigInteger range and match filters
        ExecutionContext.getContext().getQueryScope().putParam("bi_500", BigInteger.valueOf(500));
        ExecutionContext.getContext().getQueryScope().putParam("bi_1000", BigInteger.valueOf(1000));

        filterAndVerifyResults(diskTable, memTable, "sequential_bi < bi_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi >= bi_500", "sequential_bi < bi_1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi = bi_500");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bi < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi >= 500.0", "sequential_bi < 1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi = 500");

        // long range and match filters
        filterAndVerifyResults(diskTable, memTable, "sequential_val = null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val != null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val <= 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_val <= 5000", "sequential_val > 3000");
        filterAndVerifyResults(diskTable, memTable, "sequential_val = 500");

        // mixed type with complex filters
        Filter complexFilter =
                Filter.or(Filter.from("sequential_val <= 500", "sequential_val = 555", "symbol > `1000`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("sequential_val <= 500", "sequential_val = 555"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));
    }

    @Test
    public void flatPartitionsNoDataIndexAllNullTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime + i * 1_000_000_000L",
                "boolean_col = (Boolean)null", // all nulls
                "sequential_val = (Long)null", // all nulls
                "symbol = (String)null"); // all nulls
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "symbol = null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "symbol != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "symbol = `1000`");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "symbol < `1000`");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "symbol = `5000`");


        // long range and match filters
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "sequential_val = null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "sequential_val != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "sequential_val <= 500");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "sequential_val <= 5000", "sequential_val > 3000");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "sequential_val = 500");

        // mixed type with complex filters
        final Filter complexFilter =
                Filter.or(Filter.from("sequential_val = null", "symbol = null"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        // boolean column filters
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "boolean_col = null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "boolean_col != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "boolean_col = true");
    }

    // New test with custom function counting invocations
    @Test
    public void partitionedDataSerialFilterTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_kvPartitionsSerialTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "symbol = ii % 100",
                "sequential_val = ii");

        final PartitionedTable partitionedTable = largeTable.partitionBy("symbol");
        ParquetTools.writeKeyValuePartitionedTable(partitionedTable, destPath, EMPTY);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        final AtomicLong invocationCount = new AtomicLong();
        QueryScope.addParam("invocationCount", invocationCount);

        final Filter partitionFilter = RawString.of("symbol >= 0 && invocationCount.incrementAndGet() >= 0");
        final Filter serialPartitionFilter = partitionFilter.withSerial();

        final Filter nonPartitionFilter =
                RawString.of("sequential_val >= 0 && invocationCount.incrementAndGet() >= 0");
        final Filter serialNonPartitionFilter = nonPartitionFilter.withSerial();

        Table result;

        // Test non-serial partition filter
        assertEquals(0L, invocationCount.get());
        result = diskTable.where(partitionFilter).coalesce();
        assertEquals(100L, invocationCount.get()); // one per partition
        // Verify the table contents are equivalent
        assertTableEquals(result, diskTable.coalesce().where(partitionFilter));

        // Test serial partition filter
        invocationCount.set(0);
        assertEquals(0L, invocationCount.get());
        result = diskTable.where(serialPartitionFilter).coalesce();
        assertEquals(1_000_000L, invocationCount.get()); // one per row
        // Verify the table contents are equivalent
        assertTableEquals(result, diskTable.coalesce().where(serialPartitionFilter));

        // Test non-serial non-partition filter
        invocationCount.set(0);
        assertEquals(0L, invocationCount.get());
        result = diskTable.where(nonPartitionFilter).coalesce();
        assertEquals(1_000_000L, invocationCount.get()); // one per row
        // Verify the table contents are equivalent
        assertTableEquals(result, diskTable.coalesce().where(nonPartitionFilter));

        // Test serial non-partition filter
        invocationCount.set(0);
        assertEquals(0L, invocationCount.get());
        result = diskTable.where(serialNonPartitionFilter).coalesce();
        assertEquals(1_000_000L, invocationCount.get()); // one per row
        // Verify the table contents are equivalent
        assertTableEquals(result, diskTable.coalesce().where(serialNonPartitionFilter));

        // Test stateless partition filter
        final RowSetCapturingFilter statelessPartitionFilter =
                new ParallelizedRowSetCapturingFilter(RawString.of("symbol >= 0"));
        result = diskTable.where(statelessPartitionFilter).coalesce();
        assertEquals(100, statelessPartitionFilter.numRowsProcessed()); // one per partition

        // Test stateless non-partition filter
        final RowSetCapturingFilter statelessNonPartitionFilter =
                new ParallelizedRowSetCapturingFilter(RawString.of("sequential_val >= 0"));
        result = diskTable.where(statelessNonPartitionFilter).coalesce();
        assertEquals(1_000_000, statelessNonPartitionFilter.numRowsProcessed()); // one per row
    }

    @Test
    public void partitionedNoDataIndexTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_kvPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime + i * 1_000_000_000L",
                "sequential_val = ii % 117 == 0 ? null : ii", // with nulls
                "symbol = ii % 119 == 0 ? null : String.format(`s%03d`, randomInt(0,1_000))",
                "sequential_bd = java.math.BigDecimal.valueOf(ii * 0.1)",
                "sequential_bi = java.math.BigInteger.valueOf(ii)");

        final PartitionedTable partitionedTable = largeTable.partitionBy("symbol");
        ParquetTools.writeKeyValuePartitionedTable(partitionedTable, destPath, EMPTY);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol = null");
        filterAndVerifyResults(diskTable, memTable, "symbol != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "symbol = `s100`");
        filterAndVerifyResults(diskTable, memTable, "symbol < `s100`");
        filterAndVerifyResults(diskTable, memTable, "symbol = `s500`");

        // Conditional on partition column
        filterAndVerifyResults(diskTable, memTable, "symbol = `s` + `500`");
        // Serial conditional on partition column
        filterAndVerifyResults(diskTable, memTable,
                Filter.serial(Filter.and(Filter.from("symbol = `s` + `500`"))));

        // Conditional on non-partition column
        filterAndVerifyResults(diskTable, memTable, "sequential_val >= 50 + 1");
        // Serial conditional on non-partition column
        filterAndVerifyResults(diskTable, memTable,
                Filter.serial(Filter.and(Filter.from("sequential_val >= 50 + 1"))));

        // Timestamp range and match filters
        filterAndVerifyResults(diskTable, memTable, "Timestamp < '2023-01-02T00:00:00 NY'");
        filterAndVerifyResults(diskTable, memTable, "Timestamp = '2023-01-02T00:00:00 NY'");

        // BigDecimal range filters (match is complicated with BD, given
        ExecutionContext.getContext().getQueryScope().putParam("bd_500", BigDecimal.valueOf(500.0));
        ExecutionContext.getContext().getQueryScope().putParam("bd_1000", BigDecimal.valueOf(1000.00));

        filterAndVerifyResults(diskTable, memTable, "sequential_bd < bd_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd >= bd_500", "sequential_bd < bd_1000");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bd < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd >= 500.0", "sequential_bd < 1000");

        // BigInteger range and match filters
        ExecutionContext.getContext().getQueryScope().putParam("bi_500", BigInteger.valueOf(500));
        ExecutionContext.getContext().getQueryScope().putParam("bi_1000", BigInteger.valueOf(1000));

        filterAndVerifyResults(diskTable, memTable, "sequential_bi < bi_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi >= bi_500", "sequential_bi < bi_1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi = bi_500");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bi < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi >= 500.0", "sequential_bi < 1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi = 500");

        // long range and match filters
        filterAndVerifyResults(diskTable, memTable, "sequential_val = null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val != null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val <= 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_val <= 5000", "sequential_val > 3000");
        filterAndVerifyResults(diskTable, memTable, "sequential_val = 500");

        // mixed type with complex filters
        Filter complexFilter =
                Filter.or(Filter.from("sequential_val <= 500", "sequential_val = 555", "symbol > `1000`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("sequential_val <= 500", "sequential_val = 555"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));
    }

    @Test
    public void flatPartitionsColumnRenameTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime + i * 1_000_000_000L",
                "sequential_val = ii % 117 == 0 ? null : ii", // with nulls
                "symbol = ii % 119 == 0 ? null : String.format(`%04d`, randomInt(0,10_000))",
                "sequential_bd = java.math.BigDecimal.valueOf(ii * 0.1)",
                "sequential_bi = java.math.BigInteger.valueOf(ii)");
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        // Rename the columns after loading from the disk.
        final Table diskTable = ParquetTools.readTable(destPath).renameColumns(
                "Timestamp_renamed=Timestamp",
                "sequential_val_renamed=sequential_val",
                "symbol_renamed=symbol",
                "sequential_bd_renamed=sequential_bd",
                "sequential_bi_renamed=sequential_bi");
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed = null");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed != null");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed = `1000`");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed < `1000`");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed = `5000`");

        // Timestamp range and match filters
        filterAndVerifyResults(diskTable, memTable, "Timestamp_renamed < '2023-01-02T00:00:00 NY'");
        filterAndVerifyResults(diskTable, memTable, "Timestamp_renamed = '2023-01-02T00:00:00 NY'");

        // BigDecimal range filters (match is complicated with BD, given
        ExecutionContext.getContext().getQueryScope().putParam("bd_500", BigDecimal.valueOf(500.0));
        ExecutionContext.getContext().getQueryScope().putParam("bd_1000", BigDecimal.valueOf(1000.00));

        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed < bd_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed >= bd_500",
                "sequential_bd_renamed < bd_1000");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed >= 500.0", "sequential_bd_renamed < 1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed < 1000", "sequential_bd_renamed >= 500.0");

        // BigInteger range and match filters
        ExecutionContext.getContext().getQueryScope().putParam("bi_500", BigInteger.valueOf(500));
        ExecutionContext.getContext().getQueryScope().putParam("bi_1000", BigInteger.valueOf(1000));

        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed < bi_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed >= bi_500",
                "sequential_bi_renamed < bi_1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed = bi_500");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed >= 500.0", "sequential_bi_renamed < 1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed = 500");

        // long range and match filters
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed = null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed != null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed <= 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed <= 5000", "sequential_val_renamed > 3000");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed = 500");

        // mixed type with complex filters
        Filter complexFilter = Filter.or(Filter.from("sequential_val_renamed <= 500", "sequential_val_renamed = 555",
                "symbol_renamed > `1000`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("sequential_val_renamed <= 500", "sequential_val_renamed = 555"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        // Rename again and do some more tests.
        final Table renamedDiskTable = diskTable.renameColumns(
                "Timestamp_renamed_again=Timestamp_renamed",
                "symbol_renamed_again=symbol_renamed");

        final Table renamedMemTable = renamedDiskTable.select();

        // string range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed_again < `1000`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed_again = `5000`");

        // Timestamp range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "Timestamp_renamed_again < '2023-01-02T00:00:00 NY'");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "Timestamp_renamed_again = '2023-01-02T00:00:00 NY'");
    }

    @Test
    public void flatPartitionsInstructionColumnRenameTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime + i * 1_000_000_000L",
                "sequential_val = ii % 117 == 0 ? null : ii", // with nulls
                "symbol = ii % 119 == 0 ? null : String.format(`%04d`, randomInt(0,10_000))",
                "sequential_bd = java.math.BigDecimal.valueOf(ii * 0.1)",
                "sequential_bi = java.math.BigInteger.valueOf(ii)");
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        // Rename the columns using parquet instructions.
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addColumnNameMapping("Timestamp", "Timestamp_renamed")
                .addColumnNameMapping("sequential_val", "sequential_val_renamed")
                .addColumnNameMapping("symbol", "symbol_renamed")
                .addColumnNameMapping("sequential_bd", "sequential_bd_renamed")
                .addColumnNameMapping("sequential_bi", "sequential_bi_renamed")
                .build();

        final Table diskTable = ParquetTools.readTable(destPath, instructions);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed = null");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed != null");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed = `1000`");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed < `1000`");
        filterAndVerifyResults(diskTable, memTable, "symbol_renamed = `5000`");

        // Timestamp range and match filters
        filterAndVerifyResults(diskTable, memTable, "Timestamp_renamed < '2023-01-02T00:00:00 NY'");
        filterAndVerifyResults(diskTable, memTable, "Timestamp_renamed = '2023-01-02T00:00:00 NY'");

        // BigDecimal range filters (match is complicated with BD, given
        ExecutionContext.getContext().getQueryScope().putParam("bd_500", BigDecimal.valueOf(500.0));
        ExecutionContext.getContext().getQueryScope().putParam("bd_1000", BigDecimal.valueOf(1000.00));

        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed < bd_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed >= bd_500",
                "sequential_bd_renamed < bd_1000");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed >= 500.0", "sequential_bd_renamed < 1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bd_renamed < 1000", "sequential_bd_renamed >= 500.0");

        // BigInteger range and match filters
        ExecutionContext.getContext().getQueryScope().putParam("bi_500", BigInteger.valueOf(500));
        ExecutionContext.getContext().getQueryScope().putParam("bi_1000", BigInteger.valueOf(1000));

        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed < bi_500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed >= bi_500",
                "sequential_bi_renamed < bi_1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed = bi_500");
        // test integer and double values in the filter strings
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed < 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed >= 500.0", "sequential_bi_renamed < 1000");
        filterAndVerifyResults(diskTable, memTable, "sequential_bi_renamed = 500");

        // long range and match filters
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed = null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed != null");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed = 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed <= 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed <= 5000", "sequential_val_renamed > 3000");
        filterAndVerifyResults(diskTable, memTable, "sequential_val_renamed = 500");

        // mixed type with complex filters
        Filter complexFilter = Filter.or(Filter.from("sequential_val_renamed <= 500", "sequential_val_renamed = 555",
                "symbol_renamed > `1000`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("sequential_val_renamed <= 500", "sequential_val_renamed = 555"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        // Rename again and do some more tests.
        final Table renamedDiskTable = diskTable.renameColumns(
                "Timestamp_renamed_again=Timestamp_renamed",
                "symbol_renamed_again=symbol_renamed");

        final Table renamedMemTable = renamedDiskTable.select();

        // string range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed_again < `1000`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed_again = `5000`");

        // Timestamp range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "Timestamp_renamed_again < '2023-01-02T00:00:00 NY'");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "Timestamp_renamed_again = '2023-01-02T00:00:00 NY'");
    }

    @Test
    public void flatPartitionsDataIndexRandomTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "symbol = ii % 119 == 0 ? null : String.format(`%04d`, randomInt(0,10_000))",
                "exchange = randomInt(0,100)",
                "price = randomInt(0,10000) * 0.01");
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);

        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addIndexColumns("symbol")
                .addIndexColumns("exchange")
                .build();

        writeTables(destPath, randomPartitions, instructions);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol = null");
        filterAndVerifyResults(diskTable, memTable, "symbol != null");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "symbol >= `0049`");
        filterAndVerifyResults(diskTable, memTable, "symbol = `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol != null && symbol.startsWith(`002`)");

        // int range and match filters
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "exchange % 10 == 0");

        // combined filters
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "symbol != null && symbol.startsWith(`002`)", "exchange % 10 == 0");

        // mixed type with complex filters
        Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));
    }

    @Test
    public void flatPartitionsLoadedDataIndexTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "symbol = ii % 119 == 0 ? null : String.format(`%04d`, randomInt(0,97))",
                "exchange = randomInt(0,11)",
                "price = randomInt(0,10000) * 0.01");
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);

        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addIndexColumns("symbol", "exchange")
                .addIndexColumns("symbol")
                .addIndexColumns("exchange")
                .build();

        writeTables(destPath, randomPartitions, instructions);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // Turn off memoization on the tables to we get accurate results.
        QueryTable.setMemoizeResults(false);

        final List<RowSetCapturingFilter> filters = Lists.newArrayList(
                new ParallelizedRowSetCapturingFilter(RawString.of("symbol = null")),
                new ParallelizedRowSetCapturingFilter(RawString.of("symbol < `0050`")),
                new ParallelizedRowSetCapturingFilter(Filter.and(RawString.of("symbol < `0050`"),
                        RawString.of("symbol >= `0049`"))),
                new ParallelizedRowSetCapturingFilter(RawString.of("symbol = `0050`")),
                new ParallelizedRowSetCapturingFilter(RawString.of("symbol != null && symbol.startsWith(`002`)")),

                new ParallelizedRowSetCapturingFilter(RawString.of("exchange <= 10")),
                new ParallelizedRowSetCapturingFilter(Filter.and(RawString.of("exchange <= 10"),
                        RawString.of("exchange >= 9"))),
                new ParallelizedRowSetCapturingFilter(RawString.of("exchange = 10")),
                new ParallelizedRowSetCapturingFilter(RawString.of("exchange % 10 == 0")),

                new ParallelizedRowSetCapturingFilter(
                        Filter.and(RawString.of("symbol < `0050`"), RawString.of("exchange <= 10"))),
                new ParallelizedRowSetCapturingFilter(Filter.and(RawString.of("symbol < `0050`"),
                        RawString.of("exchange <= 10"), RawString.of("exchange >= 9"))),

                new ParallelizedRowSetCapturingFilter(
                        Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"))),
                new ParallelizedRowSetCapturingFilter(Filter.or(RawString.of("symbol < `1000`"),
                        RawString.of("exchange <= 10"), RawString.of("exchange >= 9"))));

        // Collect the mem table baseline results.
        final List<Long> memRowsProcessed = new ArrayList<>(filters.size());
        for (final RowSetCapturingFilter filter : filters) {
            filter.reset();
            memTable.where(filter).coalesce();
            memRowsProcessed.add(filter.numRowsProcessed());
        }

        // Collect the disk table baseline results.
        final List<Long> diskRowsProcessedLocationIndexes = new ArrayList<>(filters.size());
        for (final RowSetCapturingFilter filter : filters) {
            filter.reset();
            diskTable.where(filter).coalesce();
            diskRowsProcessedLocationIndexes.add(filter.numRowsProcessed());
        }

        // Verify that the disk table with location indexes processed strictly fewer rows than the mem table.
        for (int i = 0; i < filters.size(); i++) {
            Assert.assertTrue(
                    "Disk table with location indexes did not process fewer rows than the mem table for filter: "
                            + i + ", Disk rows processed: " + diskRowsProcessedLocationIndexes.get(i)
                            + ", Mem rows processed: " + memRowsProcessed.get(i),
                    diskRowsProcessedLocationIndexes.get(i) < memRowsProcessed.get(i));
        }

        // Create the merged in-memory index tables.
        final Table symbolIndexTable = DataIndexer.getDataIndex(diskTable, "symbol").table();
        final Table exchangeIndexTable = DataIndexer.getDataIndex(diskTable, "exchange").table();
        final Table symbolExchangeIndexTable = DataIndexer.getDataIndex(diskTable, "symbol", "exchange").table();

        // Collect the disk table baseline results.
        final List<Long> diskRowsProcessedMergedIndex = new ArrayList<>(filters.size());
        for (final RowSetCapturingFilter filter : filters) {
            filter.reset();
            diskTable.where(filter).coalesce();
            diskRowsProcessedMergedIndex.add(filter.numRowsProcessed());
        }

        // Verify that the merged indexes processed strictly fewer rows the disk table with location indexes.
        for (int i = 0; i < filters.size(); i++) {
            Assert.assertTrue(
                    "Merged indexes did not process fewer rows than Location indexes: "
                            + i + ", Merged index rows processed: " + diskRowsProcessedMergedIndex.get(i)
                            + ", Location index rows processed: " + diskRowsProcessedLocationIndexes.get(i),
                    diskRowsProcessedMergedIndex.get(i) < diskRowsProcessedLocationIndexes.get(i));
        }
    }

    @Test
    public void flatPartitionsDataIndexSequentialTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        // large consecutive blocks of sequential data
        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "symbol = ii % 119 == 0 ? null : String.format(`%04d`, (long)(ii / 1000))", // produces 0000 to 0999
                // given 1M rows
                "exchange = (int)(i / 100)", // produces 0 to 9999 given 1M rows
                "price = randomInt(0,10000) * 0.01");

        final int partitionCount = 11;
        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);

        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addIndexColumns("symbol")
                .addIndexColumns("exchange")
                .build();

        writeTables(destPath, randomPartitions, instructions);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol = null");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "symbol >= `0049`");
        filterAndVerifyResults(diskTable, memTable, "symbol = `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol != null && symbol.startsWith(`002`)");

        // int range and match filters
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "exchange % 10 == 0");

        // combined filters
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "symbol != null && symbol.startsWith(`002`)", "exchange % 10 == 0");

        // mixed type with complex filters
        Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        // Rename columns and do some more tests.
        final Table renamedDiskTable = diskTable.renameColumns(
                "symbol_renamed=symbol",
                "exchange_renamed=exchange");

        final Table renamedMemTable = renamedDiskTable.select();

        // string range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`",
                "symbol_renamed >= `0049`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed = `0050`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable,
                "symbol_renamed != null && symbol_renamed.startsWith(`002`)");

        // int range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed <= 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed <= 10", "exchange_renamed >= 9");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed = 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed % 10 == 0");

        // combined filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`", "exchange_renamed <= 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`", "exchange_renamed <= 10",
                "exchange_renamed >= 9");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`", "exchange_renamed = 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable,
                "symbol_renamed != null && symbol_renamed.startsWith(`002`)",
                "exchange_renamed % 10 == 0");

    }

    @Test
    public void flatPartitionsCachedDataIndexRandomTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "symbol = String.format(`%04d`, randomInt(0,10000))",
                "exchange = randomInt(0,100)",
                "price = randomInt(0,10000) * 0.01");
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);

        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addIndexColumns("symbol")
                .addIndexColumns("exchange")
                .build();

        writeTables(destPath, randomPartitions, instructions);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // Read the top-level index tables, causing the location indexes to be cached as well.
        final Table symbolIndexTable = DataIndexer.getDataIndex(diskTable, "symbol").table();
        final Table exchangeIndex = DataIndexer.getDataIndex(diskTable, "exchange").table();

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "symbol >= `0049`");
        filterAndVerifyResults(diskTable, memTable, "symbol = `0050`");
        filterAndVerifyResults(diskTable, memTable, "symbol.startsWith(`002`)");

        // int range and match filters
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "exchange % 10 == 0");

        // combined filters
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange <= 10", "exchange >= 9");
        filterAndVerifyResults(diskTable, memTable, "symbol < `0050`", "exchange = 10");
        filterAndVerifyResults(diskTable, memTable, "symbol.startsWith(`002`)", "exchange % 10 == 0");

        // mixed type with complex filters
        Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));

        // Rename columns and do some more tests.
        final Table renamedDiskTable = diskTable.renameColumns(
                "symbol_renamed=symbol",
                "exchange_renamed=exchange");

        final Table renamedMemTable = renamedDiskTable.select();

        // string range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`",
                "symbol_renamed >= `0049`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed = `0050`");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed.startsWith(`002`)");

        // int range and match filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed <= 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed <= 10", "exchange_renamed >= 9");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed = 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "exchange_renamed % 10 == 0");

        // combined filters
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`", "exchange_renamed <= 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`", "exchange_renamed <= 10",
                "exchange_renamed >= 9");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed < `0050`", "exchange_renamed = 10");
        filterAndVerifyResults(renamedDiskTable, renamedMemTable, "symbol_renamed.startsWith(`002`)",
                "exchange_renamed % 10 == 0");
    }

    @Test
    public void flatPartitionsWithMissingRowGroupMetadata() {
        // This dataset has two files, 0000 and 0005 that have missing row group metadata. Make sure to do some
        // tests which include row groups from these files.

        final String dirPath = ParquetTableFilterTest.class.getResource("/parquet_no_stat").getFile();

        final Table diskTable = ParquetTools.readTable(dirPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // NOTE: first file has 10k rows and `id` column is sequential, so id 0-9999 are in a file without
        // row group stats.

        filterAndVerifyResults(diskTable, memTable, "random_string = `xgsah`");
        filterAndVerifyResults(diskTable, memTable, "random_string < `zzzzz`", "random_string = `fiaai`");

        // int range and match filters
        filterAndVerifyResults(diskTable, memTable, "id <= 3000"); // entirely inside file with no metadata
        filterAndVerifyResults(diskTable, memTable, "id <= 1000", "id >= 900"); // inside
        filterAndVerifyResults(diskTable, memTable, "id <= 100000", "id >= 1900"); // all
        filterAndVerifyResults(diskTable, memTable, "id = 3000"); // inside
        filterAndVerifyResults(diskTable, memTable, "id = 50000"); // outside
        filterAndVerifyResults(diskTable, memTable, "id % 10 == 0"); // all

        // combined filters
        filterAndVerifyResults(diskTable, memTable, "random_int < 50", "id <= 3000");
        filterAndVerifyResults(diskTable, memTable, "random_int < 50", "id <= 100000", "id >= 1900");
        filterAndVerifyResults(diskTable, memTable, "random_int < 900", "id = 3000");
        filterAndVerifyResults(diskTable, memTable, "random_int < 900", "id = 50000");
    }

    /**
     * @see ParquetTableReadWriteTest#testReadingParquetDataWithEmptyRowGroups()
     */
    @Test
    public void parquetFilesWithEmptyRowGroup() {
        {
            // Single parquet file with empty row group
            final String path =
                    ParquetTableFilterTest.class.getResource("/ReferenceParquetWithEmptyRowGroup1.parquet")
                            .getFile();
            final Table diskTable = readTable(path);
            final Table memTable = diskTable.select();

            filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Foo <= 3000");
            filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Foo == null");
        }

        {
            // Single parquet file with three row groups, first and third row group are non-empty, and second row group
            // is empty.
            final String path =
                    ParquetTableFilterTest.class.getResource("/ReferenceParquetWithEmptyRowGroup2.parquet")
                            .getFile();
            final Table diskTable = readTable(path);
            final Table memTable = diskTable.select();

            filterAndVerifyResultsAllowEmpty(diskTable, memTable, "integers <= -1");
            filterAndVerifyResultsAllowEmpty(diskTable, memTable, "integers == null");
            filterAndVerifyResultsAllowEmpty(diskTable, memTable, "integers <= 1");
            filterAndVerifyResultsAllowEmpty(diskTable, memTable, "integers <= 3");
        }
    }

    /**
     * Single parquet file with three row groups, first and third row group are non-empty, and second row group is
     * empty. To generate this file, the following branch was used:
     * https://github.com/malhotrashivam/deephaven-core/tree/sm-empty-rowgroup-dict
     */
    @Test
    public void parquetFilesWithDictionaryAndEmptyRowGroups() {
        final String path =
                ParquetTableFilterTest.class.getResource("/ReferenceParquetWithDictionaryAndEmptyRowGroups.parquet")
                        .getFile();
        final Table diskTable = readTable(path);
        final Table memTable = diskTable.select();

        filterAndVerifyResults(diskTable, memTable, "animal == `Dog`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Cat`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Horse`");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "animal == `Fish`");

        final Table expected = TableTools.newTable(TableTools.col("animal", "Dog", "Cat", "Horse", "Horse"));
        assertTableEquals(memTable, expected);
    }

    @Test
    public void filterArrayColumnsTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_filterArrayColumnsTest.parquet").toString();
        final int tableSize = 1_000_000;

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "array_col = ii%2 == 0 ? null : new long[] {ii + 1}");

        writeTable(largeTable, destPath);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        filterAndVerifyResults(diskTable, memTable, "array_col == null");
        filterAndVerifyResults(diskTable, memTable, "array_col != null");
        filterAndVerifyResults(diskTable, memTable, "array_col != null && array_col[0] > 1");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "array_col != null && array_col[0] < 1");
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * schema = pa.schema([
     *     pa.field("Foo",
     *              pa.struct([pa.field("Field1", pa.int32(), nullable=False),
     *                         pa.field("Field2", pa.int32(), nullable=False)])),
     *     pa.field("Bar",
     *              pa.struct([pa.field("Field1", pa.int32(), nullable=False),
     *                         pa.field("Field2", pa.int32(), nullable=False)])),
     *     pa.field("Baz",   pa.int32()),
     *     pa.field("Longs", pa.list_(pa.int64()))
     * ])
     *
     * N = 10
     * table = pa.Table.from_pydict(
     *     {
     *         "Foo":   [{"Field1": i,      "Field2": -i}      for i in range(N)],
     *         "Bar":   [{"Field1": i * 10, "Field2": -i * 10} for i in range(N)],
     *         "Baz":   list(range(N)),
     *         "Longs": [[i * 1_000 + j for j in range(3)]     for i in range(N)],
     *     },
     *     schema=schema,
     * )
     *
     * pq.write_table(table, "NestedStruct3.parquet")
     * </pre>
     */
    @Test
    public void nestedStructsFilterTest() {
        // If we use an explicit definition, we can skip over struct columns and just read the Baz column.
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(TableDefinition.of(
                        ColumnDefinition.ofInt("Baz"),
                        ColumnDefinition.fromGenericType("Longs", long[].class, long.class)))
                .build();
        nestedStructsFilterImpl(instructions);
    }

    /**
     * @see #nestedStructsFilterTest
     */
    @Test
    public void nestedStructsFilterWithResolverTest() {
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(TableDefinition.of(
                        ColumnDefinition.ofInt("Baz"),
                        ColumnDefinition.fromGenericType("Longs", long[].class, long.class)))
                .setColumnResolverFactory((tk, tlk) -> ParquetColumnResolverMap.builder()
                        .putMap("Baz", List.of("Baz"))
                        .putMap("Longs", List.of("Longs", "list", "element"))
                        .build())
                .build();
        nestedStructsFilterImpl(instructions);
    }

    private static void nestedStructsFilterImpl(@NotNull final ParquetInstructions readInstructions) {
        final String path = ParquetTableFilterTest.class.getResource("/NestedStruct3.parquet").getFile();
        final Table diskTable = ParquetTools.readTable(path, readInstructions);
        final Table memTable = diskTable.select();
        assertTableEquals(diskTable, memTable);

        filterAndVerifyResults(diskTable, memTable, "Baz != null");
        filterAndVerifyResults(diskTable, memTable, "Baz < 5");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Baz == null");

        filterAndVerifyResults(diskTable, memTable, "Longs != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Longs == null");
    }

    @Test
    public void unsupportedColumnTypesPushdownTest() {
        final String dest = Path.of(rootFile.getPath(), "unsupportedColumnTypesPushdown.parquet").toString();

        // Array column
        {
            final Table source = TableTools.emptyTable(1_000).update(
                    "ArrayCol = ii % 2 == 0 ? null : new long[] {ii + 1}");
            assertUnsupportedPushdown(source, "ArrayCol != null", dest, EMPTY);
        }

        // StringSet column
        {
            ExecutionContext.getContext().getQueryLibrary().importClass(ArrayStringSet.class);
            ExecutionContext.getContext().getQueryLibrary().importClass(StringSet.class);
            final Table source = TableTools.emptyTable(1_000).update(
                    "StringSetCol = (StringSet) new ArrayStringSet(\"Hello\")");
            assertUnsupportedPushdown(source, "StringSetCol != null", dest, EMPTY);
        }

        // Custom codec
        {
            final Table source = TableTools.newTable(
                    new ColumnHolder<>("Decimals", BigDecimal.class, null, false,
                            BigDecimal.valueOf(123_423_367_532L), null, BigDecimal.valueOf(422_123_132_234L)));
            final ParquetInstructions writeInstructions = ParquetInstructions.builder()
                    .addColumnCodec("Decimals",
                            "io.deephaven.util.codec.BigDecimalCodec",
                            "20,1,allowrounding")
                    .build();
            assertUnsupportedPushdown(source, "Decimals != null", dest, writeInstructions);
        }
    }

    private static void assertUnsupportedPushdown(
            final Table source,
            final String filterExpr,
            final String destPath,
            final ParquetInstructions writeInstructions) {
        // Cycle to disk to get the proper column sources
        writeTable(source, destPath, writeInstructions);
        final Table disk_table = ParquetTools.readTable(destPath);

        final WhereFilter filter = getExpression(filterExpr);
        filter.init(disk_table.getDefinition());
        Assert.assertEquals("Expected a single column in the filter: " + filterExpr, 1, filter.getColumns().size());

        final AbstractColumnSource<?> diskColumnSource =
                (AbstractColumnSource<?>) disk_table.getColumnSource(filter.getColumns().get(0));

        final PushdownFilterContext context =
                diskColumnSource.makePushdownFilterContext(filter, List.of(diskColumnSource));

        final CompletableFuture<Long> costFuture = new CompletableFuture<>();
        diskColumnSource.estimatePushdownFilterCost(
                filter,
                source.getRowSet(),
                false,
                context,
                new ImmediateJobScheduler(),
                costFuture::complete,
                costFuture::completeExceptionally);
        Assert.assertTrue(costFuture.isDone());
        Assert.assertEquals(Long.MAX_VALUE, (long) costFuture.join());
    }

    /**
     * <pre>
     * import pandas as pd
     * import numpy as np
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * float_max = np.float32(float.fromhex('0x1.fffffep+127'))  # DH NULL_FLOAT
     * double_max = float.fromhex('0x1.fffffffffffffp+1023') # DH NULL_DOUBLE
     *
     * byte_vals  = np.array([-128, -42, -1, 0, 1, 42, 127],  dtype=np.int8)
     * short_vals = np.array([-32768, -12345, -1, 0, 1, 12345, 32767], dtype=np.int16)
     * char_vals  = np.array([0, 10000, 30000, 40000, 50000, 60000, 65535], dtype=np.uint16)
     * int_vals   = np.array([-2147483648, -123456, -1, 0, 1, 123456, 2147483647], dtype=np.int32)
     * long_vals  = np.array([-9223372036854775808, -1234567890123456789, -1, 0, 1, 1234567890123456789,  9223372036854775807], dtype=np.int64)
     * float_vals  = np.array([-float_max, -1.2345e5, -1.0, 0.0, 1.0, 1.2345e5, float_max], dtype=np.float32)
     * double_vals = np.array([-double_max, -1.23456789012345e12, -1.0, 0.0, 1.0, 1.23456789012345e12, double_max], dtype=np.float64)
     *
     * df = pd.DataFrame({
     *     "Bytes"  : byte_vals,
     *     "Shorts" : short_vals,
     *     "Chars"  : char_vals,
     *     "Ints"   : int_vals,
     *     "Longs"  : long_vals,
     *     "Floats" : float_vals,
     *     "Doubles": double_vals
     * })
     *
     * table = pa.Table.from_pandas(df)
     * pq.write_table(table, "ReferenceExtremeValues.parquet")
     * </pre>
     */
    @Test
    public void testForExtremes() {
        final String path = ParquetTableFilterTest.class.getResource("/ReferenceExtremeValues.parquet").getFile();
        final Table diskTable = readTable(path);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Bytes == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Bytes != null");

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Shorts == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Shorts != null");

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Chars == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Chars != null");

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Ints == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Ints != null");

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Longs == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Longs != null");

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Floats == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Floats != null");

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Doubles == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Doubles != null");
    }

    @Test
    public void testInt96Timestamps() {
        // Int96 timestamps do not have column-order as TYPE_DEFINED, so we cannot use the statistics to filter
        final String path = ParquetTableFilterTest.class.getResource("/ReferenceInt96Timestamps.parquet").getFile();
        {
            final ParquetMetadata metadata = new ParquetTableLocationKey(
                    new File(path).toURI(), 0, null, ParquetInstructions.EMPTY).getMetadata();
            final MessageType schema = metadata.getFileMetaData().getSchema();
            final Type type = schema.getType("event_time");
            assertTrue(type.isPrimitive());
            assertEquals(ColumnOrder.undefined(), type.asPrimitiveType().columnOrder());
        }
        final Table diskTable = ParquetTools.readTable(path);
        final Table memTable = diskTable.select();
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "event_time == null");
        filterAndVerifyResults(diskTable, memTable, "event_time != null");
        filterAndVerifyResults(diskTable, memTable, "event_time <= java.time.Instant.parse(\"2025-01-01T00:00:00Z\")");
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     * import numpy as np
     *
     * floats   = pa.array([1.23, np.nan, -4.56],  type=pa.float32())
     * doubles  = pa.array([1.23, np.nan, -4.56],  type=pa.float64())
     * table = pa.Table.from_arrays(
     *     [floats, doubles],
     *     names=["Floats", "Doubles"]
     * )
     * pq.write_table(table, "example.parquet")
     * </pre>
     */
    @Test
    public void testFilteringNaN() {
        // Read the reference parquet file with NaN values generated using PyArrow.
        final String path = ParquetTableFilterTest.class.getResource("/ReferenceFloatingPointNan.parquet").getFile();

        {
            // Pyarrow's parquet writing code does not write NaN values to statistics
            final Statistics<?> floatStats = getColumnStatistics(new File(path), "Floats");
            assertTrue(floatStats.hasNonNullValue());
            assertEquals(-4.56f, floatStats.genericGetMin());
            assertEquals(1.23f, floatStats.genericGetMax());
            final Statistics<?> doubleStats = getColumnStatistics(new File(path), "Doubles");
            assertTrue(doubleStats.hasNonNullValue());
            assertEquals(-4.56, doubleStats.genericGetMin());
            assertEquals(1.23, doubleStats.genericGetMax());
        }

        testFilteringNanImpl(ParquetTools.readTable(path));

        // Write a new parquet file with NaN values using DH and test the filtering.
        final String dest = Path.of(rootFile.getPath(), "filteringNan.parquet").toString();
        final Table source = newTable(
                floatCol("Floats", 1.23f, Float.NaN, -4.56f),
                doubleCol("Doubles", 1.23, Double.NaN, -4.56));
        writeTable(source, dest);

        {
            // Deephaven's parquet writing code writes NaN values to statistics, which are then corrected by Parquet
            // reading code.
            // TODO (DH-10771): Fix this so DH does not write NaN values to statistics.
            final Statistics<?> floatStats = getColumnStatistics(new File(dest), "Floats");
            assertFalse(floatStats.hasNonNullValue());
            final Statistics<?> doubleStats = getColumnStatistics(new File(dest), "Doubles");
            assertFalse(doubleStats.hasNonNullValue());
        }

        testFilteringNanImpl(readTable(dest));
    }

    @Test
    public void testEmptyMatchFilter() {
        final Table source = newTable(
                stringCol("strings", "a", "b", "c"),
                intCol("ints", 1, 2, 3));
        final String dest = Path.of(rootFile.getPath(), "testEmptyMatchFilter.parquet").toString();
        writeTable(source, dest);
        final Table diskTable = ParquetTools.readTable(dest);
        final Table memTable = diskTable.select();

        // Empty match filter should return no rows
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.REGULAR, "ints"));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.REGULAR, "strings"));

        // Inverted empty match filter should return all rows
        filterAndVerifyResults(diskTable, memTable,
                new MatchFilter(MatchOptions.INVERTED, "ints"));
        filterAndVerifyResults(diskTable, memTable,
                new MatchFilter(MatchOptions.INVERTED, "strings"));
    }

    @Test
    public void testInstantMatchFilter() {
        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);
        final Table source = TableTools.emptyTable(100).update("Timestamp = baseTime");

        final String dest = Path.of(rootFile.getPath(), "ParquetTest_InstantMatchFilter.parquet").toString();
        writeTable(source, dest);
        final Table diskTable = ParquetTools.readTable(dest);
        final Table memTable = diskTable.select();

        filterAndVerifyResults(diskTable, memTable,
                new MatchFilter(MatchOptions.REGULAR, "Timestamp", baseTime));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.INVERTED, "Timestamp", baseTime));
    }

    @Test
    public void testFilteringFloatInfinity() {
        final Table source = newTable(
                floatCol("floats", Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 1.0f, -1.0f));
        final String dest = Path.of(rootFile.getPath(), "testFilteringFloatInfinity.parquet").toString();
        writeTable(source, dest);
        {
            final Statistics<?> floatStats = getColumnStatistics(new File(dest), "floats");
            assertEquals(Float.NEGATIVE_INFINITY, floatStats.genericGetMin());
            assertEquals(Float.POSITIVE_INFINITY, floatStats.genericGetMax());
        }

        final Table diskTable = ParquetTools.readTable(dest);
        final Table memTable = diskTable.select();
        assertTableEquals(diskTable, memTable);

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "floats != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "floats == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "floats > 0");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "floats <= 0");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new FloatRangeFilter("floats", Float.NEGATIVE_INFINITY, 5.0f));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new FloatRangeFilter("floats", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new FloatRangeFilter("floats", -2.0f, Float.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new FloatRangeFilter("floats", Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.INVERTED, "floats", Float.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.INVERTED, "floats", Float.NEGATIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.REGULAR, "floats", Float.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.REGULAR, "floats", Float.NEGATIVE_INFINITY));
    }

    @Test
    public void testFilteringDoubleInfinity() {
        final Table source = newTable(
                doubleCol("doubles", Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.0f, -1.0f));
        final String dest = Path.of(rootFile.getPath(), "testFilteringDoubleInfinity.parquet").toString();
        writeTable(source, dest);
        {
            final Statistics<?> doubleStats = getColumnStatistics(new File(dest), "doubles");
            assertEquals(Double.NEGATIVE_INFINITY, doubleStats.genericGetMin());
            assertEquals(Double.POSITIVE_INFINITY, doubleStats.genericGetMax());
        }
        final Table diskTable = ParquetTools.readTable(dest);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "doubles != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "doubles == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "doubles > 0");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "doubles <= 0");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new DoubleRangeFilter("doubles", Double.NEGATIVE_INFINITY, 5.0));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new DoubleRangeFilter("doubles", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new DoubleRangeFilter("doubles", -2.0, Double.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new DoubleRangeFilter("doubles", Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.INVERTED, "doubles", Double.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.INVERTED, "doubles", Double.NEGATIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.REGULAR, "doubles", Double.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchOptions.REGULAR, "doubles", Double.NEGATIVE_INFINITY));
    }

    /**
     * Helper method to get the column statistics for the specified column name.
     */
    private static Statistics<?> getColumnStatistics(final File dest, final String columnName) {
        final ParquetMetadata metadata = new ParquetTableLocationKey(
                dest.toURI(), 0, null, ParquetInstructions.EMPTY).getMetadata();
        final MessageType schema = metadata.getFileMetaData().getSchema();
        final int colIdx = schema.getFieldIndex(columnName);
        final ColumnChunkMetaData columnChunkMetaData = metadata.getBlocks().get(0).getColumns().get(colIdx);
        return columnChunkMetaData.getStatistics();
    }

    private static void testFilteringNanImpl(final Table diskTable) {
        final Table memTable = diskTable.select();
        assertTableEquals(diskTable, memTable);

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Floats != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Floats == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Floats > 1");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Floats <= 1");

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Doubles != null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Doubles == null");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Doubles > 1");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "Doubles <= 1");
    }

    @Test
    public void testFilteringVirtualRowVariables() {
        final BiFunction<Long, Long, Boolean> function = (i, j) -> i > j;
        QueryScope.addParam("testFunction", function);

        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_virtualRowVariables") + ".parquet";
        final int tableSize = 100_000;

        final Table largeTable = TableTools.emptyTable(tableSize).update("val = ii % 50000");

        ParquetTools.writeTable(largeTable, destPath);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        filterAndVerifyResults(diskTable, memTable, "(boolean)testFunction.apply(ii, val)");
    }

    @Test
    public void testMergedTableWithParquet() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_testMergedTableWithParquet").toString();
        final int tableSize = 100_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime + i * 1_000_000_000L",
                "sequential_val = ii % 117 == 0 ? null : ii", // with nulls
                "symbol = ii % 119 == 0 ? null : String.format(`s%03d`, randomInt(0,1_000))");

        final int partitionCount = 5;
        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        final Table diskTable = ParquetTools.readTable(destPath);

        // Create another in-memory table to merge with the parquet table.
        final Instant baseTime2 = parseInstant("2015-01-02T00:00:00 NY");
        QueryScope.addParam("baseTime2", baseTime2);

        final Table largeTable2 = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime2 + i * 1_000_000_000L",
                "sequential_val = ii % 117 == 0 ? null : ii", // with nulls
                "symbol = ii % 119 == 0 ? null : String.format(`s%03d`, randomInt(0,1_000))");

        final Instant baseTime3 = parseInstant("2015-01-03T00:00:00 NY");
        QueryScope.addParam("baseTime3", baseTime3);
        final Table largeTable3 = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime3 + i * 1_000_000_000L",
                "sequential_val = 999999L", // with nulls
                "symbol = `sZZZZ`");

        Table mergedTable = TableTools.merge(diskTable, largeTable2, largeTable3);
        Table memTable = mergedTable.select();

        assertTableEquals(mergedTable, memTable);

        // Timestamp range and match filters
        filterAndVerifyResults(mergedTable, memTable, "Timestamp < '2023-01-01T01:00:00 NY'");
        filterAndVerifyResults(mergedTable, memTable, "Timestamp > '2023-01-01T01:00:00 NY'");
        filterAndVerifyResults(mergedTable, memTable, "Timestamp = '2023-01-01T01:00:00 NY'");

        // string range and match filters
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol = null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol != null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol = `1000`");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol < `1000`");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol = `5000`");

        // long range and match filters
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val = null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val != null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val <= 500");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val <= 5000", "sequential_val > 3000");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val = 500");

        // Reverse the table order and run the filters again
        mergedTable = TableTools.merge(largeTable3, largeTable2, diskTable);
        memTable = mergedTable.select();

        assertTableEquals(mergedTable, memTable);

        // Timestamp range and match filters
        filterAndVerifyResults(mergedTable, memTable, "Timestamp < '2023-01-01T01:00:00 NY'");
        filterAndVerifyResults(mergedTable, memTable, "Timestamp > '2023-01-01T01:00:00 NY'");
        filterAndVerifyResults(mergedTable, memTable, "Timestamp = '2023-01-01T01:00:00 NY'");

        // string range and match filters
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol = null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol != null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol = `1000`");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol < `1000`");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "symbol = `5000`");

        // long range and match filters
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val = null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val != null");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val <= 500");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val <= 5000", "sequential_val > 3000");
        filterAndVerifyResultsAllowEmpty(mergedTable, memTable, "sequential_val = 500");
    }

    @Test
    public void dictionaryWithNoStatisticsPartitionedTest() {
        final Table source1 = TableTools.newTable(
                stringCol("animal", "Dog", "Horse", "Zebra"));
        final Table source2 = TableTools.newTable(
                stringCol("animal", "Centipede", "Lion", "Cat", "Elephant", "Whale"));
        final Table source3 = TableTools.newTable(
                stringCol("animal", "Ant", "Horse"));

        final File destDir = new File(rootFile.getPath(), "dictionaryWithNoStatisticsPartitioned");
        assertTrue(destDir.mkdirs());

        // Disable writing row group statistics to verify filtering using dictionary
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setRowGroupInfo(RowGroupInfo.maxRows(2))
                .setWriteRowGroupStatistics(false)
                .build();

        // Write three tables to the same directory with multiple row groups
        writeTable(source1, new File(destDir, "part1.parquet").getAbsolutePath(), writeInstructions);
        writeTable(source2, new File(destDir, "part2.parquet").getAbsolutePath(), writeInstructions);
        writeTable(source3, new File(destDir, "part3.parquet").getAbsolutePath(), writeInstructions);

        // Read back as a flat partitioned table
        final Table diskTable = ParquetTools.readTable(destDir.getAbsolutePath(),
                EMPTY.withLayout(ParquetInstructions.ParquetFileLayout.FLAT_PARTITIONED));

        // Filter for values that are only in one of the source tables
        final Table memTable = diskTable.select();
        filterAndVerifyResults(diskTable, memTable, "animal == `Dog`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Horse`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Zebra`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Cat`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Whale`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Ant`");
        filterAndVerifyResults(diskTable, memTable, "animal == `Elephant` || animal == `Dog`");
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, "animal == `Parrot`");
    }

    @Test
    public void dictionaryConditionalFilterTest() {
        final Table source = TableTools.newTable(
                stringCol("animal", "Centipede", "Lion", "Elephant", "Cat", "Whale"));

        // Disable writing row group statistics to verify filtering using dictionary
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setRowGroupInfo(RowGroupInfo.maxRows(2))
                .setWriteRowGroupStatistics(false)
                .build();

        final String destPath = Path.of(rootFile.getPath(), "dictionaryConditionalFilter") + ".parquet";
        writeTable(source, destPath, writeInstructions);

        // Read back and test filtering
        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal = `Centipede`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal != `Centipede`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal = `Lion`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal = `Cat`"));

        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal = `Elephant`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal != `Elephant`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal < `Elephant`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal <= `Elephant`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal > `Elephant`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal >= `Elephant`"));

        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal = `Whale`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal != `Whale`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal <= `Whale`"));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                ConditionFilter.createConditionFilter("animal > `Whale`"));

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, ConditionFilter.createConditionFilter("animal = `Dog`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal < `Dog`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal > `Dog`"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal != `Dog`"));

        filterAndVerifyResultsAllowEmpty(diskTable, memTable, ConditionFilter.createConditionFilter("animal == null"));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable, ConditionFilter.createConditionFilter("animal != null"));

        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal.startsWith(`C`)"));
    }

    @Test
    public void dictionaryNullEntryFilterTest() {
        final Table source = TableTools.newTable(
                stringCol("animal", "Centipede", null, "Elephant", "Cat", "Cat"));

        // Disable writing row group statistics to verify filtering using dictionary
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setRowGroupInfo(RowGroupInfo.maxRows(2))
                .setWriteRowGroupStatistics(false)
                .build();

        final String destPath = Path.of(rootFile.getPath(), "dictionaryConditionalFilter") + ".parquet";
        writeTable(source, destPath, writeInstructions);

        // Read back and test filtering
        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        filterAndVerifyThrowsSame(diskTable, memTable, "animal.startsWith(`C`)");
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal = null"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal != null"));
        filterAndVerifyResults(diskTable, memTable, ConditionFilter.createConditionFilter("animal == `Cat`"));
    }

    @Test
    public void multiColumnConditionalFilters() {
        final Table source = TableTools.newTable(
                stringCol("animal", "Centipede", "Lion", "Elephant", "Cat", "Whale"),
                intCol("legs", 100, 4, 4, 4, 0),
                intCol("weight", 1, 420, 6000, 10, 150000));

        // Disable writing row group statistics to verify filtering using dictionary
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setRowGroupInfo(RowGroupInfo.maxRows(2))
                .setWriteRowGroupStatistics(false)
                .build();

        final String destPath = Path.of(rootFile.getPath(), "multiColumnConditionalFilters") + ".parquet";
        writeTable(source, destPath, writeInstructions);

        // Read back and test filtering
        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("animal = `Centipede` && legs >= 100"));
        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("animal != `Centipede` && legs < 4"));

        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("weight > 1000 || legs <= 4"));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                ConditionFilter.createStateless("weight > 1000 || legs < 4"));
    }

    @Test
    public void duplicatedColumnsConditionalFilter() {
        final Table source = TableTools.emptyTable(10).update("X = `` + ii");
        final String destPath = Path.of(rootFile.getPath(), "multiColumnConditionalFilters") + ".parquet";
        writeTable(source, destPath);
        final Table diskTable = ParquetTools.readTable(destPath);

        {
            final Function<Table, Table> transform =
                    t -> t.updateView("A = X", "B = X")
                            .where(ConditionFilter.createStateless("(A + B).length() > 2"));
            assertTableEquals(transform.apply(source), transform.apply(diskTable));
        }

        {
            final Function<Table, Table> transform =
                    t -> t.updateView("A = X", "B = X")
                            .where(ConditionFilter.createStateless("A.length() > 1 && B.length() > 1"));
            assertTableEquals(transform.apply(source), transform.apply(diskTable));
        }
    }

    public static class TestHelperClass {
        public TestHelperClass() {}

        public boolean compare(long p1, long p2) {
            return p1 > p2;
        }
    }

    @Test
    public void testWithStaticMethodReferenceFilter() {
        final Table source = TableTools.emptyTable(10).update("X = (ii % 2 == 0) ? ii : ii + 1");
        final String destPath = Path.of(rootFile.getPath(), "withStaticMethodReferenceFilter") + ".parquet";
        writeTable(source, destPath);
        final Table diskTable = ParquetTools.readTable(destPath);

        ExecutionContext.getContext().getQueryLibrary().importClass(TestHelperClass.class);

        filterAndVerifyResults(diskTable, diskTable.select(),
                ConditionFilter.createStateless("new TestHelperClass().compare(X, ii)"));
    }

    @Test
    public void testMixedDictionaryEncodingRowGroups() {
        final Table source = TableTools.newTable(
                stringCol("StringCol",
                        "This", "is", "okay", // Row group 1
                        "This is too long for dictionary", "but we keep going", null, // Row group 2
                        "Something", null)); // Row group 3

        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setRowGroupInfo(RowGroupInfo.maxRows(3))
                .setMaximumDictionarySize(15) // Force second row group to use non-dictionary encoding
                .setWriteRowGroupStatistics(false)
                .build();

        final String destPath = Path.of(rootFile.getPath(), "mixedDictionaryEncodingRowGroups") + ".parquet";
        writeTable(source, destPath, writeInstructions);

        // Verify the first and third row group are properly dictionary encoded, while the second is not
        {
            final ParquetMetadata metadata =
                    new ParquetTableLocationKey(new File(destPath).toURI(), 0, null, ParquetInstructions.EMPTY)
                            .getMetadata();
            final String firstRowGroupMetadata = metadata.getBlocks().get(0).getColumns().get(0).toString();
            assertTrue(firstRowGroupMetadata.contains("StringCol") && firstRowGroupMetadata.contains("RLE_DICTIONARY"));

            final String secondRowGroupMetadata = metadata.getBlocks().get(1).getColumns().get(0).toString();
            assertTrue(
                    secondRowGroupMetadata.contains("StringCol") && !secondRowGroupMetadata.contains("RLE_DICTIONARY"));

            final String thirdRowGroupMetadata = metadata.getBlocks().get(2).getColumns().get(0).toString();
            assertTrue(thirdRowGroupMetadata.contains("StringCol") && thirdRowGroupMetadata.contains("RLE_DICTIONARY"));
        }

        // Read back and test filtering
        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = `This`"));
        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = `but we keep going`"));
        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = null"));
        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = null || StringCol = `This`"));
        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = null || StringCol != null"));
    }

    @Test
    public void testNonDictionaryEncodingStrings() {
        final Table source = TableTools.newTable(
                stringCol("StringCol",
                        "This is too long for dictionary", "but we keep going", null, "anyways", null));

        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .setMaximumDictionarySize(15) // Force row group to use non-dictionary encoding
                .setWriteRowGroupStatistics(false)
                .build();

        final String destPath = Path.of(rootFile.getPath(), "nonDictionaryEncodingStrings") + ".parquet";
        writeTable(source, destPath, writeInstructions);

        // Verify tht the row group is not dictionary encoded
        {
            final ParquetMetadata metadata =
                    new ParquetTableLocationKey(new File(destPath).toURI(), 0, null, ParquetInstructions.EMPTY)
                            .getMetadata();
            final String rowGroupMetadata = metadata.getBlocks().get(0).getColumns().get(0).toString();
            assertTrue(rowGroupMetadata.contains("StringCol") && !rowGroupMetadata.contains("RLE_DICTIONARY"));
        }

        // Read back and test filtering
        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = `anyways`"));
        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = null"));
        filterAndVerifyResults(diskTable, memTable,
                ConditionFilter.createStateless("StringCol = null || StringCol != null"));
    }

    @Test
    public void testLocationDataIndexWithFilterBarriers() {
        final Table memTable = TableTools.emptyTable(100_000).update("A = ii % 97", "B = ii % 11", "C = ii");
        final String destPath = Path.of(rootFile.getPath(), "locationDataIndexWithFilterBarriers") + ".parquet";
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .addIndexColumns("A")
                .build();
        writeTable(memTable, destPath, writeInstructions);

        final Table diskTable = ParquetTools.readTable(destPath);
        assertTableEquals(memTable, diskTable);

        // Create some capturing filters to verify the row sets being passed through the filter chain.
        final RowSetCapturingFilter filterA = new ParallelizedRowSetCapturingFilter(RawString.of("A < 50"));
        final RowSetCapturingFilter filterB = new ParallelizedRowSetCapturingFilter(RawString.of("B < 5"));

        final List<RowSetCapturingFilter> allFilters = List.of(filterA, filterB);

        Table result;

        // Test with no barrier, expect A then B
        result = diskTable.where(Filter.and(filterA, filterB));
        assertEquals(97, filterA.numRowsProcessed()); // only indexA rows
        assertEquals(51550, filterB.numRowsProcessed());

        assertEquals(23435, result.size());
        allFilters.forEach(RowSetCapturingFilter::reset);

        // Test with no barrier, expect A then B despite user ordering
        result = diskTable.where(Filter.and(filterB, filterA));
        assertEquals(97, filterA.numRowsProcessed()); // only indexA rows
        assertEquals(51550, filterB.numRowsProcessed());

        assertEquals(23435, result.size());
        allFilters.forEach(RowSetCapturingFilter::reset);

        // Barrier to force B then A
        result = diskTable.where(Filter.and(filterB.withDeclaredBarriers("b1"), filterA.withRespectedBarriers("b1")));
        assertEquals(97, filterA.numRowsProcessed()); // only indexA rows
        assertEquals(100_000, filterB.numRowsProcessed());

        assertEquals(23435, result.size());
        allFilters.forEach(RowSetCapturingFilter::reset);

        // Barrier to force B then A
        result = diskTable.where(Filter.and(filterB.withSerial(), filterA));
        assertEquals(97, filterA.numRowsProcessed()); // only indexA rows
        assertEquals(100_000, filterB.numRowsProcessed());

        assertEquals(23435, result.size());
        allFilters.forEach(RowSetCapturingFilter::reset);

        // Inverted - Barrier to force B then A
        result = diskTable.where(Filter.and(
                WhereFilterInvertedImpl.of(filterB.withDeclaredBarriers("b1")),
                WhereFilterInvertedImpl.of(filterA.withRespectedBarriers("b1"))));
        assertEquals(97, filterA.numRowsProcessed()); // only indexA rows
        assertEquals(100_000, filterB.numRowsProcessed());

        assertEquals(26430, result.size());
        allFilters.forEach(RowSetCapturingFilter::reset);
    }

    @Test
    public void testMergedPartitioningTableColumnRegions() {
        // Partitioning columns are automatically added to a data index. We have to disable use of the data index
        // in order to test constant and null column region pushdown features.
        QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX = true;

        QueryScope.addParam("symList", List.of("alpha", "bravo", "charlie", "delta", "echo", "foxtrot"));
        final Table tmpTable = TableTools.emptyTable(100_000).update(
                "Sym = i % 7 == 6 ? (String)null : (String)symList.get(i % 7)",
                "A = ii % 97 == 0 ? null : ii % 97",
                "B = ii % 11 == 0 ? null : ii % 11",
                "C = ii");
        final PartitionedTable partitionedTable = tmpTable.partitionBy("Sym", "A");

        final String destPath = Path.of(rootFile.getPath(), "partitioningTableColumnRegions").toString();
        final ParquetInstructions writeInstructions = new ParquetInstructions.Builder()
                .build();
        writeKeyValuePartitionedTable(partitionedTable, destPath, writeInstructions);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        final Filter filterSym = RawString.of("Sym in `alpha`, `bravo`");
        final Filter filterA = RawString.of("A < 50");
        final Filter filterB = RawString.of("B < 5");


        // Create some capturing filters to verify the row sets being passed through the filter chain.
        try (final RowSetCapturingFilter capturingFilterSym = new ParallelizedRowSetCapturingFilter(filterSym);
                final RowSetCapturingFilter capturingFilterA = new ParallelizedRowSetCapturingFilter(filterA);
                final RowSetCapturingFilter capturingFilterB = new ParallelizedRowSetCapturingFilter(filterB)) {

            final List<RowSetCapturingFilter> allFilters =
                    List.of(capturingFilterSym, capturingFilterA, capturingFilterB);

            Table result;

            result = diskTable.where(capturingFilterSym).coalesce();

            // Expect one test per partitioning column region. The disk table has 7 * 97 = 679 regions
            assertEquals(679, capturingFilterSym.numRowsProcessed());
            assertEquals(28572, result.size());

            // Use the unwrapped filter to test other optimization paths (i.e. chunk filtering) and assert equality.
            assertTableEquals(result, memTable.where(filterSym));
            assertTableEquals(result, diskTable.where(filterSym));

            allFilters.forEach(RowSetCapturingFilter::reset);

            //////////////////////////////////////////////////////

            result = diskTable.where(capturingFilterA).coalesce();

            // Expect one test per partitioning column region.
            assertEquals(679, capturingFilterA.numRowsProcessed());
            assertEquals(51550, result.size());

            // Use the unwrapped filter to test other optimization paths (i.e. chunk filtering) and assert equality.
            assertTableEquals(result, memTable.where(filterA));
            assertTableEquals(result, diskTable.where(filterA));

            allFilters.forEach(RowSetCapturingFilter::reset);

            //////////////////////////////////////////////////////

            result = diskTable.where(capturingFilterB).coalesce();

            // All rows to be tested.
            assertEquals(100000, capturingFilterB.numRowsProcessed());
            assertEquals(45455, result.size());

            // Use the unwrapped filter to test other optimization paths (i.e. chunk filtering) and assert equality.
            assertTableEquals(result, memTable.where(filterB));
            assertTableEquals(result, diskTable.where(filterB));

            allFilters.forEach(RowSetCapturingFilter::reset);

            //////////////////////////////////////////////////////

            result = diskTable.where(Filter.and(capturingFilterSym, capturingFilterA)).coalesce();

            // All regions tested for Sym match
            assertEquals(679, capturingFilterSym.numRowsProcessed());
            // A subset of regions tested for A match
            assertEquals(194, capturingFilterA.numRowsProcessed());
            assertEquals(14729, result.size());

            // Use the unwrapped filter to test other optimization paths (i.e. chunk filtering) and assert equality.
            assertTableEquals(result, memTable.where(Filter.and(filterSym, filterA)));
            assertTableEquals(result, diskTable.where(Filter.and(filterSym, filterA)));

            allFilters.forEach(RowSetCapturingFilter::reset);
        }
    }
}
