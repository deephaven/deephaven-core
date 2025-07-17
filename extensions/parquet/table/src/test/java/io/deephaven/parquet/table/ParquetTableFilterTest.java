//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.filter.Filter;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.FloatRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.location.ParquetColumnResolverMap;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
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

import static io.deephaven.base.FileUtils.convertToURI;
import static io.deephaven.engine.table.impl.select.WhereFilterFactory.getExpression;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.parquet.table.ParquetTools.readTable;
import static io.deephaven.parquet.table.ParquetTools.writeTable;
import static io.deephaven.time.DateTimeUtils.parseInstant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@Category(OutOfBandTest.class)
public final class ParquetTableFilterTest {

    private static final String ROOT_FILENAME = ParquetTableFilterTest.class.getName() + "_root";
    private static final ParquetInstructions EMPTY = ParquetInstructions.EMPTY;

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
        final Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
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
        final Filter complexFilter =
                Filter.or(Filter.from("sequential_val <= 500", "sequential_val = 555", "symbol > `1000`"));
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
        final Filter complexFilter =
                Filter.or(Filter.from("sequential_val <= 500", "sequential_val = 555", "symbol > `100`"));
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
        final Filter complexFilter =
                Filter.or(Filter.from("sequential_val_renamed <= 500", "sequential_val_renamed = 555",
                        "symbol_renamed > `1000`"));
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
        final Filter complexFilter =
                Filter.or(Filter.from("sequential_val_renamed <= 500", "sequential_val_renamed = 555",
                        "symbol_renamed > `1000`"));
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
        final Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));
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
        final Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
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
        final Filter complexFilter = Filter.or(Filter.from("symbol < `1000`", "symbol > `0900`", "exchange = 10"));
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

    private static final PushdownFilterContext TEST_PUSHDOWN_FILTER_CONTEXT = new BasePushdownFilterContext() {
        @Override
        public Map<String, String> renameMap() {
            return Map.of();
        }
    };

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
        writeTable(source, destPath, writeInstructions);

        final ParquetTableLocation location = new ParquetTableLocation(
                StandaloneTableKey.getInstance(),
                new ParquetTableLocationKey(
                        convertToURI(destPath, false),
                        0, Map.of(), EMPTY),
                EMPTY);
        final WhereFilter filter = getExpression(filterExpr);
        filter.init(source.getDefinition());

        Assert.assertEquals(Long.MAX_VALUE,
                location.estimatePushdownFilterCost(
                        filter,
                        source.getRowSet(),
                        source.getRowSet(),
                        false,
                        TEST_PUSHDOWN_FILTER_CONTEXT));
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
                new MatchFilter(MatchFilter.MatchType.Regular, "ints"));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.CaseSensitivity.MatchCase, MatchFilter.MatchType.Regular, "strings"));

        // Inverted empty match filter should return all rows
        filterAndVerifyResults(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Inverted, "ints"));
        filterAndVerifyResults(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Inverted, "strings"));
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
                new MatchFilter(MatchFilter.MatchType.Regular, "Timestamp", baseTime));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Inverted, "Timestamp", baseTime));
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
                new MatchFilter(MatchFilter.MatchType.Inverted, "floats", Float.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Inverted, "floats", Float.NEGATIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Regular, "floats", Float.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Regular, "floats", Float.NEGATIVE_INFINITY));
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
                new MatchFilter(MatchFilter.MatchType.Inverted, "doubles", Double.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Inverted, "doubles", Double.NEGATIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Regular, "doubles", Double.POSITIVE_INFINITY));
        filterAndVerifyResultsAllowEmpty(diskTable, memTable,
                new MatchFilter(MatchFilter.MatchType.Regular, "doubles", Double.NEGATIVE_INFINITY));
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
}
