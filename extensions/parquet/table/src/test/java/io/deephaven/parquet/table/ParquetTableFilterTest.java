//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.filter.Filter;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.time.DateTimeUtils.parseInstant;

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
            System.out.println("Writing table " + tableName + " to " + destPath);
            ParquetTools.writeTable(table, Path.of(destPath, tableName).toString(), instructions);
        }
    }

    @Test
    public void filterDatatypeMismatch() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "sequential_val = i",
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

    private void verifyResults(Table filteredDiskTable, Table filteredMemTable) {
        Assert.assertFalse("filteredMemTable.isEmpty()", filteredMemTable.isEmpty());
        Assert.assertFalse("filteredDiskTable.isEmpty()", filteredDiskTable.isEmpty());
        assertTableEquals(filteredDiskTable, filteredMemTable);
    }

    private void filterAndVerifyResults(Table diskTable, Table memTable, String... filters) {
        verifyResults(diskTable.where(filters).coalesce(), memTable.where(filters).coalesce());
    }

    @Test
    public void flatPartitionsNoDataIndexTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "Timestamp = baseTime + i * 1_000_000_000L",
                "sequential_val = ii",
                "symbol = String.format(`%04d`, randomInt(0,10_000))",
                "sequential_bd = java.math.BigDecimal.valueOf(ii * 0.1)",
                "sequential_bi = java.math.BigInteger.valueOf(ii)");
        final int partitionCount = 11;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        // string range and match filters
        filterAndVerifyResults(diskTable, memTable, "symbol < `1000`");
        filterAndVerifyResults(diskTable, memTable, "symbol = `5000`");

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
        filterAndVerifyResults(diskTable, memTable, "sequential_val <= 500");
        filterAndVerifyResults(diskTable, memTable, "sequential_val <= 5000", "sequential_val > 3000");
        filterAndVerifyResults(diskTable, memTable, "sequential_val = 500");

        // mixed type with complex filters
        final Filter complexFilter =
                Filter.or(Filter.from("sequential_val <= 500", "sequential_val = 555", "symbol > `1000`"));
        verifyResults(diskTable.where(complexFilter), memTable.where(complexFilter));
    }

    @Test
    public void flatPartitionsDataIndexRandomTest() {
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
    public void flatPartitionsDataIndexSequentialTest() {
        final String destPath = Path.of(rootFile.getPath(), "ParquetTest_flatPartitionsTest").toString();
        final int tableSize = 1_000_000;

        final Instant baseTime = parseInstant("2023-01-01T00:00:00 NY");
        QueryScope.addParam("baseTime", baseTime);

        // large consecutive blocks of sequential data
        final Table largeTable = TableTools.emptyTable(tableSize).update(
                "symbol = String.format(`%04d`, (long)(ii / 1000))", // produces 0000 to 0999 given 1M rows
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
    }
}
