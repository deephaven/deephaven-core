//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.time.DateTimeUtils.parseInstant;

@Category(OutOfBandTest.class)
public final class ParquetTableFilterTest {

    private static final String ROOT_FILENAME = ParquetTableFilterTest.class.getName() + "_root";
    private static final int LARGE_TABLE_SIZE = 2_000_000;

    private static final ParquetInstructions EMPTY = ParquetInstructions.EMPTY;
    private static final ParquetInstructions REFRESHING = ParquetInstructions.builder().setIsRefreshing(true).build();

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
            for (int i = 0; i < numSplits / 2   ; i++) {
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
                "Timestamp = baseTime + i * 1_000_000_000L",
                "symbol = randomInt(0,10)",
                "price = randomInt(0,10000) * 0.01f",
                "str_id = `str_` + String.format(`%08d`, randomInt(0,1_000_000))",
                "indexed_val = ii % 10_000");
        final int partitionCount = 17;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        // Re-write the schema to have only long and double datatypes
        final TableDefinition readDef = TableDefinition.of(
                ColumnDefinition.ofTime("Timestamp"),
                ColumnDefinition.ofLong("symbol"),
                ColumnDefinition.ofDouble("price"),
                ColumnDefinition.ofString("str_id"),
                ColumnDefinition.ofLong("indexed_val")
        );

        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(readDef)
                .build();

        final Table fromDisk = ParquetTools.readTable(destPath, instructions);
        assertTableEquals(largeTable, fromDisk);

        final Table filtered = fromDisk.where("symbol = 0");

        assertTableEquals(largeTable.where("symbol = 0"), filtered);
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
                "symbol = randomInt(0,10000)",
                "price = randomInt(0,100000) * 0.01",
                "str_id = `str_` + String.format(`%08d`, randomInt(0,1_000_000))",
                "indexed_val = ii % 10_000",
                "sequential_bd = java.math.BigDecimal.valueOf(ii * 0.1)");
        final int partitionCount = 17;

        final Table[] randomPartitions = splitTable(largeTable, partitionCount, true);
        writeTables(destPath, randomPartitions, EMPTY);

        final Table diskTable = ParquetTools.readTable(destPath);
        final Table memTable = diskTable.select();

        assertTableEquals(diskTable, memTable);

        Table filtered;

        // string range and match filters
        filtered = diskTable.where("str_id < `aabaa`");
        assertTableEquals(memTable.where("str_id < `aabaa`"), filtered);

        filtered = diskTable.where("str_id = `aabaa`");
        assertTableEquals(memTable.where("str_id = `aabaa`"), filtered);

        // Timestamp range and match filters
        filtered = diskTable.where("Timestamp < '2023-02-01T00:00:00 NY'");
        assertTableEquals(memTable.where("Timestamp < '2023-02-01T00:00:00 NY'"), filtered);

        filtered = diskTable.where("Timestamp = '2023-01-05T00:00:00 NY'");
        assertTableEquals(memTable.where("Timestamp = '2023-01-05T00:00:00 NY'"), filtered);

        // BigDecimal range and match filters
        ExecutionContext.getContext().getQueryScope().putParam("bd_500", BigDecimal.valueOf(500.00));
        ExecutionContext.getContext().getQueryScope().putParam("bd_1000", BigDecimal.valueOf(1000.00));

        filtered = diskTable.where("sequential_bd < bd_500");
        assertTableEquals(memTable.where("sequential_bd < bd_500"), filtered);

        filtered = diskTable.where("sequential_bd >= bd_500", "sequential_bd < bd_1000");
        assertTableEquals(memTable.where("sequential_bd >= bd_500", "sequential_bd < bd_1000"), filtered);

        // long range and match filters
        filtered = diskTable.where("sequential_val <= 500");
        assertTableEquals(memTable.where("sequential_val <= 500"), filtered);

        filtered = diskTable.where("sequential_val <= 5000", "sequential_val > 3000");
        assertTableEquals(memTable.where("sequential_val <= 5000", "sequential_val > 3000"), filtered);

        filtered = diskTable.where("sequential_val = 500");
        assertTableEquals(memTable.where("sequential_val = 500"), filtered);
    }
}
