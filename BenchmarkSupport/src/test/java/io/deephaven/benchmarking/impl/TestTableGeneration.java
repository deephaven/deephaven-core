package io.deephaven.benchmarking.impl;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.QueryTableTestBase;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;

public class TestTableGeneration extends QueryTableTestBase {

    public void testCreateHistorical() {
        final PersistentBenchmarkTableBuilder builder =
            BenchmarkTools.persistentTableBuilder("Carlos", 2000);
        final BenchmarkTable bt = builder.setSeed(0xDEADBEEF)
            .addColumn(BenchmarkTools.stringCol("Stringy", 1, 10))
            .addColumn(BenchmarkTools.numberCol("C2", int.class))
            .addColumn(BenchmarkTools.numberCol("C3", double.class))
            .addColumn(BenchmarkTools.stringCol("C4", 10, 5, 7, 0xFEEDBEEF))
            .addColumn(BenchmarkTools.stringCol("Thingy", 30, 6, 6, 0xB00FB00F))
            .addGroupingColumns("Thingy")
            .setPartitioningFormula("${autobalance_single}")
            .setPartitionCount(10)
            .build();

        final Table historicalTable = bt.getTable();
        Table selected = historicalTable.select();

        // Make sure it gets recorded properly
        assertEquals(2000, selected.size());

        // Make sure we can generate more
        bt.cleanup();


        // Next make sure it's repeatable
        bt.reset();

        assertEquals("", TableTools.diff(bt.getTable(), historicalTable, 1));
    }

    public void testCreateIntraday() {
        final BenchmarkTableBuilder builder = BenchmarkTools.persistentTableBuilder("Carlos", 2000);
        final BenchmarkTable bt = builder.setSeed(0xDEADBEEF)
            .addColumn(BenchmarkTools.stringCol("Stringy", 1, 10))
            .addColumn(BenchmarkTools.numberCol("C2", int.class))
            .addColumn(BenchmarkTools.numberCol("C3", double.class))
            .addColumn(BenchmarkTools.stringCol("C4", 10, 5, 7, 0xFEEDBEEF))
            .addColumn(BenchmarkTools.stringCol("Thingy", 30, 6, 6, 0xB00FB00F))
            .build();

        final Table intradayTable = bt.getTable();

        // Make sure it gets recorded properly
        assertEquals(2000, intradayTable.size());

        // Make sure we can generate more
        bt.cleanup();

        // Next make sure it's repeatable
        bt.reset();

        assertEquals("", TableTools.diff(bt.getTable(), intradayTable, 1));
    }

    public void testCreateSparseInMemory() {
        final BenchmarkTableBuilder builder = BenchmarkTools.inMemoryTableBuilder("Carlos", 200000);
        final BenchmarkTable bt = builder.setSeed(0xDEADBEEF)
            .addColumn(BenchmarkTools.stringCol("Stringy", 1, 10))
            .addColumn(BenchmarkTools.numberCol("C2", int.class))
            .addColumn(BenchmarkTools.numberCol("C3", double.class))
            .addColumn(BenchmarkTools.stringCol("C4", 10, 5, 7, 0xFEEDBEEF))
            .addColumn(BenchmarkTools.stringCol("Thingy", 30, 6, 6, 0xB00FB00F))
            .build();

        final Table resultTable = BenchmarkTools.applySparsity(bt.getTable(), 2000, 1, 0);

        assertEquals(2000, resultTable.size());
        assertTrue(resultTable.getIndex().lastKey() > 100000);
        // Make sure we can generate more
        bt.cleanup();
    }
}
