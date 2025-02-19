//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.api.agg.Aggregation.AggSum;
import static io.deephaven.engine.table.impl.TestHierarchicalTableSnapshots.freeSnapshotTableChunks;
import static io.deephaven.engine.table.impl.TestHierarchicalTableSnapshots.snapshotToTable;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.util.TableTools.longCol;

@Category(OutOfBandTest.class)
public class TestRollupTable {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    // This is the list of supported aggregations for rollup. These are all using `intCol` as the column to aggregate
    // because the re-aggregation logic is effectively the same for all column types.
    private final Collection<Aggregation> aggs = List.of(
            AggAbsSum("absSum=intCol"),
            AggAvg("avg=intCol"),
            AggCount("count"),
            AggCountWhere("countWhere", "intCol > 50"),
            AggCountDistinct("countDistinct=intCol"),
            AggDistinct("distinct=intCol"),
            AggFirst("first=intCol"),
            AggLast("last=intCol"),
            AggMax("max=intCol"),
            AggMin("min=intCol"),
            AggSortedFirst("Sym", "firstSorted=intCol"),
            AggSortedLast("Sym", "lastSorted=intCol"),
            AggStd("std=intCol"),
            AggSum("sum=intCol"),
            AggUnique("unique=intCol"),
            AggVar("var=intCol"),
            AggWAvg("intCol", "wavg=intCol"),
            AggWSum("intCol", "wsum=intCol"));

    // Companion list of columns to compare between rollup root and the zero-key equivalent
    private final String[] columnsToCompare = new String[] {
            "absSum",
            "avg",
            "count",
            "countWhere",
            "countDistinct",
            "distinct",
            "first",
            "last",
            "max",
            "min",
            "firstSorted",
            "lastSorted",
            "std",
            "sum",
            "unique",
            "var",
            "wavg",
            "wsum"
    };

    /**
     * Perform a large table test, comparing the rollup table root to the zero-key equivalent table, incorporating all
     * supported aggregations.
     */
    @Test
    public void testRollupVsZeroKeyStatic() {
        final Random random = new Random(0);
        // Create the test table
        final ColumnInfo[] columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(10, 1_000));

        final Table testTable = getTable(false, 100_000, random, columnInfo);

        final RollupTable rollupTable = testTable.rollup(aggs, false, "Sym");
        final Table rootTable = rollupTable.getRoot();

        final Table actual = rootTable.select(columnsToCompare);
        final Table expected = testTable.aggBy(aggs);

        // Compare the zero-key equivalent table to the rollup table root
        TstUtils.assertTableEquals(actual, expected);
    }

    /**
     * Perform a large table test, comparing the rollup table root to the zero-key equivalent table, incorporating all
     * supported aggregations.
     */
    @Test
    public void testRollupVsZeroKeyIncremental() {
        for (int size = 10; size <= 1000; size *= 10) {
            testRollupIncrementalInternal("size-" + size, size);
        }
    }

    private void testRollupIncrementalInternal(final String ctxt, final int size) {
        final Random random = new Random(0);

        final ColumnInfo[] columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(10, 1_000));

        final QueryTable testTable = getTable(true, 100_000, random, columnInfo);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(
                        testTable.rollup(aggs, false, "Sym")
                                .getRoot().select(columnsToCompare),
                        testTable.aggBy(aggs))
        };

        final int steps = 100;
        for (int step = 0; step < steps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep(ctxt + " step == " + step, size, random, testTable, columnInfo, en);
        }
    }

    @Test
    public void testRollupMultipleOps() throws CsvReaderException {
        final String data = "A,B,C,N\n" +
                "Apple,One,Alpha,1\n" +
                "Apple,One,Alpha,2\n" +
                "Apple,One,Bravo,3\n" +
                "Apple,One,Bravo,4\n" +
                "Apple,One,Bravo,5\n" +
                "Apple,One,Bravo,6\n" +
                "Banana,Two,Alpha,7\n" +
                "Banana,Two,Alpha,8\n" +
                "Banana,Two,Bravo,3\n" +
                "Banana,Two,Bravo,4\n" +
                "Banana,Three,Bravo,1\n" +
                "Banana,Three,Bravo,1\n";
        final Table source = CsvTools.readCsv(new ByteArrayInputStream(data.getBytes()));

        // Make a simple rollup
        final Collection<Aggregation> aggs = List.of(
                AggCount("count"),
                AggSum("sumN=N"));

        final String[] arrayWithNull = new String[1];

        final RollupTable rollupTable = source.rollup(aggs, false, "A", "B", "C");

        // format, update multiple times, then sort by the final updateView column
        final RollupTable customRollup = rollupTable.withNodeOperations(
                rollupTable.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated)
                        .formatColumns("sumN=`#00FF00`")
                        .updateView("sumNPlus1 = sumN + 1")
                        .formatColumns("sumNPlus1=`#FF0000`")
                        .updateView("sumNPlus2 = sumNPlus1 + 1")
                        .sort("sumNPlus2"));

        final Table customKeyTable = newTable(
                intCol(customRollup.getRowDepthColumn().name(), 0),
                stringCol("A", arrayWithNull),
                stringCol("B", arrayWithNull),
                stringCol("C", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ssCustom = customRollup.makeSnapshotState();
        final Table customSnapshot =
                snapshotToTable(customRollup, ssCustom, customKeyTable, ColumnName.of("Action"), null,
                        RowSetFactory.flat(30));
        TableTools.showWithRowSet(customSnapshot);

        final Table expected = newTable(
                stringCol("A", null, "Apple", "Apple", "Apple", "Apple", "Banana", "Banana", "Banana", "Banana",
                        "Banana", "Banana"),
                stringCol("B", null, null, "One", "One", "One", null, "Three", "Three", "Two", "Two", "Two"),
                stringCol("C", null, null, null, "Alpha", "Bravo", null, null, "Bravo", null, "Bravo", "Alpha"),
                longCol("count", 12, 6, 6, 2, 4, 6, 2, 2, 4, 2, 2),
                longCol("sumN", 45, 21, 21, 3, 18, 24, 2, 2, 22, 7, 15),
                longCol("sumNPlus1", 46, 22, 22, 4, 19, 25, 3, 3, 23, 8, 16),
                longCol("sumNPlus2", 47, 23, 23, 5, 20, 26, 4, 4, 24, 9, 17));

        // Truncate the table and compare to expected.
        assertTableEquals(expected, customSnapshot.view("A", "B", "C", "count", "sumN", "sumNPlus1", "sumNPlus2"));

        freeSnapshotTableChunks(customSnapshot);
    }
}
