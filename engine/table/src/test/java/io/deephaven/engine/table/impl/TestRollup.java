//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.freeSnapshotTableChunks;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.snapshotToTable;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.util.TableTools.byteCol;

@Category(OutOfBandTest.class)
public class TestRollup extends RefreshingTableTestCase {
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

    @SuppressWarnings("rawtypes")
    private final ColumnInfo[] columnInfo = initColumnInfos(
            new String[] {"Sym", "intCol"},
            new SetGenerator<>("a", "b", "c", "d"),
            new IntGenerator(10, 100));

    private QueryTable createTable(boolean refreshing, int size, Random random) {
        return getTable(refreshing, size, random, columnInfo);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testRollup() {
        final Random random = new Random(0);
        // Create the test table
        final Table testTable = createTable(false, 100_000, random);

        final RollupTable rollupTable = testTable.rollup(aggs, false, "Sym");

        final Table rootTable = rollupTable.getRoot();

        final Table actual = rootTable.select(columnsToCompare);
        System.out.println("*** Actual ***");
        TableTools.show(actual, 100);
        final Table expected = testTable.aggBy(aggs);

        // Compare the zero-key equivalent table to the rollup table root
        TstUtils.assertTableEquals(actual, expected);
    }

    @Test
    public void testRollupIncremental() {
        for (int size = 10; size <= 1000; size *= 10) {
            testRollupIncrementalInternal("size-" + size, size);
        }
    }

    private void testRollupIncrementalInternal(final String ctxt, final int size) {
        final Random random = new Random(0);

        final QueryTable testTable = createTable(true, size * 10, random);
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
    public void testRollupWithFilter() {
        final Table source = newTable(
                stringCol("GRP", "v1", "v1", "v2", "v2", "v3"),
                intCol("AVG", 1, 2, 3, 4, 5),
                intCol("CONST", 11, 12, 13, 14, 15));
        final List<Aggregation> aggs = List.of(AggAvg("avg=AVG"));

        final RollupTable rollup1 = source.rollup(aggs, true, "GRP");
        final IllegalArgumentException ex1 = Assert.assertThrows(IllegalArgumentException.class,
                () -> rollup1.withFilter(WhereFilterFactory.getExpression("AVG >= 13")));
        assertEquals("Invalid filter found: RangeFilter(AVG greater than or equal to 13) may only use " +
                "non-aggregation columns, which are [GRP, CONST], but has used [AVG]", ex1.getMessage());
        final RuntimeException ex2 = Assert.assertThrows(RuntimeException.class,
                () -> rollup1.withFilter(WhereFilterFactory.getExpression("BOGUS = 13")));
        assertEquals("Column \"BOGUS\" doesn't exist in this table, available columns: [GRP, AVG, CONST]",
                ex2.getMessage());

        final RollupTable rollup2 = source.rollup(aggs, true, "GRP")
                .withFilter(WhereFilterFactory.getExpression("GRP = `v2`"));
        final Table snapshot2 = snapshotFilteredRollup(rollup2);

        final RollupTable rollup3 = source.rollup(aggs, true, "GRP")
                .withFilter(WhereFilterFactory.getExpression("CONST >= 13 && CONST <= 14"));
        final Table snapshot3 = snapshotFilteredRollup(rollup3);
        assertTableEquals(snapshot2, snapshot3);

        final RollupTable rollup4 = source.rollup(aggs, true, "GRP")
                .withFilter(WhereFilterFactory.getExpression("CONST >= 13 && GRP = `v2`"));
        final Table snapshot4 = snapshotFilteredRollup(rollup4);
        assertTableEquals(snapshot2, snapshot4);

        freeSnapshotTableChunks(snapshot2);
        freeSnapshotTableChunks(snapshot3);
        freeSnapshotTableChunks(snapshot4);
    }

    private Table snapshotFilteredRollup(RollupTable rollup) {
        RollupTable.NodeOperationsRecorder recorder =
                rollup.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated).sortDescending("GRP");
        final RollupTable rollupApply = rollup.withNodeOperations(recorder);
        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(rollupApply.getRowDepthColumn().name(), 0),
                stringCol("GRP", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));
        final HierarchicalTable.SnapshotState ss = rollupApply.makeSnapshotState();
        return snapshotToTable(rollupApply, ss, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
    }

    @Test
    public void testRebase() {
        final Table source1 = TableTools.newTable(stringCol("A", "Alpha", "Bravo", "Charlie", "Delta", "Charlie"),
                intCol("Sentinel", 1, 2, 3, 4, 5));

        final RollupTable rollup1a = source1.rollup(List.of(AggCount("Count")), "A")
                .withFilter(WhereFilterFactory.getExpression("A in `Bravo`, `Charlie`, `Echo`, `Golf`"));
        final RollupTable.NodeOperationsRecorder recorder =
                rollup1a.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated).sortDescending("A");
        final RollupTable rollup1 = rollup1a.withNodeOperations(recorder);

        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(rollup1.getRowDepthColumn().name(), 0),
                stringCol("A", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = rollup1.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);
        assertTableEquals(snapshot.view("A", "Count"),
                TableTools.newTable(stringCol("A", null, "Charlie", "Bravo"), longCol("Count", 3, 2, 1)));
        freeSnapshotTableChunks(snapshot);

        final Table source2 =
                TableTools.newTable(stringCol("A", "Echo", "Foxtrot", "Golf", "Hotel"), intCol("Sentinel", 6, 7, 8, 9));

        final RollupTable attributeCheck = rollup1.withAttributes(Collections.singletonMap("Haustier", "Kammerhunde"));

        final RollupTable rebased = attributeCheck.rebase(source2);

        final HierarchicalTable.SnapshotState ss2 = rebased.makeSnapshotState();
        final Table snapshot2 =
                snapshotToTable(rebased, ss2, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot2);
        assertTableEquals(TableTools.newTable(stringCol("A", null, "Golf", "Echo"), longCol("Count", 2, 1, 1)),
                snapshot2.view("A", "Count"));
        freeSnapshotTableChunks(snapshot2);

        assertEquals("Kammerhunde", rebased.getAttribute("Haustier"));
    }

    @Test
    public void testRebaseBadDef() {
        final Table source1 = TableTools.newTable(stringCol("A", "Alpha", "Bravo", "Charlie", "Delta", "Charlie"),
                intCol("Sentinel", 1, 2, 3, 4, 5));

        final RollupTable rollup1 = source1.rollup(List.of(AggCount("Count")), "A");

        final Table source2 = source1.view("Sentinel", "A");
        final IllegalArgumentException iae =
                Assert.assertThrows(IllegalArgumentException.class, () -> rollup1.rebase(source2));
        assertEquals("Cannot rebase a RollupTable with a new source definition, column order is not identical",
                iae.getMessage());

        final Table source3 = source1.updateView("A", "Sentinel", "Extra=1");
        final IllegalArgumentException iae2 =
                Assert.assertThrows(IllegalArgumentException.class, () -> rollup1.rebase(source3));
        assertEquals(
                "Cannot rebase a RollupTable with a new source definition: new source column 'Extra' is missing in existing source",
                iae2.getMessage());
    }
}
