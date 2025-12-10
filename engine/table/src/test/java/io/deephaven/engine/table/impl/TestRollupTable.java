//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import org.jspecify.annotations.NonNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.freeSnapshotTableChunks;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.snapshotToTable;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.util.TableTools.byteCol;

@Category(OutOfBandTest.class)
public class TestRollupTable extends RefreshingTableTestCase {
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
            AggWSum("intCol", "wsum=intCol"),
            AggGroup("grp=intCol"));

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
            "wsum",
            "grp"
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
    public void testRollupWithFilter() {
        final Table sourceUncounted = newTable(
                stringCol("GRP", "v1", "v1", "v2", "v2", "v3"),
                intCol("AVG", 1, 2, 3, 4, 5),
                intCol("CONST", 11, 12, 13, 14, 15));
        final AtomicInteger grpCount = new AtomicInteger(0);
        final Function<String, String> countingIdentity = (s) -> {
            grpCount.incrementAndGet();
            return s;
        };
        QueryScope.addParam("countingIdentity", countingIdentity);
        final Table source = sourceUncounted.updateView("GRP=(String)countingIdentity.apply(GRP)");

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

        grpCount.set(0);
        final RollupTable rollup2 = rollup1.withFilter(WhereFilterFactory.getExpression("GRP = `v2`"));
        assertEquals(0, grpCount.get()); // Should not rebase
        final Table snapshot2 = snapshotFilteredRollup(rollup2);

        grpCount.set(0);
        final RollupTable rollup3 = rollup1.withFilter(WhereFilterFactory.getExpression("CONST >= 13 && CONST <= 14"));
        assertEquals(2, grpCount.get()); // Should rebase
        final Table snapshot3 = snapshotFilteredRollup(rollup3);
        assertTableEquals(snapshot2, snapshot3);

        grpCount.set(0);
        final RollupTable rollup4 = rollup1.withFilter(WhereFilterFactory.getExpression("CONST >= 13 && GRP = `v2`"));
        assertEquals(7, grpCount.get()); // Should rebase
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

    @Test
    public void testInvalidSort() {
        final Table source1 = TableTools.newTable(stringCol("A", "Alpha", "Bravo", "Charlie", "Delta", "Charlie"),
                intCol("Sentinel", 1, 2, 3, 4, 5)).update("ObjCol=new Object()");

        final RollupTable rollup1a = source1.rollup(List.of(AggLast("Sentinel", "ObjCol")), "A");
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
        assertTableEquals(
                TableTools.newTable(stringCol("A", null, "Delta", "Charlie", "Bravo", "Alpha"),
                        intCol("Sentinel", 5, 4, 5, 2, 1)),
                snapshot.view("A", "Sentinel"));
        freeSnapshotTableChunks(snapshot);


        final NotSortableColumnException nse = Assert.assertThrows(NotSortableColumnException.class,
                () -> rollup1.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated).sortDescending("ObjCol"));
        assertEquals("ObjCol is not a sortable type: class java.lang.Object", nse.getMessage());
        final NotSortableColumnException nse2 = Assert.assertThrows(NotSortableColumnException.class,
                () -> rollup1.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated).sort("ObjCol"));
        assertEquals("ObjCol is not a sortable type: class java.lang.Object", nse2.getMessage());
        final NotSortableColumnException nse3 = Assert.assertThrows(NotSortableColumnException.class,
                () -> rollup1.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated)
                        .sort(List.of(SortColumn.asc(ColumnName.of("ObjCol")))));
        assertEquals("ObjCol is not a sortable type: class java.lang.Object", nse3.getMessage());
    }

    @Test
    public void testVectorKeyColumn() {
        final Table arr = emptyTable(6).update("I = i", "J = i % 3", "K = i % 2").groupBy("J");
        TableTools.showWithRowSet(arr);
        final RollupTable vectorRollup = arr.rollup(List.of(AggCount("Count")), List.of(ColumnName.of("I")));

        final IntVector[] arrayWithNull = new IntVector[1];
        final Table keyTable = newTable(
                intCol(vectorRollup.getRowDepthColumn().name(), 0),
                col("I", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = vectorRollup.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(vectorRollup, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);
        assertTableEquals(
                TableTools.newTable(
                        intCol(vectorRollup.getRowDepthColumn().name(), 1, 2, 2, 2),
                        booleanCol(vectorRollup.getRowExpandedColumn().name(), true, null, null, null),
                        col("I", null, (IntVector) new IntVectorDirect(0, 3), new IntVectorDirect(1, 4),
                                new IntVectorDirect(2, 5)),
                        longCol("Count", 3, 1, 1, 1)),
                snapshot);
        freeSnapshotTableChunks(snapshot);
    }

    @Test
    public void testRollupGroupStatic() {
        final Table source = TableTools.newTable(
                stringCol("Key1", "Alpha", "Bravo", "Alpha", "Charlie", "Charlie", "Bravo", "Bravo"),
                stringCol("Key2", "Delta", "Delta", "Echo", "Echo", "Echo", "Echo", "Echo"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));

        final RollupTable rollup1 =
                source.rollup(List.of(AggGroup("Sentinel"), AggSum("Sum=Sentinel")), "Key1", "Key2");

        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(rollup1.getRowDepthColumn().name(), 0),
                stringCol("Key1", arrayWithNull),
                stringCol("Key2", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = rollup1.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);

        final Table expected = initialExpectedGrouped(rollup1);
        assertTableEquals(expected, snapshot.dropColumns("__EXPOSED_GROUP_ROW_SETS__"));
        freeSnapshotTableChunks(snapshot);
    }

    private static Table initialExpectedGrouped(RollupTable rollup1) {
        return TableTools.newTable(intCol(rollup1.getRowDepthColumn().name(), 1, 2, 3, 3, 2, 3, 3, 2, 3),
                booleanCol(rollup1.getRowExpandedColumn().name(), true, true, null, null, true, null, null,
                        true, null),
                col("Key1", null, "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Bravo", "Charlie", "Charlie"),
                col("Key2", null, null, "Delta", "Echo", null, "Delta", "Echo", null, "Echo"),
                col("Sentinel", iv(1, 2, 3, 4, 5, 6, 7), iv(1, 3), iv(1), iv(3), iv(2, 6, 7), iv(2), iv(6, 7),
                        iv(4, 5), iv(4, 5)))
                .update("Sum=sum(Sentinel)");
    }

    private static Table secondExpectedGrouped(RollupTable rollup1) {
        return TableTools.newTable(intCol(rollup1.getRowDepthColumn().name(), 1, 2, 3, 3, 2, 3, 3, 2, 3),
                booleanCol(rollup1.getRowExpandedColumn().name(), true, true, null, null, true, null, null,
                        true, null),
                col("Key1", null, "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Bravo", "Charlie", "Charlie"),
                col("Key2", null, null, "Delta", "Echo", null, "Delta", "Echo", null, "Echo"),
                col("Sentinel", iv(1, 2, 3, 4, 5, 7, 8, 9), iv(1, 3, 8), iv(1), iv(3, 8), iv(2, 7), iv(2), iv(7),
                        iv(4, 5, 9), iv(4, 5, 9)))
                .update("Sum=sum(Sentinel)");
    }

    private static @NonNull IntVector iv(final int... ints) {
        return new IntVectorDirect(ints);
    }

    @Test
    public void testRollupGroupIncremental() {
        final QueryTable source = TstUtils.testRefreshingTable(
                stringCol("Key1", "Alpha", "Bravo", "Alpha", "Charlie", "Charlie", "Bravo", "Bravo"),
                stringCol("Key2", "Delta", "Delta", "Echo", "Echo", "Echo", "Echo", "Echo"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));

        final RollupTable rollup1 =
                source.rollup(List.of(AggGroup("Sentinel"), AggSum("Sum=Sentinel")), "Key1", "Key2");

        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(rollup1.getRowDepthColumn().name(), 0),
                stringCol("Key1", arrayWithNull),
                stringCol("Key2", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = rollup1.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);

        final Table expected = initialExpectedGrouped(rollup1);
        assertTableEquals(expected, snapshot.dropColumns("__EXPOSED_GROUP_ROW_SETS__"));
        freeSnapshotTableChunks(snapshot);

        final ControlledUpdateGraph cug = source.getUpdateGraph().cast();
        cug.runWithinUnitTestCycle(() -> {
            addToTable(source, i(10, 11), stringCol("Key1", "Alpha", "Charlie"), stringCol("Key2", "Echo", "Echo"),
                    intCol("Sentinel", 8, 9));
            removeRows(source, i(5));
            source.notifyListeners(
                    new TableUpdateImpl(i(10, 11), i(5), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        final Table snapshot2 =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot2);
        Table expected2 = secondExpectedGrouped(rollup1);
        TableTools.showWithRowSet(expected2);
        assertTableEquals(expected2, snapshot2.dropColumns("__EXPOSED_GROUP_ROW_SETS__"));
        freeSnapshotTableChunks(snapshot2);
    }
}
