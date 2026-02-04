//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.util.ColumnHolder;
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
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import org.jspecify.annotations.NonNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
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
        assertTableEquals(expected, snapshot);
        freeSnapshotTableChunks(snapshot);
    }

    @Test
    public void testRollupFormulaStatic() {
        testRollupFormulaStatic(false);
        testRollupFormulaStatic(true);
    }

    private void testRollupFormulaStatic(boolean withGroup) {
        final Table source = TableTools.newTable(
                stringCol("Key1", "Alpha", "Bravo", "Alpha", "Charlie", "Charlie", "Bravo", "Bravo"),
                stringCol("Key2", "Delta", "Delta", "Echo", "Echo", "Echo", "Echo", "Echo"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));
        TableTools.show(source);

        final List<Aggregation> aggList = new ArrayList<>();
        if (withGroup) {
            aggList.add(AggGroup("Sentinel"));
        }
        aggList.add(AggSum("Sum=Sentinel"));
        aggList.add(AggFormula("FSum", "__FORMULA_DEPTH__ == 0 ? max(Sentinel) : 1 + sum(Sentinel)"));
        aggList.add(AggFormula("KeyColumns", "__FORMULA_KEYS__"));

        final RollupTable rollup1 =
                source.rollup(
                        aggList,
                        "Key1", "Key2");

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

        TableTools.show(snapshot.view(rollup1.getRowDepthColumn().name(), rollup1.getRowExpandedColumn().name(), "Key1",
                "Key2", "Sum", "FSum"));

        final Table expectedBase = initialExpectedGrouped(rollup1);
        final Table expectedSentinel = withGroup ? expectedBase : expectedBase.dropColumns("Sentinel");
        final Table expected = expectedSentinel.update("FSum=ii == 0 ? 7 : 1 + Sum").update("KeyColumns=new io.deephaven.vector.ObjectVectorDirect(`Key1`, `Key2`).subVector(0, __DEPTH__ - 1)");
        assertTableEquals(expected, snapshot);
        freeSnapshotTableChunks(snapshot);
    }

    @Test
    public void testRollupFormulaStatic2() {
        final Table source = TableTools.newTable(
                stringCol("Account", "acct1", "acct1", "acct2", "acct2"),
                stringCol("Sym", "leg1", "leg2", "leg1", "leg2"),
                intCol("qty", 100, 100, 200, 200),
                doubleCol("Dollars", 1000, -500, 2000, -1000));

        final RollupTable rollup1 =
                source.updateView("qty=(long)qty").rollup(
                        List.of(AggFormula("qty", "__FORMULA_DEPTH__ > 0 ? first(qty) : sum(qty)").asReaggregating(),
                                AggSum("Dollars")),
                        "Account", "Sym");

        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(rollup1.getRowDepthColumn().name(), 0),
                stringCol("Account", arrayWithNull),
                stringCol("Sym", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = rollup1.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));

        final Table expected = TableTools.newTable(intCol(rollup1.getRowDepthColumn().name(), 1, 2, 3, 3, 2, 3, 3),
                booleanCol(rollup1.getRowExpandedColumn().name(), true, true, null, null, true, null, null),
                col("Account", null, "acct1", "acct1", "acct1", "acct2", "acct2", "acct2"),
                col("Sym", null, null, "leg1", "leg2", null, "leg1", "leg2"),
                longCol("qty", 300, 100, 100, 100, 200, 200, 200),
                doubleCol("Dollars", 1500, 500, 1000, -500, 1000, 2000, -1000));

        assertTableEquals(expected, snapshot);
        freeSnapshotTableChunks(snapshot);
    }

    @Test
    public void testRollupFormulaStatic3() {
        testRollupFormulaStatic3(false);
        testRollupFormulaStatic3(true);
    }

    private void testRollupFormulaStatic3(boolean hasGroup) {
        final Table source = TableTools.newTable(
                stringCol("Account", "Aardvark", "Aardvark", "Aardvark", "Aardvark", "Badger", "Badger", "Badger",
                        "Cobra", "Cobra", "Cobra", "Cobra"),
                stringCol("Sym", "Apple", "Banana", "Apple", "Apple", "Carrot", "Carrot", "Carrot", "Apple", "Apple",
                        "Apple", "Dragonfruit"),
                intCol("qty", 500, 100, 500, 200, 300, 300, 200, 100, 200, 300, 1500));
        TableTools.show(source);

        final List<Aggregation> aggList = new ArrayList<>();

        if (hasGroup) {
            aggList.add(AggGroup("gqty=qty"));
        }
        aggList.add(AggFormula("qty", "__FORMULA_DEPTH__ == 2 ? min(1000, sum(qty)) : sum(qty)").asReaggregating());
        aggList.add(AggSum("sqty=qty"));

        final RollupTable rollup1 =
                source.rollup(
                        aggList,
                        "Account", "Sym");

        final RollupTable rollup2 = rollup1.withNodeOperations(
                rollup1.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated).updateView("SumDiff=sqty-qty"));

        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(rollup1.getRowDepthColumn().name(), 0),
                stringCol("Account", arrayWithNull),
                stringCol("Sym", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = rollup2.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(rollup2, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);

        final List<ColumnHolder<?>> columnHolders = new ArrayList<>();
        columnHolders.add(intCol(rollup1.getRowDepthColumn().name(), 1, 2, 3, 3, 2, 3, 2, 3, 3));
        columnHolders.add(booleanCol(rollup1.getRowExpandedColumn().name(), true, true, null, null, true, null, true,
                null, null));
        columnHolders.add(stringCol("Account", null, "Aardvark", "Aardvark", "Aardvark", "Badger", "Badger", "Cobra",
                "Cobra", "Cobra"));
        columnHolders
                .add(stringCol("Sym", null, null, "Apple", "Banana", null, "Carrot", null, "Apple", "Dragonfruit"));
        columnHolders.add(col("gqty", iv(500, 100, 500, 200, 300, 300, 200, 100, 200, 300, 1500),
                /* aardvark */ iv(500, 100, 500, 200), iv(500, 500, 200), iv(100), /* badger */iv(300, 300, 200),
                iv(300, 300, 200), /* cobra */ iv(100, 200, 300, 1500), iv(100, 200, 300), iv(1500)));
        columnHolders.add(longCol("qty", 3500, /* aardvark */ 1100, 1000, 100, /* badger */800, 800, /* cobra */ 1600,
                600, 1000));
        final Table expected = TableTools.newTable(columnHolders.toArray(ColumnHolder[]::new))
                .update("sqty = sum(gqty)", "SumDiff=sqty-qty");

        TableTools.show(expected);

        assertTableEquals(hasGroup ? expected : expected.dropColumns("gqty"), snapshot);

        freeSnapshotTableChunks(snapshot);
    }

    @Test
    public void testRollupFormulaGroupRenames() {
        final int[] allValues = {10, 10, 10, 20, 20, 30, 30};
        final Table source = newTable(
                stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
                intCol("Value", allValues));
        final RollupTable simpleSum =
                source.rollup(List.of(AggGroup("Values=Value"), AggFormula("Sum = sum(Value)")), "Key");

        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(simpleSum.getRowDepthColumn().name(), 0),
                stringCol("Key", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = simpleSum.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(simpleSum, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);

        assertTableEquals(TableTools.newTable(intCol(simpleSum.getRowDepthColumn().name(), 1, 2, 2, 2),
                booleanCol(simpleSum.getRowExpandedColumn().name(), true, null, null, null),
                stringCol("Key", null, "Alpha", "Bravo", "Charlie"),
                col("Values", iv(allValues), iv(10, 10, 10), iv(20, 20), iv(30, 30)), longCol("Sum", 130, 30, 40, 60)),
                snapshot);

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

    private static @NonNull LongVector lv(final long... ints) {
        return new LongVectorDirect(ints);
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
        assertTableEquals(expected, snapshot);
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
        assertTableEquals(expected2, snapshot2);
        freeSnapshotTableChunks(snapshot2);

        // remove a key from source, so that reaggregate has to do some removals
        cug.runWithinUnitTestCycle(() -> {
            removeRows(source, i(0));
            source.notifyListeners(
                    new TableUpdateImpl(i(), i(0), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        final Table snapshot3 =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        Table expected3 = TableTools.newTable(intCol(rollup1.getRowDepthColumn().name(), 1, 2, 3, 2, 3, 3, 2, 3),
                booleanCol(rollup1.getRowExpandedColumn().name(), true, true, null, true, null, null,
                        true, null),
                col("Key1", null, "Alpha", "Alpha", "Bravo", "Bravo", "Bravo", "Charlie", "Charlie"),
                col("Key2", null, null, "Echo", null, "Delta", "Echo", null, "Echo"),
                col("Sentinel", iv(2, 3, 4, 5, 7, 8, 9), iv(3, 8), iv(3, 8), iv(2, 7), iv(2), iv(7),
                        iv(4, 5, 9), iv(4, 5, 9)))
                .update("Sum=sum(Sentinel)");

        assertTableEquals(expected3, snapshot3);
        freeSnapshotTableChunks(snapshot3);

        // remove everything, we want to validate the zero key removals for the operator
        cug.runWithinUnitTestCycle(() -> {
            final RowSet toRemove = source.getRowSet().copy();
            System.out.println("To Remove: " + toRemove);
            removeRows(source, toRemove);
            source.notifyListeners(
                    new TableUpdateImpl(i(), toRemove, i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        final Table snapshot4 =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        final Table expected4 = TableTools.newTable(intCol(rollup1.getRowDepthColumn().name()),
                booleanCol(rollup1.getRowExpandedColumn().name()),
                stringCol("Key1"),
                stringCol("Key2"),
                col("Sentinel", new IntVector[0]),
                longCol("Sum")).where("false");

        assertTableEquals(expected4, snapshot4);
        TableTools.showWithRowSet(snapshot4);
        freeSnapshotTableChunks(snapshot4);

        // we should make sure there are some additions in reaggregation, let's just add the whole original back
        cug.runWithinUnitTestCycle(() -> {
            final WritableRowSet toAdd = RowSetFactory.flat(7);
            TstUtils.addToTable(source, toAdd,
                    stringCol("Key1", "Alpha", "Bravo", "Alpha", "Charlie", "Charlie", "Bravo", "Bravo"),
                    stringCol("Key2", "Delta", "Delta", "Echo", "Echo", "Echo", "Echo", "Echo"),
                    intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));

            source.notifyListeners(
                    new TableUpdateImpl(toAdd, i(), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        final Table snapshot5 =
                snapshotToTable(rollup1, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));

        assertTableEquals(expected, snapshot5);
        freeSnapshotTableChunks(snapshot5);
    }

    @Test
    public void testReusedGrouping() {
        final QueryTable source = TstUtils.testRefreshingTable(
                stringCol("Key1", "Alpha", "Bravo", "Alpha", "Charlie", "Charlie", "Bravo", "Bravo"),
                stringCol("Key2", "Delta", "Delta", "Echo", "Echo", "Echo", "Echo", "Echo"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));

        final RollupTable rollup1 =
                source.rollup(List.of(AggGroup("Sentinel"), AggSum("Sum=Sentinel"), AggGroup("S2=Sentinel")), "Key1",
                        "Key2");

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

        final Table expected = initialExpectedGrouped(rollup1).update("S2=Sentinel");
        assertTableEquals(expected, snapshot);
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
        Table expected2 = secondExpectedGrouped(rollup1).update("S2=Sentinel");
        TableTools.showWithRowSet(expected2);
        assertTableEquals(expected2, snapshot2);
        freeSnapshotTableChunks(snapshot2);
    }
}
