//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.SortedLongGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.SortedBy;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.experimental.categories.Category;

import static io.deephaven.api.agg.Aggregation.AggSortedLast;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.TstUtils.addToTable;

@Category(OutOfBandTest.class)
public class TestSortedFirstOrLastByFactory extends RefreshingTableTestCase {

    private static final String[] colNames = new String[] {"Sym", "intCol", "doubleCol", "Indices"};

    private static final boolean printTableUpdates = Configuration.getInstance()
            .getBooleanForClassWithDefault(RefreshingTableTestCase.class, "printTableUpdates", false);

    public void testSortedFirstOrLastBy() {
        final int[] sizes = {10, 50, 200};
        for (final int size : sizes) {
            for (int seed = 0; seed < 1; ++seed) {
                incrementalTest(seed, size, "intCol");
                incrementalTest(seed, size, "doubleCol");
                incrementalTest(seed, size, "intCol", "doubleCol");
                incrementalTest(seed, size, "doubleCol", "intCol");
            }
        }
    }

    private void incrementalTest(int seed, int size, final String... sortColumns) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random, columnInfo = initColumnInfos(
                colNames,
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(10, 100, 0.1),
                new SetGenerator<>(10.1, 20.1, 30.1),
                new SortedLongGenerator(0, Integer.MAX_VALUE)));
        if (printTableUpdates) {
            showWithRowSet(queryTable);
        }
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> SortedBy.sortedFirstBy(queryTable.update("x=Indices"), sortColumns)),
                EvalNugget.from(() -> SortedBy.sortedLastBy(queryTable.update("x=Indices"), sortColumns)),
                new QueryTableTest.TableComparator(
                        queryTable.sort(sortColumns).head(1),
                        SortedBy.sortedFirstBy(queryTable, sortColumns)),
                new QueryTableTest.TableComparator(
                        SortedBy.sortedLastBy(queryTable, sortColumns),
                        queryTable.sort(sortColumns).tail(1)),
                EvalNugget.Sorted.from(() -> SortedBy.sortedFirstBy(queryTable.update("x=Indices"), sortColumns, "Sym"),
                        "Sym"),
                EvalNugget.Sorted.from(() -> SortedBy.sortedLastBy(queryTable.update("x=Indices"), sortColumns, "Sym"),
                        "Sym"),
                new QueryTableTest.TableComparator(
                        queryTable.sort(sortColumns).firstBy("Sym").sort("Sym"),
                        SortedBy.sortedFirstBy(queryTable, sortColumns, "Sym").sort("Sym")),
                new QueryTableTest.TableComparator(
                        queryTable.sort(sortColumns).lastBy("Sym").sort("Sym"),
                        queryTable.aggBy(AggSortedLast(List.of(sortColumns), "intCol", "doubleCol", "Indices"), "Sym")
                                .sort("Sym"))
        };
        for (int step = 0; step < 100; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Size = " + size + ", Seed = " + seed + ", Step = " + step + ", sortColumns="
                        + Arrays.toString(sortColumns));
            }
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }


    public void testIds6445() {
        final QueryTable source =
                TstUtils.testRefreshingTable(RowSetFactory.flat(5).toTracking(),
                        intCol("SFB", 2, 1, 2, 1, 2), intCol("Sentinel", 1, 2, 3, 4, 5),
                        col("DummyBucket", "A", "A", "A", "A", "A"));
        // final FuzzerPrintListener pl = new FuzzerPrintListener("source", source);
        // source.addUpdateListener(pl);

        final QueryTable sfb = (QueryTable) source.aggAllBy(AggSpec.sortedFirst("SFB"));
        final QueryTable bucketed = (QueryTable) source.aggAllBy(AggSpec.sortedFirst("SFB"), "DummyBucket");
        // final FuzzerPrintListener plsfb = new FuzzerPrintListener("sfb", sfb);
        // sfb.addUpdateListener(plsfb);

        final TableUpdateValidator tuv = TableUpdateValidator.make("source validator", source);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(failureListener);
        final TableUpdateValidator tuvsfb = TableUpdateValidator.make("sfb validator", sfb);
        final FailureListener failureListenerSfb = new FailureListener();
        tuvsfb.getResultTable().addUpdateListener(failureListenerSfb);

        final TableUpdateValidator tuvbuck = TableUpdateValidator.make("sfb validator", bucketed);
        final FailureListener failureListenerBuck = new FailureListener();
        tuvbuck.getResultTable().addUpdateListener(failureListenerBuck);

        showWithRowSet(sfb);
        TestCase.assertEquals(2, ColumnVectors.ofInt(sfb, "Sentinel").get(0));
        TestCase.assertEquals(2, ColumnVectors.ofInt(bucketed, "Sentinel").get(0));

        // this part is the original bug, if we didn't change the actual value of the row redirection; because the
        // shift modify combination left it at the same row key; we would not notice the mdoification
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update5 = new TableUpdateImpl();
            update5.added = RowSetFactory.fromKeys(0);
            update5.removed = RowSetFactory.empty();
            update5.modified = RowSetFactory.fromKeys(2, 4);
            update5.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update5.modifiedColumnSet().clear();
            update5.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(6), intCol("SFB", 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 6, 1, 2, 3, 4, 5), col("DummyBucket", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb5 = new RowSetShiftData.Builder();
            sb5.shiftRange(0, 4, 1);
            update5.shifted = sb5.build();
            source.notifyListeners(update5);
        });

        System.out.println("Updated SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        tuvbuck.deepValidation();

        // i'm concerned that if we really modify a row, but we don't detect it in the shift, so here we are just
        // shifting without modifications
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update4 = new TableUpdateImpl();
            update4.added = RowSetFactory.fromKeys(0);
            update4.removed = RowSetFactory.empty();
            update4.modified = RowSetFactory.fromKeys();
            update4.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update4.modifiedColumnSet().clear();
            update4.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(7), intCol("SFB", 4, 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 7, 6, 1, 2, 3, 4, 5), col("DummyBucket", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb4 = new RowSetShiftData.Builder();
            sb4.shiftRange(0, 5, 1);
            update4.shifted = sb4.build();
            source.notifyListeners(update4);
        });

        System.out.println("Shifted SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(1, ColumnVectors.ofInt(sfb, "Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(1, ColumnVectors.ofInt(bucketed, "Sentinel").get(0));

        // here we are shifting, but not modifying the SFB column (but are modifying sentinel)
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update3 = new TableUpdateImpl();
            update3.added = RowSetFactory.fromKeys(0);
            update3.removed = RowSetFactory.empty();
            update3.modified = RowSetFactory.fromKeys(3);
            update3.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update3.modifiedColumnSet().clear();
            update3.modifiedColumnSet().setAll("Sentinel");

            addToTable(source, RowSetFactory.flat(8), intCol("SFB", 4, 4, 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 8, 7, 6, 9, 2, 3, 4, 5),
                    col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb3 = new RowSetShiftData.Builder();
            sb3.shiftRange(0, 6, 1);
            update3.shifted = sb3.build();
            source.notifyListeners(update3);
        });

        System.out.println("Shifted and Modified SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(9, ColumnVectors.ofInt(sfb, "Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(9, ColumnVectors.ofInt(bucketed, "Sentinel").get(0));

        // we are shifting, and claiming to modify SFB but not actually doing it
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update2 = new TableUpdateImpl();
            update2.added = RowSetFactory.fromKeys(0);
            update2.removed = RowSetFactory.empty();
            update2.modified = RowSetFactory.fromKeys(4);
            update2.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update2.modifiedColumnSet().clear();
            update2.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(9), intCol("SFB", 4, 4, 4, 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 10, 8, 7, 6, 9, 2, 3, 4, 5),
                    col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb2 = new RowSetShiftData.Builder();
            sb2.shiftRange(0, 7, 1);
            update2.shifted = sb2.build();
            source.notifyListeners(update2);
        });

        System.out.println("Shifted and Modified SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(9, ColumnVectors.ofInt(sfb, "Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(9, ColumnVectors.ofInt(bucketed, "Sentinel").get(0));

        // here we are shifting, and modifying SFB but not actually doing it
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update1 = new TableUpdateImpl();
            update1.added = RowSetFactory.fromKeys(0);
            update1.removed = RowSetFactory.empty();
            update1.modified = RowSetFactory.fromKeys(4);
            update1.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update1.modifiedColumnSet().clear();
            update1.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(10), intCol("SFB", 4, 4, 4, 4, 1, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 11, 10, 8, 7, 6, 9, 2, 3, 4, 5),
                    col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb1 = new RowSetShiftData.Builder();
            sb1.shiftRange(0, 8, 1);
            update1.shifted = sb1.build();
            source.notifyListeners(update1);
        });

        System.out.println("Shifted and Really Really Modified SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(6, ColumnVectors.ofInt(sfb, "Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(6, ColumnVectors.ofInt(bucketed, "Sentinel").get(0));

        // claim to modify sfb, but don't really. Actually modify sentinel.
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = RowSetFactory.fromKeys(0);
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.fromKeys(5);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet().clear();
            update.modifiedColumnSet().setAll("SFB", "Sentinel");

            addToTable(source, RowSetFactory.flat(11), intCol("SFB", 4, 4, 4, 4, 4, 1, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 12, 11, 10, 8, 7, 13, 9, 2, 3, 4, 5),
                    col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(0, 9, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Really Really Modified SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(13, ColumnVectors.ofInt(sfb, "Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(13, ColumnVectors.ofInt(bucketed, "Sentinel").get(0));
    }
}
