/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
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
import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.table.impl.TstUtils.addToTable;

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
        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable queryTable = getTable(size, random, columnInfo = initColumnInfos(
                colNames,
                new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                new TstUtils.IntGenerator(10, 100, 0.1),
                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                new TstUtils.SortedLongGenerator(0, Integer.MAX_VALUE)));
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
        // source.listenForUpdates(pl);

        final QueryTable sfb = (QueryTable) source.aggAllBy(AggSpec.sortedFirst("SFB"));
        final QueryTable bucketed = (QueryTable) source.aggAllBy(AggSpec.sortedFirst("SFB"), "DummyBucket");
        // final FuzzerPrintListener plsfb = new FuzzerPrintListener("sfb", sfb);
        // sfb.listenForUpdates(plsfb);

        final TableUpdateValidator tuv = TableUpdateValidator.make("source validator", source);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().listenForUpdates(failureListener);
        final TableUpdateValidator tuvsfb = TableUpdateValidator.make("sfb validator", sfb);
        final FailureListener failureListenerSfb = new FailureListener();
        tuvsfb.getResultTable().listenForUpdates(failureListenerSfb);

        final TableUpdateValidator tuvbuck = TableUpdateValidator.make("sfb validator", bucketed);
        final FailureListener failureListenerBuck = new FailureListener();
        tuvbuck.getResultTable().listenForUpdates(failureListenerBuck);

        showWithRowSet(sfb);
        TestCase.assertEquals(2, sfb.getColumn("Sentinel").get(0));
        TestCase.assertEquals(2, bucketed.getColumn("Sentinel").get(0));

        // this part is the original bug, if we didn't change the actual value of the row redirection; because the
        // shift modify combination left it at the same row key; we would not notice the mdoification
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = RowSetFactory.fromKeys(0);
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.fromKeys(2, 4);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet().clear();
            update.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(6), intCol("SFB", 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 6, 1, 2, 3, 4, 5), col("DummyBucket", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(0, 4, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Updated SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        tuvbuck.deepValidation();

        // i'm concerned that if we really modify a row, but we don't detect it in the shift, so here we are just
        // shifting without modifications
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = RowSetFactory.fromKeys(0);
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.fromKeys();
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet().clear();
            update.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(7), intCol("SFB", 4, 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 7, 6, 1, 2, 3, 4, 5), col("DummyBucket", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(0, 5, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(1, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(1, bucketed.getColumn("Sentinel").get(0));

        // here we are shifting, but not modifying the SFB column (but are modifying sentinel)
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = RowSetFactory.fromKeys(0);
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.fromKeys(3);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet().clear();
            update.modifiedColumnSet().setAll("Sentinel");

            addToTable(source, RowSetFactory.flat(8), intCol("SFB", 4, 4, 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 8, 7, 6, 9, 2, 3, 4, 5),
                    col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(0, 6, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Modified SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(9, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(9, bucketed.getColumn("Sentinel").get(0));

        // we are shifting, and claiming to modify SFB but not actually doing it
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = RowSetFactory.fromKeys(0);
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.fromKeys(4);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet().clear();
            update.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(9), intCol("SFB", 4, 4, 4, 3, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 10, 8, 7, 6, 9, 2, 3, 4, 5),
                    col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(0, 7, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Modified SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(9, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(9, bucketed.getColumn("Sentinel").get(0));

        // here we are shifting, and modifying SFB but not actually doing it
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = RowSetFactory.fromKeys(0);
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.fromKeys(4);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet().clear();
            update.modifiedColumnSet().setAll("SFB");

            addToTable(source, RowSetFactory.flat(10), intCol("SFB", 4, 4, 4, 4, 1, 2, 3, 2, 3, 2),
                    intCol("Sentinel", 11, 10, 8, 7, 6, 9, 2, 3, 4, 5),
                    col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(0, 8, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Really Really Modified SFB");
        showWithRowSet(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(6, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(6, bucketed.getColumn("Sentinel").get(0));

        // claim to modify sfb, but don't really. Actually modify sentinel.
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
        TestCase.assertEquals(13, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(13, bucketed.getColumn("Sentinel").get(0));
    }
}
