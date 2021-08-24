/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.configuration.Configuration;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.SortedBy;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.*;
import static io.deephaven.db.v2.TstUtils.addToTable;
import static io.deephaven.db.v2.by.ComboAggregateFactory.AggCombo;
import static io.deephaven.db.v2.by.ComboAggregateFactory.AggSortedLast;

@Category(OutOfBandTest.class)
public class TestSortedFirstOrLastByFactory extends LiveTableTestCase {

    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING =
        Configuration.getInstance().getBooleanForClassWithDefault(
            TestSortedFirstOrLastByFactory.class, "CompilerTools.logEnabled", false);

    private static final String[] colNames = new String[] {"Sym", "intCol", "doubleCol", "Keys"};
    private static final boolean printTableUpdates = Configuration.getInstance()
        .getBooleanForClassWithDefault(LiveTableTestCase.class, "printTableUpdates", false);

    private boolean oldLogEnabled;
    private boolean oldCheckLtm;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        oldLogEnabled = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        oldCheckLtm = LiveTableMonitor.DEFAULT.setCheckTableOperations(false);
        UpdatePerformanceTracker.getInstance().enableUnitTestMode();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            CompilerTools.setLogEnabled(oldLogEnabled);
            LiveTableMonitor.DEFAULT.setCheckTableOperations(oldCheckLtm);
            ChunkPoolReleaseTracking.checkAndDisable();
        }
    }

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
            showWithIndex(queryTable);
        }
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget
                    .from(() -> SortedBy.sortedFirstBy(queryTable.update("x=Keys"), sortColumns)),
                EvalNugget
                    .from(() -> SortedBy.sortedLastBy(queryTable.update("x=Keys"), sortColumns)),
                new QueryTableTest.TableComparator(
                    queryTable.sort(sortColumns).head(1),
                    SortedBy.sortedFirstBy(queryTable, sortColumns)),
                new QueryTableTest.TableComparator(
                    SortedBy.sortedLastBy(queryTable, sortColumns),
                    queryTable.sort(sortColumns).tail(1)),
                EvalNugget.Sorted.from(
                    () -> SortedBy.sortedFirstBy(queryTable.update("x=Keys"), sortColumns, "Sym"),
                    "Sym"),
                EvalNugget.Sorted.from(
                    () -> SortedBy.sortedLastBy(queryTable.update("x=Keys"), sortColumns, "Sym"),
                    "Sym"),
                new QueryTableTest.TableComparator(
                    queryTable.sort(sortColumns).firstBy("Sym").sort("Sym"),
                    SortedBy.sortedFirstBy(queryTable, sortColumns, "Sym").sort("Sym")),
                new QueryTableTest.TableComparator(
                    queryTable.sort(sortColumns).lastBy("Sym").sort("Sym"),
                    queryTable
                        .by(AggCombo(AggSortedLast(sortColumns, "intCol", "doubleCol", "Keys")),
                            "Sym")
                        .sort("Sym"))
        };
        for (int step = 0; step < 100; step++) {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Size = " + size + ", Seed = " + seed + ", Step = " + step
                    + ", sortColumns=" + Arrays.toString(sortColumns));
            }
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }


    public void testIds6445() {
        final QueryTable source = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(5),
            intCol("SFB", 2, 1, 2, 1, 2), intCol("Sentinel", 1, 2, 3, 4, 5),
            col("DummyBucket", "A", "A", "A", "A", "A"));
        // final FuzzerPrintListener pl = new FuzzerPrintListener("source", source);
        // source.listenForUpdates(pl);

        final QueryTable sfb = (QueryTable) source.by(new SortedFirstBy("SFB"));
        final QueryTable bucketed = (QueryTable) source.by(new SortedFirstBy("SFB"), "DummyBucket");
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

        TableTools.showWithIndex(sfb);
        TestCase.assertEquals(2, sfb.getColumn("Sentinel").get(0));
        TestCase.assertEquals(2, bucketed.getColumn("Sentinel").get(0));

        // this part is the original bug, if we didn't change the actual value of the redirection
        // index; because the
        // shift modify combination left it at the same index; we would not notice the mdoification
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByValues(0);
            update.removed = Index.FACTORY.getEmptyIndex();
            update.modified = Index.FACTORY.getIndexByValues(2, 4);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet.clear();
            update.modifiedColumnSet.setAll("SFB");

            addToTable(source, Index.FACTORY.getFlatIndex(6), intCol("SFB", 3, 2, 3, 2, 3, 2),
                intCol("Sentinel", 6, 1, 2, 3, 4, 5),
                col("DummyBucket", "A", "A", "A", "A", "A", "A"));

            final IndexShiftData.Builder sb = new IndexShiftData.Builder();
            sb.shiftRange(0, 4, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Updated SFB");
        TableTools.showWithIndex(sfb);
        tuvsfb.deepValidation();
        tuvbuck.deepValidation();

        // i'm concerned that if we really modify a row, but we don't detect it in the shift, so
        // here we are just shifting without modifications
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByValues(0);
            update.removed = Index.FACTORY.getEmptyIndex();
            update.modified = Index.FACTORY.getIndexByValues();
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet.clear();
            update.modifiedColumnSet.setAll("SFB");

            addToTable(source, Index.FACTORY.getFlatIndex(7), intCol("SFB", 4, 3, 2, 3, 2, 3, 2),
                intCol("Sentinel", 7, 6, 1, 2, 3, 4, 5),
                col("DummyBucket", "A", "A", "A", "A", "A", "A", "A"));

            final IndexShiftData.Builder sb = new IndexShiftData.Builder();
            sb.shiftRange(0, 5, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted SFB");
        TableTools.showWithIndex(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(1, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(1, bucketed.getColumn("Sentinel").get(0));

        // here we are shifting, but not modifying the SFB column (but are modifying sentinel)
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByValues(0);
            update.removed = Index.FACTORY.getEmptyIndex();
            update.modified = Index.FACTORY.getIndexByValues(3);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet.clear();
            update.modifiedColumnSet.setAll("Sentinel");

            addToTable(source, Index.FACTORY.getFlatIndex(8), intCol("SFB", 4, 4, 3, 2, 3, 2, 3, 2),
                intCol("Sentinel", 8, 7, 6, 9, 2, 3, 4, 5),
                col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A"));

            final IndexShiftData.Builder sb = new IndexShiftData.Builder();
            sb.shiftRange(0, 6, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Modified SFB");
        TableTools.showWithIndex(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(9, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(9, bucketed.getColumn("Sentinel").get(0));

        // we are shifting, and claiming to modify SFB but not actually doing it
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByValues(0);
            update.removed = Index.FACTORY.getEmptyIndex();
            update.modified = Index.FACTORY.getIndexByValues(4);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet.clear();
            update.modifiedColumnSet.setAll("SFB");

            addToTable(source, Index.FACTORY.getFlatIndex(9),
                intCol("SFB", 4, 4, 4, 3, 2, 3, 2, 3, 2),
                intCol("Sentinel", 10, 8, 7, 6, 9, 2, 3, 4, 5),
                col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final IndexShiftData.Builder sb = new IndexShiftData.Builder();
            sb.shiftRange(0, 7, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Modified SFB");
        TableTools.showWithIndex(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(9, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(9, bucketed.getColumn("Sentinel").get(0));

        // here we are shifting, and modifying SFB but not actually doing it
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByValues(0);
            update.removed = Index.FACTORY.getEmptyIndex();
            update.modified = Index.FACTORY.getIndexByValues(4);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet.clear();
            update.modifiedColumnSet.setAll("SFB");

            addToTable(source, Index.FACTORY.getFlatIndex(10),
                intCol("SFB", 4, 4, 4, 4, 1, 2, 3, 2, 3, 2),
                intCol("Sentinel", 11, 10, 8, 7, 6, 9, 2, 3, 4, 5),
                col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final IndexShiftData.Builder sb = new IndexShiftData.Builder();
            sb.shiftRange(0, 8, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Really Really Modified SFB");
        TableTools.showWithIndex(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(6, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(6, bucketed.getColumn("Sentinel").get(0));

        // claim to modify sfb, but don't really. Actually modify sentinel.
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByValues(0);
            update.removed = Index.FACTORY.getEmptyIndex();
            update.modified = Index.FACTORY.getIndexByValues(5);
            update.modifiedColumnSet = source.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet.clear();
            update.modifiedColumnSet.setAll("SFB", "Sentinel");

            addToTable(source, Index.FACTORY.getFlatIndex(11),
                intCol("SFB", 4, 4, 4, 4, 4, 1, 2, 3, 2, 3, 2),
                intCol("Sentinel", 12, 11, 10, 8, 7, 13, 9, 2, 3, 4, 5),
                col("DummyBucket", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"));

            final IndexShiftData.Builder sb = new IndexShiftData.Builder();
            sb.shiftRange(0, 9, 1);
            update.shifted = sb.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted and Really Really Modified SFB");
        TableTools.showWithIndex(sfb);
        tuvsfb.deepValidation();
        TestCase.assertEquals(13, sfb.getColumn("Sentinel").get(0));
        tuvbuck.deepValidation();
        TestCase.assertEquals(13, bucketed.getColumn("Sentinel").get(0));
    }
}
