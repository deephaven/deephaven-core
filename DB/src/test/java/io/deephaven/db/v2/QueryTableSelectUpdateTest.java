package io.deephaven.db.v2;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.QueryTableTestBase.ListenerWithGlobals;
import io.deephaven.db.v2.QueryTableTestBase.TableComparator;
import io.deephaven.db.v2.select.DhFormulaColumn;
import io.deephaven.db.v2.select.FormulaCompilationException;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongSparseArraySource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.RuntimeMemory;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Assert;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.*;

/**
 * Test QueryTable select and update operations.
 */
public class QueryTableSelectUpdateTest {
    private JUnit4QueryTableTestBase base = new JUnit4QueryTableTestBase();

    @Before
    public void setUp() throws Exception {
        base.setUp();
    }

    @After
    public void tearDown() throws Exception {
        base.tearDown();
    }

    @Test
    public void testSelectAndUpdate() {
        final QueryTable table1 =
            (QueryTable) TstUtils.testRefreshingTable(i(2, 4, 6)).select("x = i*3", "y = \"\" + k");
        TestCase.assertEquals(3, table1.size());
        TestCase
            .assertEquals(Arrays.asList(0, 3, 6),
                Arrays.asList(table1.getColumn("x").get(0, table1.size())));
        TestCase.assertEquals(Arrays.asList("2", "4", "6"),
            Arrays.asList(table1.getColumn("y").get(0, table1.size())));

        final QueryTable table =
            TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", 'a', 'b', 'c'));

        showWithIndex(table);
        QueryTable table2 = (QueryTable) table.select("x = x * 2", "z = y");
        showWithIndex(table2);

        final Table table3 = table.update("q = i", "q = q + 1", "p = q+10");
        final Listener table2Listener = base.newListenerWithGlobals(table2);
        table2.listenForUpdates(table2Listener);
        TestCase
            .assertEquals(Arrays.asList(2, 4, 6),
                Arrays.asList(table2.getColumn("x").get(0, table2.size())));
        TestCase.assertEquals(Arrays.asList('a', 'b', 'c'),
            Arrays.asList(table2.getColumn("z").get(0, table2.size())));
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            show(table3);
            addToTable(table, i(7, 9), c("x", 4, 5), c("y", 'd', 'e'));
            table.notifyListeners(i(7, 9), i(), i());
        });
        showWithIndex(table);
        showWithIndex(table2);
        TestCase.assertEquals(5, table.size());
        TestCase.assertEquals(5, table2.size());
        showWithIndex(table3);
        TestCase.assertEquals(Arrays.asList(1, 2, 3, 4, 5),
            Arrays.asList(table3.getColumn("q").get(0, table3.size())));
        TestCase.assertEquals(Arrays.asList(11, 12, 13, 14, 15),
            Arrays.asList(table3.getColumn("p").get(0, table3.size())));
        TestCase.assertEquals(Arrays.asList(2, 4, 6, 8, 10),
            Arrays.asList(table2.getColumn("x").get(0, table2.size())));
        TestCase.assertEquals(Arrays.asList('a', 'b', 'c', 'd', 'e'),
            Arrays.asList(table2.getColumn("z").get(0, table2.size())));
        TestCase.assertEquals(i(7, 9), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(), base.modified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), c("x", 3, 10), c("y", 'e', 'd'));
            table.notifyListeners(i(), i(), i(7, 9));
        });
        TestCase.assertEquals(5, table.size());
        TestCase.assertEquals(5, table2.size());

        TestCase.assertEquals(Arrays.asList(2, 4, 6, 6, 20),
            Arrays.asList(table2.getColumn("x").get(0, table2.size())));
        TestCase.assertEquals(Arrays.asList('a', 'b', 'c', 'e', 'd'),
            Arrays.asList(table2.getColumn("z").get(0, table2.size())));
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(7, 9), base.modified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table, i(2, 6, 7));
            table.notifyListeners(i(), i(2, 6, 7), i());
        });
        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());

        TestCase
            .assertEquals(Arrays.asList(4, 20),
                Arrays.asList(table2.getColumn("x").get(0, table2.size())));
        TestCase
            .assertEquals(Arrays.asList('b', 'd'),
                Arrays.asList(table2.getColumn("z").get(0, table2.size())));
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(2, 6, 7), base.removed);
        TestCase.assertEquals(i(), base.modified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table, i(9));
            addToTable(table, i(2, 4, 6), c("x", 1, 22, 3), c("y", 'a', 'x', 'c'));
            table.notifyListeners(i(2, 6), i(9), i(4));
        });
        TestCase.assertEquals(3, table.size());
        TestCase.assertEquals(3, table2.size());
        TestCase
            .assertEquals(Arrays.asList(2, 44, 6),
                Arrays.asList(table2.getColumn("x").get(0, table2.size())));
        TestCase.assertEquals(Arrays.asList('a', 'x', 'c'),
            Arrays.asList(table2.getColumn("z").get(0, table2.size())));
        TestCase.assertEquals(i(2, 6), base.added);
        TestCase.assertEquals(i(9), base.removed);
        TestCase.assertEquals(i(4), base.modified);

        final QueryTable table4 =
            (QueryTable) TableTools.emptyTable(3).select("x = i*2", "y = \"\" + x");
        TestCase
            .assertEquals(Arrays.asList(0, 2, 4),
                Arrays.asList(table4.getColumn("x").get(0, table4.size())));
        TestCase.assertEquals(Arrays.asList("0", "2", "4"),
            Arrays.asList(table4.getColumn("y").get(0, table4.size())));

        final QueryTable table5 = (QueryTable) table4.update("z = x", "x = z + 1", "t = x - 3");
        TestCase
            .assertEquals(Arrays.asList(0, 2, 4),
                Arrays.asList(table5.getColumn("z").get(0, table5.size())));
        TestCase
            .assertEquals(Arrays.asList(1, 3, 5),
                Arrays.asList(table5.getColumn("x").get(0, table5.size())));
        TestCase
            .assertEquals(Arrays.asList(-2, 0, 2),
                Arrays.asList(table5.getColumn("t").get(0, table5.size())));

        final QueryTable table6 =
            TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", 'a', 'b', 'c'));
        table2 = (QueryTable) table6.update("z = x", "x = z + 1", "t = x - 3");
        final Listener table2Listener2 = base.newListenerWithGlobals(table2);
        table2.listenForUpdates(table2Listener2);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table6, i(7, 9), c("x", 4, 5), c("y", 'd', 'e'));
            table6.notifyListeners(i(7, 9), i(), i());
        });

        TestCase.assertEquals(5, table6.size());
        TestCase.assertEquals(5, table2.size());
        assertTableEquals(table6.update("z = x", "x = z + 1", "t = x - 3"), table2);
        TestCase.assertEquals(i(7, 9), base.added);
        TestCase.assertEquals(i(), base.modified);
        TestCase.assertEquals(i(), base.removed);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table6, i(7, 9), c("x", 3, 10), c("y", 'e', 'd'));
            table6.notifyListeners(i(), i(), i(7, 9));
        });

        TestCase.assertEquals(5, table6.size());
        TestCase.assertEquals(5, table2.size());
        assertTableEquals(table6.update("z = x", "x = z + 1", "t = x - 3"), table2);
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(7, 9), base.modified);
        TestCase.assertEquals(i(), base.removed);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table6, i(2, 6, 7));
            table6.notifyListeners(i(), i(2, 6, 7), i());
        });

        TestCase.assertEquals(2, table6.size());
        TestCase.assertEquals(2, table2.size());
        show(table6);
        show(table2);
        assertTableEquals(table6.update("z = x", "x = z + 1", "t = x - 3"), table2);
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(2, 6, 7), base.removed);
        TestCase.assertEquals(i(), base.modified);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table6, i(9));
            addToTable(table6, i(2, 4, 6), c("x", 1, 22, 3), c("y", 'a', 'x', 'c'));
            table6.notifyListeners(i(2, 6), i(9), i(4));
        });

        TestCase.assertEquals(3, table6.size());
        TestCase.assertEquals(3, table2.size());
        assertTableEquals(table6.update("z = x", "x = z + 1", "t = x - 3"), table2);
        TestCase.assertEquals(i(2, 6), base.added);
        TestCase.assertEquals(i(9), base.removed);
        TestCase.assertEquals(i(4), base.modified);
    }

    private static int callCount = 0;

    public static int callCounter(int x) {
        callCount++;
        return x * 3;
    }

    @Test
    public void testLazyUpdate() {
        // Skip this test if we are using kernel formulas, because FormulaKernel ignores lazy, and
        // therefore all these
        // callCounts are all going to be wrong.
        if (DhFormulaColumn.useKernelFormulasProperty) {
            // We'd rather use Assume.assumeFalse() here, but we can't because we're using an old
            // JUnit in this file.
            return;
        }

        QueryLibrary.importStatic(QueryTableSelectUpdateTest.class);

        QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("A", 1, 2, 3));
        table = (QueryTable) table.lazyUpdate(
            "B=" + QueryTableSelectUpdateTest.class.getCanonicalName() + ".callCounter(A)");
        TestCase.assertEquals(3, table.size());
        TestCase.assertEquals(0, callCount);
        TestCase
            .assertEquals(Arrays.asList(3, 6, 9),
                Arrays.asList(table.getColumn("B").get(0, table.size())));
        TestCase.assertEquals(3, callCount);
        TestCase
            .assertEquals(Arrays.asList(3, 6, 9),
                Arrays.asList(table.getColumn("B").get(0, table.size())));
        TestCase.assertEquals(3, callCount);

        callCount = 0;
        QueryTable table2 =
            TstUtils.testRefreshingTable(i(2, 4, 6, 8, 10, 12), c("A", 1, 2, 3, 2, 3, 1));
        table2 = (QueryTable) table2.lazyUpdate(
            "B=" + QueryTableSelectUpdateTest.class.getCanonicalName() + ".callCounter(A)");
        TestCase.assertEquals(6, table2.size());
        TestCase.assertEquals(0, callCount);
        TestCase.assertEquals(Arrays.asList(3, 6, 9, 6, 9, 3),
            Arrays.asList(table2.getColumn("B").get(0, table2.size())));
        TestCase.assertEquals(3, callCount);
        TestCase.assertEquals(Arrays.asList(3, 6, 9, 6, 9, 3),
            Arrays.asList(table2.getColumn("B").get(0, table2.size())));
        TestCase.assertEquals(3, callCount);
        TestCase.assertEquals(3, table2.getColumnSource("B").getInt(2));
        TestCase.assertEquals(3, table2.getColumnSource("B").get(2));
        TestCase.assertEquals(3, table2.getColumnSource("B").getPrevInt(2));
        TestCase.assertEquals(3, table2.getColumnSource("B").getPrev(2));
    }

    private EvalNugget partialEvalNuggetFrom(Table sourceTable, boolean indexPositionChangesAllowed,
        Supplier<Table> makeTable) {
        return new PartialEvalNugget(sourceTable, indexPositionChangesAllowed) {
            @Override
            protected Table e() {
                return makeTable.get();
            }
        };
    }

    private abstract class PartialEvalNugget extends EvalNugget {
        // We listen to table updates, so we know what to compare.
        class UpdateListener extends InstrumentedListener {
            UpdateListener() {
                super("Update RefreshProcedure");
            }

            Index added, removed, modified;
            Throwable exception = null;

            @Override
            public void onUpdate(Index added, Index removed, Index modified) {
                this.added = added.clone();
                this.removed = removed.clone();
                this.modified = modified.clone();
            }

            @Override
            public void onFailureInternal(Throwable originalException,
                UpdatePerformanceTracker.Entry sourceEntry) {
                exception = originalException;
            }

            void close() {
                if (this.added != null) {
                    this.added.close();
                }
                if (this.removed != null) {
                    this.removed.close();
                }
                if (this.modified != null) {
                    this.modified.close();
                }
                this.added = this.removed = this.modified = null;
            }
        }

        private final UpdateListener listener1;
        private final UpdateListener listener2;
        int maxDiffLines = 10;
        private final Table sourceTable;
        private final boolean indexPositionChangesAllowed;

        PartialEvalNugget(Table sourceTable, boolean indexPositionChangesAllowed) {
            this.sourceTable = sourceTable;
            listener1 = new UpdateListener();
            ((QueryTable) sourceTable).listenForUpdates(listener1);

            listener2 = new UpdateListener();
            ((QueryTable) originalValue).listenForUpdates(listener2);

            this.indexPositionChangesAllowed = indexPositionChangesAllowed;
        }

        public void validate(final String msg) {
            Assert.assertNull(listener1.exception);
            Assert.assertNull(listener2.exception);

            if (listener1.added == null) {
                Assert.assertNull(listener2.added);
                return;
            }

            Table recomputedValue = e();
            if (LiveTableTestCase.printTableUpdates) {
                try {
                    System.out.println("Recomputed Value:");
                    io.deephaven.db.tables.utils.TableTools.showWithIndex(recomputedValue, 100);
                    System.out.println("Iterative Value:");
                    io.deephaven.db.tables.utils.TableTools.showWithIndex(originalValue, 100);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            List<String> issues = new ArrayList<>();
            StringBuilder result = new StringBuilder();
            if (originalValue.size() != recomputedValue.size()) {
                issues.add("Result table has size " + originalValue.size() + " vs. expected "
                    + recomputedValue.size());
            }

            if (indexPositionChangesAllowed) {
                // we should make sure that our added + removed is equal to the source added +
                // removed size
                long sourceSizeChange = listener1.added.size() - listener1.removed.size();
                long resultSizeChange = listener2.added.size() - listener2.removed.size();
                if (sourceSizeChange != resultSizeChange) {
                    issues.add("Source changed size by " + sourceSizeChange
                        + ", but result changed size by " + resultSizeChange);
                }
            } else {
                Index sourceAddedPositions = sourceTable.getIndex().invert(listener1.added);
                Index sourceRemovedPositions =
                    sourceTable.getIndex().getPrevIndex().invert(listener1.removed);
                Index sourceModifiedPositions = sourceTable.getIndex().invert(listener1.modified);

                Index resultAddedPositions = originalValue.getIndex().invert(listener2.added);
                Index resultRemovedPositions =
                    originalValue.getIndex().getPrevIndex().invert(listener2.removed);
                Index resultModifiedPositions = originalValue.getIndex().invert(listener2.modified);

                if (!sourceAddedPositions.equals(resultAddedPositions)) {
                    issues.add("Source Positions Added, " + sourceAddedPositions
                        + ", does not match result positions added, " + resultAddedPositions);
                }
                if (!sourceRemovedPositions.equals(resultRemovedPositions)) {
                    issues.add("Source Positions Removed, " + sourceRemovedPositions
                        + ", does not match result positions removed, " + resultRemovedPositions);
                }
                if (!sourceModifiedPositions.equals(resultModifiedPositions)) {
                    issues.add("Source Positions Modified, " + sourceModifiedPositions
                        + ", does not match result positions modified, " + resultModifiedPositions);
                }
            }

            Index indexToCheck = listener1.added.clone();
            indexToCheck.insert(listener1.modified);
            Index checkInvert = sourceTable.getIndex().invert(indexToCheck);

            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Positions to validate: " + checkInvert);

                final Index.SequentialBuilder originalBuilder =
                    Index.FACTORY.getSequentialBuilder();
                final Index.SequentialBuilder recomputedBuilder =
                    Index.FACTORY.getSequentialBuilder();
                checkInvert
                    .forEach(x -> originalBuilder.appendKey(originalValue.getIndex().get(x)));
                checkInvert
                    .forEach(x -> recomputedBuilder.appendKey(recomputedValue.getIndex().get(x)));

                System.out.println("Original Sub Table: " + checkInvert);
                TableTools.showWithIndex(originalValue.getSubTable(originalBuilder.getIndex()));
                System.out.println("Recomputed Sub Table: " + checkInvert);
                TableTools.showWithIndex(recomputedValue.getSubTable(recomputedBuilder.getIndex()));
            }

            Map<String, ? extends ColumnSource> originalColumns =
                originalValue.getColumnSourceMap();
            Map<String, ? extends ColumnSource> recomputedColumns =
                recomputedValue.getColumnSourceMap();

            for (Map.Entry<String, ? extends ColumnSource> stringColumnSourceEntry : recomputedColumns
                .entrySet()) {
                String columnName = stringColumnSourceEntry.getKey();
                ColumnSource originalColumnSource = originalColumns.get(columnName);
                ColumnSource recomputedColumn = stringColumnSourceEntry.getValue();

                for (Index.Iterator iterator = checkInvert.iterator(); iterator.hasNext();) {
                    int position = (int) iterator.nextLong();
                    long originalKey = originalValue.getIndex().get(position);
                    long recomputedKey = recomputedValue.getIndex().get(position);

                    Object original = originalColumnSource.get(originalKey);
                    Object recomputed = recomputedColumn.get(recomputedKey);
                    if (original != recomputed
                        && (original == null || !original.equals(recomputed))) {
                        issues.add("Mismatch at position " + position + "column " + columnName
                            + ": " + original + " != " + recomputed);
                    }
                }
            }


            int count = 0;
            for (String issue : issues) {
                if (count > maxDiffLines) {
                    result.append("... and ").append(issues.size() - count).append(" more issues");
                }
                result.append(issue).append("\n");
                count++;
            }

            Assert.assertEquals(msg, "", result.toString());

            listener1.close();
            listener2.close();
        }
    }

    private static final boolean RUN_SPARSE_REDIRECTION_UPDATE_TEST =
        Configuration.getInstance().getBooleanWithDefault("runSparseRedirectionUpdateTest", false);

    @Test
    public void testSparseRedirectedUpdate() {
        // just skip this test, it is there to
        // Assume.assumeTrue("We are purposefully skipping this very long running test that does not
        // actually verify anything.", RUN_SPARSE_REDIRECTION_UPDATE_TEST);
        // the assumeTrue is not working, maybe because of the old junit version?
        if (RUN_SPARSE_REDIRECTION_UPDATE_TEST) {
            final boolean startUpdate = QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE;

            try {
                QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = true;
                doTestSparseRedirectedUpdate();
            } finally {
                QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = startUpdate;
            }
        }
    }

    private void doTestSparseRedirectedUpdate() {
        System.gc();

        final QueryTable leftTable =
            new QueryTable(Index.FACTORY.getFlatIndex(99), Collections.emptyMap());
        leftTable.setRefreshing(true);
        final QueryTable rightTable =
            new QueryTable(Index.FACTORY.getFlatIndex(1), Collections.emptyMap());
        rightTable.setRefreshing(true);

        final Table leftWithKey = leftTable.updateView("Key=`a`", "LI=ii");
        final Table rightWithKey = rightTable.updateView("Key=`a`", "RI=ii");

        final Table joined = leftWithKey.join(rightWithKey, "Key", 2);

        final Table updated = joined.update("LRI=LI*RI", "Str=Long.toString(LRI)");

        System.gc();
        System.gc();
        final long startUsedMemory =
            RuntimeMemory.getInstance().totalMemory() - RuntimeMemory.getInstance().freeMemory();

        for (int step = 0; step < 10000; ++step) {
            final int fstep = step;

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                final long keyToAdd = fstep + 1;
                final Index addedIndex = i(keyToAdd);
                final Index removedIndex = (fstep % 2 == 0) ? i(fstep) : i();
                addToTable(rightTable, addedIndex);
                removeRows(rightTable, removedIndex);
                rightTable.notifyListeners(addedIndex, removedIndex, i());

                if (fstep % 100 == 99) {
                    addToTable(leftTable, i(fstep));
                    leftTable.notifyListeners(i(fstep), i(), i());
                }
            });

            System.gc();
            final long totalMemory = RuntimeMemory.getInstance().totalMemory();
            final long freeMemory = RuntimeMemory.getInstance().freeMemory();
            final long usedMemory = totalMemory - freeMemory;
            final long deltaUsed = usedMemory - startUsedMemory;
            System.out
                .println("Step = " + step + ", " + deltaUsed + "(" + usedMemory + "total) used, "
                    + freeMemory + " free / " + totalMemory + " total, " + " updated size="
                    + updated.size() + ", memory/row=" + (deltaUsed / updated.size()));
        }
    }

    @Test
    public void testUpdateIncremental() {
        for (int seed = 0; seed < 5; ++seed) {
            testUpdateIncremental(seed, false);
            testUpdateIncremental(seed, true);
        }
    }

    private void testUpdateIncremental(int seed, boolean useRedirection) {
        final boolean startSelect = QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT;
        final boolean startUpdate = QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE;

        try {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = useRedirection;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = useRedirection;
            try (final SafeCloseable ignored =
                LivenessScopeStack.open(new LivenessScope(true), true)) {
                testUpdateIncremental(seed, new MutableInt(100));
            }
        } finally {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = startSelect;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = startUpdate;
        }
    }

    private void testUpdateIncremental(final int seed, MutableInt numSteps) {
        final Random random = new Random(seed);
        final TstUtils.ColumnInfo[] columnInfo;
        final int size = 25;
        final QueryTable queryTable = getTable(size, random,
            columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("a", "b", "c", "d", "e"),
                new IntGenerator(10, 100),
                new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.update("intCol=intCol * 2")),
                EvalNugget.from(() -> queryTable.update("intCol=intCol + doubleCol")),
                EvalNugget.from(() -> queryTable.update("newCol=intCol / 2", "newCol2=newCol * 4")),
                EvalNugget.from(
                    () -> queryTable.update("newCol=intCol / 2").update("newCol2=newCol * 4")),
                EvalNugget.from(() -> queryTable.select("intCol=intCol * 2")),
                EvalNugget.from(() -> queryTable.select("intCol=intCol + doubleCol")),
                EvalNugget.from(
                    () -> queryTable.select("newCol=intCol / 2").update("newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.update("intCol=intCol * 2")),
                EvalNugget.from(() -> sortedTable.update("intCol=intCol + doubleCol")),
                EvalNugget
                    .from(() -> sortedTable.update("newCol=intCol / 2", "newCol2=newCol * 4")),
                EvalNugget.from(
                    () -> sortedTable.update("newCol=intCol / 2").update("newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.select("intCol=intCol * 2")),
                EvalNugget.from(() -> sortedTable.select("intCol=intCol + doubleCol")),
                EvalNugget.from(
                    () -> sortedTable.select("newCol=intCol / 2").update("newCol2=newCol * 4")),
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * 4")),
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * newCol")),
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i] * repeatedCol")),
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("newCol2=intCol / 2", "newCol=newCol2_[i] + 7")),
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("newCol=intCol_[i]")),
                partialEvalNuggetFrom(sortedTable, false,
                    () -> sortedTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * 4")),
                partialEvalNuggetFrom(sortedTable, false,
                    () -> sortedTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * newCol")),
                partialEvalNuggetFrom(sortedTable, false,
                    () -> sortedTable.update("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i] * repeatedCol")),
                partialEvalNuggetFrom(sortedTable, false,
                    () -> sortedTable.update("newCol2=intCol / 2", "newCol=newCol2_[i] + 7")),
                partialEvalNuggetFrom(sortedTable, false,
                    () -> sortedTable.update("newCol=intCol_[i]")),
                partialEvalNuggetFrom(queryTable, true,
                    () -> queryTable.select("newCol=intCol_[i]")),
                partialEvalNuggetFrom(queryTable, true,
                    () -> queryTable.select("newCol=intCol / 2", "newCol2=newCol_[i] * 4")),
                partialEvalNuggetFrom(queryTable, true,
                    () -> queryTable.select("newCol=intCol / 2", "newCol2=newCol_[i] * newCol")),
                partialEvalNuggetFrom(queryTable, true,
                    () -> queryTable.select("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i] * repeatedCol")),
                partialEvalNuggetFrom(queryTable, true,
                    () -> queryTable.select("newCol2=intCol / 2", "newCol=newCol2_[i] + 7")),
                partialEvalNuggetFrom(sortedTable, true,
                    () -> sortedTable.select("newCol=intCol_[i]")),
                partialEvalNuggetFrom(sortedTable, true,
                    () -> sortedTable.select("newCol=intCol / 2", "newCol2=newCol_[i] * 4")),
                partialEvalNuggetFrom(sortedTable, true,
                    () -> sortedTable.select("newCol=intCol / 2", "newCol2=newCol_[i] * newCol")),
                partialEvalNuggetFrom(sortedTable, true,
                    () -> sortedTable.select("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i] * repeatedCol")),
                partialEvalNuggetFrom(sortedTable, true,
                    () -> sortedTable.select("newCol2=intCol / 2", "newCol=newCol2_[i] + 7")),
                // This case is rather nasty, because we have an intermediate column to deal with.
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("newCol2=intCol / 2", "newCol=newCol2",
                        "newCol=newCol_[i] + 7")),
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("newCol=intCol / 2", "newCol=newCol_[i] + 7")),
                new UpdateValidatorNugget(
                    queryTable.select("newCol=intCol / 2", "newCol=newCol_[i] + 7")),
                // Let's change the type of a column.
                EvalNugget.from(() -> queryTable.select("intCol = intCol/2")),
                EvalNugget.from(() -> queryTable.update("intCol = intCol/2")),
                EvalNugget.from(() -> queryTable.update("newCol = `` + intCol/2")),
                EvalNugget.from(() -> queryTable.update("newCol = intCol > 50")),
                // Let's create a datetime and use it as an override
                partialEvalNuggetFrom(queryTable, false,
                    () -> queryTable.update("Time = new DBDateTime(0) + intCol * MINUTE")
                        .update("Diff = Time_[i]")),
                partialEvalNuggetFrom(queryTable, true,
                    () -> queryTable.select("Time = new DBDateTime(0) + intCol * MINUTE")
                        .select("Time", "Diff = Time_[i]")),
        };

        final int maxSteps = numSteps.intValue();
        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Step = " + numSteps.intValue());
            }
            LiveTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testUpdateIncrementalWithI() throws IOException {
        Random random = new Random(0);
        ColumnInfo columnInfo[];
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
            columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("a", "b", "c", "d", "e"),
                new IntGenerator(10, 100),
                new SetGenerator<>(10.1, 20.1, 30.1)));

        EvalNuggetInterface[] en = new TableComparator[] {
                new TableComparator(queryTable.update("CI=i"), queryTable.update("CI=i")),
                new TableComparator(queryTable.update("CK=k"), queryTable.update("CK=k")),
                new TableComparator(queryTable.update("CI=i", "CK=k"),
                    queryTable.update("CI=i").update("CK=k")),
                new TableComparator(queryTable.update("CI=i", "PI=CI_[i-1]"),
                    queryTable.update("CI=i").update("PI=CI_[i-1]")),

        };

        for (int i = 0; i < 50; i++) {
            LiveTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }

    }

    @Test
    public void testUpdateEmptyTable() throws IOException {
        QueryTable table = TstUtils.testRefreshingTable(i());
        QueryTable table2 = (QueryTable) table.update("x = i*3", "y = \"\" + k");
        ListenerWithGlobals listener = base.newListenerWithGlobals(table2);
        table2.listenForUpdates(listener);

        TestCase.assertEquals(0, table.size());
        TestCase.assertEquals(0, table2.size());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            show(table2);
            addToTable(table, i(7, 9));
            table.notifyListeners(i(7, 9), i(), i());
        });


        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        TestCase.assertEquals(Arrays.asList(0, 3),
            Arrays.asList(table2.getColumn("x").get(0, table2.size())));
        TestCase
            .assertEquals(Arrays.asList("7", "9"),
                Arrays.asList(table2.getColumn("y").get(0, table2.size())));
        TestCase.assertEquals(base.added, i(7, 9));
        TestCase.assertEquals(base.removed, i());
        TestCase.assertEquals(base.modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
        });
    }

    @Test
    public void testUpdateIndex() throws IOException {
        QueryTable table = TstUtils.testRefreshingTable(i());
        QueryTable table2 = (QueryTable) table.update("Position=i", "Key=\"\" + k");
        ListenerWithGlobals listener = base.newListenerWithGlobals(table2);
        table2.listenForUpdates(listener);

        TestCase.assertEquals(0, table.size());
        TestCase.assertEquals(0, table2.size());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            show(table2);
            addToTable(table, i(7, 9));
            table.notifyListeners(i(7, 9), i(), i());
        });


        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        TestCase.assertEquals(Arrays.asList(0, 1),
            Arrays.asList(table2.getColumn("Position").get(0, table2.size())));
        TestCase.assertEquals(Arrays.asList("7", "9"),
            Arrays.asList(table2.getColumn("Key").get(0, table2.size())));
        TestCase.assertEquals(base.added, i(7, 9));
        TestCase.assertEquals(base.removed, i());
        TestCase.assertEquals(base.modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            table.notifyListeners(i(), i(), i(9));
        });

        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        TestCase.assertEquals(Arrays.asList(0, 1),
            Arrays.asList(table2.getColumn("Position").get(0, table2.size())));
        TestCase.assertEquals(Arrays.asList("7", "9"),
            Arrays.asList(table2.getColumn("Key").get(0, table2.size())));
        TestCase.assertEquals(base.added, i());
        TestCase.assertEquals(base.removed, i());
        TestCase.assertEquals(base.modified, i(9));
    }

    @Test
    public void testUpdateArrayColumns() throws IOException {
        QueryTable table = TstUtils.testRefreshingTable(i());
        QueryTable table2 = (QueryTable) table.update("Position=i", "PrevI=Position_[i-1]");
        // QueryTable table2 = (QueryTable) table.update("Position=i", "Key=\"\" + k",
        // "PrevI=Position_[i-1]");
        ListenerWithGlobals listener = base.newListenerWithGlobals(table2);
        table2.listenForUpdates(listener);

        TestCase.assertEquals(0, table.size());
        TestCase.assertEquals(0, table2.size());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            show(table2);
            addToTable(table, i(7, 9));
            table.notifyListeners(i(7, 9), i(), i());
        });


        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        TestCase.assertEquals(Arrays.asList(0, 1),
            Arrays.asList(table2.getColumn("Position").get(0, table2.size())));
        // assertEquals(Arrays.asList("7", "9"), Arrays.asList(table2.getColumn("Key").get(0,
        // table2.size())));
        TestCase.assertEquals(Arrays.asList(null, 0),
            Arrays.asList(table2.getColumn("PrevI").get(0, table2.size())));
        TestCase.assertEquals(i(7, 9), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(), base.modified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            table.notifyListeners(i(), i(), i(9));
        });

        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        TestCase.assertEquals(Arrays.asList(0, 1),
            Arrays.asList(table2.getColumn("Position").get(0, table2.size())));
        // assertEquals(Arrays.asList("7", "9"), Arrays.asList(table2.getColumn("Key").get(0,
        // table2.size())));
        TestCase.assertEquals(Arrays.asList(null, 0),
            Arrays.asList(table2.getColumn("PrevI").get(0, table2.size())));
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(9), base.modified);
    }

    @Test
    public void testLargeStringConstants() {
        String a = new String(new char[32000]).replace("\0", "A");
        String b = new String(new char[32000]).replace("\0", "B");
        Table x = TableTools.emptyTable(1).update("C=`" + a + "` + `" + b + "`");
        TestCase.assertEquals(1, x.getColumns().length);

        a = new String(new char[40000]).replace("\0", "A");
        b = new String(new char[40000]).replace("\0", "B");
        x = TableTools.emptyTable(1).update("C=String.join(\"" + a
            + "\", Integer.toString(new Random().nextInt()), \"" + b + "\")");
        TestCase.assertEquals(1, x.getColumns().length);
    }

    @Test
    public void testStaticAddressSpace() {
        final Table source = emptyTable(1000000).update("A=ii");
        final Table x = source.where("A%2==0");
        final Table xs = x.select();
        TableTools.showWithIndex(xs);
        assertTableEquals(x, xs);
        // overhead would be greater than 10%, so we expect a flat table
        TestCase.assertTrue(xs.isFlat());
        TestCase.assertEquals(x.size() - 1, xs.getIndex().lastKey());

        final Table x2 = source.where("A % 100 > 5");
        final Table x2s = x2.select();
        assertTableEquals(x2, x2s);
        TableTools.showWithIndex(x2s);
        // overhead is only 5%, so we expect a pass-through index
        TestCase.assertSame(x2s.getIndex(), x2.getIndex());

        // simulation of two partitions, we want to keep the index
        final Table x3 = source.where("A < 250000 || A > 750000");
        final Table x3s = x3.select();
        assertTableEquals(x3, x3s);
        TestCase.assertSame(x3s.getIndex(), x3.getIndex());

        // simulation of selecting 80% of the data, we need to flatten
        final Table x4 = source.where("((int)(A / 1000) % 5 != 2)");
        final Table x4s = x4.select();
        assertTableEquals(x4, x4s);
        TestCase.assertEquals(x4s.getIndex().lastKey(), x4.size() - 1);
        TestCase.assertTrue(x4s.isFlat());
    }

    @Test
    public void testSelectReuse() {
        final QueryTable table =
            TstUtils.testRefreshingTable(i(1, 1L << 20 + 1), longCol("Value", 1, 2));

        final Table selected = table.select();
        assertTableEquals(table, selected);
        final Table withUpdateView = selected.updateView("Value2=Value*2");
        final Table selected2 = withUpdateView.select();
        assertTableEquals(table, selected2.dropColumns("Value2"));

        TestCase.assertSame(selected.getColumnSource("Value"), selected2.getColumnSource("Value"));
        TestCase.assertNotSame(withUpdateView.getColumnSource("Value2"),
            selected2.getColumnSource("Value2"));
        TestCase.assertTrue(selected2.getColumnSource("Value2") instanceof LongSparseArraySource);

        assertTableEquals(prevTable(table), prevTable(selected));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), longCol("Value", 3));
            table.notifyListeners(i(2), i(), i());
        });

        assertTableEquals(table, selected);
        assertTableEquals(prevTable(table), prevTable(selected));

        TableTools.show(table);
        TableTools.show(selected);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(1L << 20 + 2), longCol("Value", 4));
            table.notifyListeners(i(1L << 20 + 2), i(), i());
        });

        TableTools.show(table);
        TableTools.show(selected);

        assertTableEquals(table, selected);
        assertTableEquals(prevTable(table), prevTable(selected));
    }

    @Test
    public void testSparseSelect() {
        int size = 1000;
        for (int seed = 0; seed < 10; ++seed) {
            System.out.println(DBDateTime.now() + ": Size = " + size + ", seed=" + seed);
            try (final SafeCloseable ignored =
                LivenessScopeStack.open(new LivenessScope(true), true)) {
                testSparseSelect(size, seed);
            }
        }
        size = 10000;
        for (int seed = 0; seed < 1; ++seed) {
            System.out.println(DBDateTime.now() + ": Size = " + size + ", seed=" + seed);
            try (final SafeCloseable ignored =
                LivenessScopeStack.open(new LivenessScope(true), true)) {
                testSparseSelect(size, seed);
            }
        }
    }

    private void testSparseSelect(int size, int seed) {
        final Random random = new Random(seed);
        final TstUtils.ColumnInfo[] columnInfo;

        final QueryTable queryTable = getTable(size, random,
            columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol", "doubleCol", "boolCol", "floatCol", "longCol",
                        "charCol", "byteCol", "shortCol", "dbDateTime"},
                new SetGenerator<>("a", "b", "c", "d", "e"),
                new IntGenerator(10, 100),
                new SetGenerator<>(10.1, 20.1, 30.1),
                new BooleanGenerator(0.5, 0.1),
                new FloatGenerator(-1000.0f, 1000.0f),
                new LongGenerator(),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator(),
                new UnsortedDateTimeGenerator(DBTimeUtils.convertDateTime("2019-01-10T00:00:00 NY"),
                    DBTimeUtils.convertDateTime("2019-01-20T00:00:00 NY"))));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNuggetInterface en[] = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable, "Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.partialSparseSelect(queryTable, "Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable, "boolCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable, "dbDateTime");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(sortedTable);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(
                            TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(
                            TableTools.merge(queryTable, queryTable, queryTable, queryTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(
                            TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable)
                                .by("Sym").sort("Sym").ungroup());
                    }
                },
                new TableComparator(
                    TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable).by("Sym")
                        .sort("Sym").ungroup(),
                    SparseSelect.sparseSelect(
                        TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable)
                            .by("Sym").sort("Sym").ungroup())),
                new TableComparator(queryTable, SparseSelect.sparseSelect(queryTable)),
                new TableComparator(queryTable,
                    SparseSelect.partialSparseSelect(queryTable,
                        Arrays.asList("shortCol", "dbDateTime"))),
                new TableComparator(sortedTable, SparseSelect.sparseSelect(sortedTable))
        };

        for (int step = 0; step < 25; step++) {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Step: " + step);
            }
            LiveTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testSparseSelectWideIndex() {
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        final int[] intVals = new int[63];
        for (int ii = 0; ii < 63; ii++) {
            builder.appendKey(1L << ii);
            intVals[ii] = ii;
        }
        final QueryTable table =
            TstUtils.testRefreshingTable(builder.getIndex(), intCol("Value", intVals));
        final Table selected = SparseSelect.sparseSelect(table);
        final String diff = TableTools.diff(selected, table, 10);
        TestCase.assertEquals("", diff);
    }

    @Test
    public void testSparseSelectSkipMemoryColumns() {
        final int[] intVals = {1, 2, 3, 4, 5};
        final Table table =
            TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(5), intCol("Value", intVals))
                .update("V2=Value*2");
        final Table selected = SparseSelect.sparseSelect(table);
        assertTableEquals(table, selected);
        TestCase.assertSame(table.getColumnSource("V2"), selected.getColumnSource("V2"));
        TestCase.assertNotSame(table.getColumnSource("Value"), selected.getColumnSource("Value"));

        final Table tt = TableTools.newTable(intCol("Val", intVals)).updateView("V2=Val*2");
        TestCase.assertTrue(tt.getColumnSource("Val") instanceof ArrayBackedColumnSource);
        final Table selected2 = SparseSelect.sparseSelect(tt);
        TestCase.assertSame(tt.getColumnSource("Val"), selected2.getColumnSource("Val"));
        TestCase.assertNotSame(tt.getColumnSource("V2"), selected2.getColumnSource("V2"));
    }

    @Test
    public void testSparseSelectReuse() {

        final QueryTable table =
            TstUtils.testRefreshingTable(i(1, 1L << 20 + 1), longCol("Value", 1, 2));


        final Table selected = SparseSelect.sparseSelect(table);
        final String diff = TableTools.diff(selected, table, 10);
        TestCase.assertEquals("", diff);

        final String diffPrev = TableTools.diff(prevTable(selected), prevTable(table), 10);
        TestCase.assertEquals("", diffPrev);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), longCol("Value", 3));
            table.notifyListeners(i(2), i(), i());
        });

        final String diff2 = TableTools.diff(selected, table, 10);
        TestCase.assertEquals("", diff2);

        final String diffPrev2 = TableTools.diff(prevTable(selected), prevTable(table), 10);
        TestCase.assertEquals("", diffPrev2);

        TableTools.show(table);
        TableTools.show(selected);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(1L << 20 + 2), longCol("Value", 4));
            table.notifyListeners(i(1L << 20 + 2), i(), i());
        });

        TableTools.show(table);
        TableTools.show(selected);

        final String diff3 = TableTools.diff(selected, table, 10);
        TestCase.assertEquals("", diff3);

        final String diffPrev3 = TableTools.diff(prevTable(selected), prevTable(table), 10);
        TestCase.assertEquals("", diffPrev3);
    }

    @Test
    public void testIds5212() {
        final Table testtest = TableTools.emptyTable(554).view("Quantity=k").where("Quantity>200");
        final Table test = testtest.update("Quantity=100", "Test=Quantity");

        final int[] testArray = (int[]) test.getColumn("Test").getDirect();
        final int[] expected = new int[test.intSize()];
        Arrays.fill(expected, 100);
        BaseArrayTestCase.assertEquals(expected, testArray);
    }

    /**
     * In IDS-5614 it was observed that a dynamic table that starts out empty won't do its formula
     * initialization (compilation, param grabbing etc) until later. This test confirms that this is
     * fixed by setting a param to a valid value, calling QueryTable.update() and then setting it to
     * a string value that won't compile. If the compilation/param grabbing is lazy, this test will
     * fail. If you want to see the test fail under the old behavior, you would need to undo the
     * bugfix that prevent this from happening: 1. In DHFormulaColumn.generateClassBody(), remove
     * this 'if' guard and make the setting of params unconditional [adding this guard was the
     * first, coarser bug fix] if (params == null) { // remove this if params =
     * QueryScope.getDefaultInstance().getParams(userParams); // keep this line } 2. Conditionally
     * invoke select.getDataView() (via SelectColumnLayer#getChunkSource()) only if it is needed in
     * SelectColumnLayer#applyUpdate.
     */
    @Test
    public void testEagerParamBinding() {
        QueryScope.addParam("scale", 12);
        final QueryTable table = TstUtils.testRefreshingTable(i());
        final QueryTable table2 = (QueryTable) table.update("A = i * scale");
        QueryScope.addParam("scale", "Multiplying i by this string will not compile");
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            show(table);
            addToTable(table, i(10, 20));
            table.notifyListeners(i(10, 20), i(), i());
        });
        showWithIndex(table2);
    }

    @Test
    public void testExpectedParamFailure() {
        try {
            QueryScope.addParam("scale2", 12);
            final QueryTable table = TstUtils.testRefreshingTable(i());
            table.update("A = i * scale");
            TestCase.fail("Bad formula failed to throw exception");
        } catch (FormulaCompilationException fce) {
            // Expected
        }
    }

    @Test
    public void testIds5746() {
        final Table x = TableTools.emptyTable(2).update("result = `blah`", "L = result.length()");

        final int[] testArray = (int[]) x.getColumn("L").getDirect();
        final int[] expected = new int[] {4, 4};
        BaseArrayTestCase.assertEquals(expected, testArray);
    }

    @Test
    public void testDoubleSelect() {
        final Table x = TableTools.emptyTable(100000).updateView("Q=ii").where("Q%2==0");
        final Table y = x.select("Q", "Q");
        final Table z = x.select("Q", "Q=Q");
        final Table w = x.select("Q=Q", "Q");

        TableTools.showWithIndex(x);
        TableTools.showWithIndex(y);
        TableTools.showWithIndex(z);
        TableTools.showWithIndex(w);

        TestCase.assertEquals("", TableTools.diff(x, y, 10));
        TestCase.assertEquals("", TableTools.diff(x, z, 10));
        TestCase.assertEquals("", TableTools.diff(x, w, 10));
    }

    @Test
    public void testFlattenSelect() {
        final Table input = emptyTable(100000).updateView("A=ii", "B=ii % 1000", "C=ii % 2 == 0");
        final Table evens = input.where("C");
        assertTableEquals(evens, evens.flatten().select());
    }
}
