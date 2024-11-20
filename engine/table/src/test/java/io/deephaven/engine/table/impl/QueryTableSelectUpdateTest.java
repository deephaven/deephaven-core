//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.*;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.literal.Literal;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.QueryTableTestBase.ListenerWithGlobals;
import io.deephaven.engine.testutil.QueryTableTestBase.TableComparator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.LongVector;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test QueryTable select and update operations.
 */
public class QueryTableSelectUpdateTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testSelectAndUpdate() {
        testSelectAndUpdate(false);
        testSelectAndUpdate(true);
    }

    public void testSelectAndUpdate(boolean useRedirection) {
        final boolean startSelect = QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT;
        final boolean startUpdate = QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE;

        try {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = useRedirection;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = useRedirection;
            try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                doTestSelectAndUpdate();
            }
        } finally {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = startSelect;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = startUpdate;
        }
    }

    public void doTestSelectAndUpdate() {
        final QueryTable table1 =
                (QueryTable) TstUtils.testRefreshingTable(i(2, 4, 6).toTracking())
                        .select("x = i*3", "y = \"\" + k");

        assertTableEquals(TableTools.newTable(intCol("x", 0, 3, 6), stringCol("y", "2", "4", "6")), table1);

        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", 'a', 'b', 'c'));

        TableTools.showWithRowSet(table);
        QueryTable table2 = (QueryTable) table.select("x = x * 2", "z = y");
        TableTools.showWithRowSet(table2);

        assertTableEquals(TableTools.newTable(intCol("x", 2, 4, 6), charCol("z", 'a', 'b', 'c')), table2);

        final Table table3 = table.update("q = i", "q = q + 1", "p = q+10");

        assertTableEquals(TableTools.newTable(intCol("x", 1, 2, 3), charCol("y", 'a', 'b', 'c'), intCol("q", 1, 2, 3),
                intCol("p", 11, 12, 13)), table3);

        final ShiftObliviousListener table2Listener = base.newListenerWithGlobals(table2);
        table2.addUpdateListener(table2Listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), col("x", 4, 5), col("y", 'd', 'e'));
            table.notifyListeners(i(7, 9), i(), i());
        });
        TableTools.showWithRowSet(table);
        TableTools.showWithRowSet(table2);
        TableTools.showWithRowSet(table3);

        assertTableEquals(TableTools.newTable(intCol("x", 1, 2, 3, 4, 5), charCol("y", 'a', 'b', 'c', 'd', 'e'),
                intCol("q", 1, 2, 3, 4, 5), intCol("p", 11, 12, 13, 14, 15)), table3);
        assertTableEquals(TableTools.newTable(intCol("x", 2, 4, 6, 8, 10), charCol("z", 'a', 'b', 'c', 'd', 'e')),
                table2);

        TestCase.assertEquals(i(7, 9), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(), base.modified);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), col("x", 3, 10), col("y", 'e', 'd'));
            table.notifyListeners(i(), i(), i(7, 9));
        });

        assertTableEquals(TableTools.newTable(intCol("x", 1, 2, 3, 3, 10), charCol("y", 'a', 'b', 'c', 'e', 'd'),
                intCol("q", 1, 2, 3, 4, 5), intCol("p", 11, 12, 13, 14, 15)), table3);
        assertTableEquals(TableTools.newTable(intCol("x", 2, 4, 6, 6, 20), charCol("z", 'a', 'b', 'c', 'e', 'd')),
                table2);

        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(7, 9), base.modified);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table, i(2, 6, 7));
            table.notifyListeners(i(), i(2, 6, 7), i());
        });

        assertTableEquals(
                TableTools.newTable(intCol("x", 2, 10), charCol("y", 'b', 'd'), intCol("q", 2, 5), intCol("p", 12, 15)),
                table3);
        assertTableEquals(TableTools.newTable(intCol("x", 4, 20), charCol("z", 'b', 'd')), table2);

        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(2, 6, 7), base.removed);
        TestCase.assertEquals(i(), base.modified);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table, i(9));
            addToTable(table, i(2, 4, 6), col("x", 1, 22, 3), col("y", 'a', 'x', 'c'));
            table.notifyListeners(i(2, 6), i(9), i(4));
        });
        TestCase.assertEquals(3, table2.size());

        assertTableEquals(TableTools.newTable(intCol("x", 1, 22, 3), charCol("y", 'a', 'x', 'c'), intCol("q", 1, 2, 3),
                intCol("p", 11, 12, 13)), table3);
        assertTableEquals(TableTools.newTable(intCol("x", 2, 44, 6), charCol("z", 'a', 'x', 'c')), table2);

        TestCase.assertEquals(i(2, 6), base.added);
        TestCase.assertEquals(i(9), base.removed);
        TestCase.assertEquals(i(4), base.modified);

        final QueryTable table4 = (QueryTable) TableTools.emptyTable(3).select("x = i*2", "y = \"\" + x");
        assertTableEquals(TableTools.newTable(intCol("x", 0, 2, 4), stringCol("y", "0", "2", "4")), table4);

        final QueryTable table5 = (QueryTable) table4.update("z = x", "x = z + 1", "t = x - 3");
        assertTableEquals(TableTools.newTable(intCol("x", 1, 3, 5), stringCol("y", "0", "2", "4"), intCol("z", 0, 2, 4),
                intCol("t", -2, 0, 2)), table5);


        final QueryTable table6 = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", 'a', 'b', 'c'));
        final Table table7 = table6.update("z = x", "x = z + 1", "t = x - 3");
        assertTableEquals(TableTools.newTable(intCol("x", 2, 3, 4), charCol("y", 'a', 'b', 'c'), intCol("z", 1, 2, 3),
                intCol("t", -1, 0, 1)), table7);

        final ShiftObliviousListener table7Listener2 = base.newListenerWithGlobals(table7);
        table7.addUpdateListener(table7Listener2);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table6, i(7, 9), col("x", 4, 5), col("y", 'd', 'e'));
            table6.notifyListeners(i(7, 9), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("x", 2, 3, 4, 5, 6), charCol("y", 'a', 'b', 'c', 'd', 'e'),
                intCol("z", 1, 2, 3, 4, 5), intCol("t", -1, 0, 1, 2, 3)), table7);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table6, i(7, 9), col("x", 3, 10), col("y", 'e', 'd'));
            table6.notifyListeners(i(), i(), i(7, 9));
        });

        assertTableEquals(TableTools.newTable(intCol("x", 2, 3, 4, 4, 11), charCol("y", 'a', 'b', 'c', 'e', 'd'),
                intCol("z", 1, 2, 3, 3, 10), intCol("t", -1, 0, 1, 1, 8)), table7);
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(7, 9), base.modified);
        TestCase.assertEquals(i(), base.removed);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table6, i(2, 6, 7));
            table6.notifyListeners(i(), i(2, 6, 7), i());
        });

        assertTableEquals(
                TableTools.newTable(intCol("x", 3, 11), charCol("y", 'b', 'd'), intCol("z", 2, 10), intCol("t", 0, 8)),
                table7);
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(2, 6, 7), base.removed);
        TestCase.assertEquals(i(), base.modified);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table6, i(9));
            addToTable(table6, i(2, 4, 6), col("x", 1, 22, 3), col("y", 'a', 'x', 'c'));
            table6.notifyListeners(i(2, 6), i(9), i(4));
        });

        assertTableEquals(TableTools.newTable(intCol("x", 2, 23, 4), charCol("y", 'a', 'x', 'c'), intCol("z", 1, 22, 3),
                intCol("t", -1, 20, 1)), table7);
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
        // Skip this test if we are using kernel formulas, because FormulaKernel ignores lazy, and therefore all these
        // callCounts are all going to be wrong.
        if (DhFormulaColumn.useKernelFormulasProperty) {
            // We'd rather use Assume.assumeFalse() here, but we can't because we're using an old JUnit in this file.
            return;
        }

        ExecutionContext.getContext().getQueryLibrary().importStatic(QueryTableSelectUpdateTest.class);

        QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), col("A", 1, 2, 3));
        table = (QueryTable) table
                .lazyUpdate("B=" + QueryTableSelectUpdateTest.class.getCanonicalName() + ".callCounter(A)");
        TestCase.assertEquals(3, table.size());
        TestCase.assertEquals(0, callCount);
        assertArrayEquals(new int[] {3, 6, 9}, ColumnVectors.ofInt(table, "B").toArray());
        TestCase.assertEquals(3, callCount);
        assertArrayEquals(new int[] {3, 6, 9}, ColumnVectors.ofInt(table, "B").toArray());
        TestCase.assertEquals(3, callCount);

        callCount = 0;
        QueryTable table2 = TstUtils.testRefreshingTable(i(2, 4, 6, 8, 10, 12).toTracking(),
                col("A", 1, 2, 3, 2, 3, 1));
        table2 = (QueryTable) table2
                .lazyUpdate("B=" + QueryTableSelectUpdateTest.class.getCanonicalName() + ".callCounter(A)");
        TestCase.assertEquals(6, table2.size());
        TestCase.assertEquals(0, callCount);
        assertArrayEquals(new int[] {3, 6, 9, 6, 9, 3}, ColumnVectors.ofInt(table2, "B").toArray());
        TestCase.assertEquals(3, callCount);
        assertArrayEquals(new int[] {3, 6, 9, 6, 9, 3}, ColumnVectors.ofInt(table2, "B").toArray());
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

    private abstract static class PartialEvalNugget extends EvalNugget {
        // We listen to table updates, so we know what to compare.
        static class UpdateListener extends ShiftObliviousInstrumentedListener {
            UpdateListener() {
                super("Update RefreshProcedure");
            }

            RowSet added, removed, modified;
            Throwable exception = null;

            @Override
            public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                this.added = added.copy();
                this.removed = removed.copy();
                this.modified = modified.copy();
            }

            @Override
            public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
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
            sourceTable.addUpdateListener(listener1);

            listener2 = new UpdateListener();
            originalValue.addUpdateListener(listener2);

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
            if (RefreshingTableTestCase.printTableUpdates) {
                try {
                    System.out.println("Recomputed Value:");
                    TableTools.showWithRowSet(recomputedValue, 100);
                    System.out.println("Iterative Value:");
                    TableTools.showWithRowSet(originalValue, 100);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            List<String> issues = new ArrayList<>();
            StringBuilder result = new StringBuilder();
            if (originalValue.size() != recomputedValue.size()) {
                issues.add("Result table has size " + originalValue.size() + " vs. expected " + recomputedValue.size());
            }

            if (indexPositionChangesAllowed) {
                // we should make sure that our added + removed is equal to the source added + removed size
                long sourceSizeChange = listener1.added.size() - listener1.removed.size();
                long resultSizeChange = 0;
                // if the update was determined to be irrelevant, listener2 may not receive any event
                if (listener2.added != null && listener2.removed != null) {
                    resultSizeChange = listener2.added.size() - listener2.removed.size();
                }
                if (sourceSizeChange != resultSizeChange) {
                    issues.add("Source changed size by " + sourceSizeChange + ", but result changed size by "
                            + resultSizeChange);
                }
            } else {
                RowSet sourceAddedPositions = sourceTable.getRowSet().invert(listener1.added);
                RowSet sourceRemovedPositions = sourceTable.getRowSet().copyPrev().invert(listener1.removed);
                RowSet sourceModifiedPositions = sourceTable.getRowSet().invert(listener1.modified);

                RowSet resultAddedPositions = originalValue.getRowSet().invert(listener2.added);
                RowSet resultRemovedPositions = originalValue.getRowSet().copyPrev().invert(listener2.removed);
                RowSet resultModifiedPositions = originalValue.getRowSet().invert(listener2.modified);

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

            WritableRowSet rowSetToCheck = listener1.added.copy();
            rowSetToCheck.insert(listener1.modified);
            RowSet checkInvert = sourceTable.getRowSet().invert(rowSetToCheck);

            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Positions to validate: " + checkInvert);

                final RowSetBuilderSequential originalBuilder = RowSetFactory.builderSequential();
                final RowSetBuilderSequential recomputedBuilder = RowSetFactory.builderSequential();
                checkInvert.forAllRowKeys(x -> originalBuilder.appendKey(originalValue.getRowSet().get(x)));
                checkInvert.forAllRowKeys(x -> recomputedBuilder.appendKey(recomputedValue.getRowSet().get(x)));

                System.out.println("Original Sub Table: " + checkInvert);
                TableTools.showWithRowSet(originalValue.getSubTable(originalBuilder.build().toTracking()));
                System.out.println("Recomputed Sub Table: " + checkInvert);
                TableTools.showWithRowSet(recomputedValue.getSubTable(recomputedBuilder.build().toTracking()));
            }

            Map<String, ? extends ColumnSource<?>> originalColumns = originalValue.getColumnSourceMap();
            Map<String, ? extends ColumnSource<?>> recomputedColumns = recomputedValue.getColumnSourceMap();

            for (Map.Entry<String, ? extends ColumnSource<?>> stringColumnSourceEntry : recomputedColumns.entrySet()) {
                String columnName = stringColumnSourceEntry.getKey();
                ColumnSource<?> originalColumnSource = originalColumns.get(columnName);
                ColumnSource<?> recomputedColumn = stringColumnSourceEntry.getValue();

                for (RowSet.Iterator iterator = checkInvert.iterator(); iterator.hasNext();) {
                    int position = (int) iterator.nextLong();
                    long originalKey = originalValue.getRowSet().get(position);
                    long recomputedKey = recomputedValue.getRowSet().get(position);

                    Object original = originalColumnSource.get(originalKey);
                    Object recomputed = recomputedColumn.get(recomputedKey);
                    if (original != recomputed && (original == null || !original.equals(recomputed))) {
                        issues.add("Mismatch at position " + position + "column " + columnName + ": " + original
                                + " != " + recomputed);
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
        // Assume.assumeTrue("We are purposefully skipping this very long running test that does not actually verify
        // anything.", RUN_SPARSE_REDIRECTION_UPDATE_TEST);
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

        final QueryTable leftTable = new QueryTable(RowSetFactory.flat(99).toTracking(),
                Collections.emptyMap());
        leftTable.setRefreshing(true);
        final QueryTable rightTable = new QueryTable(RowSetFactory.flat(1).toTracking(),
                Collections.emptyMap());
        rightTable.setRefreshing(true);

        final Table leftWithKey = leftTable.updateView("Key=`a`", "LI=ii");
        final Table rightWithKey = rightTable.updateView("Key=`a`", "RI=ii");

        final Table joined = leftWithKey.join(rightWithKey, List.of(JoinMatch.parse("Key")), emptyList(), 2);

        final Table updated = joined.update("LRI=LI*RI", "Str=Long.toString(LRI)");

        System.gc();
        System.gc(); // What I tell you two times is true (Snark or not).
        final RuntimeMemory.Sample sample = new RuntimeMemory.Sample();
        RuntimeMemory.getInstance().read(sample);

        final long startUsedMemory = sample.totalMemory - sample.freeMemory;

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < 10000; ++step) {
            final int fstep = step;

            updateGraph.runWithinUnitTestCycle(() -> {
                final long keyToAdd = fstep + 1;
                final RowSet addedRowSet = i(keyToAdd);
                final RowSet removedRowSet = (fstep % 2 == 0) ? i(fstep) : i();
                addToTable(rightTable, addedRowSet);
                removeRows(rightTable, removedRowSet);
                rightTable.notifyListeners(addedRowSet, removedRowSet, i());

                if (fstep % 100 == 99) {
                    addToTable(leftTable, i(fstep));
                    leftTable.notifyListeners(i(fstep), i(), i());
                }
            });

            System.gc();
            RuntimeMemory.getInstance().read(sample);
            final long totalMemory = sample.totalMemory;
            final long freeMemory = sample.freeMemory;
            final long usedMemory = totalMemory - freeMemory;
            final long deltaUsed = usedMemory - startUsedMemory;
            System.out.println("Step = " + step + ", " + deltaUsed + "(" + usedMemory + "total) used, " + freeMemory
                    + " free / " + totalMemory + " total, " + " updated size=" + updated.size() + ", memory/row="
                    + (deltaUsed / updated.size()));
        }
    }

    @Test
    public void testSelectRedirectionFlat() {
        final boolean startSelect = QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT;
        final boolean startUpdate = QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE;

        try {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = true;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = true;

            final QueryTable test1 = TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                    intCol("Sentinel", 1, 2, 3, 4));

            final Table select1 = test1.select();
            final Table selectFlat = test1.flatten().select();

            final ColumnSource<?> select1Column = select1.getColumnSource("Sentinel");
            final ColumnSource<?> selectFlatColumn = selectFlat.getColumnSource("Sentinel");
            Assert.assertTrue(select1Column instanceof RedirectedColumnSource);
            Assert.assertTrue(selectFlatColumn instanceof SparseArrayColumnSource);
        } finally {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = startSelect;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = startUpdate;
        }
    }

    @Test
    public void testUpdateIncremental() {
        for (int seed = 0; seed < 3; ++seed) {
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testUpdateIncremental(seed, false);
            }
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testUpdateIncremental(seed, true);
            }
        }
    }

    private void testUpdateIncremental(int seed, boolean useRedirection) {
        final boolean startSelect = QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT;
        final boolean startUpdate = QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE;

        try {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = useRedirection;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = useRedirection;
            try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                testUpdateIncremental(seed, new MutableInt(100));
            }
        } finally {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = startSelect;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = startUpdate;
        }
    }

    private void testUpdateIncremental(final int seed, MutableInt numSteps) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final int size = 25;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.update("intCol=intCol * 2")),
                EvalNugget.from(() -> queryTable.update("newCol=intCol / 2", "newCol2=newCol * 4")),
                EvalNugget.from(() -> queryTable.update("newCol=intCol / 2").update("newCol2=newCol * 4")),
                EvalNugget.from(() -> queryTable.select("intCol=intCol + doubleCol")),
                EvalNugget.from(() -> queryTable.select("newCol=intCol / 2").update("newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.update("intCol=intCol + doubleCol")),
                EvalNugget.from(() -> sortedTable.update("newCol=intCol / 2", "newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.update("newCol=intCol / 2").update("newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.select("intCol=intCol * 2")),
                EvalNugget.from(() -> sortedTable.select("newCol=intCol / 2").update("newCol2=newCol * 4")),
                partialEvalNuggetFrom(queryTable, false,
                        () -> queryTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * 4")),
                partialEvalNuggetFrom(queryTable, false,
                        () -> queryTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * newCol")),
                partialEvalNuggetFrom(queryTable, false,
                        () -> queryTable.update("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                                "repeatedCol=newCol_[i] * repeatedCol")),
                partialEvalNuggetFrom(queryTable, false,
                        () -> queryTable.update("newCol2=intCol / 2", "newCol=newCol2_[i] + 7")),
                partialEvalNuggetFrom(queryTable, false, () -> queryTable.update("newCol=intCol_[i]")),
                partialEvalNuggetFrom(sortedTable, false,
                        () -> sortedTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * 4")),
                partialEvalNuggetFrom(sortedTable, false,
                        () -> sortedTable.update("newCol=intCol / 2", "newCol2=newCol_[i] * newCol")),
                partialEvalNuggetFrom(sortedTable, false,
                        () -> sortedTable.update("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                                "repeatedCol=newCol_[i] * repeatedCol")),
                partialEvalNuggetFrom(sortedTable, false,
                        () -> sortedTable.update("newCol2=intCol / 2", "newCol=newCol2_[i] + 7")),
                partialEvalNuggetFrom(sortedTable, false, () -> sortedTable.update("newCol=intCol_[i]")),
                partialEvalNuggetFrom(queryTable, true, () -> queryTable.select("newCol=intCol_[i]")),
                partialEvalNuggetFrom(queryTable, true,
                        () -> queryTable.select("newCol=intCol / 2", "newCol2=newCol_[i] * 4")),
                partialEvalNuggetFrom(queryTable, true,
                        () -> queryTable.select("newCol=intCol / 2", "newCol2=newCol_[i] * newCol")),
                partialEvalNuggetFrom(queryTable, true,
                        () -> queryTable.select("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                                "repeatedCol=newCol_[i] * repeatedCol")),
                partialEvalNuggetFrom(queryTable, true,
                        () -> queryTable.select("newCol2=intCol / 2", "newCol=newCol2_[i] + 7")),
                partialEvalNuggetFrom(sortedTable, true, () -> sortedTable.select("newCol=intCol_[i]")),
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
                        () -> queryTable.update("newCol2=intCol / 2", "newCol=newCol2", "newCol=newCol_[i] + 7")),
                partialEvalNuggetFrom(queryTable, false,
                        () -> queryTable.update("newCol=intCol / 2", "newCol=newCol_[i] + 7")),
                new UpdateValidatorNugget(queryTable.select("newCol=intCol / 2", "newCol=newCol_[i] + 7")),
                // Let's change the type of a column.
                EvalNugget.from(() -> queryTable.select("intCol = intCol/2")),
                EvalNugget.from(() -> queryTable.update("newCol = `` + intCol/2")),
                EvalNugget.from(() -> queryTable.update("newCol = intCol > 50")),
                // Let's create an Instant and use it as an override
                partialEvalNuggetFrom(queryTable, false,
                        () -> queryTable.update("Time = DateTimeUtils.epochNanosToInstant(0) + intCol * MINUTE")
                                .update("Diff = Time_[i]")),
                partialEvalNuggetFrom(queryTable, true,
                        () -> queryTable.select("Time = DateTimeUtils.epochNanosToInstant(0) + intCol * MINUTE")
                                .select("Time", "Diff = Time_[i]")),
        };

        final int maxSteps = numSteps.get();
        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + numSteps.get());
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testUpdateIncrementalRandomized() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 2, 1);
        final boolean old = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        try {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

            for (int seed = 0; seed < 5; ++seed) {
                System.out.println("Seed: " + seed);
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testUpdateIncrementalRandomized(seed, false, 25);
                }
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testUpdateIncrementalRandomized(seed, true, 25);
                }
            }
        } finally {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = old;
        }
    }

    @Test
    public void testUpdateIncrementalRandomizedLarge() {
        // this test has large enough size that we will have individual column updates spread across threads
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 2, 1);
        final boolean old = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        try {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

            for (int seed = 0; seed < 1; ++seed) {
                System.out.println("Seed: " + seed);
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testUpdateIncrementalRandomized(seed, false, 4096);
                }
            }
        } finally {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = old;
        }
    }

    private void testUpdateIncrementalRandomized(int seed, boolean useRedirection, int size) {
        final boolean startSelect = QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT;
        final boolean startUpdate = QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE;

        try {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = useRedirection;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = useRedirection;
            testUpdateIncrementalRandomized(seed, new MutableInt(100), size);
        } finally {
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_SELECT = startSelect;
            QueryTable.USE_REDIRECTED_COLUMNS_FOR_UPDATE = startUpdate;
        }
    }

    private void testUpdateIncrementalRandomized(final int seed, MutableInt numSteps, int size) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.select("intCol=intCol * 2", "multiplication=intCol * doubleCol",
                        "intQuad=intCol * 2", "Sym", "newCol=7", "FC=Sym.charAt(0)", "UC=Sym.toUpperCase()",
                        "Concat=UC + FC")),
        };

        final int maxSteps = numSteps.get();
        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + numSteps.get());
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testUpdateIncrementalWithI() {
        Random random = new Random(0);
        ColumnInfo<?, ?>[] columnInfo;
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        EvalNuggetInterface[] en = new TableComparator[] {
                new TableComparator(queryTable.update("CI=i"), queryTable.update("CI=i")),
                new TableComparator(queryTable.update("CK=k"), queryTable.update("CK=k")),
                new TableComparator(queryTable.update("CI=i", "CK=k"), queryTable.update("CI=i").update("CK=k")),
                new TableComparator(queryTable.update("CI=i", "PI=CI_[i-1]"),
                        queryTable.update("CI=i").update("PI=CI_[i-1]")),

        };

        for (int i = 0; i < 50; i++) {
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }

    }

    @Test
    public void testUpdateEmptyTable() {
        QueryTable table = TstUtils.testRefreshingTable(i().toTracking());
        QueryTable table2 = (QueryTable) table.update("x = i*3", "y = \"\" + k");
        ListenerWithGlobals listener = base.newListenerWithGlobals(table2);
        table2.addUpdateListener(listener);

        TestCase.assertEquals(0, table.size());
        TestCase.assertEquals(0, table2.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            show(table2);
            addToTable(table, i(7, 9));
            table.notifyListeners(i(7, 9), i(), i());
        });


        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        assertArrayEquals(new int[] {0, 3}, ColumnVectors.ofInt(table2, "x").toArray());
        assertArrayEquals(new String[] {"7", "9"}, ColumnVectors.ofObject(table2, "y", String.class).toArray());
        TestCase.assertEquals(base.added, i(7, 9));
        TestCase.assertEquals(base.removed, i());
        TestCase.assertEquals(base.modified, i());

        updateGraph.runWithinUnitTestCycle(() -> {
        });
    }

    @Test
    public void testUpdateIndex() {
        QueryTable table = TstUtils.testRefreshingTable(i().toTracking());
        QueryTable table2 = (QueryTable) table.update("Position=i", "Key=\"\" + k");
        ListenerWithGlobals listener = base.newListenerWithGlobals(table2);
        table2.addUpdateListener(listener);

        TestCase.assertEquals(0, table.size());
        TestCase.assertEquals(0, table2.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            show(table2);
            addToTable(table, i(7, 9));
            table.notifyListeners(i(7, 9), i(), i());
        });


        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(table2, "Position").toArray());
        assertArrayEquals(new String[] {"7", "9"}, ColumnVectors.ofObject(table2, "Key", String.class).toArray());
        TestCase.assertEquals(base.added, i(7, 9));
        TestCase.assertEquals(base.removed, i());
        TestCase.assertEquals(base.modified, i());

        updateGraph.runWithinUnitTestCycle(() -> table.notifyListeners(i(), i(), i(9)));

        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(table2, "Position").toArray());
        assertArrayEquals(new String[] {"7", "9"}, ColumnVectors.ofObject(table2, "Key", String.class).toArray());
        TestCase.assertEquals(base.added, i());
        TestCase.assertEquals(base.removed, i());
        TestCase.assertEquals(base.modified, i(9));
    }

    @Test
    public void testUpdateArrayColumns() {
        QueryTable table = TstUtils.testRefreshingTable(i().toTracking());
        QueryTable table2 = (QueryTable) table.update("Position=i", "PrevI=Position_[i-1]");
        // QueryTable table2 = (QueryTable) table.update("Position=i", "Key=\"\" + k", "PrevI=Position_[i-1]");
        ListenerWithGlobals listener = base.newListenerWithGlobals(table2);
        table2.addUpdateListener(listener);

        TestCase.assertEquals(0, table.size());
        TestCase.assertEquals(0, table2.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            show(table2);
            addToTable(table, i(7, 9));
            table.notifyListeners(i(7, 9), i(), i());
        });


        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(table2, "Position").toArray());
        // assertArrayEquals(new String[] {"7", "9"}, ColumnVectors.ofObject(table2, "Key", String.class).toArray());
        assertArrayEquals(new int[] {NULL_INT, 0}, ColumnVectors.ofInt(table2, "PrevI").toArray());
        TestCase.assertEquals(i(7, 9), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(), base.modified);

        updateGraph.runWithinUnitTestCycle(() -> table.notifyListeners(i(), i(), i(9)));

        TestCase.assertEquals(2, table.size());
        TestCase.assertEquals(2, table2.size());
        show(table2);
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(table2, "Position").toArray());
        // assertArrayEquals(new String[] {"7", "9"}, ColumnVectors.ofObject(table2, "Key", String.class).toArray());
        assertArrayEquals(new int[] {NULL_INT, 0}, ColumnVectors.ofInt(table2, "PrevI").toArray());

        // note this modification is not reported to table2 since `update` is smart enough to notice that no columns
        // are actually modified in the result table
        TestCase.assertEquals(i(7, 9), base.added);
        TestCase.assertEquals(i(), base.removed);
        TestCase.assertEquals(i(), base.modified);
    }

    @Test
    public void testLargeStringConstants() {
        String a = new String(new char[32000]).replace("\0", "A");
        String b = new String(new char[32000]).replace("\0", "B");
        Table x = TableTools.emptyTable(1).update("C=`" + a + "` + `" + b + "`");
        TestCase.assertEquals(1, x.numColumns());

        a = new String(new char[40000]).replace("\0", "A");
        b = new String(new char[40000]).replace("\0", "B");
        x = TableTools.emptyTable(1)
                .update("C=String.join(\"" + a + "\", Integer.toString(new Random().nextInt()), \"" + b + "\")");
        TestCase.assertEquals(1, x.numColumns());
    }

    @Test
    public void testStaticAddressSpace() {
        final Table source = emptyTable(1000000).update("A=ii");
        final Table x = source.where("A%2==0");
        final Table xs = x.select();
        TableTools.showWithRowSet(xs);
        assertTableEquals(x, xs);
        // overhead would be greater than 10%, so we expect a flat table
        TestCase.assertTrue(xs.isFlat());
        TestCase.assertEquals(x.size() - 1, xs.getRowSet().lastRowKey());

        final Table x2 = source.where("A % 100 > 5");
        final Table x2s = x2.select();
        assertTableEquals(x2, x2s);
        TableTools.showWithRowSet(x2s);
        // overhead is only 5%, so we expect a pass-through RowSet
        TestCase.assertSame(x2s.getRowSet(), x2.getRowSet());

        // simulation of two partitions, we want to keep the RowSet
        final Table x3 = source.where("A < 250000 || A > 750000");
        final Table x3s = x3.select();
        assertTableEquals(x3, x3s);
        TestCase.assertSame(x3s.getRowSet(), x3.getRowSet());

        // simulation of selecting 80% of the data, we need to flatten
        final Table x4 = source.where("((int)(A / 1000) % 5 != 2)");
        final Table x4s = x4.select();
        assertTableEquals(x4, x4s);
        TestCase.assertEquals(x4s.getRowSet().lastRowKey(), x4.size() - 1);
        TestCase.assertTrue(x4s.isFlat());
    }

    @Test
    public void testSelectReuse() {
        final QueryTable table = TstUtils.testRefreshingTable(i(1, 1L << 20 + 1).toTracking(),
                longCol("Value", 1, 2));

        final Table selected = table.select();
        assertTableEquals(table, selected);
        final Table withUpdateView = selected.updateView("Value2=Value*2");
        final Table selected2 = withUpdateView.select();
        assertTableEquals(table, selected2.dropColumns("Value2"));

        TestCase.assertSame(selected.getColumnSource("Value"), selected2.getColumnSource("Value"));
        TestCase.assertNotSame(withUpdateView.getColumnSource("Value2"), selected2.getColumnSource("Value2"));
        TestCase.assertTrue(selected2.<Long>getColumnSource("Value2") instanceof LongSparseArraySource);

        assertTableEquals(prevTable(table), prevTable(selected));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), longCol("Value", 3));
            table.notifyListeners(i(2), i(), i());
        });

        assertTableEquals(table, selected);
        assertTableEquals(prevTable(table), prevTable(selected));

        TableTools.show(table);
        TableTools.show(selected);


        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(1L << 20 + 2), longCol("Value", 4));
            table.notifyListeners(i(1L << 20 + 2), i(), i());
        });

        TableTools.show(table);
        TableTools.show(selected);

        assertTableEquals(table, selected);
        assertTableEquals(prevTable(table), prevTable(selected));
    }

    @Test
    public void testIds5212() {
        final Table testtest = TableTools.emptyTable(554).view("Quantity=k").where("Quantity>200");
        final Table test = testtest.update("Quantity=100", "Test=Quantity");

        final int[] testArray = ColumnVectors.ofInt(test, "Test").toArray();
        final int[] expected = new int[test.intSize()];
        Arrays.fill(expected, 100);
        BaseArrayTestCase.assertEquals(expected, testArray);
    }

    /**
     * In IDS-5614 it was observed that a dynamic table that starts out empty won't do its formula initialization
     * (compilation, param grabbing etc) until later. This test confirms that this is fixed by setting a param to a
     * valid value, calling QueryTable.update() and then setting it to a string value that won't compile. If the
     * compilation/param grabbing is lazy, this test will fail. If you want to see the test fail under the old behavior,
     * you would need to undo the bugfix that prevent this from happening: 1. In DHFormulaColumn.generateClassBody(),
     * remove this 'if' guard and make the setting of params unconditional [adding this guard was the first, coarser bug
     * fix] if (params == null) { // remove this if params = QueryScope.getDefaultInstance().getParams(userParams); //
     * keep this line } 2. Conditionally invoke select.getDataView() (via SelectColumnLayer#getChunkSource()) only if it
     * is needed in SelectColumnLayer#applyUpdate.
     */
    @Test
    public void testEagerParamBinding() {
        QueryScope.addParam("scale", 12);
        final QueryTable table = TstUtils.testRefreshingTable(i().toTracking());
        final QueryTable table2 = (QueryTable) table.update("A = i * scale");
        QueryScope.addParam("scale", "Multiplying i by this string will not compile");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            show(table);
            addToTable(table, i(10, 20));
            table.notifyListeners(i(10, 20), i(), i());
        });
        TableTools.showWithRowSet(table2);
    }

    @Test
    public void testExpectedParamFailure() {
        try {
            QueryScope.addParam("scale2", 12);
            final QueryTable table = TstUtils.testRefreshingTable(i().toTracking());
            table.update("A = i * scale");
            TestCase.fail("Bad formula failed to throw exception");
        } catch (FormulaCompilationException fce) {
            // Expected
        }
    }

    @Test
    public void testIds5746() {
        final Table x = TableTools.emptyTable(2).update("result = `blah`", "L = result.length()");

        final int[] testArray = ColumnVectors.ofInt(x, "L").toArray();
        final int[] expected = new int[] {4, 4};
        BaseArrayTestCase.assertEquals(expected, testArray);
    }

    @Test
    public void testDoubleSelect() {
        final Table x = TableTools.emptyTable(100000).updateView("Q=ii").where("Q%2==0");
        final Table y = x.select("Q", "Q");
        final Table z = x.select("Q", "Q=Q");
        final Table w = x.select("Q=Q", "Q");

        TableTools.showWithRowSet(x);
        TableTools.showWithRowSet(y);
        TableTools.showWithRowSet(z);
        TableTools.showWithRowSet(w);

        assertTableEquals(x, y);
        assertTableEquals(x, z);
        assertTableEquals(x, w);
    }

    @Test
    public void testFlattenSelect() {
        final Table input = emptyTable(100000).updateView("A=ii", "B=ii % 1000", "C=ii % 2 == 0");
        final Table evens = input.where("C");
        assertTableEquals(evens, evens.flatten().select());
    }

    @Test
    public void testStaticSelectFlattenDateTimeCol() {
        final Table input = emptyTable(10).view("A=ii", "B = DateTimeUtils.now()").where("A % 2 == 0");
        final Table output = input.select("B");
        Assert.assertEquals(5, output.size());
        Assert.assertTrue(output.isFlat());
    }

    @Test
    public void testRedirectUpdate() {
        final Table input = emptyTable(1000000).updateView("A=ii", "B=ii % 1000", "C=ii % 2 == 0");
        final Table evens = input.where("C");
        final Table updated = evens.update("D=A+B");
        Assert.assertSame(updated.getRowSet(), evens.getRowSet());

        final Table fullUpdate = input.update("D=A+B");
        assertTableEquals(updated, fullUpdate.where("C"));

        final ColumnSource<?> fcs = fullUpdate.getColumnSource("D");
        Assert.assertTrue(fcs instanceof InMemoryColumnSource);

        final ColumnSource<?> cs = updated.getColumnSource("D");
        Assert.assertTrue(cs instanceof RedirectedColumnSource);

        Assert.assertEquals(4L, updated.getColumnSource("D").get(updated.getRowSet().get(1)));
        Assert.assertEquals(8L, updated.getColumnSource("D").getPrev(updated.getRowSet().prev().get(2)));
    }

    @Test
    public void testRegressionGH3562() {
        final Table src = TableTools.emptyTable(3).update("A = true", "B = ii != 1", "C = ii != 2");
        final Table result = src.select("And = and(A, B, C)", "Or = or(A, B, C)");

        final Table expected = TableTools.newTable(
                TableTools.col("And", true, false, false),
                TableTools.col("Or", true, true, true));

        assertTableEquals(expected, result);
    }

    @Test
    public void testStaticSelectPreserveAlreadyFlattenedColumns() {
        final Table source = emptyTable(10).updateView("I = ii").where("I % 2 == 0");
        final Table result = source.select("Foo = I", "Bar = Foo", "Baz = I");

        Assert.assertTrue(result.isFlat());

        final ColumnSource<?> foo = result.getColumnSource("Foo");
        final ColumnSource<?> bar = result.getColumnSource("Bar");
        final ColumnSource<?> baz = result.getColumnSource("Baz");
        result.getRowSet().forAllRowKeys(rowKey -> {
            Assert.assertEquals(rowKey * 2, foo.getLong(rowKey));
            Assert.assertEquals(rowKey * 2, bar.getLong(rowKey));
            Assert.assertEquals(rowKey * 2, baz.getLong(rowKey));
        });

        Assert.assertSame(foo, bar);
        Assert.assertSame(foo, baz);
    }

    @Test
    public void testStaticSelectPreserveColumn() {
        final Table source = emptyTable(10).select("I = ii").where("I % 2 == 0");
        final Table result = source.select("Foo = I", "Bar = Foo", "Baz = I");

        Assert.assertFalse(result.isFlat());

        final ColumnSource<?> orig = source.getColumnSource("I");
        final ColumnSource<?> foo = result.getColumnSource("Foo");
        final ColumnSource<?> bar = result.getColumnSource("Bar");
        final ColumnSource<?> baz = result.getColumnSource("Baz");
        result.getRowSet().forAllRowKeys(rowKey -> {
            Assert.assertEquals(rowKey, foo.getLong(rowKey));
            Assert.assertEquals(rowKey, bar.getLong(rowKey));
            Assert.assertEquals(rowKey, baz.getLong(rowKey));
        });

        // These columns were preserved and no flattening occurred.
        Assert.assertSame(orig, foo);
        Assert.assertSame(orig, bar);
        Assert.assertSame(orig, baz);
    }

    @Test
    public void testStaticSelectFlattenNotReusedWithRename() {
        final Table source = emptyTable(10).updateView("I = ii").where("I % 2 == 0");
        // we must use a vector column to prevent the inner column from being preserved
        final Table result = source.select(
                "Foo = I", "I = new io.deephaven.vector.LongVectorDirect(0L, 1L)", "Baz = I");

        Assert.assertTrue(result.isFlat());

        final ColumnSource<?> orig = source.getColumnSource("I");
        final ColumnSource<?> foo = result.getColumnSource("Foo");
        final ColumnSource<?> newI = result.getColumnSource("I");
        final ColumnSource<?> baz = result.getColumnSource("Baz");
        result.getRowSet().forAllRowKeys(rowKey -> {
            Assert.assertEquals(rowKey * 2, foo.getLong(rowKey));
            for (int ii = 0; ii < 2; ++ii) {
                Assert.assertEquals(ii, ((LongVector) newI.get(rowKey)).get(ii));
                Assert.assertEquals(ii, ((LongVector) baz.get(rowKey)).get(ii));
            }
        });

        Assert.assertNotSame(orig, foo); // this column was flattened
        Assert.assertNotSame(newI, baz); // vector columns cannot be preserved; so this should be a copy
    }

    @Test
    public void testStaticSelectRevertInternalFlatten() {
        // there is some special logic that prevents an internal flatten if it also needs to preserve an original column
        final Table source = emptyTable(10)
                .select("I = ii")
                .updateView("J = ii")
                .where("I % 2 == 0");

        // here `Foo` should be flattened, but `Bar` must be preserved; `Baz` is just for fun
        final Table result = source.select("Foo = J", "Bar = I", "Baz = Foo");

        Assert.assertFalse(result.isFlat());

        final ColumnSource<?> foo = result.getColumnSource("Foo");
        final ColumnSource<?> bar = result.getColumnSource("Bar");
        final ColumnSource<?> baz = result.getColumnSource("Baz");
        result.getRowSet().forAllRowKeys(rowKey -> {
            Assert.assertEquals(rowKey, foo.getLong(rowKey));
            Assert.assertEquals(rowKey, bar.getLong(rowKey));
            Assert.assertEquals(rowKey, baz.getLong(rowKey));
        });

        // Note that Foo is still being "selected" and therefore "brought into memory"
        Assert.assertNotSame(foo, source.getColumnSource("J"));
        Assert.assertSame(bar, source.getColumnSource("I"));
        Assert.assertSame(baz, foo);
    }

    @Test
    public void testAliasColumnSelectRefreshing() {
        final long size = 100;
        final AtomicInteger numCalls = new AtomicInteger();
        QueryScope.addParam("counter", numCalls);
        final QueryTable source = testRefreshingTable(RowSetFactory.flat(size).toTracking());
        final Table result = source.update("id = counter.getAndIncrement()")
                .select("id_a = id", "id_b = id");

        final ColumnSource<?> id_a = result.getColumnSource("id_a");
        final ColumnSource<?> id_b = result.getColumnSource("id_b");
        Assert.assertSame(id_a, id_b);
        Assert.assertEquals(numCalls.intValue(), size);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final WritableRowSet added = RowSetFactory.fromRange(size, size * 2 - 1);
            addToTable(source, added);
            source.notifyListeners(added, i(), i());
        });

        Assert.assertEquals(numCalls.intValue(), 2 * size);
    }

    @FunctionalInterface
    private interface TableOpInvoker {
        Table invoke(Table source, String... args);
    }

    @Test
    public void testPropagationOfAttributes() {
        final TableOpInvoker[] tableOps = new TableOpInvoker[] {
                TableOperations::select,
                TableOperations::update,
                TableOperations::view,
                TableOperations::updateView,
                TableOperations::lazyUpdate
        };

        // Add-only with no shift column; propagate
        final BaseTable<?> addonly = testRefreshingTable(RowSetFactory.empty().toTracking());
        addonly.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(addonly, "I = ii");
            Assert.assertTrue(result.isAddOnly());
        }

        // Add-only with positive shift column; don't propagate (generates modifies if adds between existing rows)
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(addonly, "I = ii", "J = I_[ii + 1]");
            Assert.assertFalse(result.isAddOnly());
        }

        // Add-only with negative shift column; don't propagate (generates modifies if adds between existing rows)
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(addonly, "I = ii", "J = I_[ii - 1]");
            Assert.assertFalse(result.isAddOnly());
        }

        // Append-only with no shift column; propagate
        final BaseTable<?> appendOnly = testRefreshingTable(RowSetFactory.empty().toTracking());
        appendOnly.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(appendOnly, "I = ii");
            Assert.assertTrue(result.isAppendOnly());
        }

        // Append-only with positive shift column; don't propagate (shift depends on future rows thus generates mods)
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(appendOnly, "I = ii", "J = I_[ii + 1]");
            Assert.assertFalse(result.isAppendOnly());
        }

        // Append-only with negative shift column; propagate (shift depends on rows that will never change)
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(appendOnly, "I = ii", "J = I_[ii - 1]");
            Assert.assertTrue(result.isAppendOnly());
        }

        // Blink with no shift column; propagate
        final BaseTable<?> blink = testRefreshingTable(RowSetFactory.empty().toTracking());
        blink.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, Boolean.TRUE);
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(blink, "I = ii");
            Assert.assertTrue(result.isBlink());
        }

        // Blink with positive shift column; propagate (no rows are saved across cycles)
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(blink, "I = ii", "J = I_[ii + 1]");
            Assert.assertTrue(result.isBlink());
        }

        // Blink with negative shift column; propagate (no rows are saved across cycles)
        for (TableOpInvoker op : tableOps) {
            final BaseTable<?> result = (BaseTable<?>) op.invoke(blink, "I = ii", "J = I_[ii - 1]");
            Assert.assertTrue(result.isBlink());
        }
    }

    @Test
    public void testFilterExpression() {
        final Filter filter = FilterIn.of(ColumnName.of("A"), Literal.of(1), Literal.of(3));
        final Table t = TableTools.newTable(intCol("A", 1, 1, 2, 3, 5, 8));
        final Table wm = t.wouldMatch(new WouldMatchPair("AWM", filter));

        // use an update
        final Table up = t.update(List.of(Selectable.of(ColumnName.of("AWM"), filter)));
        assertTableEquals(wm, up);

        // use an updateView
        final Table upv = t.updateView(List.of(Selectable.of(ColumnName.of("AWM"), filter)));
        assertTableEquals(wm, upv);

        // Test the getBoolean method
        assertEquals(true, upv.getColumnSource("AWM").get(t.getRowSet().get(0)));
        assertEquals(false, upv.getColumnSource("AWM").get(t.getRowSet().get(2)));

        // and now a more generic WhereFilter

        final Filter filter2 = WhereFilterFactory.getExpression("A == 1 || A==3");
        final Table wm2 = t.wouldMatch(new WouldMatchPair("AWM", filter2));

        // use an update
        final Table up2 = t.update(List.of(Selectable.of(ColumnName.of("AWM"), filter2)));
        assertTableEquals(wm2, up2);

        // a Filter where nothing is true, to check that state
        final Table upvf = t.updateView(List.of(Selectable.of(ColumnName.of("AWM"), Filter.ofFalse())));
        assertTableEquals(t.updateView("AWM=false"), upvf);

        // a Filter where everything is true
        final Table upvt = t.updateView(List.of(Selectable.of(ColumnName.of("AWM"), Filter.ofTrue())));
        assertTableEquals(t.updateView("AWM=true"), upvt);

        // a Filter where the last value in the chunk is true
        final Filter filter3 = WhereFilterFactory.getExpression("A in 8");
        final Table wm3 = t.wouldMatch(new WouldMatchPair("AWM", filter3));
        final Table upv3 = t.updateView(List.of(Selectable.of(ColumnName.of("AWM"), filter3)));
        assertTableEquals(wm3, upv3);
    }

    @Test
    public void testFilterExpressionGetPrev() {
        final Filter filter = FilterIn.of(ColumnName.of("A"), Literal.of(2), Literal.of(4));
        final QueryTable t = TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(), intCol("A", 1, 2, 3, 4));
        // noinspection resource
        final TrackingWritableRowSet rs = t.getRowSet().writableCast();

        // use an updateView
        final Table upv = t.updateView(List.of(Selectable.of(ColumnName.of("AWM"), filter)));

        // Test the getBoolean method
        ColumnSource<Object> resultColumn = upv.getColumnSource("AWM");
        assertEquals(false, resultColumn.get(rs.get(0)));
        assertEquals(true, resultColumn.get(rs.get(1)));
        assertEquals(false, resultColumn.get(rs.get(2)));
        assertEquals(true, resultColumn.get(rs.get(3)));

        // and do it with chunks
        try (final CloseableIterator<Object> awm = upv.columnIterator("AWM")) {
            assertEquals(Arrays.asList(false, true, false, true), awm.stream().collect(Collectors.toList()));
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            assertEquals(false, resultColumn.get(t.getRowSet().get(0)));
            assertEquals(true, resultColumn.get(t.getRowSet().get(1)));
            assertEquals(false, resultColumn.get(t.getRowSet().get(2)));
            assertEquals(true, resultColumn.get(t.getRowSet().get(3)));

            final RowSet prevRowset = rs.prev();
            assertEquals(false, resultColumn.getPrev(prevRowset.get(0)));
            assertEquals(true, resultColumn.getPrev(prevRowset.get(1)));
            assertEquals(false, resultColumn.getPrev(prevRowset.get(2)));
            assertEquals(true, resultColumn.getPrev(prevRowset.get(3)));

            addToTable(t, i(1, 2, 9), intCol("A", 2, 2, 4));
            removeRows(t, i(8));
            rs.insert(i(1, 9));
            rs.remove(8);
            t.notifyListeners(i(1, 9), i(8), i());

            // with a chunk
            try (final ChunkSource.GetContext fc = resultColumn.makeGetContext(4)) {
                final ObjectChunk<Boolean, ? extends Values> prevValues =
                        resultColumn.getPrevChunk(fc, prevRowset).asObjectChunk();
                assertEquals(false, prevValues.get(0));
                assertEquals(true, prevValues.get(1));
                assertEquals(false, prevValues.get(2));
                assertEquals(true, prevValues.get(3));
            }

            assertEquals(false, resultColumn.getPrev(prevRowset.get(0)));
            assertEquals(true, resultColumn.getPrev(prevRowset.get(1)));
            assertEquals(false, resultColumn.getPrev(prevRowset.get(2)));
            assertEquals(true, resultColumn.getPrev(prevRowset.get(3)));

            assertEquals(true, resultColumn.get(rs.get(0)));
            assertEquals(true, resultColumn.get(rs.get(1)));
            assertEquals(true, resultColumn.get(rs.get(2)));
            assertEquals(false, resultColumn.get(rs.get(3)));
            assertEquals(true, resultColumn.get(rs.get(4)));
        });
    }

    @Test
    public void testFilterExpressionFillChunkPerformance() {
        testFilterExpressionFillChunkPerformance(1.0);
        testFilterExpressionFillChunkPerformance(.9999);
        testFilterExpressionFillChunkPerformance(.999);
        testFilterExpressionFillChunkPerformance(.8725);
        testFilterExpressionFillChunkPerformance(.75);
        testFilterExpressionFillChunkPerformance(.5);
        testFilterExpressionFillChunkPerformance(.25);
        testFilterExpressionFillChunkPerformance(.125);
        testFilterExpressionFillChunkPerformance(0.001);
        testFilterExpressionFillChunkPerformance(0.0001);
    }

    public void testFilterExpressionFillChunkPerformance(final double density) {
        final int numIterations = 1;
        final int size = 100_000;
        final Filter filter = FilterIn.of(ColumnName.of("A"), Literal.of(1));

        final Random random = new Random(20241120);
        final List<Boolean> values = IntStream.range(0, size).mapToObj(ignored -> random.nextDouble() < density)
                .collect(Collectors.toList());
        QueryScope.addParam("values", values);
        final Table t = TableTools.emptyTable(size).update("A=(Boolean)(values[i]) ? 1: 0");
        QueryScope.addParam("values", null);

        final Table upv = t.updateView(List.of(Selectable.of(ColumnName.of("AWM"), filter)));
        final long startTime = System.nanoTime();
        for (int iters = 0; iters < numIterations; ++iters) {
            final long trueValues = upv.columnIterator("AWM").stream().filter(x -> (Boolean) x).count();
            assertEquals(values.stream().filter(x -> x).count(), trueValues);
        }
        final long endTime = System.nanoTime();
        final double duration = endTime - startTime;
        System.out.println("Density: " + new DecimalFormat("0.0000").format(density) + ", Nanos: " + (long) duration
                + ", per cell=" + new DecimalFormat("0.00").format(duration / (size * numIterations)));
    }

    @Test
    public void testFilterExpressionArray() {
        final Filter filter = WhereFilterFactory.getExpression("A=A_[i-1]");
        final Filter filterArrayOnly = WhereFilterFactory.getExpression("A=A_.size() = 1");
        final Filter filterKonly = WhereFilterFactory.getExpression("A=k+1");
        final QueryTable setTable = TstUtils.testRefreshingTable(intCol("B"));
        final Filter whereIn = new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpression("A=B"));
        final QueryTable table = TstUtils.testRefreshingTable(intCol("A", 1, 1, 2, 3, 5, 8, 9, 9));

        final UncheckedTableException wme = Assert.assertThrows(UncheckedTableException.class,
                () -> table.wouldMatch(new WouldMatchPair("AWM", filter)));
        Assert.assertEquals("wouldMatch filters cannot use virtual row variables (i, ii, and k): [A=A_[i-1]]",
                wme.getMessage());

        final UncheckedTableException wme2 = Assert.assertThrows(UncheckedTableException.class,
                () -> table.wouldMatch(new WouldMatchPair("AWM", filterArrayOnly)));
        Assert.assertEquals("wouldMatch filters cannot use column Vectors (_ syntax): [A=A_.size() = 1]",
                wme2.getMessage());

        final UncheckedTableException upe = Assert.assertThrows(UncheckedTableException.class,
                () -> table.update(List.of(Selectable.of(ColumnName.of("AWM"), filter))));
        Assert.assertEquals(
                "Cannot use a filter with column Vectors (_ syntax) in select, view, update, or updateView: A=A_[i-1]",
                upe.getMessage());

        final UncheckedTableException uve = Assert.assertThrows(UncheckedTableException.class,
                () -> table.updateView(List.of(Selectable.of(ColumnName.of("AWM"), filter))));
        Assert.assertEquals(
                "Cannot use a filter with column Vectors (_ syntax) in select, view, update, or updateView: A=A_[i-1]",
                uve.getMessage());

        final UncheckedTableException se = Assert.assertThrows(UncheckedTableException.class,
                () -> table.select(List.of(Selectable.of(ColumnName.of("AWM"), filterKonly))));
        Assert.assertEquals(
                "Cannot use a filter with virtual row variables (i, ii, or k) in select, view, update, or updateView: A=k+1",
                se.getMessage());

        final UncheckedTableException ve = Assert.assertThrows(UncheckedTableException.class,
                () -> table.view(List.of(Selectable.of(ColumnName.of("AWM"), filterKonly))));
        Assert.assertEquals(
                "Cannot use a filter with virtual row variables (i, ii, or k) in select, view, update, or updateView: A=k+1",
                ve.getMessage());

        final UncheckedTableException dw = Assert.assertThrows(UncheckedTableException.class,
                () -> table.view(List.of(Selectable.of(ColumnName.of("AWM"), whereIn))));
        Assert.assertEquals(
                "Cannot use a refreshing filter in select, view, update, or updateView: DynamicWhereFilter([A=B])",
                dw.getMessage());

    }

    @Test
    public void testFilterExpressionTicking() {
        for (int seed = 0; seed < 5; ++seed) {
            testFilterExpressionTicking(seed, new MutableInt(100));
        }
    }

    private void testFilterExpressionTicking(final int seed, final MutableInt numSteps) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final int size = 25;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(queryTable.wouldMatch("SM=Sym in `b`, `d`"),
                        queryTable.update(List.of(Selectable.of(ColumnName.of("SM"),
                                WhereFilterFactory.getExpression("Sym in `b`, `d`"))))),
                new TableComparator(queryTable.wouldMatch("SM=Sym in `b`, `d`"),
                        queryTable.updateView(List.of(Selectable.of(ColumnName.of("SM"),
                                WhereFilterFactory.getExpression("Sym in `b`, `d`"))))),
                new TableComparator(queryTable.wouldMatch("IM=intCol < 50"),
                        queryTable.update(List.of(
                                Selectable.of(ColumnName.of("IM"), WhereFilterFactory.getExpression("intCol < 50"))))),
                new TableComparator(queryTable.wouldMatch("IM=intCol < 50"),
                        queryTable.updateView(List.of(
                                Selectable.of(ColumnName.of("IM"), WhereFilterFactory.getExpression("intCol < 50"))))),
                new TableComparator(queryTable.wouldMatch("IM=Sym= (intCol%2 == 0? `a` : `b`)"),
                        queryTable.update(List.of(Selectable.of(ColumnName.of("IM"),
                                WhereFilterFactory.getExpression("Sym= (intCol%2 == 0? `a` : `b`)"))))),
                new TableComparator(queryTable.wouldMatch("IM=Sym= (intCol%2 == 0? `a` : `b`)"),
                        queryTable.updateView(List.of(Selectable.of(ColumnName.of("IM"),
                                WhereFilterFactory.getExpression("Sym= (intCol%2 == 0? `a` : `b`)"))))),
        };

        final int maxSteps = numSteps.get();
        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + numSteps.get());
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testAlwaysUpdate() {
        final MutableInt count = new MutableInt(0);
        final MutableInt count2 = new MutableInt(0);
        QueryScope.addParam("__COUNT1", count);
        QueryScope.addParam("__COUNT2", count2);
        final QueryTable base = testRefreshingTable(intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9),
                stringCol("Thing", "A", "B", "C", "D", "E", "F", "G", "H", "I"));

        final SelectColumn sc1 = SelectColumnFactory.getExpression("NormalCount=__COUNT1.getAndIncrement()");
        final SelectColumn sc2 =
                SelectColumnFactory.getExpression("AlwaysCount=__COUNT2.getAndIncrement()")
                        .withRecomputeOnModifiedRow();

        final Table withUpdates = base.update(Arrays.asList(sc1, sc2));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8},
                ColumnVectors.ofInt(withUpdates, "NormalCount").toArray());
        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8},
                ColumnVectors.ofInt(withUpdates, "AlwaysCount").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(9), intCol("Sentinel", 10), stringCol("Thing", "J"));
            base.notifyListeners(i(9), i(), i());
        });

        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                ColumnVectors.ofInt(withUpdates, "NormalCount").toArray());
        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                ColumnVectors.ofInt(withUpdates, "AlwaysCount").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(0, 2, 4), intCol("Sentinel", 1, 3, 5), stringCol("Thing", "a", "c", "e"));
            base.notifyListeners(i(), i(), i(0, 2, 4));
        });

        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                ColumnVectors.ofInt(withUpdates, "NormalCount").toArray());
        assertArrayEquals(new int[] {10, 1, 11, 3, 12, 5, 6, 7, 8, 9},
                ColumnVectors.ofInt(withUpdates, "AlwaysCount").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(base, i(5));
            base.notifyListeners(i(), i(5), i());
        });

        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 6, 7, 8, 9},
                ColumnVectors.ofInt(withUpdates, "NormalCount").toArray());
        assertArrayEquals(new int[] {10, 1, 11, 3, 12, 6, 7, 8, 9},
                ColumnVectors.ofInt(withUpdates, "AlwaysCount").toArray());
    }

    @Test
    public void testAlwaysUpdateMCS() {
        final AtomicInteger a = new AtomicInteger(100);
        QueryScope.addParam("a", a);
        final AtomicInteger b = new AtomicInteger(200);
        QueryScope.addParam("b", b);
        final QueryTable base = testRefreshingTable(RowSetFactory.fromKeys(10, 11).toTracking(),
                intCol("Sentinel", 1, 2), intCol("B", 10, 11));

        final SelectColumn x =
                SelectColumnFactory.getExpression("X = a.getAndIncrement()").withRecomputeOnModifiedRow();

        final Table withUpdates = base.update(Arrays.asList(x, SelectColumnFactory.getExpression("Y=1"),
                SelectColumnFactory.getExpression("Z=B+b.getAndIncrement()")));

        final PrintListener pl = new PrintListener("withUpdates", withUpdates, 10);

        final SimpleListener simpleListener = new SimpleListener(withUpdates);
        withUpdates.addUpdateListener(simpleListener);

        assertTableEquals(TableTools.newTable(intCol("Sentinel", 1, 2), intCol("B", 10, 11), intCol("X", 100, 101),
                intCol("Y", 1, 1), intCol("Z", 210, 212)), withUpdates);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(10), intCol("Sentinel", 3), intCol("B", 10));
            base.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(10), RowSetShiftData.EMPTY, base.newModifiedColumnSet("Sentinel")));
        });

        assertTableEquals(TableTools.newTable(intCol("Sentinel", 3, 2), intCol("B", 10, 11), intCol("X", 102, 101),
                intCol("Y", 1, 1), intCol("Z", 210, 212)), withUpdates);

        assertEquals(1, simpleListener.count);
        assertEquals(i(), simpleListener.update.added());
        assertEquals(i(), simpleListener.update.removed());
        assertEquals(i(10), simpleListener.update.modified());
        assertEquals(((QueryTable) withUpdates).newModifiedColumnSet("Sentinel", "X"),
                simpleListener.update.modifiedColumnSet());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(11), intCol("Sentinel", 4), intCol("B", 12));
            base.notifyListeners(new TableUpdateImpl(i(), i(), i(11), RowSetShiftData.EMPTY,
                    base.newModifiedColumnSet("Sentinel", "B")));
        });

        assertTableEquals(TableTools.newTable(intCol("Sentinel", 3, 4), intCol("B", 10, 12), intCol("X", 102, 103),
                intCol("Y", 1, 1), intCol("Z", 210, 214)), withUpdates);

        assertEquals(2, simpleListener.count);
        assertEquals(i(), simpleListener.update.added());
        assertEquals(i(), simpleListener.update.removed());
        assertEquals(i(11), simpleListener.update.modified());
        assertEquals(((QueryTable) withUpdates).newModifiedColumnSet("Sentinel", "B", "X", "Z"),
                simpleListener.update.modifiedColumnSet());

        QueryScope.addParam("a", null);
        QueryScope.addParam("b", null);
    }
}
