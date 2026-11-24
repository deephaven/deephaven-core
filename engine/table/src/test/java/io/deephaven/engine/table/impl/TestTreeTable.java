//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.by.AggregationOperatorException;
import io.deephaven.engine.table.impl.hierarchical.TreeTableFilter;
import io.deephaven.engine.table.impl.hierarchical.TreeTableImpl;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.UniqueIntGenerator;
import io.deephaven.engine.testutil.sources.IntTestSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.function.ThrowingRunnable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.deephaven.engine.testutil.HierarchicalTableTestTools.freeSnapshotTableChunks;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.snapshotToTable;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class TestTreeTable extends RefreshingTableTestCase {
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testRebase() {
        final Table source1 = TableTools.newTable(intCol("ID", 1, 2, 3, 4, 5),
                intCol("Parent", NULL_INT, 7, NULL_INT, 1, 1),
                intCol("Sentinel", 101, 102, 103, 104, 105));

        final TreeTable tree1a = source1.tree("ID", "Parent");
        final TreeTable.NodeOperationsRecorder recorder =
                tree1a.makeNodeOperationsRecorder().sortDescending("Sentinel");
        final TreeTable tree = tree1a.withNodeOperations(recorder);

        final Table keyTable = newTable(
                intCol(tree.getRowDepthColumn().name(), 0),
                intCol("ID", 1),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = tree.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(tree, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);
        assertTableEquals(
                TableTools.newTable(intCol("ID", 3, 1, 5, 4), intCol("Parent", NULL_INT, NULL_INT, 1, 1),
                        intCol("Sentinel", 103, 101, 105, 104)),
                snapshot.view("ID", "Parent", "Sentinel"));
        freeSnapshotTableChunks(snapshot);

        final Table source2 = TreeTable.promoteOrphans(source1, "ID", "Parent");

        final TreeTable withAttributes = tree.withAttributes(Collections.singletonMap("Dog", "BestFriend"));

        final TreeTable rebased = withAttributes.rebase(source2);

        final HierarchicalTable.SnapshotState ss2 = rebased.makeSnapshotState();
        final Table snapshot2 =
                snapshotToTable(rebased, ss2, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot2);
        assertTableEquals(
                TableTools.newTable(intCol("ID", 3, 2, 1, 5, 4), intCol("Parent", NULL_INT, NULL_INT, NULL_INT, 1, 1),
                        intCol("Sentinel", 103, 102, 101, 105, 104)),
                snapshot2.view("ID", "Parent", "Sentinel"));
        freeSnapshotTableChunks(snapshot2);

        assertEquals(rebased.getAttribute("Dog"), "BestFriend");
    }

    @Test
    public void testFilterRejectsCrossRowDependencies() {
        // a node-level filter reads the rows of one node, and the source-level filter refilters only the rows that the
        // source adds or modifies, so a tree filter may not depend on other rows
        final Table source = TableTools.newTable(intCol("ID", 1, 2, 3, 4),
                intCol("Parent", NULL_INT, 1, 1, 2));
        final TreeTable tree = source.tree("ID", "Parent");
        for (final TreeTable filterable : List.of(tree, tree.withNodeFilterColumns(List.of(ColumnName.of("ID"))))) {
            for (final String formula : new String[] {
                    "ii == 3", "i + 0 > 1", "k + 0 > 1", "ID_[0] == 1", "ID_[i - 1] == 1"}) {
                final IllegalArgumentException ex = Assert.assertThrows(formula, IllegalArgumentException.class,
                        () -> filterable.withFilter(WhereFilterFactory.getExpression(formula)));
                Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(
                        "cannot use cross-row dependencies (virtual row variables or column arrays)"));
            }
        }
        // a filter that decides each row alone keeps the matching rows and their ancestors
        assertTableEquals(source.where("ID in 1, 2, 4"),
                tree.withFilter(WhereFilterFactory.getExpression("ID == 4")).getSource());
    }

    @Test
    public void testRebaseBadDef() {
        final Table source1 = TableTools.newTable(intCol("ID", 1, 2, 3, 4, 5),
                intCol("Parent", NULL_INT, 7, NULL_INT, 1, 1),
                intCol("Sentinel", 101, 102, 103, 104, 105));

        final TreeTable tree1a = source1.tree("ID", "Parent");
        final TreeTable.NodeOperationsRecorder recorder =
                tree1a.makeNodeOperationsRecorder().sortDescending("Sentinel");
        final TreeTable tree = tree1a.withNodeOperations(recorder);

        final Table source2 = source1.view("Parent", "ID", "Sentinel");
        final IllegalArgumentException iae =
                Assert.assertThrows(IllegalArgumentException.class, () -> tree.rebase(source2));
        assertEquals("Cannot rebase a TreeTable with a new source definition, column order is not identical",
                iae.getMessage());

        final Table source3 = source1.updateView("Extra=1");
        final IllegalArgumentException iae2 =
                Assert.assertThrows(IllegalArgumentException.class, () -> tree.rebase(source3));
        assertEquals(
                "Cannot rebase a TreeTable with a new source definition: new source column 'Extra' is missing in existing source",
                iae2.getMessage());
    }

    @Test
    public void testMismatchedParentAndId() {
        final Table source =
                emptyTable(10).update("ID=ii", "Parent=ii == 0 ? null : 63 - Long.numberOfLeadingZeros(ii)");

        final NoSuchColumnException missingParent =
                Assert.assertThrows(NoSuchColumnException.class, () -> source.tree("ID", "FooBar"));
        assertEquals("tree parent column: Unknown column names [FooBar], available column names are [ID, Parent]",
                missingParent.getMessage());
        final NoSuchColumnException missingId =
                Assert.assertThrows(NoSuchColumnException.class, () -> source.tree("FooBar", "Parent"));
        assertEquals("tree identifier column: Unknown column names [FooBar], available column names are [ID, Parent]",
                missingId.getMessage());

        final InvalidColumnException ice =
                Assert.assertThrows(InvalidColumnException.class, () -> source.tree("ID", "Parent"));
        assertEquals(
                "tree parent and identifier columns must have the same data type, but parent is [Parent, int] and identifier is [ID, long]",
                ice.getMessage());
    }

    @Test
    public void testDH21297() {
        final IntTestSource sentinel = new IntTestSource();
        final IntTestSource parent = new IntTestSource();

        final QueryTable source = new QueryTable(i().toTracking(),
                Map.of("Sentinel", sentinel, "Parent", parent));
        source.setRefreshing(true);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source,
                    i(0, 10, 11, 12),
                    col("Sentinel", 0, 1, 2, 3),
                    col("Parent", NULL_INT, 0, 0, 0));
            source.notifyListeners(i(0, 10, 11, 12), i(), i());
        });

        final TreeTable treed = source.tree("Sentinel", "Parent");

        final Table filteredSource = treed.getSource().apply(new TreeTableFilter.Operator((TreeTableImpl) treed,
                WhereFilterFactory.getExpressions("Sentinel in 1, 2, 3, 4, 5, 6")));

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 1, 2, 3}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());

        System.out.println("\n\nAdding keys 4,5");

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source,
                    i(1, 21),
                    col("Sentinel", 4, 5),
                    col("Parent", NULL_INT, 4));
            source.notifyListeners(i(1, 21), i(), i());
        });

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 4, 1, 2, 3, 5}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());

        System.out.println("\n\nShifting keys 0-1,+4");

        // Shift some parent rows
        updateGraph.runWithinUnitTestCycle(() -> {
            // shift the values
            parent.shift(0, 1, 4);
            sentinel.shift(0, 1, 4);

            // create the shift update
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 1, 4);
            final RowSetShiftData shiftData = builder.build();

            shiftData.apply(source.getRowSet().writableCast());
            final TableUpdate update = new TableUpdateImpl(i(), i(), i(), shiftData, ModifiedColumnSet.EMPTY);
            source.notifyListeners(update);
        });

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 4, 1, 2, 3, 5}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());

        System.out.println("\n\nShifting keys 4-5,-4 and 10-21,+1");

        // Shift some parent rows
        updateGraph.runWithinUnitTestCycle(() -> {
            // shift the values
            parent.shift(4, 5, -4);
            sentinel.shift(4, 5, -4);
            parent.shift(10, 21, 1);
            sentinel.shift(10, 21, 1);

            // create the shift update
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(4, 5, -4);
            builder.shiftRange(10, 21, 1);
            final RowSetShiftData shiftData = builder.build();

            shiftData.apply(source.getRowSet().writableCast());
            final TableUpdate update = new TableUpdateImpl(i(), i(), i(), shiftData, ModifiedColumnSet.EMPTY);
            source.notifyListeners(update);
        });

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 4, 1, 2, 3, 5}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());
    }

    @Test
    public void testFilterRejectsParentCycle() throws Exception {
        runWithTimeout(() -> {
            // row 1 is a root; rows 3 and 4 name each other as parents, and row 2 hangs below that cycle
            final Table source = TableTools.newTable(intCol("ID", 1, 2, 3, 4),
                    intCol("Parent", NULL_INT, 3, 4, 3));
            final TreeTable tree = source.tree("ID", "Parent");

            // a match whose ancestors never reach the cycle is unaffected
            assertTableEquals(source.where("ID = 1"),
                    tree.withFilter(WhereFilterFactory.getExpression("ID = 1")).getSource());

            for (final String formula : new String[] {"ID = 2", "ID = 3", "ID in 1, 4"}) {
                assertParentCycle(formula,
                        () -> tree.withFilter(WhereFilterFactory.getExpression(formula)));
            }
        });
    }

    @Test
    public void testFilterRejectsSelfParent() throws Exception {
        runWithTimeout(() -> {
            final Table source = TableTools.newTable(intCol("ID", 1, 2, 3),
                    intCol("Parent", NULL_INT, 2, 2));
            final TreeTable tree = source.tree("ID", "Parent");
            for (final String formula : new String[] {"ID = 2", "ID = 3"}) {
                assertParentCycle(formula,
                        () -> tree.withFilter(WhereFilterFactory.getExpression(formula)));
            }
        });
    }

    @Test
    public void testFilterSharedAncestorsAtDifferentDepths() throws Exception {
        runWithTimeout(() -> {
            // 1 <- 2 <- 3 <- 4, 1 <- 5, 2 <- 6, and 7 <- 8 where 7's parent does not exist; 9 and 10 are neither
            // matches nor ancestors of a match
            final Table source = TableTools.newTable(intCol("ID", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                    intCol("Parent", NULL_INT, 1, 2, 3, 1, 2, 99, 7, 1, 99));
            final TreeTable tree = source.tree("ID", "Parent");

            // the matches reach rows 1 and 2 from different depths, and row 2 is both a match and an ancestor
            assertTableEquals(source.where("ID in 1, 2, 3, 4, 5, 6, 7, 8"),
                    tree.withFilter(WhereFilterFactory.getExpression("ID in 2, 4, 5, 6, 8")).getSource());
        });
    }

    @Test
    public void testFilterIncremental() {
        for (int seed = 0; seed < 5; ++seed) {
            testFilterIncremental(seed);
        }
    }

    private void testFilterIncremental(final int seed) {
        final int size = 100;
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfos;
        final QueryTable source = getTable(size, random, columnInfos = initColumnInfos(new String[] {"ID", "Sentinel"},
                new UniqueIntGenerator(1, 10 * size),
                new IntGenerator(0, 1_000_000)));
        // each parent identifier is one of the five below the row's own identifier, so the parents never form a cycle,
        // and modifying Sentinel moves a row to a different parent
        final Table withParent = source.update("Parent = ID <= 1 ? NULL_INT : ID - 1 - Sentinel % Math.min(ID - 1, 5)");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> withParent.tree("ID", "Parent")
                        .withFilter(WhereFilterFactory.getExpression("Sentinel % 5 == 0")).getSource()),
                EvalNugget.from(() -> withParent.tree("ID", "Parent")
                        .withFilter(WhereFilterFactory.getExpression("ID % 7 == 3")).getSource()),
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < 50; ++step) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, source, columnInfos));
            validate("seed=" + seed + ", step=" + step, en);
        }
    }

    @Test
    public void testFilterAfterParentRemoved() {
        final QueryTable source = testRefreshingTable(i(10, 20).toTracking(),
                intCol("ID", 1, 2),
                intCol("Parent", NULL_INT, 1));
        final TreeTable tree = source.tree("ID", "Parent");
        final Table filtered = tree.withFilter(WhereFilterFactory.getExpression("ID = 2")).getSource();
        assertTableEquals(source, filtered);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(source, i(10));
            source.notifyListeners(i(), i(10), i());
        });
        assertTableEquals(source, filtered);

        // the matched row is filtered again, and its parent identifier no longer names a row
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(20), intCol("ID", 2), intCol("Parent", 1));
            source.notifyListeners(i(), i(), i(20));
        });
        assertTableEquals(source, filtered);
        assertFalse(filtered.isFailed());
    }

    @Test
    public void testFilterParentCycleOnModify() throws Exception {
        runWithTimeout(() -> {
            final QueryTable source = testRefreshingTable(i(10, 20, 30, 40).toTracking(),
                    intCol("ID", 1, 2, 3, 4),
                    intCol("Parent", NULL_INT, 1, 2, 3));
            final TreeTable tree = source.tree("ID", "Parent");
            final Table filtered = tree.withFilter(WhereFilterFactory.getExpression("ID = 4")).getSource();
            assertTableEquals(source, filtered);

            // the root becomes a child of its own descendant
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            allowingError(() -> updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(source, i(10), intCol("ID", 1), intCol("Parent", 4));
                source.notifyListeners(i(), i(), i(10));
            }), TestTreeTable::containsParentCycleError);
            assertTrue(filtered.isFailed());
        });
    }

    @Test
    public void testFilterParentCycleOnAdd() throws Exception {
        runWithTimeout(() -> {
            // the matched row 2 is an orphan until a row with its parent identifier arrives
            final QueryTable source = testRefreshingTable(i(10, 20).toTracking(),
                    intCol("ID", 1, 2),
                    intCol("Parent", NULL_INT, 5));
            final TreeTable tree = source.tree("ID", "Parent");
            final Table filtered = tree.withFilter(WhereFilterFactory.getExpression("ID = 2")).getSource();
            assertTableEquals(source.where("ID = 2"), filtered);

            // the new row's parent is the matched row, which closes a cycle
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            allowingError(() -> updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(source, i(30), intCol("ID", 5), intCol("Parent", 2));
                source.notifyListeners(i(30), i(), i());
            }), TestTreeTable::containsParentCycleError);
            assertTrue(filtered.isFailed());
        });
    }

    @Test
    public void testDuplicateIdentifiers() {
        final Table source = TableTools.newTable(intCol("ID", 1, 2, 2),
                intCol("Parent", NULL_INT, 1, 1));
        final AggregationOperatorException aoe =
                Assert.assertThrows(AggregationOperatorException.class, () -> source.tree("ID", "Parent"));
        Throwable rootCause = aoe;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        assertEquals("Adding invalid number (2) of \"unique\" row keys for destination 1", rootCause.getMessage());
    }

    private static void assertParentCycle(final String message, final ThrowingRunnable operation) {
        final IllegalStateException ise = Assert.assertThrows(message, IllegalStateException.class, operation);
        assertTrue(ise.getMessage(), ise.getMessage().contains("contain a cycle"));
    }

    private static boolean containsParentCycleError(final List<Throwable> errors) {
        return errors.stream().anyMatch(error -> error instanceof IllegalStateException
                && error.getMessage().contains("contain a cycle"));
    }

    /**
     * Runs {@code test} on a daemon thread with this test's execution context, failing if it does not finish within a
     * minute, so that a filter which never terminates fails the test rather than hanging the suite.
     */
    private static void runWithTimeout(final Runnable test) throws Exception {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "TestTreeTable-runWithTimeout");
            thread.setDaemon(true);
            return thread;
        });
        try {
            final Future<?> result = executor.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    test.run();
                }
            });
            try {
                result.get(1, TimeUnit.MINUTES);
            } catch (final ExecutionException ee) {
                if (ee.getCause() instanceof Error) {
                    throw (Error) ee.getCause();
                }
                throw (Exception) ee.getCause();
            } catch (final TimeoutException te) {
                Assert.fail("test did not finish within a minute");
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
