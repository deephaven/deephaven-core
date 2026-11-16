//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterImpl;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataIndexPushdownPreviousValuesTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final int SIZE = 100;
    private static final long TIMEOUT_SECONDS = 10;

    /**
     * A table that shares the row set of an indexed parent filters with the parent's index. A where on that table,
     * initialized off the update thread in a cycle that neither the table nor the index has reached, filters with
     * previous values, and must read the index's previous row sets even after the index updates the row sets in place.
     * <p>
     * A filter that blocks once inside the index pushdown holds the where while only the index's update runs.
     */
    @Test
    public void whereOnSharedRowSetChildReadsPreviousIndexRowSets() throws Exception {
        final int[] syms = IntStream.range(0, SIZE).map(ii -> ii % 2 == 0 ? 1 : 2).toArray();
        final QueryTable parent = testRefreshingTable(RowSetFactory.flat(SIZE).toTracking(), intCol("Sym", syms));
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(parent, "Sym");
        final QueryTable indexTable = (QueryTable) dataIndex.table();
        final QueryTable child = (QueryTable) parent.updateView("Other = Sym * 2");
        assertSame(parent.getRowSet(), child.getRowSet());
        assertTrue(DataIndexer.hasDataIndex(child, "Sym"));

        final RowSet previousMatches =
                RowSetFactory.fromKeys(LongStream.range(0, SIZE).filter(k -> k % 2 == 0).toArray());
        final WritableRowSet currentMatches = previousMatches.copy();
        currentMatches.remove(0);
        currentMatches.insert(1);

        final State state = new State();
        final BlockingFilter filter = new BlockingFilter(dataIndex.rowSetColumnName(), state);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutionContext context = ExecutionContext.getContext();
        final ExecutorService pool = Executors.newSingleThreadExecutor();
        boolean cycleOpen = false;
        boolean sourcesRefreshed = false;
        try {
            updateGraph.startCycleForUnitTests(false);
            cycleOpen = true;
            // Row 0 leaves Sym 1, and row 1 joins Sym 1.
            addToTable(parent, i(0, 1), intCol("Sym", 2, 1));
            parent.notifyListeners(i(), i(), i(0, 1));
            final long step = updateGraph.clock().currentStep();
            assertFalse(child.satisfied(step));
            assertFalse(indexTable.satisfied(step));

            final Future<Table> whereFuture = pool.submit(() -> {
                try (final SafeCloseable ignored = context.open()) {
                    return child.where(filter);
                }
            });
            assertTrue("The where did not reach the index pushdown",
                    state.inPushdown.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Run only the index's update, which changes the index's row sets in place.
            updateGraph.markSourcesRefreshedForUnitTests();
            sourcesRefreshed = true;
            updateGraph.flushOneNotificationForUnitTests();
            // The flushed notification must be the index's update. Otherwise the index still holds the previous row
            // sets, and the where returns the previous matches whether or not the pushdown reads previous values.
            assertEquals("index row set for Sym 1 after the index's update", currentMatches,
                    currentRowSet(indexTable, dataIndex.rowSetColumnName(), 1));
            assertFalse(child.satisfied(step));
            state.release.countDown();

            final Table result;
            try {
                result = whereFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final TimeoutException e) {
                // Let the where finish before failing, so that the where's thread does not outlive the test.
                updateGraph.flushAllNormalNotificationsForUnitTests();
                whereFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                fail("The where did not complete while the child was unsatisfied");
                return;
            }
            assertFalse(child.satisfied(step));
            // The where started from the previous state, so the where's initial result is the previous matches.
            final RowSet initial = result.getRowSet().copy();

            updateGraph.completeCycleForUnitTests();
            cycleOpen = false;
            assertEquals("initial result", previousMatches, initial);
            assertEquals("result after the cycle", currentMatches, result.getRowSet());
        } finally {
            state.release.countDown();
            if (cycleOpen) {
                // A cycle cannot complete until the sources are refreshed, and a failure here would hide the first.
                if (!sourcesRefreshed) {
                    updateGraph.markSourcesRefreshedForUnitTests();
                }
                updateGraph.completeCycleForUnitTests();
            }
            pool.shutdownNow();
        }
    }

    /**
     * Copy the current row set of the index row for Sym {@code key}. The index updates the row sets in place, so the
     * copy shows how far the index's update has run.
     */
    private static RowSet currentRowSet(final Table indexTable, final String rowSetColumnName, final int key) {
        try (final CloseableIterator<Integer> keys = indexTable.columnIterator("Sym");
                final CloseableIterator<RowSet> rowSets = indexTable.columnIterator(rowSetColumnName)) {
            while (keys.hasNext()) {
                final int sym = keys.next();
                final RowSet rowSet = rowSets.next();
                if (sym == key) {
                    return rowSet.copy();
                }
            }
        }
        throw new AssertionError("The index has no row for Sym " + key);
    }

    private static final class State {
        final CountDownLatch inPushdown = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean blocked = new AtomicBoolean();
    }

    /**
     * Matches Sym 1 through a wrapped {@link MatchFilter}, and blocks the first time the filter is applied to the index
     * table, until the test releases the filter. The filter is not itself a {@link MatchFilter}, so filtering the index
     * table calls {@link #filter} rather than probing the aggregation that builds the index table.
     */
    private static final class BlockingFilter extends WhereFilterImpl {

        private final String rowSetColumnName;
        private final State state;
        private final MatchFilter matchFilter = new MatchFilter(MatchOptions.REGULAR, "Sym", 1);

        BlockingFilter(final String rowSetColumnName, final State state) {
            this.rowSetColumnName = rowSetColumnName;
            this.state = state;
        }

        @Override
        public List<String> getColumns() {
            return matchFilter.getColumns();
        }

        @Override
        public List<String> getColumnArrays() {
            return matchFilter.getColumnArrays();
        }

        @Override
        public void init(@NotNull final TableDefinition tableDefinition) {
            matchFilter.init(tableDefinition);
        }

        private void maybeBlock(final Table table) {
            if (table.hasColumns(rowSetColumnName) && state.blocked.compareAndSet(false, true)) {
                state.inPushdown.countDown();
                try {
                    state.release.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            maybeBlock(table);
            return matchFilter.filter(selection, fullSet, table, usePrev);
        }

        @NotNull
        @Override
        public WritableRowSet filterInverse(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            maybeBlock(table);
            return matchFilter.filterInverse(selection, fullSet, table, usePrev);
        }

        @Override
        public boolean isSimpleFilter() {
            return true;
        }

        @Override
        public void setRecomputeListener(final RecomputeListener result) {}

        @Override
        public boolean canMemoize() {
            return false;
        }

        @Override
        public WhereFilter copy() {
            return new BlockingFilter(rowSetColumnName, state);
        }
    }
}
