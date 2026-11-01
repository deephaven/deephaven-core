//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TableBackedDataIndexTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final int SIZE = 40;

    @Test
    public void testStaticLookupsNeverChange() {
        final Table source = TableTools.emptyTable(SIZE).update("Key = (int) (ii % 2)");
        final DataIndex.RowKeyLookup lookup = DataIndexer.getOrCreateDataIndex(source, "Key").rowKeyLookup();
        assertEquals(1, lookup.apply(1, false));
        assertEquals(QueryConstants.NULL_LONG, lookup.lastStateChangeStep());
    }

    @Test
    public void testLastStateChangeStepAcrossUpdateCycle() {
        final QueryTable source = makeSource();
        final DataIndex index = DataIndexer.getOrCreateDataIndex(source, "Key");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        // one lookup, used across the cycle
        final DataIndex.RowKeyLookup lookup = index.rowKeyLookup();
        assertTrue(lookup.apply(11, false) < 0);

        updateGraph.startCycleForUnitTests(false);
        addToTable(source, i(SIZE), intCol("Key", 11), intCol("Value", SIZE));
        source.notifyListeners(i(SIZE), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();
        final long cycleStep = updateGraph.clock().currentStep();
        assertTrue(lookup.lastStateChangeStep() < cycleStep);
        flushIndexUpdate(updateGraph, index);

        // the index's update began on the cycle's step
        assertEquals(cycleStep, lookup.lastStateChangeStep());
        updateGraph.completeCycleForUnitTests();

        assertEquals(cycleStep, lookup.lastStateChangeStep());
        final long rowKey = lookup.apply(11, false);
        assertEquals(11, index.table().getColumnSource("Key").getInt(rowKey));
    }

    /**
     * A filter that looks up its source's index reports the step on which the index began changing, so that a snapshot
     * attempt reading previous values through the index is rejected and retried.
     */
    @Test
    public void testFilterReportsItsIndexChange() {
        final QueryTable source = makeSource();
        final DataIndex index = DataIndexer.getOrCreateDataIndex(source, "Key");
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(TableTools.newTable(intCol("Key", 3)), true, new MatchPair("Key", "Key"));
        filter.beginOperation(source).close();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        addToTable(source, i(SIZE), intCol("Key", 11), intCol("Value", SIZE));
        source.notifyListeners(i(SIZE), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();
        final long step = updateGraph.clock().currentStep();
        assertTrue(filter.lastStateChangeStep() < step);
        flushIndexUpdate(updateGraph, index);
        assertEquals(step, filter.lastStateChangeStep());
        updateGraph.completeCycleForUnitTests();
    }

    /**
     * A table that shares its source's rows and key column shares its index too. Once the index's aggregation begins
     * its update, and before the index table is satisfied, a where on that table would filter previous values, which
     * the index's lookup no longer answers for. Its concurrent attempts are abandoned and retried, and it then waits
     * for the update graph lock, filtering the cycle's current values once the cycle completes.
     */
    @Test
    public void testWhereInOnSharedIndexDuringIndexUpdate() throws Exception {
        final QueryTable source = makeSource();
        final DataIndex index = DataIndexer.getOrCreateDataIndex(source, "Key");
        // like a view, but notified only when the test opens its gate, so that it is not satisfied in between
        final MutableBoolean gateOpen = new MutableBoolean();
        final QueryTable shared = source.withAdditionalColumns(Map.of());
        shared.addParentReference(source);
        shared.addParentReference(new NotificationQueue.Dependency() {
            @Override
            public boolean satisfied(final long step) {
                return gateOpen.booleanValue();
            }

            @Override
            public UpdateGraph getUpdateGraph() {
                return source.getUpdateGraph();
            }

            @Override
            public LogOutput append(final LogOutput logOutput) {
                return logOutput.append("gate");
            }
        });
        assertNotNull(DataIndexer.getDataIndex(shared, "Key"));
        final Table set = TableTools.newTable(intCol("Key", 3, 11));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutionContext context = ExecutionContext.getContext();
        final ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            final DataIndex.RowKeyLookup lookup = index.rowKeyLookup();
            updateGraph.startCycleForUnitTests(false);
            addToTable(source, i(SIZE), intCol("Key", 11), intCol("Value", SIZE));
            source.notifyListeners(i(SIZE), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            final long step = updateGraph.clock().currentStep();
            // run the index's aggregation, which notifies the index table's own listener
            while (lookup.lastStateChangeStep() < step) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
            assertFalse(index.table().satisfied(step));
            assertFalse(shared.satisfied(step));

            final MutableObject<Thread> whereThread = new MutableObject<>();
            final CountDownLatch whereStarted = new CountDownLatch(1);
            final Future<Table> filtered = pool.submit(() -> {
                whereThread.setValue(Thread.currentThread());
                whereStarted.countDown();
                try (final SafeCloseable ignored = context.open()) {
                    return shared.whereIn(set, "Key");
                }
            });
            assertTrue(whereStarted.await(30, TimeUnit.SECONDS));
            // the cycle holds the update graph lock, which the where waits for once its concurrent attempts fail
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!(LockSupport.getBlocker(whereThread.getValue()) instanceof AbstractQueuedSynchronizer)) {
                assertFalse("the where completed during the index's update", filtered.isDone());
                assertTrue("the where never waited for the update graph lock", System.nanoTime() < deadline);
                Thread.sleep(1);
            }

            gateOpen.setTrue();
            shared.notifyListeners(i(SIZE), i(), i());
            updateGraph.completeCycleForUnitTests();
            final Table result = filtered.get(30, TimeUnit.SECONDS);
            assertArrayEquals(IntStream.concat(IntStream.range(0, SIZE / 2).map(ii -> 2 * ii), IntStream.of(SIZE))
                    .toArray(), values(result));

            // the result follows the next cycle
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(source, i(SIZE + 1), intCol("Key", 3), intCol("Value", SIZE + 1));
                source.notifyListeners(i(SIZE + 1), i(), i());
                shared.notifyListeners(i(SIZE + 1), i(), i());
            });
            assertArrayEquals(IntStream.concat(IntStream.range(0, SIZE / 2).map(ii -> 2 * ii),
                    IntStream.of(SIZE, SIZE + 1)).toArray(), values(result));
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * A source of {@link #SIZE} rows alternating between keys 3 and 5, so that a where on it uses its index.
     */
    private static QueryTable makeSource() {
        return testRefreshingTable(RowSetFactory.flat(SIZE).toTracking(),
                intCol("Key", IntStream.range(0, SIZE).map(ii -> ii % 2 == 0 ? 3 : 5).toArray()),
                intCol("Value", IntStream.range(0, SIZE).toArray()));
    }

    /**
     * Run notifications until the index has processed this cycle's update, returning the cycle's step.
     */
    private static long flushIndexUpdate(final ControlledUpdateGraph updateGraph, final DataIndex index) {
        final long step = updateGraph.clock().currentStep();
        while (!index.table().satisfied(step)) {
            assertTrue(updateGraph.flushOneNotificationForUnitTests());
        }
        return step;
    }

    private static int[] values(final Table table) {
        return ColumnVectors.ofInt(table, "Value").toArray();
    }
}
