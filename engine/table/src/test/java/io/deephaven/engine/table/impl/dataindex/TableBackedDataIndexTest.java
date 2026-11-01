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
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    public void testStaticLookupsValid() {
        final Table source = TableTools.emptyTable(SIZE).update("Key = (int) (ii % 2)");
        final DataIndex.RowKeyLookup lookup = DataIndexer.getOrCreateDataIndex(source, "Key").rowKeyLookup();
        assertEquals(1, lookup.apply(1, false));
        assertTrue(lookup.lookupsValid(false, ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
    }

    @Test
    public void testLookupsValidAcrossUpdateCycle() {
        final QueryTable source = makeSource();
        final DataIndex index = DataIndexer.getOrCreateDataIndex(source, "Key");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final long idleStep = updateGraph.clock().currentStep();
        final DataIndex.RowKeyLookup beforeCycle = index.rowKeyLookup();
        assertTrue(beforeCycle.lookupsValid(false, idleStep));

        updateGraph.startCycleForUnitTests(false);
        addToTable(source, i(SIZE), intCol("Key", 11), intCol("Value", SIZE));
        source.notifyListeners(i(SIZE), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();
        final long cycleStep = updateGraph.clock().currentStep();
        final DataIndex.RowKeyLookup beforeIndexUpdate = index.rowKeyLookup();
        assertTrue(beforeIndexUpdate.lookupsValid(true, cycleStep));
        flushIndexUpdate(updateGraph, index);

        // the index's update began after these lookups were created
        assertFalse(beforeCycle.lookupsValid(false, idleStep));
        assertFalse(beforeIndexUpdate.lookupsValid(true, cycleStep));
        // created after the index's update, a lookup answers for the cycle's current values, but not its previous ones
        // or an earlier step's
        final DataIndex.RowKeyLookup afterIndexUpdate = index.rowKeyLookup();
        assertTrue(afterIndexUpdate.lookupsValid(false, cycleStep));
        assertFalse(afterIndexUpdate.lookupsValid(true, cycleStep));
        assertFalse(afterIndexUpdate.lookupsValid(false, idleStep));
        updateGraph.completeCycleForUnitTests();

        assertTrue(afterIndexUpdate.lookupsValid(false, cycleStep));
        // the next cycle's previous values are this cycle's current ones
        assertTrue(afterIndexUpdate.lookupsValid(true, cycleStep + 1));
        assertTrue(index.rowKeyLookup().lookupsValid(false, cycleStep));
    }

    /**
     * A table that shares its source's rows and key column shares its index too. Once the index's aggregation begins
     * its update, and before the index table is satisfied, a where on that table filters previous values, and can not
     * use the index's lookup.
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
            final DataIndex.RowKeyLookup beforeCycle = index.rowKeyLookup();
            updateGraph.startCycleForUnitTests(false);
            addToTable(source, i(SIZE), intCol("Key", 11), intCol("Value", SIZE));
            source.notifyListeners(i(SIZE), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            final long step = updateGraph.clock().currentStep();
            // run the index's aggregation, which notifies the index table's own listener
            while (beforeCycle.lookupsValid(false, step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
            assertFalse(index.table().satisfied(step));
            assertFalse(shared.satisfied(step));

            final Table filtered = pool.submit(() -> {
                try (final SafeCloseable ignored = context.open()) {
                    return shared.whereIn(set, "Key");
                }
            }).get(30, TimeUnit.SECONDS);
            assertArrayEquals(IntStream.range(0, SIZE / 2).map(ii -> 2 * ii).toArray(), values(filtered));

            gateOpen.setTrue();
            shared.notifyListeners(i(SIZE), i(), i());
            updateGraph.completeCycleForUnitTests();
            assertArrayEquals(IntStream.concat(IntStream.range(0, SIZE / 2).map(ii -> 2 * ii), IntStream.of(SIZE))
                    .toArray(), values(filtered));
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
