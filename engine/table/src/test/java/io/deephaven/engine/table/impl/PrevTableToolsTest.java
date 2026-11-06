//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PrevTableToolsTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * A static table has no previous state apart from the table's current state, so the previous table is the table
     * itself.
     */
    @Test
    public void prevTableOfStaticTable() {
        final Table table = TableTools.emptyTable(10).update("A = ii");
        assertSame(table, PrevTableTools.prevTable(table));
    }

    /**
     * During an update cycle, the previous table of a refreshing table presents the rows and values from before the
     * cycle's changes.
     */
    @Test
    public void prevTableOfRefreshingTable() {
        final QueryTable table = testRefreshingTable(i(0, 2, 4).toTracking(),
                intCol("A", 1, 2, 3), stringCol("B", "a", "b", "c"));
        final Table before = TableTools.newTable(intCol("A", 1, 2, 3), stringCol("B", "a", "b", "c"));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 6), intCol("A", 20, 4), stringCol("B", "bb", "d"));
            removeRows(table, i(0));
            table.notifyListeners(i(6), i(0), i(2));

            final Table prev = PrevTableTools.prevTable(table);
            assertFalse(prev.isRefreshing());
            assertEquals(i(0, 2, 4), prev.getRowSet());
            assertTableEquals(before, prev);
        });
    }

    /**
     * The previous table keeps every attribute of the table, such as sortedness and the merged table attribute, and
     * keeps flatness.
     */
    @Test
    public void prevTableKeepsAttributes() {
        final QueryTable table = testRefreshingTable(i(0, 2, 4).toTracking(), intCol("A", 3, 1, 2));
        final Table sortedFlat = table.sort("A").flatten()
                .withAttributes(Map.of(Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE));
        assertTrue(sortedFlat.isRefreshing());
        assertTrue(sortedFlat.isFlat());
        assertEquals(Optional.of(SortingOrder.Ascending), SortedColumnsAttribute.getOrderForColumn(sortedFlat, "A"));

        // Previous values are defined only during the updating phase, so make the previous table inside a cycle.
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final Table prev = PrevTableTools.prevTable(sortedFlat);
            assertTrue(prev.isFlat());
            assertEquals(Optional.of(SortingOrder.Ascending), SortedColumnsAttribute.getOrderForColumn(prev, "A"));
            assertEquals(Boolean.TRUE, prev.getAttribute(Table.MERGED_TABLE_ATTRIBUTE));
        });
    }

    /**
     * A data index updates the row sets in the index table in place. During an update cycle, after the index has
     * updated, the previous table of the index table still presents the index's previous row sets.
     */
    @Test
    public void prevTableOfRefreshingDataIndexTable() {
        final QueryTable table = testRefreshingTable(i(0, 1, 2, 3).toTracking(), intCol("Sym", 1, 2, 1, 2));
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "Sym");
        final Table indexTable = dataIndex.table();
        final String rowSetColumnName = dataIndex.rowSetColumnName();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            // Row 0 moves from Sym 1 to Sym 2.
            addToTable(table, i(0), intCol("Sym", 2));
            table.notifyListeners(i(), i(), i(0));
            updateGraph.flushAllNormalNotificationsForUnitTests();
            assertTrue(indexTable.satisfied(updateGraph.clock().currentStep()));
            assertEquals(Map.of(1, i(2), 2, i(0, 1, 3)), rowSetsByKey(indexTable, rowSetColumnName));

            final Table prev = PrevTableTools.prevTable(indexTable);
            assertEquals(Map.of(1, i(0, 2), 2, i(1, 3)), rowSetsByKey(prev, rowSetColumnName));
        });
    }

    private static Map<Integer, RowSet> rowSetsByKey(final Table indexTable, final String rowSetColumnName) {
        final Map<Integer, RowSet> rowSetsByKey = new HashMap<>();
        try (final CloseableIterator<Integer> keys = indexTable.columnIterator("Sym");
                final CloseableIterator<RowSet> rowSets = indexTable.columnIterator(rowSetColumnName)) {
            while (keys.hasNext()) {
                rowSetsByKey.put(keys.next(), rowSets.next().copy());
            }
        }
        return rowSetsByKey;
    }
}
