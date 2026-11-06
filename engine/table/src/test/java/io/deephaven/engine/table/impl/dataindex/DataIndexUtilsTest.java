//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DataIndexUtilsTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * A static index table that the filter reads under the key columns' own names is the result. Otherwise the result
     * is a table built from the index table's own row set and sources. A key column that the filter reads under two
     * names is the one source under both names, and both names take the key column's sort order.
     */
    @Test
    public void indexTableForFilterOnStaticIndexTable() {
        final Table table = TableTools.emptyTable(100).update("A = ii % 10");
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final String rowSetColumnName = dataIndex.rowSetColumnName();
        final Table indexTable = dataIndex.table().sort("A");
        assertTrue(SortedColumnsAttribute.isSortedBy(indexTable, "A", SortingOrder.Ascending));

        assertSame(indexTable,
                DataIndexUtils.indexTableForFilter(indexTable, rowSetColumnName, List.of("A"), Map.of()));

        final Table result = DataIndexUtils.indexTableForFilter(
                indexTable, rowSetColumnName, List.of("A", "A2"), Map.of("A2", "A"));
        assertFalse(result.isRefreshing());
        assertEquals(List.of("A", "A2", rowSetColumnName), result.getDefinition().getColumnNames());
        assertSame(indexTable.getRowSet(), result.getRowSet());
        assertSame(indexTable.getColumnSource("A"), result.getColumnSource("A"));
        assertSame(indexTable.getColumnSource("A"), result.getColumnSource("A2"));
        assertSame(indexTable.getColumnSource(rowSetColumnName), result.getColumnSource(rowSetColumnName));
        assertTrue(SortedColumnsAttribute.isSortedBy(result, "A", SortingOrder.Ascending));
        assertTrue(SortedColumnsAttribute.isSortedBy(result, "A2", SortingOrder.Ascending));
    }

    /**
     * A filter that swaps the names of two key columns reads the sorted key column A as B, and the unsorted key column
     * B as A, so the sort order follows key column A to the name B, and the name A is not sorted.
     */
    @Test
    public void indexTableForFilterOnSwappedNamesMovesSortOrder() {
        final Table table = TableTools.emptyTable(100).update("A = ii % 10", "B = 9 - A");
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A", "B");
        final String rowSetColumnName = dataIndex.rowSetColumnName();
        final Table indexTable = dataIndex.table().sort("A");
        assertTrue(SortedColumnsAttribute.isSortedBy(indexTable, "A", SortingOrder.Ascending));
        assertFalse(SortedColumnsAttribute.getOrderForColumn(indexTable, "B").isPresent());

        final Table result = DataIndexUtils.indexTableForFilter(
                indexTable, rowSetColumnName, List.of("A", "B"), Map.of("A", "B", "B", "A"));
        assertSame(indexTable.getColumnSource("B"), result.getColumnSource("A"));
        assertSame(indexTable.getColumnSource("A"), result.getColumnSource("B"));
        assertFalse(SortedColumnsAttribute.getOrderForColumn(result, "A").isPresent());
        assertTrue(SortedColumnsAttribute.isSortedBy(result, "B", SortingOrder.Ascending));
    }

    /**
     * A refreshing index table is presented by a static table built from the index table's own row set and sources,
     * even when the filter reads every key column under the key column's own name, since the result serves one filter
     * operation only. The filter reads key column A as X and as B, and key column B as Y, so the filter's B is the
     * index table's A, and the filter's Y is the index table's B.
     */
    @Test
    public void indexTableForFilterOnRefreshingIndexTable() {
        final QueryTable table = testRefreshingTable(i(0, 1, 2).toTracking(),
                intCol("A", 1, 2, 1), intCol("B", 10, 20, 30));
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A", "B");
        final Table indexTable = dataIndex.table();
        assertTrue(indexTable.isRefreshing());
        final String rowSetColumnName = dataIndex.rowSetColumnName();

        final Table sameNames = DataIndexUtils.indexTableForFilter(indexTable, rowSetColumnName,
                List.of("A", "B"), Map.of());
        assertFalse(sameNames.isRefreshing());
        assertSame(indexTable.getRowSet(), sameNames.getRowSet());
        assertSame(indexTable.getColumnSource("A"), sameNames.getColumnSource("A"));
        assertSame(indexTable.getColumnSource("B"), sameNames.getColumnSource("B"));

        final Table result = DataIndexUtils.indexTableForFilter(indexTable, rowSetColumnName,
                List.of("X", "B", "Y"), Map.of("X", "A", "B", "A", "Y", "B"));
        assertFalse(result.isRefreshing());
        assertEquals(List.of("X", "B", "Y", rowSetColumnName), result.getDefinition().getColumnNames());
        assertSame(indexTable.getRowSet(), result.getRowSet());
        assertSame(indexTable.getColumnSource("A"), result.getColumnSource("X"));
        assertSame(indexTable.getColumnSource("A"), result.getColumnSource("B"));
        assertSame(indexTable.getColumnSource("B"), result.getColumnSource("Y"));
        assertSame(indexTable.getColumnSource(rowSetColumnName), result.getColumnSource(rowSetColumnName));
        // Y is defined before B, so Y reads the index table's B.
        assertTableEquals(indexTable.view("X = A", "Y = B", "B = A"), result.view("X", "Y", "B"));
    }
}
