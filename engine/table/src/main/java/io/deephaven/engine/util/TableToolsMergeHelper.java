/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sources.UnionColumnSource;
import io.deephaven.engine.table.impl.sources.UnionSourceManager;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;

/**
 * Helper for coalescing and de-unioning tables prior to a merge. Only for engine-internal usage.
 */
public class TableToolsMergeHelper {

    /**
     * Given a table that consists of only UnionColumnSources, produce a list of new tables that represent each one of
     * the unioned sources. This basically will undo the merge operation, so that the constituents can be reused in a
     * new merge operation. The UnionSourceManager must be shared across all the columns.
     *
     * @param table that has only UnionSourceColumns (with the same manager)
     * @return the list of component tables from the manager.
     */
    private static Collection<? extends Table> getComponentTables(Table table) {

        Map<String, ColumnSource<?>> columnSourceMap = ((QueryTable) table).getColumnSourceMap();
        Assert.assertion(columnSourceMap.size() > 0, "columnSourceMap.size() > 0");

        UnionSourceManager unionSourceManager = null;

        for (Map.Entry<String, ColumnSource<?>> entry : columnSourceMap.entrySet()) {
            UnionColumnSource<?> unionColumnSource = (UnionColumnSource<?>) entry.getValue();
            UnionSourceManager thisUnionSourceManager = unionColumnSource.getUnionSourceManager();

            if (unionSourceManager == null) {
                unionSourceManager = thisUnionSourceManager;
            } else if (unionSourceManager != thisUnionSourceManager) {
                throw new RuntimeException(
                        "A table exists with columns from multiple UnionSourceManagers, this doesn't make any sense.");
            }
        }

        if (unionSourceManager == null)
            throw new IllegalStateException("UnionSourceManager is null!");

        if (unionSourceManager.getColumnSources().equals(columnSourceMap)) {
            // we've got the original merged table, we can just go ahead and use the components as is
            return unionSourceManager.getComponentTables();
        } else {
            // the merged table has had things renamed, viewed, dropped or otherwise messed with; so we need to create
            // brand new component tables, using the sources from the original that parallel the merged table
            Collection<Table> componentTables = unionSourceManager.getComponentTables();
            ArrayList<Table> result = new ArrayList<>();
            int componentIndex = 0;
            for (Table componentAsTable : componentTables) {
                QueryTable component = (QueryTable) componentAsTable;
                // copy the sub sources over
                Map<String, ColumnSource<?>> componentSources = new LinkedHashMap<>();
                for (Map.Entry<String, ColumnSource<?>> entry : columnSourceMap.entrySet()) {
                    final UnionColumnSource<?> unionSource = (UnionColumnSource<?>) entry.getValue();
                    componentSources.put(entry.getKey(), unionSource.getSubSource(componentIndex));
                }
                componentIndex++;

                QueryTable viewedTable = new QueryTable(component.getRowSet(), componentSources);
                if (component.isRefreshing()) {
                    component.listenForUpdates(
                            new BaseTable.ListenerImpl("union view", component, viewedTable));
                }
                result.add(viewedTable);
            }
            return result;
        }
    }

    @Nullable
    static List<Table> getTablesToMerge(Stream<Table> tables, int sizeEstimate) {
        final List<Table> tableList = new ArrayList<>((sizeEstimate * 4) / 3 + 1);

        tables.forEach((table) -> {
            if (table == null) {
                return;
            }
            if (table instanceof UncoalescedTable) {
                table = table.coalesce();
            }

            if (canBreakOutUnionedTable(table)) {
                tableList.addAll(getComponentTables(table));
            } else {
                tableList.add(table);
            }
        });

        if (tableList.isEmpty()) {
            return null;
        }

        return tableList;
    }

    /**
     * Determine if table is the output of a previous merge operation and can be broken into constituents.
     *
     * @param table the table to check
     * @return true if the table is the result of a previous merge.
     */
    private static boolean canBreakOutUnionedTable(Table table) {
        if (!(table instanceof QueryTable)) {
            return false;
        }
        QueryTable queryTable = (QueryTable) table;
        if (!table.hasAttribute(Table.MERGED_TABLE_ATTRIBUTE)) {
            return false;
        }
        Map<String, ColumnSource<?>> columnSourceMap = queryTable.getColumnSourceMap();
        if (columnSourceMap.isEmpty()) {
            return false;
        }
        if (!columnSourceMap.values().stream().allMatch(cs -> cs instanceof UnionColumnSource)) {
            return false;
        }

        final UnionColumnSource<?> columnSource = (UnionColumnSource<?>) columnSourceMap.values().iterator().next();
        return columnSource.getUnionSourceManager().isUsingComponentsSafe();
    }
}
