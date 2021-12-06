package io.deephaven.engine.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.UnionColumnSource;
import io.deephaven.engine.table.impl.sources.UnionSourceManager;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is not intended for public API consumers.
 *
 */
public class TableToolsMergeHelper {

    public static Table mergeTableMap(LocalTableMap tableMap) {
        final List<Table> tablesToMergeOrNull = getTablesToMerge(tableMap.values().stream(), tableMap.size());
        final List<Table> tablesToMerge = tablesToMergeOrNull == null ? Collections.emptyList() : tablesToMergeOrNull;
        return mergeInternal(tableMap.getConstituentDefinitionOrErr(), tablesToMerge, tableMap);
    }

    /**
     * @param tableDef = The definition to apply to the result table.
     * @param tables = The list of tables to merge -- all elements must be non-null and un-partitioned.
     * @return A new table, containing all the rows from tables, respecting the input ordering.
     */
    public static Table mergeInternal(TableDefinition tableDef, List<Table> tables,
            NotificationQueue.Dependency parentDependency) {
        final Set<String> targetColumnNames =
                tableDef.getColumnStream().map(ColumnDefinition::getName).collect(Collectors.toSet());

        boolean isStatic = true;
        for (int ti = 0; ti < tables.size(); ++ti) {
            // verify the column names are exactly the same as our target
            final TableDefinition definition = tables.get(ti).getDefinition();
            final Set<String> columnNames =
                    definition.getColumnStream().map(ColumnDefinition::getName).collect(Collectors.toSet());
            isStatic &= !tables.get(ti).isRefreshing();

            if (!targetColumnNames.containsAll(columnNames) || !columnNames.containsAll(targetColumnNames)) {
                final Set<String> missingTargets = new HashSet<>(targetColumnNames);
                missingTargets.removeAll(columnNames);
                columnNames.removeAll(targetColumnNames);
                if (missingTargets.isEmpty()) {
                    throw new UnsupportedOperationException(
                            "Column mismatch for table " + ti + ", additional columns: " + columnNames);
                } else if (columnNames.isEmpty()) {
                    throw new UnsupportedOperationException(
                            "Column mismatch for table " + ti + ", missing columns: " + missingTargets);
                } else {
                    throw new UnsupportedOperationException("Column mismatch for table " + ti + ", missing columns: "
                            + missingTargets + ", additional columns: " + columnNames);
                }
            }

            // TODO: Make this check better? It's slightly too permissive, if we want identical column sets, and not
            // permissive enough if we want the "merge non-conflicting defs with nulls" behavior.
            try {
                definition.checkCompatibility(tableDef);
            } catch (RuntimeException e) {
                throw new UnsupportedOperationException("Table definition mismatch for table " + ti, e);
            }
        }

        if (!isStatic) {
            UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        }

        final UnionSourceManager unionSourceManager = new UnionSourceManager(tableDef, parentDependency);
        final QueryTable queryTable = unionSourceManager.getResult();

        for (Table table : tables) {
            unionSourceManager.addTable((QueryTable) table.coalesce(), false);
        }

        queryTable.setAttribute(Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE);

        return queryTable;
    }

    /**
     * Given a table that consists of only UnionColumnSources, produce a list of new tables that represent each one of
     * the unioned sources. This basically will undo the merge operation, so that the consituents can be reused in an
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
            if (table instanceof UncoalescedTable || table instanceof TableMapProxyHandler.TableMapProxy) {
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
