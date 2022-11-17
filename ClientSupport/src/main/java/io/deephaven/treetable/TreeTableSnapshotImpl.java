/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.table.impl.hierarchical.TreeTableFilter;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.table.sort.SortDirective;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.deephaven.treetable.TreeConstants.RE_TREE_KEY;
import static io.deephaven.treetable.TreeConstants.ROOT_TABLE_KEY;

class TreeTableSnapshotImpl extends AbstractTreeSnapshotImpl<TreeTableInfo> {

    private ReverseLookup masterRll;
    private TableMap masterTableMap;
    private Table sourceTable;
    private boolean rootTableChanged = false;

    /**
     * Construct a new query that will create a flat snapshot of the tree table using a flat viewport beginning at the
     * specified rows and columns, applying the specified sorts and filters if required to fetch tables
     *
     * @param baseTableId The Id of the base table. Used to maintain client state.
     * @param baseTable The base table to use if sorts/filters must be applied.
     * @param tablesByKey The tables within the tree for which viewports are being tracked, separated by table key.
     * @param firstRow The first row of the flat viewport.
     * @param lastRow The last row of the flat viewport.
     * @param columns The columns to include in the viewport
     * @param filters The filters to apply to new tables.
     * @param sorts The sorts to apply to new tables.
     * @param client The client issuing the TSQ.
     * @param includedOps The set of operations performed by the client since the last TSQ.
     */
    TreeTableSnapshotImpl(int baseTableId,
            BaseHierarchicalTable baseTable,
            Map<Object, TableDetails> tablesByKey,
            long firstRow,
            long lastRow,
            BitSet columns,
            @NotNull WhereFilter[] filters,
            @NotNull List<SortDirective> sorts,
            TreeTableClientTableManager.Client client,
            Set<TreeSnapshotQuery.Operation> includedOps) {
        super(baseTableId, baseTable, tablesByKey, firstRow, lastRow, columns, filters, sorts, client, includedOps);
    }

    @Override
    Table prepareRootTable() {
        final BaseHierarchicalTable baseTable = getBaseTable();
        Table prepared = tryGetRetainedTable(ROOT_TABLE_KEY);
        if (prepared == null) {
            final WhereFilter[] filters = getFilters();
            final List<SortDirective> directives = getDirectives();

            if (filters.length == 0 && directives.isEmpty()) {
                prepared = prepareTableInternal(baseTable.getRootTable());
            } else {
                boolean reTreeRequired = false;

                if (filters.length > 0) {
                    prepared = TreeTableFilter.rawFilterTree(baseTable, filters);
                    reTreeRequired = true;
                } else {
                    prepared = baseTable.getSourceTable();
                }

                if (reTreeRequired) {
                    final BaseHierarchicalTable reTreed =
                            (BaseHierarchicalTable) TreeTableFilter.toTreeTable(prepared, baseTable);

                    // We need to retain this reference or we will leak it.
                    retainTable(RE_TREE_KEY, reTreed);
                    prepared = reTreed.getRootTable();
                } else {
                    prepared = baseTable.getRootTable();
                }

                prepared = prepareTableInternal(prepared);
            }

            retainTable(ROOT_TABLE_KEY, prepared);
            rootTableChanged = true;
        }

        BaseHierarchicalTable treeForDisplay = (BaseHierarchicalTable) tryGetRetainedTable(RE_TREE_KEY);
        if (treeForDisplay == null) {
            treeForDisplay = baseTable;
        }

        masterRll = (ReverseLookup) treeForDisplay.getAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE);
        masterTableMap = (TableMap) treeForDisplay.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_ATTRIBUTE);
        sourceTable = treeForDisplay.getSourceTable();

        return prepared;
    }

    @Override
    NotificationQueue.Dependency getRootDependency() {
        return (NotificationQueue.Dependency) masterTableMap;
    }

    @Override
    boolean rootTableChanged() {
        return rootTableChanged;
    }

    @Override
    Table getSourceTable() {
        return sourceTable;
    }

    @Override
    Table prepareTableInternal(Table t) {
        t = applyColumnFormats(t);
        if (!getDirectives().isEmpty()) {
            t = attachReverseLookup(applySorts(t));
        }

        return t;
    }

    @Override
    ReverseLookup getReverseLookup(Table t) {
        final ReverseLookup tableRll = (ReverseLookup) t.getAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE);
        return tableRll == null ? masterRll : tableRll;
    }

    @Override
    TableMap getTableMap(Table t) {
        return masterTableMap;
    }

    @Override
    boolean isKeyValid(boolean usePrev, Table t, long key) {
        return (usePrev ? t.getRowSet().findPrev(key) : t.getRowSet().find(key)) >= 0;
    }

    @Override
    boolean verifyChild(TableDetails parentDetail, TableDetails childDetail, long childKeyPos, boolean usePrev) {
        final TrackingRowSet parentRowSet = parentDetail.getTable().getRowSet();
        return usePrev ? parentRowSet.findPrev(childKeyPos) >= 0
                : parentRowSet.find(childKeyPos) >= 0;
    }
}
