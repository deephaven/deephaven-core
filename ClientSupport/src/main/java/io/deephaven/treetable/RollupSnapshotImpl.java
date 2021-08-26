package io.deephaven.treetable;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.HierarchicalTable;
import io.deephaven.db.v2.ReverseLookup;
import io.deephaven.db.v2.RollupInfo;
import io.deephaven.db.v2.TableMap;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.table.sort.SortDirective;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.treetable.TreeTableConstants.RE_TREE_KEY;

class RollupSnapshotImpl<CLIENT_TYPE extends TreeTableClientTableManager.Client<CLIENT_TYPE>>
    extends AbstractTreeSnapshotImpl<RollupInfo, CLIENT_TYPE> {
    private boolean rootTableChanged = false;
    private Table sourceTable;
    private final PreparedSort constituentSort;

    /**
     * Construct a new query that will create a flat snapshot of the tree table using a flat
     * viewport beginning at the specified rows and columns, applying the specified sorts and
     * filters if required to fetch tables
     *
     * @param baseTableId The Id of the base table. Used to maintain client state.
     * @param baseTable The base table to use if sorts/filters must be applied.
     * @param tablesByKey The tables within the tree for which viewports are being tracked,
     *        separated by table key.
     * @param firstRow The first row of the flat viewport.
     * @param lastRow The last row of the flat viewport.
     * @param columns The columns to include in the viewport
     * @param filters The filters to apply to new tables.
     * @param sorts The sorts to apply to new tables.
     * @param client The client issuing the TSQ.
     * @param includedOps The set of operations performed by the client since the last TSQ.
     */
    RollupSnapshotImpl(int baseTableId,
        HierarchicalTable baseTable,
        Map<Object, TableDetails> tablesByKey,
        long firstRow,
        long lastRow,
        BitSet columns,
        @NotNull SelectFilter[] filters,
        @NotNull List<SortDirective> sorts,
        CLIENT_TYPE client,
        Set<TreeSnapshotQuery.Operation> includedOps) {
        super(baseTableId, baseTable, tablesByKey, firstRow, lastRow, columns, filters, sorts,
            client, includedOps);

        if (getInfo().includesConstituents()) {
            final List<SortDirective> updated = maybeComputeConstituentSorts(sorts);
            constituentSort = new PreparedSort(updated);
            constituentSort.computeSortingData();
        } else
            constituentSort = null;
    }

    @Override
    ReverseLookup getReverseLookup(Table t) {
        return (ReverseLookup) t.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE);
    }

    @Override
    Table prepareRootTable() {
        final HierarchicalTable baseTable = getBaseTable();
        Table prepared = tryGetRetainedTable(TreeTableConstants.ROOT_TABLE_KEY);
        if (prepared == null) {
            final HierarchicalTable filteredTable = applyFilters(baseTable);
            if (filteredTable != baseTable) {
                // We need to retain this reference or we will leak it.
                retainTable(RE_TREE_KEY, filteredTable);
            }

            prepared = prepareTableInternal(filteredTable.getRawRootTable());
            retainTable(TreeTableConstants.ROOT_TABLE_KEY, prepared);
            rootTableChanged = true;
        }

        HierarchicalTable treeForDisplay = (HierarchicalTable) tryGetRetainedTable(RE_TREE_KEY);
        if (treeForDisplay == null) {
            treeForDisplay = baseTable;
        }
        sourceTable = treeForDisplay.getSourceTable();

        return prepared;
    }

    @Override
    NotificationQueue.Dependency getRootDependency() {
        return null;
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
        return getDirectives().isEmpty() ? t : attachReverseLookup(applySorts(t));
    }

    /**
     * For Rollups, if constituents are included, it's possible for the column type to be different
     * at the leaf level. This will cause a host of potential problems for formatting, so we will
     * eliminate format columns for any column that either can't be found in the leaf table, or has
     * a different column type than the root.
     *
     * @param t the table to update the columns for
     * @param initial the initial set of filter columns
     * @return a reduced set of format columns
     */
    @Override
    SelectColumn[] processFormatColumns(Table t, SelectColumn[] initial) {
        if (getInfo().includesConstituents() && t.hasAttribute(Table.ROLLUP_LEAF_ATTRIBUTE)) {
            final Map<String, ? extends ColumnSource> currentColumns = t.getColumnSourceMap();
            final HierarchicalTable baseTable = getBaseTable();
            return Arrays.stream(initial)
                .filter(col -> {
                    col.initDef(t.getDefinition().getColumnNameMap());
                    final List<String> requiredColumns = col.getColumns();
                    for (final String colName : requiredColumns) {
                        final ColumnSource currentColumn = currentColumns.get(colName);
                        if (currentColumn == null) {
                            return false;
                        }

                        if (currentColumn.getType() != baseTable.getColumn(colName).getType()) {
                            return false;
                        }
                    }

                    return true;
                }).toArray(SelectColumn[]::new);
        }

        return initial;
    }

    @Override
    boolean isKeyValid(boolean usePrev, Table t, long key) {
        return true;
    }

    private HierarchicalTable applyFilters(@NotNull HierarchicalTable table) {
        final SelectFilter[] filters = getFilters();
        if (filters.length == 0) {
            return table;
        }

        final Table source = Require.neqNull(table.getSourceTable(), "Hierarchical source table");
        final RollupInfo info = getInfo();
        return (HierarchicalTable) source.where(filters).rollup(info.factory,
            info.getLeafType() == RollupInfo.LeafType.Constituent, info.getSelectColumns());
    }

    @Override
    Table applySorts(@NotNull Table table) {
        if (table.hasAttribute(Table.ROLLUP_LEAF_ATTRIBUTE) && getInfo().includesConstituents()) {
            return constituentSort.applySorts(table);
        } else
            return super.applySorts(table);
    }

    /**
     * Attach a reverse lookup listener to the specified table.
     */
    Table attachReverseLookup(Table table) {
        if (table.hasAttribute(Table.ROLLUP_LEAF_ATTRIBUTE)) {
            return table;
        }

        return super.attachReverseLookup(table);
    }

    @Override
    TableMap getTableMap(Table t) {
        return Require.neqNull(
            (TableMap) t.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE),
            "Child Table Map");
    }

    private List<SortDirective> maybeComputeConstituentSorts(List<SortDirective> sorts) {
        if (sorts.isEmpty()) {
            return sorts;
        }

        final Map<String, String> nameMap = getInfo().getMatchPairs().stream()
            .collect(Collectors.toMap(MatchPair::left, MatchPair::right));

        // Note that we can't use getSourceTable() here because it won't have been initialized until
        // after
        // getSnapshot() is invoked.
        final Table sourceTable = getBaseTable().getSourceTable();
        final List<SortDirective> updated = new ArrayList<>();
        for (final SortDirective dir : sorts) {
            // If the source has the column... awesome!
            if (sourceTable.hasColumns(dir.getColumnName())) {
                updated.add(dir);
                continue;
            }
            // Try mapping the column back to an original.
            // In most cases this will succeed -- a notable exception will be Count
            final String maybeSourceColumn = nameMap.get(dir.getColumnName());
            if (!StringUtils.isNullOrEmpty(maybeSourceColumn)
                && sourceTable.hasColumns(maybeSourceColumn)) {
                updated.add(
                    new SortDirective(maybeSourceColumn, dir.getDirection(), dir.isAbsolute()));
            }
        }

        return updated;
    }
}
