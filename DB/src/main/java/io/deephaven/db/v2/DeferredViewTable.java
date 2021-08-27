/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.select.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;

/**
 * An uncoalesced table with view and where operations to be applied after {@link #coalesce()} is forced.
 */
public class DeferredViewTable extends RedefinableTable {

    private final TableReference tableReference;
    protected final String[] deferredDropColumns;
    protected final SelectColumn[] deferredViewColumns;
    protected final SelectFilter[] deferredFilters;

    public DeferredViewTable(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @Nullable final String[] deferredDropColumns,
            @Nullable final SelectColumn[] deferredViewColumns,
            @Nullable final SelectFilter[] deferredFilters) {
        super(definition, description);
        this.tableReference = tableReference;
        this.deferredDropColumns =
                deferredDropColumns == null ? CollectionUtil.ZERO_LENGTH_STRING_ARRAY : deferredDropColumns;
        this.deferredViewColumns =
                deferredViewColumns == null ? SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY : deferredViewColumns;
        this.deferredFilters = deferredFilters == null ? SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY : deferredFilters;
        if (deferredFilters != null) {
            for (final SelectFilter sf : deferredFilters) {
                if (sf instanceof LivenessReferent) {
                    manage((LivenessReferent) sf);
                }
            }
        }

        // we really only expect one of these things to be set!
        final boolean haveDrop = this.deferredDropColumns.length > 0;
        final boolean haveView = this.deferredViewColumns.length > 0;
        final boolean haveFilter = this.deferredFilters.length > 0;

        manage(tableReference);

        if (haveDrop && haveFilter) {
            throw new IllegalStateException("Why do we have a drop and a filter all at the same time?");
        }
        if (haveView && haveFilter) {
            throw new IllegalStateException("Why do we have a view and a filter all at the same time?");
        }
    }

    @Override
    public Table where(SelectFilter... filters) {
        return getResultTableWithWhere(filters);
    }

    private Table getResultTableWithWhere(SelectFilter... selectFilters) {
        if (getCoalesced() != null) {
            return coalesce().where(selectFilters);
        }

        final SelectFilter[] allFilters = Stream.concat(Arrays.stream(deferredFilters), Arrays.stream(selectFilters))
                .map(SelectFilter::copy).toArray(SelectFilter[]::new);

        TableReference.TableAndRemainingFilters tableAndRemainingFilters;
        if (allFilters.length == 0) {
            tableAndRemainingFilters = tableReference.getWithWhere();
            Table result = tableAndRemainingFilters.table;
            result = applyDeferredViews(result);
            result = result.where(tableAndRemainingFilters.remainingFilters);
            copyAttributes(result, CopyAttributeOperation.Coalesce);
            setCoalesced((BaseTable) result);
            return result;
        }

        PreAndPostFilters preAndPostFilters = applyFilterRenamings(allFilters);
        tableAndRemainingFilters = tableReference.getWithWhere(preAndPostFilters.preViewFilters);

        Table localResult = tableAndRemainingFilters.table;
        if (localResult instanceof DeferredViewTable) {
            localResult = ((DeferredViewTable) localResult)
                    .getResultTableWithWhere(tableAndRemainingFilters.remainingFilters);
        } else {
            localResult = localResult.where(Arrays.stream(tableAndRemainingFilters.remainingFilters)
                    .map(SelectFilter::copy).toArray(SelectFilter[]::new));
        }

        localResult = applyDeferredViews(localResult);
        if (preAndPostFilters.postViewFilters.length > 0) {
            localResult = localResult.where(preAndPostFilters.postViewFilters);
        }
        if (selectFilters.length == 0) {
            copyAttributes(localResult, CopyAttributeOperation.Coalesce);
            setCoalesced((BaseTable) localResult);
        }
        return localResult;
    }

    private Table applyDeferredViews(Table result) {
        if (result instanceof DeferredViewTable) {
            result = result.coalesce();
        }
        if (deferredDropColumns.length > 0) {
            result = result.dropColumns(deferredDropColumns);
        }
        if (deferredViewColumns.length > 0) {
            result = result
                    .view(Arrays.stream(deferredViewColumns).map(SelectColumn::copy).toArray(SelectColumn[]::new));
        }
        return result;
    }

    private static class PreAndPostFilters {

        private final SelectFilter[] preViewFilters;
        private final SelectFilter[] postViewFilters;

        private PreAndPostFilters(SelectFilter[] preViewFilters, SelectFilter[] postViewFilters) {
            this.preViewFilters = preViewFilters;
            this.postViewFilters = postViewFilters;
        }
    }

    private PreAndPostFilters applyFilterRenamings(SelectFilter[] filters) {
        ArrayList<SelectFilter> preViewFilters = new ArrayList<>();
        ArrayList<SelectFilter> postViewFilters = new ArrayList<>();

        for (SelectFilter filter : filters) {
            filter.init(definition);
            ArrayList<String> usedColumns = new ArrayList<>();
            usedColumns.addAll(filter.getColumns());
            usedColumns.addAll(filter.getColumnArrays());

            Map<String, String> renames = new HashMap<>();
            boolean postFilter = false;

            for (String column : usedColumns) {
                for (SelectColumn selectColumn : deferredViewColumns) {
                    if (selectColumn.getName().equals(column)) {
                        if (selectColumn instanceof SwitchColumn) {
                            selectColumn.initDef(tableReference.getDefinition().getColumnNameMap());
                            selectColumn = ((SwitchColumn) selectColumn).getRealColumn();
                        }

                        // we need to rename this column
                        if (selectColumn instanceof SourceColumn) {
                            // this is a rename of the getSourceName to the innerName
                            renames.put(selectColumn.getName(), ((SourceColumn) selectColumn).getSourceName());
                        } else {
                            postFilter = true;
                            break;
                        }
                    }
                }
            }

            if (postFilter) {
                postViewFilters.add(filter);
            } else {
                if (!renames.isEmpty()) {
                    if (filter instanceof io.deephaven.db.v2.select.MatchFilter) {
                        io.deephaven.db.v2.select.MatchFilter matchFilter =
                                (io.deephaven.db.v2.select.MatchFilter) filter;
                        Assert.assertion(renames.size() == 1, "Match Filters should only use one column!");
                        String newName = renames.get(matchFilter.getColumnName());
                        Assert.neqNull(newName, "newName");
                        preViewFilters.add(matchFilter.renameFilter(newName));
                    } else if (filter instanceof ConditionFilter) {
                        ConditionFilter conditionFilter = (ConditionFilter) filter;
                        preViewFilters.add(conditionFilter.renameFilter(renames));
                    } else {
                        postViewFilters.add(filter);
                    }
                } else {
                    preViewFilters.add(filter);
                }
            }
        }

        return new PreAndPostFilters(preViewFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY),
                postViewFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
    }

    @Override
    protected DynamicTable doCoalesce() {
        Table result;
        if (deferredFilters.length > 0) {
            PreAndPostFilters preAndPostFilters = applyFilterRenamings(deferredFilters);

            TableReference.TableAndRemainingFilters tarf =
                    tableReference.getWithWhere(preAndPostFilters.preViewFilters);
            result = tarf.table;
            result = result.where(tarf.remainingFilters);
            result = result.where(preAndPostFilters.postViewFilters);
        } else {
            result = tableReference.get();
            result = applyDeferredViews(result);
        }
        copyAttributes(result, CopyAttributeOperation.Coalesce);
        return (BaseTable) result;
    }

    @Override
    public Table selectDistinct(SelectColumn... columns) {
        /* If the cachedResult table has already been created, we can just use that. */
        if (getCoalesced() != null) {
            return coalesce().selectDistinct(columns);
        }

        /* If we have any manual filters, then we must coalesce the table. */
        if ((Arrays.stream(deferredFilters).anyMatch(sf -> !sf.isAutomatedFilter()))) {
            return coalesce().selectDistinct(columns);
        }

        /* If we have changed the partitioning columns, we should perform the selectDistinct on the coalesced table. */
        if (deferredViewColumns.length > 0) {
            if (tableReference.getDefinition().getPartitioningColumns().stream().anyMatch(
                    cd -> Arrays.stream(deferredViewColumns).anyMatch(dvc -> dvc.getName().equals(cd.getName())))) {
                return coalesce().selectDistinct(columns);
            }
        }

        /* If the cachedResult is not yet created, we first ask for a selectDistinct cachedResult. */
        Table selectDistinct = tableReference.selectDistinct(columns);
        return selectDistinct == null ? coalesce().selectDistinct(columns) : selectDistinct;
    }

    @Override
    protected Table redefine(TableDefinition newDefinition) {
        ColumnDefinition<?>[] cDefs = newDefinition.getColumns();
        SelectColumn[] newView = new SelectColumn[cDefs.length];
        for (int cdi = 0; cdi < cDefs.length; ++cdi) {
            newView[cdi] = new SourceColumn(cDefs[cdi].getName());
        }
        DeferredViewTable deferredViewTable = new DeferredViewTable(newDefinition, description + "-redefined",
                new SimpleTableReference(this), null, newView, null);
        deferredViewTable.setRefreshing(isRefreshing());
        return deferredViewTable;
    }

    @Override
    protected Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns, Map<String, Set<String>> columnDependency) {
        DeferredViewTable deferredViewTable = new DeferredViewTable(newDefinitionExternal, description + "-redefined",
                new SimpleTableReference(this), null, viewColumns, null);
        deferredViewTable.setRefreshing(isRefreshing());
        return deferredViewTable;
    }

    /**
     * The table reference hides the table underlying table from us.
     */
    public static abstract class TableReference extends LivenessArtifact implements SimpleReference<Table> {

        TableReference(Table t) {
            if (t.isLive()) {
                manage(t);
            }
        }

        /**
         * Returns the table in a form that the user can run queries on it. This may be as simple as returning a
         * reference, but for amorphous tables, this means we need to do the work to instantiate it.
         *
         * @return the table
         */
        public abstract Table get();

        /**
         * Get the definition, without instantiating the table.
         *
         * @return the definition of the table
         */

        public abstract TableDefinition getDefinition();

        /**
         * What size should the uninitialized table return.
         *
         * @return the size
         */
        public abstract long getSize();

        public static class TableAndRemainingFilters {

            public TableAndRemainingFilters(Table table, SelectFilter[] remainingFilters) {
                this.table = table;
                this.remainingFilters = remainingFilters;
            }

            private final Table table;
            private final SelectFilter[] remainingFilters;
        }

        /**
         * Get the table in a form that the user can run queries on it. All of the filters that can be run efficiently
         * should be run before instantiating the full table should be run. Other filters are returned in the
         * remainingFilters field.
         *
         * @param selectFilters filters to maybe apply before returning the table
         * @return the instantiated table and a set of filters that were not applied.
         */
        public TableAndRemainingFilters getWithWhere(SelectFilter... selectFilters) {
            return new TableAndRemainingFilters(get(), selectFilters);
        }

        /**
         * If possible to execute a selectDistinct without instantiating the full table, then do so. Otherwise return
         * null.
         *
         * @param columns the columns to selectDistinct
         * @return null if the operation can not be performed on an uninstantiated table, otherwise a new table with the
         *         distinct values from strColumns.
         */
        public Table selectDistinct(SelectColumn[] columns) {
            return null;
        }

        @Override
        public final void clear() {}
    }

    public static class SimpleTableReference extends TableReference {

        private final Table table;

        public SimpleTableReference(Table table) {
            super(table);
            this.table = table;
        }

        @Override
        public long getSize() {
            return QueryConstants.NULL_LONG;
        }

        @Override
        public TableDefinition getDefinition() {
            return table.getDefinition();
        }

        @Override
        public Table get() {
            return table;
        }
    }
}
