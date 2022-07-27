/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.ExecutionContextImpl;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.impl.select.*;
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
    protected final WhereFilter[] deferredFilters;
    private final ExecutionContext deferredExecutionContext;

    public DeferredViewTable(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @Nullable final String[] deferredDropColumns,
            @Nullable final SelectColumn[] deferredViewColumns,
            @Nullable final WhereFilter[] deferredFilters) {
        super(definition, description);
        this.deferredExecutionContext = ExecutionContextImpl.makeSystemicExecutionContext();
        this.tableReference = tableReference;
        this.deferredDropColumns =
                deferredDropColumns == null ? CollectionUtil.ZERO_LENGTH_STRING_ARRAY : deferredDropColumns;
        this.deferredViewColumns =
                deferredViewColumns == null ? SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY : deferredViewColumns;
        this.deferredFilters = deferredFilters == null ? WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY : deferredFilters;
        if (deferredFilters != null) {
            for (final WhereFilter sf : deferredFilters) {
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
    public Table where(Collection<? extends Filter> filters) {
        final ExecutionContext whereExecutionContext = ExecutionContextImpl.makeSystemicExecutionContext();
        WhereFilter[] whereFilters = WhereFilter.from(filters);
        final ExecutionContext[] executionContexts = new ExecutionContext[whereFilters.length];
        Arrays.fill(executionContexts, whereExecutionContext);
        return getResultTableWithWhere(whereFilters, executionContexts);
    }

    private Table getResultTableWithWhere(WhereFilter[] whereFilters, ExecutionContext[] whereExecutionContexts) {
        if (getCoalesced() != null) {
            return doApplyWhereFilters(coalesce(), whereFilters, whereExecutionContexts);
        }

        if (deferredFilters.length == 0 && whereFilters.length == 0) {
            return deferredExecutionContext.apply(() -> {
                TableReference.TableAndRemainingFilters tableAndRemainingFilters = tableReference.getWithWhere(
                        WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY,
                        ExecutionContext.ZERO_LENGTH_EXECUTION_CONTEXT_ARRAY);
                Table result = tableAndRemainingFilters.table;
                result = applyDeferredViews(result);
                result = result.where(tableAndRemainingFilters.remainingFilters);
                copyAttributes(result, CopyAttributeOperation.Coalesce);
                setCoalesced(result);
                return result;
            });
        }

        final WhereFilter[] allFilters = Stream.concat(Arrays.stream(deferredFilters), Arrays.stream(whereFilters))
                .map(WhereFilter::copy).toArray(WhereFilter[]::new);

        // initialize execution context list
        final ExecutionContext[] executionContexts = new ExecutionContext[allFilters.length];
        Arrays.fill(executionContexts, 0, deferredFilters.length, deferredExecutionContext);
        System.arraycopy(whereExecutionContexts, 0,
                executionContexts, deferredFilters.length, whereExecutionContexts.length);

        // Split into pre-view/post-view filters
        final PreAndPostFilters filters = applyFilterRenamings(allFilters, executionContexts);

        final TableReference.TableAndRemainingFilters tableAndRemainingFilters =
                tableReference.getWithWhere(filters.preViewFilters, filters.preExecutionContexts);

        // apply pre-view filters
        Table localResult = tableAndRemainingFilters.table;
        if (localResult instanceof DeferredViewTable) {
            localResult = ((DeferredViewTable) localResult)
                    .getResultTableWithWhere(
                            tableAndRemainingFilters.remainingFilters,
                            tableAndRemainingFilters.remainingExecutionContexts);
        } else {
            localResult = doApplyWhereFilters(
                    localResult,
                    tableAndRemainingFilters.remainingFilters,
                    tableAndRemainingFilters.remainingExecutionContexts);
        }

        // apply deferred views
        final Table finalLocalResult = localResult;
        localResult = deferredExecutionContext.apply(() -> applyDeferredViews(finalLocalResult));

        // apply post-view filters
        if (filters.postViewFilters.length > 0) {
            localResult = doApplyWhereFilters(localResult, filters.postViewFilters, filters.postExecutionContexts);
        }

        // if there were no additional filters, we can cache the result
        if (whereFilters.length == 0) {
            copyAttributes(localResult, CopyAttributeOperation.Coalesce);
            setCoalesced(localResult);
        }
        return localResult;
    }

    public static Table doApplyWhereFilters(
            Table result,
            WhereFilter[] whereFilters,
            ExecutionContext[] whereExecutionContexts) {
        int firstIndex = 0;
        for (int ii = 1; ii <= whereFilters.length; ii++) {
            if (ii < whereFilters.length && whereExecutionContexts[ii] == whereExecutionContexts[ii - 1]) {
                continue;
            }

            final boolean isAllFilters = firstIndex == 0 && ii == whereFilters.length;
            final WhereFilter[] sharedFilters = isAllFilters ? whereFilters
                    : Arrays.copyOfRange(whereFilters, firstIndex, ii);
            // TODO (review): do I need to WhereFilter#copy here?
            final Table finalResult = result;
            result = whereExecutionContexts[firstIndex].apply(() -> finalResult.where(sharedFilters));
            firstIndex = ii;
        }

        return result;
    }

    private Table applyDeferredViews(Table result) {
        if (result instanceof DeferredViewTable) {
            result = result.coalesce();
        }
        if (deferredDropColumns.length > 0) {
            result = result.dropColumns(deferredDropColumns);
        }
        if (deferredViewColumns.length > 0) {
            result = result.view(Arrays.stream(deferredViewColumns)
                    .map(SelectColumn::copy)
                    .toArray(SelectColumn[]::new));
        }
        return result;
    }

    private static class PreAndPostFilters {

        private final WhereFilter[] preViewFilters;
        private final ExecutionContext[] preExecutionContexts;
        private final WhereFilter[] postViewFilters;
        private final ExecutionContext[] postExecutionContexts;

        private PreAndPostFilters(
                WhereFilter[] preViewFilters,
                ExecutionContext[] preExecutionContexts,
                WhereFilter[] postViewFilters,
                ExecutionContext[] postExecutionContexts) {
            this.preViewFilters = preViewFilters;
            this.preExecutionContexts = preExecutionContexts;
            this.postViewFilters = postViewFilters;
            this.postExecutionContexts = postExecutionContexts;
        }
    }

    private PreAndPostFilters applyFilterRenamings(WhereFilter[] filters, ExecutionContext[] executionContexts) {
        ArrayList<WhereFilter> preViewFilters = new ArrayList<>();
        ArrayList<ExecutionContext> preViewExecutionContexts = new ArrayList<>();
        ArrayList<WhereFilter> postViewFilters = new ArrayList<>();
        ArrayList<ExecutionContext> postViewExecutionContexts = new ArrayList<>();

        final Map<String, SelectColumn> deferredColumnMap = new HashMap<>();
        for (SelectColumn sc : deferredViewColumns) {
            deferredColumnMap.put(sc.getName(), sc);
        }

        for (int ii = 0; ii < filters.length; ++ii) {
            final WhereFilter filter = filters[ii];
            final ExecutionContext filterExecutionContext = executionContexts[ii];
            executionContexts[ii].apply(() -> {
                filter.init(definition);
            });
            ArrayList<String> usedColumns = new ArrayList<>();
            usedColumns.addAll(filter.getColumns());
            usedColumns.addAll(filter.getColumnArrays());

            Map<String, String> renames = new HashMap<>();
            boolean postFilter = false;

            for (String column : usedColumns) {
                SelectColumn selectColumn = deferredColumnMap.get(column);
                if (selectColumn == null) {
                    continue;
                }

                if (selectColumn instanceof SwitchColumn) {
                    final SwitchColumn switchColumn = (SwitchColumn) selectColumn;
                    deferredExecutionContext.apply(() -> {
                        switchColumn.initDef(tableReference.getDefinition().getColumnNameMap());
                    });
                    selectColumn = switchColumn.getRealColumn();
                }

                // we need to rename this column
                if (selectColumn instanceof SourceColumn) {
                    // this is a re-name of the getSourceName to the innerName
                    renames.put(selectColumn.getName(), ((SourceColumn) selectColumn).getSourceName());
                } else {
                    postFilter = true;
                    break;
                }
            }

            if (postFilter) {
                postViewFilters.add(filter);
                postViewExecutionContexts.add(filterExecutionContext);
            } else if (renames.isEmpty()) {
                preViewFilters.add(filter);
                preViewExecutionContexts.add(filterExecutionContext);
            } else if (filter instanceof MatchFilter) {
                MatchFilter matchFilter = (MatchFilter) filter;
                Assert.assertion(renames.size() == 1, "Match Filters should only use one column!");
                String newName = renames.get(matchFilter.getColumnName());
                Assert.neqNull(newName, "newName");
                preViewFilters.add(matchFilter.renameFilter(newName));
                preViewExecutionContexts.add(filterExecutionContext);
            } else if (filter instanceof ConditionFilter) {
                ConditionFilter conditionFilter = (ConditionFilter) filter;
                preViewFilters.add(conditionFilter.renameFilter(renames));
                preViewExecutionContexts.add(filterExecutionContext);
            } else {
                postViewFilters.add(filter);
                postViewExecutionContexts.add(filterExecutionContext);
            }
        }

        return new PreAndPostFilters(
                preViewFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY),
                preViewExecutionContexts.toArray(ExecutionContext.ZERO_LENGTH_EXECUTION_CONTEXT_ARRAY),
                postViewFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY),
                postViewExecutionContexts.toArray(ExecutionContext.ZERO_LENGTH_EXECUTION_CONTEXT_ARRAY));
    }

    @Override
    protected Table doCoalesce() {
        Table result;
        if (deferredFilters.length > 0) {
            final ExecutionContext[] executionContexts = new ExecutionContext[deferredFilters.length];
            Arrays.fill(executionContexts, deferredExecutionContext);
            final PreAndPostFilters filters = applyFilterRenamings(deferredFilters, executionContexts);

            TableReference.TableAndRemainingFilters tarf =
                    tableReference.getWithWhere(filters.preViewFilters, filters.preExecutionContexts);
            result = tarf.table;
            result = doApplyWhereFilters(result, tarf.remainingFilters, tarf.remainingExecutionContexts);
            result = doApplyWhereFilters(result, filters.postViewFilters, filters.postExecutionContexts);
        } else {
            result = deferredExecutionContext.apply(() -> applyDeferredViews(tableReference.get()));
        }
        copyAttributes(result, CopyAttributeOperation.Coalesce);
        return result;
    }

    @Override
    public Table selectDistinct(Collection<? extends Selectable> columns) {
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
        Table selectDistinct = tableReference.selectDistinctInternal(columns);
        return selectDistinct == null ? coalesce().selectDistinct(columns) : selectDistinct;
    }

    @Override
    protected Table redefine(TableDefinition newDefinition) {
        final List<ColumnDefinition<?>> cDefs = newDefinition.getColumns();
        SelectColumn[] newView = new SelectColumn[cDefs.size()];
        for (int cdi = 0; cdi < newView.length; ++cdi) {
            newView[cdi] = new SourceColumn(cDefs.get(cdi).getName());
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
            if (t.isRefreshing()) {
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

            public TableAndRemainingFilters(
                    Table table,
                    WhereFilter[] remainingFilters,
                    ExecutionContext[] executionContexts) {
                this.table = table;
                this.remainingFilters = remainingFilters;
                this.remainingExecutionContexts = executionContexts;
            }

            private final Table table;
            private final WhereFilter[] remainingFilters;
            private final ExecutionContext[] remainingExecutionContexts;
        }

        /**
         * Get the table in a form that the user can run queries on it. All of the filters that can be run efficiently
         * should be run before instantiating the full table should be run. Other filters are returned in the
         * remainingFilters field.
         *
         * @param whereFilters filters to maybe apply before returning the table
         * @return the instantiated table and a set of filters that were not applied.
         */
        public TableAndRemainingFilters getWithWhere(WhereFilter[] whereFilters, ExecutionContext[] executionContexts) {
            return new TableAndRemainingFilters(get(), whereFilters, executionContexts);
        }

        /**
         * If possible to execute a selectDistinct without instantiating the full table, then do so. Otherwise return
         * null.
         *
         * @param columns the columns to selectDistinct
         * @return null if the operation can not be performed on an uninstantiated table, otherwise a new table with the
         *         distinct values from strColumns.
         */
        public Table selectDistinctInternal(Collection<? extends Selectable> columns) {
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
