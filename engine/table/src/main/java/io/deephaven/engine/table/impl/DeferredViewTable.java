/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.impl.select.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An uncoalesced table with view and where operations to be applied after {@link #coalesce()} is forced.
 */
public class DeferredViewTable extends RedefinableTable<DeferredViewTable> {

    private final TableReference tableReference;
    protected final String[] deferredDropColumns;
    protected final SelectColumn[] deferredViewColumns;
    protected final WhereFilter[] deferredFilters;

    public DeferredViewTable(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @Nullable final String[] deferredDropColumns,
            @Nullable final SelectColumn[] deferredViewColumns,
            @Nullable final WhereFilter[] deferredFilters) {
        super(definition, description);
        this.tableReference = tableReference;
        this.deferredDropColumns =
                deferredDropColumns == null ? CollectionUtil.ZERO_LENGTH_STRING_ARRAY : deferredDropColumns;
        this.deferredViewColumns =
                deferredViewColumns == null ? SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY : deferredViewColumns;
        final TableDefinition parentDefinition = tableReference.getDefinition();
        SelectAndViewAnalyzer.initializeSelectColumns(
                parentDefinition.getColumnNameMap(), this.deferredViewColumns);
        this.deferredFilters = deferredFilters == null ? WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY : deferredFilters;
        for (final WhereFilter sf : this.deferredFilters) {
            sf.init(parentDefinition);
            if (sf instanceof LivenessReferent) {
                manage((LivenessReferent) sf);
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
        WhereFilter[] whereFilters = WhereFilter.from(filters);
        for (WhereFilter filter : whereFilters) {
            filter.init(definition);
        }
        return getResultTableWithWhere(whereFilters);
    }

    private Table getResultTableWithWhere(WhereFilter... whereFilters) {
        if (getCoalesced() != null) {
            return coalesce().where(whereFilters);
        }

        final WhereFilter[] allFilters = Stream.concat(Arrays.stream(deferredFilters), Arrays.stream(whereFilters))
                .map(WhereFilter::copy).toArray(WhereFilter[]::new);

        TableReference.TableAndRemainingFilters tableAndRemainingFilters;
        if (allFilters.length == 0) {
            tableAndRemainingFilters = tableReference.getWithWhere();
            Table result = tableAndRemainingFilters.table;
            result = applyDeferredViews(result);
            result = result.where(tableAndRemainingFilters.remainingFilters);
            copyAttributes((BaseTable<?>) result, CopyAttributeOperation.Coalesce);
            setCoalesced(result);
            return result;
        }

        PreAndPostFilters preAndPostFilters = applyFilterRenamings(allFilters);
        tableAndRemainingFilters = tableReference.getWithWhere(preAndPostFilters.preViewFilters);

        Table localResult = tableAndRemainingFilters.table;
        if (localResult instanceof DeferredViewTable) {
            localResult = ((DeferredViewTable) localResult)
                    .getResultTableWithWhere(tableAndRemainingFilters.remainingFilters);
        } else {
            localResult = localResult.where(WhereFilter.copyFrom(tableAndRemainingFilters.remainingFilters));
        }

        localResult = applyDeferredViews(localResult);
        if (preAndPostFilters.postViewFilters.length > 0) {
            localResult = localResult.where(preAndPostFilters.postViewFilters);
        }
        if (whereFilters.length == 0) {
            copyAttributes((BaseTable<?>) localResult, CopyAttributeOperation.Coalesce);
            setCoalesced(localResult);
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
            result = result.view(SelectColumn.copyFrom(deferredViewColumns));
        }
        return result;
    }

    private static class PreAndPostFilters {

        private final WhereFilter[] preViewFilters;
        private final WhereFilter[] postViewFilters;

        private PreAndPostFilters(WhereFilter[] preViewFilters, WhereFilter[] postViewFilters) {
            this.preViewFilters = preViewFilters;
            this.postViewFilters = postViewFilters;
        }
    }

    private PreAndPostFilters applyFilterRenamings(WhereFilter[] filters) {
        ArrayList<WhereFilter> preViewFilters = new ArrayList<>();
        ArrayList<WhereFilter> postViewFilters = new ArrayList<>();

        final Set<String> postViewColumns = new HashSet<>();
        final Map<String, String> renames = new HashMap<>();

        // prepare data structures needed to determine if filter can be applied pre-view or if it must be post-view
        for (SelectColumn selectColumn : deferredViewColumns) {
            final String outerName = selectColumn.getName();
            if (selectColumn.isRetain()) {
                continue;
            }
            if (selectColumn instanceof SwitchColumn) {
                selectColumn = ((SwitchColumn) selectColumn).getRealColumn();
            }

            // This column is being defined; whatever we currently believe might be wrong
            renames.remove(outerName);
            postViewColumns.remove(outerName);

            if (selectColumn instanceof SourceColumn) {
                // this is a renamed column
                final String innerName = ((SourceColumn) selectColumn).getSourceName();
                final String sourceName = renames.getOrDefault(innerName, innerName);

                if (postViewColumns.contains(sourceName)) {
                    // but it is renamed to a deferred column
                    postViewColumns.add(outerName);
                } else {
                    // this is renamed to a real source column
                    renames.put(outerName, sourceName);
                }
            } else {
                // this is a deferred column
                postViewColumns.add(outerName);
            }
        }

        for (final WhereFilter filter : filters) {
            filter.init(definition);

            final boolean isPostView = Stream.of(filter.getColumns(), filter.getColumnArrays())
                    .flatMap(Collection::stream)
                    .anyMatch(postViewColumns::contains);
            if (isPostView) {
                postViewFilters.add(filter);
                continue;
            }

            final Map<String, String> myRenames = Stream.of(filter.getColumns(), filter.getColumnArrays())
                    .flatMap(Collection::stream)
                    .filter(renames::containsKey)
                    .collect(Collectors.toMap(Function.identity(), renames::get));

            if (myRenames.isEmpty()) {
                preViewFilters.add(filter);
            } else if (filter instanceof MatchFilter) {
                MatchFilter matchFilter = (MatchFilter) filter;
                Assert.assertion(myRenames.size() == 1, "Match Filters should only use one column!");
                String newName = myRenames.get(matchFilter.getColumnName());
                Assert.neqNull(newName, "newName");
                preViewFilters.add(matchFilter.renameFilter(newName));
            } else if (filter instanceof ConditionFilter) {
                ConditionFilter conditionFilter = (ConditionFilter) filter;
                preViewFilters.add(conditionFilter.renameFilter(myRenames));
            } else {
                postViewFilters.add(filter);
            }
        }

        return new PreAndPostFilters(preViewFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY),
                postViewFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
    }

    @Override
    protected Table doCoalesce() {
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
        copyAttributes((BaseTable<?>) result, CopyAttributeOperation.Coalesce);
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
    protected DeferredViewTable copy() {
        final DeferredViewTable result = new DeferredViewTable(definition, description, new SimpleTableReference(this),
                null, null, null);
        result.setRefreshing(isRefreshing());
        LiveAttributeMap.copyAttributes(this, result, ak -> true);
        return result;
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

            public TableAndRemainingFilters(Table table, WhereFilter[] remainingFilters) {
                this.table = table;
                this.remainingFilters = remainingFilters;
            }

            private final Table table;
            private final WhereFilter[] remainingFilters;
        }

        /**
         * Get the table in a form that the user can run queries on it. All of the filters that can be run efficiently
         * should be run before instantiating the full table should be run. Other filters are returned in the
         * remainingFilters field.
         *
         * @param whereFilters filters to maybe apply before returning the table
         * @return the instantiated table and a set of filters that were not applied.
         */
        protected TableAndRemainingFilters getWithWhere(WhereFilter... whereFilters) {
            return new TableAndRemainingFilters(get(), whereFilters);
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
