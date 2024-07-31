//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.util.type.ArrayTypeUtils;
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
                deferredDropColumns == null ? ArrayTypeUtils.EMPTY_STRING_ARRAY : deferredDropColumns;
        this.deferredViewColumns =
                deferredViewColumns == null ? SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY : deferredViewColumns;
        final TableDefinition parentDefinition = tableReference.getDefinition();
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        SelectAndViewAnalyzer.initializeSelectColumns(
                parentDefinition.getColumnNameMap(), this.deferredViewColumns, compilationProcessor);
        this.deferredFilters = deferredFilters == null ? WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY : deferredFilters;
        for (final WhereFilter sf : this.deferredFilters) {
            sf.init(parentDefinition, compilationProcessor);
            if (sf instanceof LivenessReferent && sf.isRefreshing()) {
                manage((LivenessReferent) sf);
                setRefreshing(true);
            }
        }
        compilationProcessor.compile();

        // we really only expect one of these things to be set!
        final boolean haveDrop = this.deferredDropColumns.length > 0;
        final boolean haveView = this.deferredViewColumns.length > 0;
        final boolean haveFilter = this.deferredFilters.length > 0;

        if (tableReference.isRefreshing()) {
            manage(tableReference);
            setRefreshing(true);
        }

        if (haveDrop && haveFilter) {
            throw new IllegalStateException("Why do we have a drop and a filter all at the same time?");
        }
        if (haveView && haveFilter) {
            throw new IllegalStateException("Why do we have a view and a filter all at the same time?");
        }
    }

    @Override
    public Table where(Filter filter) {
        final WhereFilter[] whereFilters = WhereFilter.fromInternal(filter);
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        for (WhereFilter f : whereFilters) {
            f.init(definition, compilationProcessor);
        }
        compilationProcessor.compile();
        return getResultTableWithWhere(whereFilters);
    }

    private Table getResultTableWithWhere(WhereFilter... whereFilters) {
        {
            final Table coalesced = getCoalesced();
            if (Liveness.verifyCachedObjectForReuse(coalesced)) {
                return coalesced.where(Filter.and(whereFilters));
            }
        }

        final WhereFilter[] allFilters = Stream.concat(
                Arrays.stream(deferredFilters).map(WhereFilter::copy),
                Arrays.stream(whereFilters))
                .toArray(WhereFilter[]::new);

        if (allFilters.length == 0) {
            Table result = tableReference.get();
            result = applyDeferredViews(result);
            copyAttributes((BaseTable<?>) result, CopyAttributeOperation.Coalesce);
            setCoalesced(result);
            return result;
        }

        final PreAndPostFilters preAndPostFilters = applyFilterRenamings(allFilters);
        final TableReference.TableAndRemainingFilters tableAndRemainingFilters =
                tableReference.getWithWhere(preAndPostFilters.preViewFilters);

        Table localResult = tableAndRemainingFilters.table;
        if (tableAndRemainingFilters.remainingFilters.length != 0) {
            localResult = localResult.where(Filter.and(tableAndRemainingFilters.remainingFilters));
        }

        localResult = applyDeferredViews(localResult);
        if (preAndPostFilters.postViewFilters.length > 0) {
            localResult = localResult.where(Filter.and(preAndPostFilters.postViewFilters));
        }

        if (whereFilters.length == 0) {
            // The result is effectively the same as if we called doCoalesce()
            copyAttributes((BaseTable<?>) localResult, CopyAttributeOperation.Coalesce);
            setCoalesced(localResult);
        }
        return localResult;
    }

    private Table applyDeferredViews(Table result) {
        if (deferredDropColumns.length > 0) {
            result = result.dropColumns(deferredDropColumns);
        }
        if (deferredViewColumns.length > 0) {
            result = result.view(List.of(SelectColumn.copyFrom(deferredViewColumns)));
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

        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        for (final WhereFilter filter : filters) {
            filter.init(definition, compilationProcessor);

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
                final MatchFilter matchFilter = (MatchFilter) filter;
                Assert.assertion(myRenames.size() == 1, "Match Filters should only use one column!");
                final WhereFilter newFilter = matchFilter.renameFilter(myRenames);
                newFilter.init(tableReference.getDefinition(), compilationProcessor);
                preViewFilters.add(newFilter);
            } else if (filter instanceof ConditionFilter) {
                final ConditionFilter conditionFilter = (ConditionFilter) filter;
                final ConditionFilter newFilter = conditionFilter.renameFilter(myRenames);
                newFilter.init(tableReference.getDefinition(), compilationProcessor);
                preViewFilters.add(newFilter);
            } else {
                postViewFilters.add(filter);
            }
        }
        compilationProcessor.compile();

        return new PreAndPostFilters(preViewFilters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY),
                postViewFilters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY));
    }

    @Override
    protected Table doCoalesce() {
        Table result;
        if (deferredFilters.length > 0) {
            final PreAndPostFilters preAndPostFilters = applyFilterRenamings(WhereFilter.copyFrom(deferredFilters));
            final TableReference.TableAndRemainingFilters tarf =
                    tableReference.getWithWhere(preAndPostFilters.preViewFilters);
            result = tarf.table;
            if (tarf.remainingFilters.length != 0) {
                result = result.where(Filter.and(tarf.remainingFilters));
            }
            result = applyDeferredViews(result);
            if (preAndPostFilters.postViewFilters.length > 0) {
                result = result.where(Filter.and(preAndPostFilters.postViewFilters));
            }
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
        {
            final Table coalesced = getCoalesced();
            if (Liveness.verifyCachedObjectForReuse(coalesced)) {
                return coalesced.selectDistinct(columns);
            }
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
        final DeferredViewTable result = new DeferredViewTable(definition, description, new TableReference(this),
                null, null, null);
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
        return new DeferredViewTable(newDefinition, description + "-redefined",
                new TableReference(this), null, newView, null);
    }

    @Override
    protected Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns) {
        return new DeferredViewTable(newDefinitionExternal, description + "-redefined",
                new TableReference(this), null, viewColumns, null);
    }

    /**
     * The table reference hides the table underlying table from us.
     */
    public static class TableReference extends LivenessArtifact {

        protected final Table table;

        private final boolean isRefreshing;

        TableReference(Table t) {
            this.table = t;
            isRefreshing = t.isRefreshing();
            if (isRefreshing) {
                manage(t);
            }
        }

        /**
         * Is the node updating?
         *
         * @return true if the node is updating; false otherwise.
         */
        public final boolean isRefreshing() {
            return isRefreshing;
        }

        /**
         * Returns the table in a form that the user can run queries on it. This may be as simple as returning a
         * reference, but for uncoalesced tables, this means we need to do the work to instantiate it.
         *
         * @return the table
         */
        public Table get() {
            return table.coalesce();
        }

        /**
         * Get the definition, without instantiating the table.
         *
         * @return the definition of the table
         */

        public TableDefinition getDefinition() {
            return table.getDefinition();
        }

        public static class TableAndRemainingFilters {

            public TableAndRemainingFilters(Table table, WhereFilter[] remainingFilters) {
                this.table = table;
                this.remainingFilters = remainingFilters;
            }

            private final Table table;
            private final WhereFilter[] remainingFilters;
        }

        /**
         * Get the table in a form that the user can run queries on it. All the filters that can be run efficiently
         * should be run before coalescing the full table should be run. Other filters are returned in the
         * remainingFilters field.
         *
         * @param whereFilters filters to maybe apply before returning the table
         * @return the instantiated table and a set of filters that were not applied.
         */
        protected TableAndRemainingFilters getWithWhere(WhereFilter... whereFilters) {
            return new TableAndRemainingFilters(get(), whereFilters);
        }

        /**
         * If possible to execute a selectDistinct without instantiating the full table, then do so. Otherwise, return
         * null.
         *
         * @param columns the columns to selectDistinct
         * @return null if the operation can not be performed on an uncoalesced table, otherwise a new table with the
         *         distinct values from strColumns.
         */
        public Table selectDistinctInternal(Collection<? extends Selectable> columns) {
            return null;
        }
    }
}
