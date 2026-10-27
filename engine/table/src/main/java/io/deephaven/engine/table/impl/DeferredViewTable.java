//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.filter.ExtractBarriers;
import io.deephaven.engine.table.impl.filter.ExtractInnerConjunctiveFilters;
import io.deephaven.engine.table.impl.filter.ExtractRespectedBarriers;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
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
 * <p>
 * Each DeferredViewTable defers at most one kind of operation: dropped columns, view columns, a rename or filters. A
 * further operation wraps the table in another DeferredViewTable. The static factories, one per kind of operation, are
 * the only way to create a DeferredViewTable.
 */
public class DeferredViewTable extends RedefinableTable<DeferredViewTable> {

    private final TableReference tableReference;
    protected final String[] deferredDropColumns;
    protected final SelectColumn[] deferredViewColumns;
    protected final WhereFilter[] deferredFilters;
    /**
     * Whether this table holds nothing but the columns of one {@link Table#renameColumns(Collection) renameColumns}
     * call. When true, {@link #deferredViewColumns} hold one {@link SourceColumn} per column of the result, and the
     * columns apply simultaneously rather than in order; {@link #deferredDropColumns} and {@link #deferredFilters} are
     * empty. A later operation never adds to a renaming table, but wraps the renaming table in another
     * DeferredViewTable. The rename columns share {@link #deferredViewColumns} with view columns, so that every reader
     * of the view columns, such as {@link #selectDistinct(Collection)}, also sees the rename columns.
     */
    private final boolean isRenameOnly;

    /**
     * Create a DeferredViewTable that defers no operation, and so coalesces to the referenced table.
     *
     * @param definition The definition of the new table
     * @param description A description of the new table
     * @param tableReference The reference to the table to wrap
     * @param attributes The attributes map to use, or else {@code null} to allocate a new attributes map
     * @return the new table
     */
    static DeferredViewTable wrap(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @Nullable final Map<String, Object> attributes) {
        return new DeferredViewTable(definition, description, tableReference, null, null, null, null, attributes);
    }

    /**
     * Create a DeferredViewTable that drops columns of the referenced table.
     *
     * @param definition The definition of the new table
     * @param description A description of the new table
     * @param tableReference The reference to the table to drop columns from
     * @param dropColumns The names of the columns to drop
     * @return the new table
     */
    static DeferredViewTable withDropColumns(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @NotNull final String[] dropColumns) {
        return new DeferredViewTable(definition, description, tableReference, dropColumns, null, null, null, null);
    }

    /**
     * Create a DeferredViewTable that applies view columns to the referenced table. The view columns apply in order, as
     * in {@link Table#view(Collection)}: each view column may read the result of an earlier view column.
     *
     * @param definition The definition of the new table
     * @param description A description of the new table
     * @param tableReference The reference to the table to view
     * @param viewColumns The view columns to apply
     * @return the new table
     */
    static DeferredViewTable withView(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @NotNull final SelectColumn[] viewColumns) {
        return new DeferredViewTable(definition, description, tableReference, null, viewColumns, null, null, null);
    }

    /**
     * Create a DeferredViewTable that filters the referenced table.
     *
     * @param definition The definition of the new table
     * @param description A description of the new table
     * @param tableReference The reference to the table to filter
     * @param filters The filters to apply
     * @return the new table
     */
    static DeferredViewTable withFilters(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @NotNull final WhereFilter[] filters) {
        return new DeferredViewTable(definition, description, tableReference, null, null, null, filters, null);
    }

    /**
     * Create a DeferredViewTable that renames the columns of the referenced table. Unlike the view columns of
     * {@link #withView}, which apply in order, the rename columns apply simultaneously, as
     * {@link Table#renameColumns(Collection)} requires.
     *
     * @param definition The definition of the renamed table
     * @param description A description of the renamed table
     * @param tableReference The reference to the table to rename
     * @param renameColumns One SourceColumn per column of {@code definition}, in the same order
     * @return the renamed table
     */
    static DeferredViewTable withRename(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @NotNull final SourceColumn[] renameColumns) {
        return new DeferredViewTable(definition, description, tableReference, null, null, renameColumns, null, null);
    }

    private DeferredViewTable(@NotNull final TableDefinition definition,
            @NotNull final String description,
            @NotNull final TableReference tableReference,
            @Nullable final String[] deferredDropColumns,
            @Nullable final SelectColumn[] deferredViewColumns,
            @Nullable final SourceColumn[] deferredRenameColumns,
            @Nullable final WhereFilter[] deferredFilters,
            @Nullable final Map<String, Object> attributes) {
        super(definition, description, attributes);
        this.tableReference = tableReference;

        // each factory supplies at most one kind of operation; check before compiling or managing anything
        final boolean haveDrop = deferredDropColumns != null && deferredDropColumns.length > 0;
        final boolean haveView = deferredViewColumns != null && deferredViewColumns.length > 0;
        final boolean haveRename = deferredRenameColumns != null && deferredRenameColumns.length > 0;
        final boolean haveFilter = deferredFilters != null && deferredFilters.length > 0;
        Assert.assertion((haveDrop ? 1 : 0) + (haveView ? 1 : 0) + (haveRename ? 1 : 0) + (haveFilter ? 1 : 0) <= 1,
                "at most one of haveDrop, haveView, haveRename and haveFilter",
                haveDrop, "haveDrop", haveView, "haveView", haveRename, "haveRename", haveFilter, "haveFilter");

        this.deferredDropColumns =
                deferredDropColumns == null ? ArrayTypeUtils.EMPTY_STRING_ARRAY : deferredDropColumns;
        this.isRenameOnly = haveRename;
        if (haveRename) {
            this.deferredViewColumns = deferredRenameColumns;
        } else {
            this.deferredViewColumns =
                    deferredViewColumns == null ? SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY : deferredViewColumns;
        }
        this.deferredFilters = deferredFilters == null ? WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY : deferredFilters;

        final TableDefinition parentDefinition = tableReference.getDefinition();
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        if (isRenameOnly) {
            // every rename column reads the parent's column, never the result of another rename column
            for (final SelectColumn renameColumn : this.deferredViewColumns) {
                renameColumn.initDef(parentDefinition.getColumnNameMap(), compilationProcessor);
            }
        } else {
            SelectAndViewAnalyzer.initializeSelectColumns(
                    parentDefinition.getColumnNameMap(), this.deferredViewColumns, compilationProcessor);
        }
        for (final WhereFilter sf : this.deferredFilters) {
            sf.init(parentDefinition, compilationProcessor);
            if (sf instanceof LivenessReferent && sf.isRefreshing()) {
                manage((LivenessReferent) sf);
                setRefreshing(true);
            }
        }
        compilationProcessor.compile();

        if (tableReference.isRefreshing()) {
            manage(tableReference);
            setRefreshing(true);
        }
    }

    @Override
    public Table where(Filter filter) {
        final WhereFilter[] whereFilters = WhereFilter.fromInternal(filter);

        final WhereFilter[] innerFilters = Arrays.stream(whereFilters)
                .flatMap(ExtractInnerConjunctiveFilters::stream)
                .toArray(WhereFilter[]::new);

        if (innerFilters.length == 0) {
            return coalesce();
        }

        // only the pre-view filters are passed down to the coalesce check and enable automatic coalescing
        final PreAndPostFilters preAndPostFilters = applyFilterRenamings(WhereFilter.copyFrom(innerFilters));
        if (tableReference.shouldCoalesce(preAndPostFilters.preViewFilters)) {
            final WhereFilter[] allFilters = concat(deferredFilters, innerFilters);
            SplitAndApply splitAndApply = splitAndApplyFilters(allFilters, tableReference);
            Table result = splitAndApply.result;
            if (splitAndApply.postViewFilters.length != 0) {
                result = result.where(Filter.and(splitAndApply.postViewFilters));
            }
            return result;
        }
        return withFilters(getDefinition(), getDescription() + "-filtered",
                new CopiedTableReference(this, tableReference), innerFilters);
    }

    @NotNull
    private static WhereFilter[] concat(WhereFilter[] filters1, WhereFilter[] filters2) {
        if (filters1.length == 0) {
            return filters2;
        }
        if (filters2.length == 0) {
            return filters1;
        }
        final WhereFilter[] allFilters = Arrays.copyOf(filters1, filters1.length + filters2.length);
        System.arraycopy(filters2, 0, allFilters, filters1.length, filters2.length);
        return allFilters;
    }

    private Table applyDeferredViews(Table result) {
        if (deferredDropColumns.length > 0) {
            result = result.dropColumns(deferredDropColumns);
        }
        if (deferredViewColumns.length > 0) {
            if (isRenameOnly) {
                // rename the coalesced table
                result = result.renameColumns(Arrays.stream(deferredViewColumns)
                        .filter(renameColumn -> !renameColumn.isRetain())
                        .map(renameColumn -> Pair.of(
                                ColumnName.of(((SourceColumn) renameColumn).getSourceName()),
                                ColumnName.of(renameColumn.getName())))
                        .collect(Collectors.toList()));
            } else {
                result = result.view(List.of(SelectColumn.copyFrom(deferredViewColumns)));
            }
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
            if (isRenameOnly) {
                // a rename column reads the parent's column, never the result of an earlier rename column
                renames.put(outerName, ((SourceColumn) selectColumn).getSourceName());
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
        boolean serialFilterFound = false;
        final Set<Object> postViewBarriers = new HashSet<>();
        for (final WhereFilter filter : filters) {
            filter.init(definition, compilationProcessor);

            final boolean isPostView = Stream.of(filter.getColumns(), filter.getColumnArrays())
                    .flatMap(Collection::stream)
                    .anyMatch(postViewColumns::contains);

            final boolean hasPostViewBarrier =
                    ExtractRespectedBarriers.stream(filter).anyMatch(postViewBarriers::contains);
            if (isPostView || serialFilterFound || hasPostViewBarrier) {
                // if this filter is serial, all subsequent filters must be postViewFilters
                if (filter.isSerial()) {
                    serialFilterFound = true;
                }
                postViewFilters.add(filter);
                postViewBarriers.addAll(ExtractBarriers.of(filter));
                continue;
            }

            // a filter that uses a column both as a value and as an array, e.g. "X == X_[0]", reports the column in
            // both lists
            final Map<String, String> myRenames = Stream.of(filter.getColumns(), filter.getColumnArrays())
                    .flatMap(Collection::stream)
                    .filter(renames::containsKey)
                    .distinct()
                    .collect(Collectors.toMap(Function.identity(), renames::get));

            if (myRenames.isEmpty()) {
                preViewFilters.add(filter);
                continue;
            }

            // Single filters rename through RenameableFilter; this visitor handles wrappers and AND/OR itself, see
            // RenameableFilter.
            final WhereFilter preFilter = filter.walk(new WhereFilter.Visitor<>() {
                @Override
                public WhereFilter visitOther(WhereFilter filter) {
                    if (ConditionFilter.extractConditionFilter(filter)
                            .map(AbstractConditionFilter::hasConstantArrayAccess)
                            .orElse(false)) {
                        // a filter with constant array access, such as A_[i - 1], is rebuilt from the formula text
                        // where the filter is executed (ShiftedColumnsFactory.where), which drops the renames, so
                        // the filter stays after the view
                        return null;
                    }
                    if (filter instanceof RenameableFilter renameable) {
                        return renameable.renameFilter(myRenames);
                    }
                    return null;
                }

                @Override
                public WhereFilter visit(WhereFilterInvertedImpl filter) {
                    final WhereFilter innerPreFilter = filter.getWrappedFilter().walk(this);
                    return innerPreFilter == null
                            ? null
                            : WhereFilterInvertedImpl.of(innerPreFilter);
                }

                @Override
                public WhereFilter visit(WhereFilterSerialImpl filter) {
                    // serial filters cannot be run out of order w.r.t. the set of filters that come before and the
                    // set of filters that come after.
                    return null;
                }

                @Override
                public WhereFilter visit(WhereFilterWithDeclaredBarriersImpl filter) {
                    final WhereFilter innerPreFilter = filter.getWrappedFilter().walk(this);
                    return innerPreFilter == null
                            ? null
                            : WhereFilterWithDeclaredBarriersImpl.of(innerPreFilter, filter.declaredBarriers());
                }

                @Override
                public WhereFilter visit(WhereFilterWithRespectedBarriersImpl filter) {
                    final WhereFilter innerPreFilter = filter.getWrappedFilter().walk(this);
                    return innerPreFilter == null
                            ? null
                            : WhereFilterWithRespectedBarriersImpl.of(innerPreFilter, filter.respectedBarriers());
                }

                @Override
                public WhereFilter visit(DisjunctiveFilter filter) {
                    final List<WhereFilter> subFilters = filter.getFilters();
                    final WhereFilter[] wrappedFilters = new WhereFilter[subFilters.size()];
                    for (int ii = 0; ii < wrappedFilters.length; ++ii) {
                        final WhereFilter subWrap = subFilters.get(ii).walk(this);
                        if (subWrap == null) {
                            return null;
                        }
                        wrappedFilters[ii] = subWrap;
                    }
                    return DisjunctiveFilter.makeDisjunctiveFilter(wrappedFilters);
                }

                @Override
                public WhereFilter visit(ConjunctiveFilter filter) {
                    final List<WhereFilter> subFilters = filter.getFilters();
                    final WhereFilter[] wrappedFilters = new WhereFilter[subFilters.size()];
                    for (int ii = 0; ii < wrappedFilters.length; ++ii) {
                        final WhereFilter subWrap = subFilters.get(ii).walk(this);
                        if (subWrap == null) {
                            return null;
                        }
                        wrappedFilters[ii] = subWrap;
                    }
                    return ConjunctiveFilter.makeConjunctiveFilter(wrappedFilters);
                }
            });

            if (preFilter != null) {
                preFilter.init(tableReference.getDefinition(), compilationProcessor);
                preViewFilters.add(preFilter);
            } else {
                // if this filter is serial, all subsequent filters must be postViewFilters
                if (filter.isSerial()) {
                    serialFilterFound = true;
                }

                final Collection<Object> newBarriers = ExtractBarriers.of(filter);
                final Optional<Object> dupBarrier = newBarriers.stream()
                        .filter(postViewBarriers::contains)
                        .findFirst();
                if (dupBarrier.isPresent()) {
                    throw new IllegalArgumentException("Filter Barriers must be unique! Found duplicate: " +
                            dupBarrier.get());
                }
                postViewFilters.add(filter);
                postViewBarriers.addAll(newBarriers);
            }
        }

        // we must declare any preFilter barriers in the postFilter at the front of the list
        final Set<Object> preViewBarriers = preViewFilters.stream()
                .flatMap(ExtractBarriers::stream)
                .collect(Collectors.toSet());
        if (!preViewBarriers.isEmpty() && !postViewFilters.isEmpty()) {
            postViewFilters.add(0,
                    WhereAllFilter.INSTANCE.withDeclaredBarriers(preViewBarriers.toArray(Object[]::new)));
        }
        compilationProcessor.compile();

        return new PreAndPostFilters(preViewFilters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY),
                postViewFilters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY));
    }

    private SplitAndApply splitAndApplyFilters(WhereFilter[] allFilters, final TableReference tableReference) {
        final PreAndPostFilters preAndPostFilters = applyFilterRenamings(WhereFilter.copyFrom(allFilters));
        final TableReference.TableAndRemainingFilters tarf =
                tableReference.getWithWhere(preAndPostFilters.preViewFilters);
        Table result = tarf.table;
        if (tarf.remainingFilters.length != 0) {
            result = result.where(Filter.and(tarf.remainingFilters));
        }
        result = applyDeferredViews(result);
        return new SplitAndApply(preAndPostFilters.postViewFilters, result);
    }

    private static class SplitAndApply {
        public final WhereFilter[] postViewFilters;
        public final Table result;

        public SplitAndApply(WhereFilter[] postViewFilters, Table result) {
            this.postViewFilters = postViewFilters;
            this.result = result;
        }
    }

    @Override
    protected Table doCoalesce() {
        Table result;
        if (deferredFilters.length > 0) {
            SplitAndApply splitAndApply = splitAndApplyFilters(deferredFilters, tableReference);
            result = splitAndApply.result;
            // we cannot have a deferred filter that uses a view on this level, because no factory creates a
            // DVT with both a view and a filter
            Assert.eqZero(splitAndApply.postViewFilters.length, "splitAndApply.postViewFilters.length");
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

        /* Determine if we are using any of the deferred views in this selectDistinct. */
        final List<SelectColumn> selectColumns = Arrays.asList(SelectColumn.from(columns));
        try {
            SelectAndViewAnalyzer.initializeSelectColumns(getDefinition().getColumnNameMap(),
                    selectColumns.toArray(SelectColumn[]::new));
        } catch (Exception e) {
            return coalesce().selectDistinct(columns);
        }

        final Set<String> outputColumnNames = Arrays.stream(deferredViewColumns)
                .filter(sc -> !sc.isRetain())
                .map(SelectColumn::getName)
                .collect(Collectors.toSet());
        if (selectColumns.stream().anyMatch(sc -> !sc.getColumnArrays().isEmpty()
                || sc.getColumns().stream().anyMatch(outputColumnNames::contains))) {
            return coalesce().selectDistinct(columns);
        }

        /* If the cachedResult is not yet created, we first ask for a selectDistinct cachedResult. */
        Table selectDistinct = tableReference.selectDistinctInternal(columns);
        return selectDistinct == null ? coalesce().selectDistinct(columns) : selectDistinct;
    }

    @Override
    protected DeferredViewTable copy(@NotNull final Map<String, Object> attributes) {
        return wrap(definition, getDescription(), new CopiedTableReference(this, tableReference), attributes);
    }

    @Override
    protected DeferredViewTable redefineImpl(TableDefinition newDefinition) {
        final List<ColumnDefinition<?>> cDefs = newDefinition.getColumns();
        SelectColumn[] newView = new SelectColumn[cDefs.size()];
        for (int cdi = 0; cdi < newView.length; ++cdi) {
            newView[cdi] = new SourceColumn(cDefs.get(cdi).getName());
        }
        return withView(newDefinition, getDescription() + "-redefined",
                new CopiedTableReference(this, tableReference), newView);
    }

    @Override
    protected Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns) {
        return withView(newDefinitionExternal, getDescription() + "-redefined",
                new CopiedTableReference(this, tableReference), viewColumns);
    }

    @Override
    protected Table redefineWithRename(TableDefinition newDefinitionExternal, SourceColumn[] renameColumns) {
        return withRename(newDefinitionExternal, getDescription() + "-redefined",
                new CopiedTableReference(this, tableReference), renameColumns);
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

        /**
         * Should this set of filter force a coalescing of the table?
         * 
         * @param whereFilters the filters that are to be applied
         * @return true if this set of filters should force a coalescing of the table, false otherwise.
         */
        protected boolean shouldCoalesce(WhereFilter... whereFilters) {
            return false;
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

    private class CopiedTableReference extends TableReference {
        private final TableReference innerTableReference;

        CopiedTableReference(Table table, final TableReference tableReference) {
            super(table);
            this.innerTableReference = tableReference;
        }

        @Override
        protected boolean shouldCoalesce(WhereFilter... whereFilters) {
            return this.innerTableReference.shouldCoalesce(whereFilters);
        }

        @Override
        protected TableAndRemainingFilters getWithWhere(WhereFilter... whereFilters) {
            final WhereFilter[] allFilters;
            if (deferredFilters.length == 0) {
                allFilters = whereFilters;
            } else {
                allFilters = concat(deferredFilters, whereFilters);
            }

            SplitAndApply splitAndApply = splitAndApplyFilters(allFilters, innerTableReference);

            return new TableAndRemainingFilters(splitAndApply.result, splitAndApply.postViewFilters);
        }
    }
}
