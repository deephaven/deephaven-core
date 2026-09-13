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
        return new DeferredViewTable(getDefinition(), getDescription() + "-filtered",
                new CopiedTableReference(this, tableReference), null, null, innerFilters);
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
            final Table renamed = applyDeferredRenamesSimultaneously(result);
            result = renamed != null
                    ? renamed
                    : result.view(List.of(SelectColumn.copyFrom(deferredViewColumns)));
        }
        return result;
    }

    /**
     * Apply {@link #deferredViewColumns} when they are pure renames that {@code view} could not evaluate correctly.
     *
     * <p>
     * {@code view} evaluates its columns in order, each against a name space that already holds the ones before it.
     * That is right for computed columns and wrong for a rename, which is simultaneous. A rename that reassigns a name
     * another column reads from -- a swap {@code (B = A, A = B)}, a chain {@code (B = A, C = B)}, a rotation -- has
     * later columns resolve against the values the earlier ones just produced. The result is silently wrong data:
     * {@code readTable(...).renameColumns("A = B", "B = A").coalesce()} gave both columns the values of the first.
     *
     * <p>
     * {@link Table#renameColumns} is simultaneous, so replay the rename with it and then project the deferred columns'
     * own names to fix their order and drop anything the deferred view does not include. That projection is a
     * {@code view} of bare column names, where every name maps to itself, so it cannot reintroduce the hazard.
     *
     * @param source The table to apply the deferred view columns to
     * @return The renamed table, or {@code null} if the deferred view columns are not a rename, or are a rename
     *         {@code view} already handles correctly, in which case the caller should use {@code view} as before
     */
    /**
     * A deferred view that is a pure rename which reassigns a name another of its columns reads from.
     *
     * <p>
     * This is the shape that {@code view}'s sequential evaluation gets wrong, and it is recognized once here because
     * two places have to agree about it: {@link #applyDeferredViews}, which materializes the columns, and
     * {@link #applyFilterRenamings}, which maps filter column names through the same view. Both were composing
     * sequentially, and a rename is simultaneous.
     */
    private static final class SimultaneousRename {

        private final String[] targetNames;
        private final String[] sourceNames;

        private SimultaneousRename(final String[] targetNames, final String[] sourceNames) {
            this.targetNames = targetNames;
            this.sourceNames = sourceNames;
        }

        /**
         * @param deferredViewColumns The deferred view columns to classify
         * @param innerColumnNames The column names available in the table the view is applied to
         * @return The rename, or {@code null} if {@code deferredViewColumns} is not a pure rename, is one that
         *         {@code view}'s sequential evaluation already handles correctly, or is inherently sequential
         */
        @Nullable
        private static SimultaneousRename of(
                @NotNull final SelectColumn[] deferredViewColumns,
                @NotNull final Set<String> innerColumnNames) {
            final int numColumns = deferredViewColumns.length;
            final String[] targetNames = new String[numColumns];
            final String[] sourceNames = new String[numColumns];
            for (int ci = 0; ci < numColumns; ++ci) {
                SelectColumn selectColumn = deferredViewColumns[ci];
                if (selectColumn instanceof SwitchColumn) {
                    selectColumn = ((SwitchColumn) selectColumn).getRealColumn();
                }
                if (!(selectColumn instanceof SourceColumn)) {
                    // Not a pure rename; sequential semantics are what the caller asked for.
                    return null;
                }
                targetNames[ci] = selectColumn.getName();
                sourceNames[ci] = ((SourceColumn) selectColumn).getSourceName();
                if (!innerColumnNames.contains(sourceNames[ci])) {
                    // This column reads a name the inner table does not have, so an earlier entry in this same list
                    // must produce it. That view is inherently sequential -- updateView("T = X", "X = Y", "Y = T")
                    // swaps X and Y through a temporary and depends on being evaluated in order.
                    return null;
                }
            }

            // The hazard is a name reassigned before a *later* column reads it: sequential evaluation would hand
            // that column the new value. A collision with an earlier column is harmless, since that column read the
            // name before it was reassigned -- so requiring cj > ci confines this to the renames that were actually
            // broken and leaves every one view already evaluates correctly on its existing path.
            for (int ci = 0; ci < numColumns; ++ci) {
                if (targetNames[ci].equals(sourceNames[ci])) {
                    // Keeps its name, so it defines nothing that could shadow another column's source.
                    continue;
                }
                for (int cj = ci + 1; cj < numColumns; ++cj) {
                    if (targetNames[ci].equals(sourceNames[cj])) {
                        return new SimultaneousRename(targetNames, sourceNames);
                    }
                }
            }
            return null;
        }

        /**
         * @return Map from this view's result names to the names they read, resolved against the original name space
         */
        private Map<String, String> outerToInnerNames() {
            final Map<String, String> renames = new HashMap<>(targetNames.length);
            for (int ci = 0; ci < targetNames.length; ++ci) {
                renames.put(targetNames[ci], sourceNames[ci]);
            }
            return renames;
        }
    }

    @Nullable
    private Table applyDeferredRenamesSimultaneously(@NotNull final Table source) {
        final SimultaneousRename rename =
                SimultaneousRename.of(deferredViewColumns, source.getDefinition().getColumnNameSet());
        if (rename == null) {
            return null;
        }
        final String[] targetNames = rename.targetNames;
        final String[] sourceNames = rename.sourceNames;
        final int numColumns = targetNames.length;

        // One canonical target per source column, so a source read by several deferred columns is renamed once.
        final Map<String, String> sourceToCanonicalTarget = new LinkedHashMap<>(numColumns);
        for (int ci = 0; ci < numColumns; ++ci) {
            sourceToCanonicalTarget.putIfAbsent(sourceNames[ci], targetNames[ci]);
        }
        final List<Pair> renamePairs = new ArrayList<>(sourceToCanonicalTarget.size());
        sourceToCanonicalTarget.forEach((sourceName, targetName) -> {
            if (!targetName.equals(sourceName)) {
                renamePairs.add(Pair.of(ColumnName.of(sourceName), ColumnName.of(targetName)));
            }
        });

        final List<Selectable> projection = new ArrayList<>(numColumns);
        for (int ci = 0; ci < numColumns; ++ci) {
            final String canonicalTarget = sourceToCanonicalTarget.get(sourceNames[ci]);
            projection.add(targetNames[ci].equals(canonicalTarget)
                    ? ColumnName.of(targetNames[ci])
                    : Selectable.of(ColumnName.of(targetNames[ci]), ColumnName.of(canonicalTarget)));
        }
        return source.renameColumns(renamePairs).view(projection);
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

        // A pure rename that reassigns a name another of its columns reads from is simultaneous, and the loop below
        // is not: it resolves each column's source through the entries already added, so a swap would map a filter
        // column back to the name its sibling had just taken over. applyDeferredViews materializes such a view with
        // renameColumns, and the filter mapping has to match it. Nothing here is view-computed, so there are no
        // post-view columns to find.
        final SimultaneousRename simultaneousRename = SimultaneousRename.of(
                deferredViewColumns, tableReference.getDefinition().getColumnNameSet());
        if (simultaneousRename != null) {
            renames.putAll(simultaneousRename.outerToInnerNames());
        } else {

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

            // distinct() because a filter that names both a column and its column-array form -- "Col == Col_[0]"
            // -- reports that column in getColumns() and again in getColumnArrays(), and Collectors.toMap has no
            // merge function. Both entries would map it to the same inner name, so the duplicate was only ever a
            // crash: IllegalStateException: Duplicate key Col.
            final Map<String, String> myRenames = Stream.of(filter.getColumns(), filter.getColumnArrays())
                    .flatMap(Collection::stream)
                    .filter(renames::containsKey)
                    .distinct()
                    .collect(Collectors.toMap(Function.identity(), renames::get));

            if (myRenames.isEmpty()) {
                preViewFilters.add(filter);
                continue;
            }

            final WhereFilter preFilter = filter.walk(new WhereFilter.Visitor<>() {
                @Override
                public WhereFilter visitOther(WhereFilter filter) {
                    if (filter instanceof MatchFilter) {
                        return ((MatchFilter) filter).renameFilter(myRenames);
                    }
                    if (filter instanceof ConditionFilter) {
                        return ((ConditionFilter) filter).renameFilter(myRenames);
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
            // we cannot have a deferred filter that uses a view on this level, because we prohibit
            // the view and filter from being present on the same DVT in the constructor
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
    protected DeferredViewTable copy() {
        final DeferredViewTable result =
                new DeferredViewTable(definition, getDescription(), new CopiedTableReference(this, tableReference),
                        null, null, null);
        LiveAttributeMap.copyAttributes(this, result, ak -> true);
        return result;
    }

    @Override
    protected DeferredViewTable redefineImpl(TableDefinition newDefinition) {
        final List<ColumnDefinition<?>> cDefs = newDefinition.getColumns();
        SelectColumn[] newView = new SelectColumn[cDefs.size()];
        for (int cdi = 0; cdi < newView.length; ++cdi) {
            newView[cdi] = new SourceColumn(cDefs.get(cdi).getName());
        }
        return new DeferredViewTable(newDefinition, getDescription() + "-redefined",
                new CopiedTableReference(this, tableReference), null, newView, null);
    }

    @Override
    protected Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns) {
        return new DeferredViewTable(newDefinitionExternal, getDescription() + "-redefined",
                new CopiedTableReference(this, tableReference), null, viewColumns, null);
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
