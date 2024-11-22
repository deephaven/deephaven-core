//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.filter.Filter;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The FilterSelectColumn wraps a {@link Filter} and producing a column of true or false Boolean values described by the
 * filter.
 * <p>
 * This SelectColumn is appropriate as an argument to a {@link Table#view(Collection)} or
 * {@link Table#updateView(String...)}, lazily evaluating the equivalent of a {@link Table#wouldMatch(String...)}
 * operation. Although select and update can also use Filters, wouldMatch provides a more efficient path for realized
 * results as it stores and updates only a {@link RowSet}. The FilterSelectColumn can only evaluate the Filter one chunk
 * at a time, and must write to an in-memory {@link Boolean} {@link ColumnSource}.
 */
class FilterSelectColumn implements SelectColumn {

    // We don't actually care to do anything, but cannot return null
    private static final Formula.FillContext FILL_CONTEXT_INSTANCE = new Formula.FillContext() {};

    @NotNull
    private final String destName;
    @NotNull
    private final WhereFilter filter;

    private Table tableToFilter;
    /**
     * We store a copy of our table definition, to ensure that it is identical between initDef and initInputs.
     */
    private TableDefinition computedDefinition;

    /**
     * Create a FilterSelectColumn with the given name and {@link WhereFilter}.
     *
     * @param destName the name of the result column
     * @param filter the filter that is evaluated to true or false for each row of the table
     * @return a new FilterSelectColumn representing the provided filter.
     */
    static FilterSelectColumn of(@NotNull final String destName, @NotNull final Filter filter) {
        return new FilterSelectColumn(destName, WhereFilter.of(filter));
    }

    private FilterSelectColumn(@NotNull final String destName, @NotNull final WhereFilter filter) {
        this.destName = destName;
        this.filter = filter;
    }

    @Override
    public String toString() {
        return "filter(" + filter + ')';
    }

    @Override
    public List<String> initInputs(final TrackingRowSet rowSet,
            final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        tableToFilter = new QueryTable(rowSet, columnsOfInterest);
        if (!computedDefinition.equals(tableToFilter.getDefinition())) {
            throw new IllegalStateException(
                    "Definition changed between initDef and initInputs in FilterSelectColumn: initDef="
                            + computedDefinition + ", initInputs" + tableToFilter.getDefinition());
        }
        return filter.getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        filter.init(computedDefinition = TableDefinition.of(columnDefinitionMap.values()));
        return checkForInvalidFilters();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        filter.init(computedDefinition = TableDefinition.of(columnDefinitionMap.values()), compilationRequestProcessor);
        return checkForInvalidFilters();
    }

    /**
     * Validates the filter to ensure it does not contain invalid filters such as column vectors or virtual row
     * variables. Throws an {@link UncheckedTableException} if any invalid filters are found.
     *
     * @return the list of columns required by the filter.
     */
    private List<String> checkForInvalidFilters() {
        if (!filter.getColumnArrays().isEmpty()) {
            throw new UncheckedTableException(
                    "Cannot use a filter with column Vectors (_ syntax) in select, view, update, or updateView: "
                            + filter);
        }
        if (filter.hasVirtualRowVariables()) {
            throw new UncheckedTableException(
                    "Cannot use a filter with virtual row variables (i, ii, or k) in select, view, update, or updateView: "
                            + filter);
        }
        if (filter.isRefreshing()) {
            /*
             * TODO: DH-18052: updateView and view should support refreshing Filter Expressions
             *
             * This would enable us to use a whereIn or whereNotIn for things like conditional formatting; which could
             * be attractive. However, a join or wouldMatch gets you there without the additional complexity.
             *
             * Supporting this requires SelectColumn dependencies, which have not previously existed. Additionally, if
             * we were to support these for select and update (as opposed to view and updateView), then the filter could
             * require recomputing the entire result table whenever anything changes.
             */
            throw new UncheckedTableException(
                    "Cannot use a refreshing filter in select, view, update, or updateView: " + filter);
        }

        return filter.getColumns();
    }

    @Override
    public Class<?> getReturnedType() {
        return Boolean.class;
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return null;
    }

    @Override
    public List<String> getColumns() {
        return filter.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        /* This should always be empty, because initDef throws when arrays or virtual row variables are used. */
        return List.of();
    }

    @Override
    public boolean hasVirtualRowVariables() {
        /* This should always be false, because initDef throws when arrays or ii and friends are used. */
        return false;
    }

    @NotNull
    @Override
    public ColumnSource<Boolean> getDataView() {
        return new ViewColumnSource<>(Boolean.class, new FilterFormula(), isStateless());
    }

    @NotNull
    @Override
    public ColumnSource<?> getLazyView() {
        return getDataView();
    }

    @Override
    public String getName() {
        return destName;
    }

    @Override
    public MatchPair getMatchPair() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final WritableColumnSource<?> newDestInstance(final long size) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(size, Boolean.class);
    }

    @Override
    public final WritableColumnSource<?> newFlatDestInstance(final long size) {
        return InMemoryColumnSource.getImmutableMemoryColumnSource(size, Boolean.class, null);
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean isStateless() {
        return filter.permitParallelization();
    }

    @Override
    public FilterSelectColumn copy() {
        return new FilterSelectColumn(destName, filter.copy());
    }

    @Override
    public void validateSafeForRefresh(final BaseTable<?> sourceTable) {
        filter.validateSafeForRefresh(sourceTable);
    }

    private class FilterFormula extends Formula {
        public FilterFormula() {
            super(null);
        }

        @Override
        public Boolean getBoolean(final long rowKey) {
            try (final WritableRowSet selection = RowSetFactory.fromKeys(rowKey);
                    final WritableRowSet filteredRowSet =
                            filter.filter(selection, tableToFilter.getRowSet(), tableToFilter, false)) {
                return filteredRowSet.isNonempty();
            }
        }

        @Override
        public Boolean getPrevBoolean(final long rowKey) {
            try (final WritableRowSet selection = RowSetFactory.fromKeys(rowKey);
                    final WritableRowSet filteredRowSet = filter.filter(selection,
                            tableToFilter.getRowSet().prev(), tableToFilter, true)) {
                return filteredRowSet.isNonempty();
            }
        }

        @Override
        public Object get(final long rowKey) {
            return getBoolean(rowKey);
        }

        @Override
        public Object getPrev(final long rowKey) {
            return getPrevBoolean(rowKey);
        }

        @Override
        public ChunkType getChunkType() {
            return ChunkType.Object;
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity) {
            return FILL_CONTEXT_INSTANCE;
        }

        @Override
        public void fillChunk(
                @NotNull final FillContext fillContext,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            doFill(rowSequence, destination, false);
        }

        @Override
        public void fillPrevChunk(
                @NotNull final FillContext fillContext,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            doFill(rowSequence, destination, true);
        }

        private void doFill(@NotNull final RowSequence rowSequence, final WritableChunk<? super Values> destination,
                final boolean usePrev) {
            final WritableObjectChunk<Boolean, ?> booleanDestination = destination.asWritableObjectChunk();
            booleanDestination.setSize(rowSequence.intSize());
            final RowSet fullSet = usePrev ? tableToFilter.getRowSet().prev() : tableToFilter.getRowSet();

            try (final RowSet inputRowSet = rowSequence.asRowSet();
                    final RowSet filtered = filter.filter(inputRowSet, fullSet, tableToFilter, usePrev)) {
                if (filtered.size() == inputRowSet.size()) {
                    // if everything matches, short circuit the iteration
                    booleanDestination.fillWithValue(0, booleanDestination.size(), true);
                    return;
                }

                int offset = 0;

                try (final RowSequence.Iterator inputRows = inputRowSet.getRowSequenceIterator();
                        final RowSet.Iterator trueRows = filtered.iterator()) {
                    while (trueRows.hasNext()) {
                        final long nextTrue = trueRows.nextLong();
                        // Find all the false rows between the last consumed input row and the next true row
                        final int falsesSkipped = (int) inputRows.advanceAndGetPositionDistance(nextTrue + 1) - 1;
                        if (falsesSkipped > 0) {
                            booleanDestination.fillWithValue(offset, falsesSkipped, false);
                            offset += falsesSkipped;
                        }
                        booleanDestination.set(offset++, true);
                    }
                }

                final int remainingFalses = booleanDestination.size() - offset;
                // Fill everything else up with false, because we've exhausted the trues
                if (remainingFalses > 0) {
                    booleanDestination.fillWithValue(offset, remainingFalses, false);
                }
            }
        }
    }
}
