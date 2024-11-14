//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.qst.column.header.ColumnHeader;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The FilterSelectColumn wraps a {@link io.deephaven.api.filter.Filter} and producing a column of true or false Boolean
 * values described by the filter.
 *
 * <p>
 * This column is appropriate as an argument to a {@link Table#view(Collection)} or {@link Table#updateView(String...)},
 * lazily evaluating the equivalent of a {@link Table#wouldMatch(String...)} operation. Although select and update can
 * also use Filters, the wouldMatch provides a more efficient path for realized results as it stores and updates only a
 * {@link RowSet}. The FilterSelectColumn can only evaluate the filter one chunk at a time, and must write to an
 * in-memory Boolean column source.
 * </p>
 */
class FilterSelectColumn implements SelectColumn {

    // We don't actually care to do anything, but cannot return null
    private static final Formula.FillContext FILL_CONTEXT_INSTANCE = new Formula.FillContext() {};

    @NotNull
    private final String destName;
    @NotNull
    private final WhereFilter filter;

    private RowSet rowSet;
    private Table tableToFilter;

    /**
     * Create a FilterSelectColumn with the given name and {@link WhereFilter}.
     *
     * @param destName the name of the result column
     * @param filter the filter that is evaluated to true or false for each row of the table
     * @return a new FilterSelectColumn representing the provided filter.
     */
    static FilterSelectColumn of(@NotNull final String destName, @NotNull final WhereFilter filter) {
        return new FilterSelectColumn(destName, filter);
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
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        this.rowSet = rowSet;
        this.tableToFilter = new QueryTable(rowSet, columnsOfInterest);
        return filter.getColumns();
    }

    @Override
    public List<String> initDef(@NotNull Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        filter.init(TableDefinition.of(columnDefinitionMap.values()));
        return checkForInvalidFilters();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        filter.init(TableDefinition.of(columnDefinitionMap.values()), compilationRequestProcessor);
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
        return List.of();
    }

    @Override
    public boolean hasVirtualRowVariables() {
        /* This should always be false, because initDef throws when arrays or ii and friends are used. */
        return filter.hasVirtualRowVariables();
    }

    @NotNull
    @Override
    public ColumnSource<Boolean> getDataView() {
        return new ViewColumnSource<>(Boolean.class, new FilterFormula(), false);
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
        return false;
    }

    @Override
    public FilterSelectColumn copy() {
        return new FilterSelectColumn(destName, filter.copy());
    }

    private class FilterFormula extends Formula {
        public FilterFormula() {
            super(null);
        }

        @Override
        public Boolean getBoolean(long rowKey) {
            WritableRowSet filteredIndex = filter.filter(RowSetFactory.fromKeys(rowKey), rowSet, tableToFilter, false);
            return filteredIndex.isNonempty();
        }

        @Override
        public Boolean getPrevBoolean(long rowKey) {
            WritableRowSet filteredIndex = filter.filter(RowSetFactory.fromKeys(rowKey), rowSet, tableToFilter, true);
            return filteredIndex.isNonempty();
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

        private void doFill(@NotNull RowSequence rowSequence, WritableChunk<? super Values> destination,
                boolean usePrev) {
            final WritableObjectChunk<Boolean, ?> booleanDestination = destination.asWritableObjectChunk();
            booleanDestination.setSize(rowSequence.intSize());
            try (final RowSet inputRowSet = rowSequence.asRowSet();
                    final RowSet filtered = filter.filter(inputRowSet, rowSet, tableToFilter, usePrev);
                    final RowSet.Iterator inputIt = inputRowSet.iterator();
                    final RowSet.Iterator trueIt = filtered.iterator()) {
                long nextTrue = trueIt.hasNext() ? trueIt.nextLong() : -1;
                int offset = 0;
                while (nextTrue >= 0) {
                    // the input iterator is a superset of the true iterator, so we can always find out what
                    // the next value is without needing to check hasNext
                    final long nextInput = inputIt.nextLong();
                    final boolean found = nextInput == nextTrue;
                    booleanDestination.set(offset++, found);
                    if (found) {
                        nextTrue = trueIt.hasNext() ? trueIt.nextLong() : -1;
                    }
                }
                // fill everything else up with false, because nothing else can match
                booleanDestination.fillWithBoxedValue(offset, booleanDestination.size() - offset, false);
            }
        }
    }
}
