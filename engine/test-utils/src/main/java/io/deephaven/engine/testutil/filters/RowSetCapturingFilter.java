//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.filters;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterImpl;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A WhereFilter that captures the RowSets it filters, allowing for inspection of the behavior of the
 * {@code AbstractFilterExecution}.
 * <p>
 * This filter is intended to be used in tests with well-defined execution boundaries (either static content or using a
 * {@link io.deephaven.engine.testutil.ControlledUpdateGraph ControlledUpdateGraph}).
 * <p>
 * Once used, or between-uses, it is expected that the {@link #reset()} method is called to clear the captured RowSets.
 */
public class RowSetCapturingFilter extends WhereFilterImpl implements SafeCloseable {
    private final List<RowSet> rowSets;
    private final WhereFilter innerFilter;

    /**
     * Creates a RowSetCapturingFilter that assumes an always-true filter.
     */
    public RowSetCapturingFilter() {
        this(null, new ArrayList<>());
    }

    /**
     * Creates a RowSetCapturingFilter that wraps the provided filter.
     *
     * @param filter the filter to wrap, may be null
     */
    public RowSetCapturingFilter(final Filter filter) {
        this(filter == null ? null : WhereFilter.of(filter), new ArrayList<>());
    }

    /**
     * Creates a RowSetCapturingFilter that wraps the provided filter and accumulates captured RowSets in the provided
     * list.
     *
     * @param filter the filter to wrap, may be null
     *
     */
    private RowSetCapturingFilter(final WhereFilter filter, final List<RowSet> rowSets) {
        this.rowSets = rowSets;
        this.innerFilter = filter;
    }

    @Override
    public List<String> getColumns() {
        return innerFilter == null ? Collections.emptyList() : innerFilter.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return innerFilter == null ? Collections.emptyList() : innerFilter.getColumnArrays();
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
        if (innerFilter != null) {
            innerFilter.init(tableDefinition);
        }
    }

    @Override
    public void init(@NotNull TableDefinition tableDefinition,
            @NotNull QueryCompilerRequestProcessor compilationProcessor) {
        if (innerFilter != null) {
            innerFilter.init(tableDefinition, compilationProcessor);
        }
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        synchronized (rowSets) {
            rowSets.add(selection.copy());
        }
        if (innerFilter != null) {
            return innerFilter.filter(selection, fullSet, table, usePrev);
        }
        return selection.copy();
    }

    @Override
    public boolean isSimpleFilter() {
        return innerFilter == null || innerFilter.isSimpleFilter();
    }

    @Override
    public boolean permitParallelization() {
        return innerFilter == null || innerFilter.permitParallelization();
    }

    @Override
    public void setRecomputeListener(WhereFilter.RecomputeListener result) {
        if (innerFilter != null) {
            innerFilter.setRecomputeListener(result);
        }
    }

    @Override
    public WhereFilter copy() {
        if (innerFilter != null) {
            final WhereFilter newInner = innerFilter.copy();
            if (newInner != innerFilter) {
                // note we share the rowset collection
                return new RowSetCapturingFilter(newInner, rowSets);
            }
        }
        return this;
    }

    @Override
    public void close() {
        reset();
    }

    /**
     * Frees all captured RowSets and clears the internal list of RowSets.
     */
    public void reset() {
        synchronized (rowSets) {
            SafeCloseable.closeAll(rowSets.stream());
            rowSets.clear();
        }
    }

    /**
     * Returns a copy of the RowSets captured by this filter. The RowSets are owned by this filter and should not be
     * modified or closed by the caller.
     *
     * @return a list of RowSets captured by this filter
     */
    public List<RowSet> rowSets() {
        synchronized (rowSets) {
            return new ArrayList<>(rowSets);
        }
    }

    /**
     * @return the total number of rows processed by this filter, which is the sum of the sizes of all captured RowSets
     */
    public long numRowsProcessed() {
        synchronized (rowSets) {
            return rowSets.stream().mapToLong(RowSet::size).sum();
        }
    }
}
