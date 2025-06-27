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

public class RowSetCapturingFilter extends WhereFilterImpl implements SafeCloseable {
    private final List<RowSet> rowSets = new ArrayList<>();
    private final WhereFilter innerFilter;

    public RowSetCapturingFilter() {
        this(null);
    }

    public RowSetCapturingFilter(final Filter filter) {
        this.innerFilter = filter == null ? null : WhereFilter.of(filter);
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
        return this;
    }

    @Override
    public void close() {
        reset();
    }

    public void reset() {
        synchronized (rowSets) {
            SafeCloseable.closeAll(rowSets.stream());
            rowSets.clear();
        }
    }

    public List<RowSet> rowSets() {
        synchronized (rowSets) {
            return new ArrayList<>(rowSets);
        }
    }

    public long numRowsProcessed() {
        synchronized (rowSets) {
            return rowSets.stream().mapToLong(RowSet::size).sum();
        }
    }
}
