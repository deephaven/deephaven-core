//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.DependencyStreamProvider;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class WhereFilterDelegatingBase
        extends WhereFilterLivenessArtifactImpl
        implements DependencyStreamProvider {

    protected final WhereFilter filter;

    protected WhereFilterDelegatingBase(WhereFilter filter) {
        this.filter = Objects.requireNonNull(filter);
        if (filter instanceof LivenessArtifact && filter.isRefreshing()) {
            manage((LivenessArtifact) filter);
        }
    }

    @Override
    public Stream<NotificationQueue.Dependency> getDependencyStream() {
        if (filter instanceof NotificationQueue.Dependency) {
            return Stream.of((NotificationQueue.Dependency) filter);
        } else if (filter instanceof DependencyStreamProvider) {
            return ((DependencyStreamProvider) filter).getDependencyStream();
        }
        return Stream.empty();
    }

    public WhereFilter getWrappedFilter() {
        return filter;
    }

    @Override
    public List<String> getColumns() {
        return filter.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return filter.getColumnArrays();
    }

    @Override
    public void init(@NotNull TableDefinition tableDefinition) {
        init(tableDefinition, QueryCompilerRequestProcessor.immediate());
    }

    @Override
    public void init(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        filter.init(tableDefinition, compilationProcessor);
    }

    @Override
    public SafeCloseable beginOperation(@NotNull final Table sourceTable) {
        return filter.beginOperation(sourceTable);
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        filter.validateSafeForRefresh(sourceTable);
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return filter.filter(selection, fullSet, table, usePrev);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return filter.filterInverse(selection, fullSet, table, usePrev);
    }

    @Override
    public boolean isSimpleFilter() {
        return filter.isSimpleFilter();
    }

    @Override
    public boolean isRefreshing() {
        return filter.isRefreshing();
    }

    @Override
    public void setRecomputeListener(RecomputeListener result) {
        filter.setRecomputeListener(result);
    }

    @Override
    public boolean isAutomatedFilter() {
        return filter.isAutomatedFilter();
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        filter.setAutomatedFilter(value);
    }

    @Override
    public boolean canMemoize() {
        return filter.canMemoize();
    }

    @Override
    public boolean permitParallelization() {
        return filter.permitParallelization();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhereFilterDelegatingBase that = (WhereFilterDelegatingBase) o;
        return filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        // Use class hashcode to improve hashcode to account for if a filter and any of its wrappers are in the same
        // HashMap
        return getClass().hashCode() ^ filter.hashCode();
    }

    @VisibleForTesting
    WhereFilter filter() {
        return filter;
    }
}
