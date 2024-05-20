//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.impl.DependencyStreamProvider;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Stream;

public abstract class ComposedFilter extends WhereFilterLivenessArtifactImpl implements DependencyStreamProvider {
    final WhereFilter[] componentFilters;

    ComposedFilter(WhereFilter[] componentFilters) {
        for (final WhereFilter componentFilter : componentFilters) {
            if (componentFilter instanceof ReindexingFilter) {
                throw new UnsupportedOperationException(
                        "ComposedFilters do not support ReindexingFilters: " + componentFilter);
            }
        }
        this.componentFilters = componentFilters;

        for (WhereFilter f : this.componentFilters) {
            if (f instanceof LivenessArtifact && f.isRefreshing()) {
                manage((LivenessArtifact) f);
            }
        }

        setAutomatedFilter(Arrays.stream(componentFilters).allMatch(WhereFilter::isAutomatedFilter));
    }

    @Override
    public List<String> getColumns() {
        final Set<String> result = new HashSet<>();

        for (WhereFilter filter : componentFilters) {
            result.addAll(filter.getColumns());
        }

        return new ArrayList<>(result);
    }

    @Override
    public List<String> getColumnArrays() {
        final Set<String> result = new HashSet<>();

        for (WhereFilter filter : componentFilters) {
            result.addAll(filter.getColumnArrays());
        }

        return new ArrayList<>(result);
    }

    @Override
    public void init(@NotNull TableDefinition tableDefinition) {
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        init(tableDefinition, compilationProcessor);
        compilationProcessor.compile();
    }

    @Override
    public void init(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        for (WhereFilter filter : componentFilters) {
            filter.init(tableDefinition, compilationProcessor);
        }
    }

    @Override
    public SafeCloseable beginOperation(@NotNull final Table sourceTable) {
        return Arrays.stream(componentFilters)
                .map((final WhereFilter whereFilter) -> whereFilter.beginOperation(sourceTable))
                .collect(SafeCloseableList.COLLECTOR);
    }

    @Override
    public void validateSafeForRefresh(@NotNull final BaseTable<?> sourceTable) {
        for (WhereFilter filter : componentFilters) {
            filter.validateSafeForRefresh(sourceTable);
        }
    }

    @Override
    public boolean isSimpleFilter() {
        for (WhereFilter filter : componentFilters) {
            if (!filter.isSimpleFilter()) {
                return false;
            }
        }
        return true;
    }

    protected WhereFilter[] getComponentFilters() {
        return componentFilters;
    }

    @TestUseOnly
    public List<WhereFilter> getFilters() {
        return Collections.unmodifiableList(Arrays.asList(componentFilters));
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        for (WhereFilter filter : getComponentFilters()) {
            filter.setRecomputeListener(listener);
        }
    }

    @Override
    public boolean isRefreshing() {
        return Arrays.stream(componentFilters).anyMatch(WhereFilter::isRefreshing);
    }


    @Override
    public Stream<NotificationQueue.Dependency> getDependencyStream() {
        return Stream.concat(
                Arrays.stream(componentFilters).filter(f -> f instanceof NotificationQueue.Dependency)
                        .map(f -> (NotificationQueue.Dependency) f),
                Arrays.stream(componentFilters).filter(f -> f instanceof DependencyStreamProvider)
                        .flatMap(f -> ((DependencyStreamProvider) f).getDependencyStream()));
    }

    @Override
    public boolean canMemoize() {
        return Arrays.stream(componentFilters).allMatch(WhereFilter::canMemoize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final ComposedFilter that = (ComposedFilter) o;
        return Arrays.equals(componentFilters, that.componentFilters);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(componentFilters);
    }

    @Override
    public boolean permitParallelization() {
        return Arrays.stream(componentFilters).allMatch(WhereFilter::permitParallelization);
    }
}
