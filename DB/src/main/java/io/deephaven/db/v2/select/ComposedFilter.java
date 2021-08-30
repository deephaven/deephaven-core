/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.v2.DependencyStreamProvider;
import io.deephaven.util.annotations.TestUseOnly;

import java.util.*;
import java.util.stream.Stream;

public abstract class ComposedFilter extends SelectFilterLivenessArtifactImpl
    implements DependencyStreamProvider {
    final SelectFilter[] componentFilters;

    ComposedFilter(SelectFilter[] componentFilters) {
        for (final SelectFilter componentFilter : componentFilters) {
            if (componentFilter instanceof ReindexingFilter) {
                throw new UnsupportedOperationException(
                    "ComposedFilters do not support ReindexingFilters: " + componentFilter);
            }
        }
        this.componentFilters = componentFilters;

        for (SelectFilter f : this.componentFilters) {
            if (f instanceof LivenessArtifact) {
                manage((LivenessArtifact) f);
            }
        }

        setAutomatedFilter(
            Arrays.stream(componentFilters).allMatch(SelectFilter::isAutomatedFilter));
    }

    @Override
    public List<String> getColumns() {
        final Set<String> result = new HashSet<>();

        for (SelectFilter filter : componentFilters) {
            result.addAll(filter.getColumns());
        }

        return new ArrayList<>(result);
    }

    @Override
    public List<String> getColumnArrays() {
        final Set<String> result = new HashSet<>();

        for (SelectFilter filter : componentFilters) {
            result.addAll(filter.getColumnArrays());
        }

        return new ArrayList<>(result);
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        for (SelectFilter filter : componentFilters) {
            filter.init(tableDefinition);
        }
    }

    @Override
    public boolean isSimpleFilter() {
        for (SelectFilter filter : componentFilters) {
            if (!filter.isSimpleFilter()) {
                return false;
            }
        }
        return true;
    }

    protected SelectFilter[] getComponentFilters() {
        return componentFilters;
    }

    @TestUseOnly
    public List<SelectFilter> getFilters() {
        return Collections.unmodifiableList(Arrays.asList(componentFilters));
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        for (SelectFilter filter : getComponentFilters()) {
            filter.setRecomputeListener(listener);
        }
    }

    @Override
    public boolean isRefreshing() {
        return Arrays.stream(componentFilters).anyMatch(SelectFilter::isRefreshing);
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
        return Arrays.stream(componentFilters).allMatch(SelectFilter::canMemoize);
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
}
