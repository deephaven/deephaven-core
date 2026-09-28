//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.*;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This will return the ReindexingFilter instances contained within a WhereFilter, if any exist. Note that this removes
 * all inversion, barrier and serial wrappers and the returned filters should not be used directly for filtering.
 */
public enum ExtractReindexingFilters implements WhereFilter.Visitor<Collection<ReindexingFilter>> {
    EXTRACT_REINDEXING_FILTERS;

    public static Collection<ReindexingFilter> of(final WhereFilter filter) {
        return filter.walk(EXTRACT_REINDEXING_FILTERS);
    }

    @Override
    public Collection<ReindexingFilter> visitOther(final WhereFilter filter) {
        if (filter instanceof ReindexingFilter) {
            return List.of((ReindexingFilter) filter);
        }
        return List.of();
    }

    @Override
    public Collection<ReindexingFilter> visit(final WhereFilterInvertedImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<ReindexingFilter> visit(final WhereFilterSerialImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<ReindexingFilter> visit(final WhereFilterWithDeclaredBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<ReindexingFilter> visit(final WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<ReindexingFilter> visit(final DisjunctiveFilter filter) {
        // DisjunctiveFilter should disallow reindexing filter components, but visit the children anyway because
        // this could change.
        return filter.getFilters().stream()
                .flatMap(whereFilter -> of(whereFilter).stream())
                .collect(Collectors.toList());
    }

    @Override
    public Collection<ReindexingFilter> visit(final ConjunctiveFilter filter) {
        // ConjunctiveFilter should disallow reindexing filter components, but visit the children anyway because
        // this could change.
        return filter.getFilters().stream()
                .flatMap(whereFilter -> of(whereFilter).stream())
                .collect(Collectors.toList());
    }
}
