//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.api.filter.*;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Performs a recursive "barrier-extraction" against {@code filter}. If {@code filter}, or any sub-filter, is a
 * {@link FilterWithDeclaredBarriers}, {@link FilterWithDeclaredBarriers#declaredBarriers()} will be included in the
 * returned collection. Otherwise, an empty collection will be returned.
 */
public enum ExtractBarriers implements WhereFilter.Visitor<Collection<Object>> {
    EXTRACT_BARRIERS;

    public static Collection<Object> of(WhereFilter filter) {
        return filter.walkWhereFilter(EXTRACT_BARRIERS);
    }

    @Override
    public Collection<Object> visitWhereFilterOther(WhereFilter filter) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visitWhereFilter(WhereFilterInvertedImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Object> visitWhereFilter(WhereFilterSerialImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Object> visitWhereFilter(WhereFilterWithDeclaredBarriersImpl filter) {
        final Set<Object> resultBarriers = new HashSet<>(List.of(filter.declaredBarriers()));
        resultBarriers.addAll(of(filter.getWrappedFilter()));
        return resultBarriers;
    }

    @Override
    public Collection<Object> visitWhereFilter(WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Object> visitWhereFilter(DisjunctiveFilter filter) {
        final Set<Object> barriers = new HashSet<>();
        for (final WhereFilter subFilter : filter.getFilters()) {
            barriers.addAll(of(subFilter));
        }
        return barriers;
    }

    @Override
    public Collection<Object> visitWhereFilter(ConjunctiveFilter filter) {
        final Set<Object> barriers = new HashSet<>();
        for (final WhereFilter subFilter : filter.getFilters()) {
            barriers.addAll(of(subFilter));
        }
        return barriers;
    }
}
