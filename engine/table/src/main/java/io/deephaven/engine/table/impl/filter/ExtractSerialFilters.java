//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.api.filter.*;
import io.deephaven.engine.table.impl.select.*;

import java.util.*;

/**
 * Performs a recursive filter extraction against {@code filter}. If {@code filter}, or any sub-filter, is a
 * {@link FilterSerial} or {@link WhereFilterSerialImpl}, the filter will be included in the returned collection.
 * Otherwise, an empty collection will be returned.
 */
public enum ExtractSerialFilters implements WhereFilter.Visitor<Collection<Filter>> {
    EXTRACT_SERIAL_FILTERS;

    public static Collection<Filter> of(WhereFilter filter) {
        return filter.walkWhereFilter(EXTRACT_SERIAL_FILTERS);
    }

    @Override
    public Collection<Filter> visitWhereFilterOther(WhereFilter filter) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visitWhereFilter(WhereFilterInvertedImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Filter> visitWhereFilter(WhereFilterSerialImpl filter) {
        return List.of(filter); // return this filter
    }

    @Override
    public Collection<Filter> visitWhereFilter(WhereFilterWithDeclaredBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Filter> visitWhereFilter(WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Filter> visitWhereFilter(DisjunctiveFilter disjunctiveFilters) {
        final List<Filter> serialFilters = new ArrayList<>();
        for (final WhereFilter filter : disjunctiveFilters.getFilters()) {
            serialFilters.addAll(of(filter));
        }
        return serialFilters;
    }

    @Override
    public Collection<Filter> visitWhereFilter(ConjunctiveFilter conjunctiveFilters) {
        final List<Filter> serialFilters = new ArrayList<>();
        for (final WhereFilter filter : conjunctiveFilters.getFilters()) {
            serialFilters.addAll(of(filter));
        }
        return serialFilters;
    }
}
