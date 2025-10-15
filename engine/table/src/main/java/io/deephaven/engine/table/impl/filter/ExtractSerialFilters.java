//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.*;
import io.deephaven.api.filter.Filter.Visitor;
import io.deephaven.engine.table.impl.select.*;

import java.util.*;

/**
 * Performs a recursive filter extraction against {@code filter}. If {@code filter}, or any sub-filter, is a
 * {@link FilterSerial} or {@link WhereFilterSerialImpl}, the filter will be included in the returned collection.
 * Otherwise, an empty collection will be returned.
 */
public enum ExtractSerialFilters implements Visitor<Collection<Filter>>, WhereFilter.Visitor<Collection<Filter>> {
    INSTANCE;

    public static Collection<Filter> of(Filter filter) {
        if (filter instanceof WhereFilter) {
            final Collection<Filter> retVal =
                    ((WhereFilter) filter).walkWhereFilter(INSTANCE);
            return retVal == null ? Collections.emptyList() : retVal;
        }
        return filter.walk(INSTANCE);
    }

    @Override
    public Collection<Filter> visit(FilterIsNull isNull) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visit(FilterComparison comparison) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visit(FilterIn in) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visit(FilterNot<?> not) {
        return not.filter().walk(this);
    }

    @Override
    public Collection<Filter> visit(FilterOr ors) {
        final List<Filter> serialFilters = new ArrayList<>();
        for (final Filter filter : ors.filters()) {
            serialFilters.addAll(of(filter));
        }
        return serialFilters;
    }

    @Override
    public Collection<Filter> visit(FilterAnd ands) {
        final List<Filter> serialFilters = new ArrayList<>();
        for (final Filter filter : ands.filters()) {
            serialFilters.addAll(of(filter));
        }
        return serialFilters;
    }

    @Override
    public Collection<Filter> visit(FilterPattern pattern) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visit(FilterSerial serial) {
        return List.of(serial); // return this filter
    }

    @Override
    public Collection<Filter> visit(FilterWithDeclaredBarriers declaredBarrier) {
        return of(declaredBarrier.filter());
    }

    @Override
    public Collection<Filter> visit(FilterWithRespectedBarriers respectedBarrier) {
        return of(respectedBarrier.filter());
    }

    @Override
    public Collection<Filter> visit(Function function) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visit(Method method) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visit(boolean literal) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Filter> visit(RawString rawString) {
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
        for (final Filter filter : disjunctiveFilters.getFilters()) {
            serialFilters.addAll(of(filter));
        }
        return serialFilters;
    }

    @Override
    public Collection<Filter> visitWhereFilter(ConjunctiveFilter conjunctiveFilters) {
        final List<Filter> serialFilters = new ArrayList<>();
        for (final Filter filter : conjunctiveFilters.getFilters()) {
            serialFilters.addAll(of(filter));
        }
        return serialFilters;
    }
}
