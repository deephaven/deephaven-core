//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.*;
import io.deephaven.api.filter.Filter.Visitor;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Performs a recursive "barrier-extraction" against {@code filter}. If {@code filter}, or any sub-filter, is a
 * {@link FilterWithDeclaredBarrier}, {@link FilterWithDeclaredBarrier#barriers()} will be included in the returned
 * collection. Otherwise, an empty collection will be returned.
 */
public enum ExtractBarriers implements Visitor<Collection<Object>>, WhereFilter.Visitor<Collection<Object>> {
    INSTANCE;

    public static Collection<Object> of(Filter filter) {
        if (filter instanceof WhereFilter) {
            final Collection<Object> retVal =
                    ((WhereFilter) filter).walkWhereFilter(INSTANCE);
            return retVal == null ? Collections.emptyList() : retVal;
        }
        return filter.walk(INSTANCE);
    }

    @Override
    public Collection<Object> visit(FilterIsNull isNull) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterComparison comparison) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterIn in) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterNot<?> not) {
        return not.filter().walk(this);
    }

    @Override
    public Collection<Object> visit(FilterOr ors) {
        final Set<Object> barriers = new HashSet<>();
        for (final Filter filter : ors.filters()) {
            barriers.addAll(of(filter));
        }
        return barriers;
    }

    @Override
    public Collection<Object> visit(FilterAnd ands) {
        final Set<Object> barriers = new HashSet<>();
        for (final Filter filter : ands.filters()) {
            barriers.addAll(of(filter));
        }
        return barriers;
    }

    @Override
    public Collection<Object> visit(FilterPattern pattern) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterSerial serial) {
        return of(serial.filter());
    }

    @Override
    public Collection<Object> visit(FilterWithDeclaredBarrier declaredBarrier) {
        final Set<Object> resultBarriers = new HashSet<>(List.of(declaredBarrier.barriers()));
        resultBarriers.addAll(of(declaredBarrier.filter()));
        return resultBarriers;
    }

    @Override
    public Collection<Object> visit(FilterWithRespectedBarrier respectedBarrier) {
        return of(respectedBarrier.filter());
    }

    @Override
    public Collection<Object> visit(Function function) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(Method method) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(boolean literal) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(RawString rawString) {
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
    public Collection<Object> visitWhereFilter(WhereFilterWithDeclaredBarrierImpl filter) {
        final Set<Object> resultBarriers = new HashSet<>(List.of(filter.declaredBarriers()));
        resultBarriers.addAll(of(filter.getWrappedFilter()));
        return resultBarriers;
    }

    @Override
    public Collection<Object> visitWhereFilter(WhereFilterWithRespectedBarrierImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Object> visitWhereFilter(DisjunctiveFilter filter) {
        final Set<Object> barriers = new HashSet<>();
        for (final Filter subFilter : filter.getFilters()) {
            barriers.addAll(of(subFilter));
        }
        return barriers;
    }

    @Override
    public Collection<Object> visitWhereFilter(ConjunctiveFilter filter) {
        final Set<Object> barriers = new HashSet<>();
        for (final Filter subFilter : filter.getFilters()) {
            barriers.addAll(of(subFilter));
        }
        return barriers;
    }
}
