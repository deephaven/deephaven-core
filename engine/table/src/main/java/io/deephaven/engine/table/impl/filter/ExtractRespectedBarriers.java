//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterBarrier;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterRespectsBarrier;
import io.deephaven.api.filter.FilterSerial;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterRespectsBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Performs a recursive "respected-barrier-extraction" against {@code filter}. If {@code filter}, or any sub-filter, is
 * a {@link FilterRespectsBarrier}, {@link FilterRespectsBarrier#respectedBarriers()} will be included in the returned
 * collection. Otherwise, an empty collection will be returned.
 */
public enum ExtractRespectedBarriers
        implements Filter.Visitor<Collection<Object>>, WhereFilter.Visitor<Collection<Object>> {
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
        return of(not.filter());
    }

    @Override
    public Collection<Object> visit(FilterOr ors) {
        final Set<Object> barriers = new HashSet<>();
        for (final Filter subFilter : ors.filters()) {
            barriers.addAll(of(subFilter));
        }
        return barriers;
    }

    @Override
    public Collection<Object> visit(FilterAnd ands) {
        final Set<Object> barriers = new HashSet<>();
        for (final Filter subFilter : ands.filters()) {
            barriers.addAll(of(subFilter));
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
    public Collection<Object> visit(FilterBarrier barrier) {
        return of(barrier.filter());
    }

    @Override
    public Collection<Object> visit(FilterRespectsBarrier respectsBarrier) {
        final Set<Object> barriers = new HashSet<>(Arrays.asList(respectsBarrier.respectedBarriers()));
        barriers.addAll(of(respectsBarrier.filter()));
        return barriers;
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
    public Collection<Object> visitWhereFilter(WhereFilterBarrierImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Collection<Object> visitWhereFilter(WhereFilterRespectsBarrierImpl filter) {
        final Set<Object> barriers = new HashSet<>(Arrays.asList(filter.respectedBarriers()));
        barriers.addAll(of(filter.getWrappedFilter()));
        return barriers;
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
