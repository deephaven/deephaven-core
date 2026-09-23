//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegating;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;

import java.util.stream.Stream;

/**
 * Streams a {@link WhereFilter} together with every filter it wraps or is composed of, at any depth, parents before
 * their children. Unlike the other visitors here, which descend only through the wrapper types
 * {@link WhereFilter.Visitor} names, this also descends into any other {@link WhereFilterDelegating} implementation, so
 * a question asked of every filter in the stream cannot be answered wrongly by a wrapper that fails to delegate it.
 *
 * <p>
 * NB: This is only for inspecting the filters in a tree, for instance to check their types or properties. A streamed
 * filter is detached from the wrappers and composed filters above it: it has lost their inversion, barriers, serial
 * ordering, and conjunction or disjunction with its siblings. None of the streamed filters may be used directly for
 * filtering.
 * </p>
 */
public enum ExtractAllFilters implements WhereFilter.Visitor<Stream<WhereFilter>> {
    EXTRACT_ALL_FILTERS;

    public static Stream<WhereFilter> stream(final WhereFilter filter) {
        return filter.walk(EXTRACT_ALL_FILTERS);
    }

    private static Stream<WhereFilter> withWrapped(final WhereFilter filter, final WhereFilter wrapped) {
        return Stream.concat(Stream.of(filter), stream(wrapped));
    }

    @Override
    public Stream<WhereFilter> visitOther(final WhereFilter filter) {
        if (filter instanceof WhereFilterDelegating) {
            return withWrapped(filter, ((WhereFilterDelegating) filter).getWrappedFilter());
        }
        return Stream.of(filter);
    }

    @Override
    public Stream<WhereFilter> visit(final WhereFilterInvertedImpl filter) {
        return withWrapped(filter, filter.getWrappedFilter());
    }

    @Override
    public Stream<WhereFilter> visit(final WhereFilterSerialImpl filter) {
        return withWrapped(filter, filter.getWrappedFilter());
    }

    @Override
    public Stream<WhereFilter> visit(final WhereFilterWithDeclaredBarriersImpl filter) {
        return withWrapped(filter, filter.getWrappedFilter());
    }

    @Override
    public Stream<WhereFilter> visit(final WhereFilterWithRespectedBarriersImpl filter) {
        return withWrapped(filter, filter.getWrappedFilter());
    }

    @Override
    public Stream<WhereFilter> visit(final DisjunctiveFilter filter) {
        return Stream.concat(Stream.of(filter), filter.getFilters().stream().flatMap(ExtractAllFilters::stream));
    }

    @Override
    public Stream<WhereFilter> visit(final ConjunctiveFilter filter) {
        return Stream.concat(Stream.of(filter), filter.getFilters().stream().flatMap(ExtractAllFilters::stream));
    }
}
