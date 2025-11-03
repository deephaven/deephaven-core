//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.api.filter.FilterWithRespectedBarriers;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Performs a recursive "respected-barrier-extraction" against {@code filter}. If {@code filter}, or any sub-filter, is
 * a {@link FilterWithRespectedBarriers}, {@link FilterWithRespectedBarriers#respectedBarriers()} will be included in
 * the returned collection. Otherwise, an empty collection will be returned.
 */
public enum ExtractRespectedBarriers implements WhereFilter.Visitor<Stream<Object>> {
    EXTRACT_RESPECTED_BARRIERS;

    public static Collection<Object> of(WhereFilter filter) {
        return stream(filter).collect(Collectors.toSet());
    }

    public static Stream<Object> stream(WhereFilter filter) {
        return filter.walkWhereFilter(EXTRACT_RESPECTED_BARRIERS);
    }

    @Override
    public Stream<Object> visitWhereFilterOther(WhereFilter filter) {
        return Stream.empty();
    }

    @Override
    public Stream<Object> visitWhereFilter(WhereFilterInvertedImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<Object> visitWhereFilter(WhereFilterSerialImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<Object> visitWhereFilter(WhereFilterWithDeclaredBarriersImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<Object> visitWhereFilter(WhereFilterWithRespectedBarriersImpl filter) {
        return Stream.concat(Stream.of(filter.respectedBarriers()), stream(filter.getWrappedFilter()));
    }

    @Override
    public Stream<Object> visitWhereFilter(DisjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractRespectedBarriers::stream);
    }

    @Override
    public Stream<Object> visitWhereFilter(ConjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractRespectedBarriers::stream);
    }
}
