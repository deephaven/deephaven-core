//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Performs a recursive "barrier-extraction" against {@code filter}. If {@code filter}, or any sub-filter, is a
 * {@link WhereFilterWithDeclaredBarriersImpl}, {@link WhereFilterWithDeclaredBarriersImpl#declaredBarriers()} will be
 * included in the returned collection. Otherwise, an empty collection will be returned.
 */
public enum ExtractBarriers implements WhereFilter.Visitor<Stream<Object>> {
    EXTRACT_BARRIERS;

    public static Collection<Object> of(WhereFilter filter) {
        try (final Stream<Object> stream = stream(filter)) {
            return stream.collect(Collectors.toSet());
        }
    }

    public static Stream<Object> stream(WhereFilter filter) {
        return filter.walk(EXTRACT_BARRIERS);
    }

    @Override
    public Stream<Object> visitOther(WhereFilter filter) {
        return Stream.empty();
    }

    @Override
    public Stream<Object> visit(WhereFilterInvertedImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<Object> visit(WhereFilterSerialImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<Object> visit(WhereFilterWithDeclaredBarriersImpl filter) {
        return Stream.concat(Stream.of(filter.declaredBarriers()), stream(filter.getWrappedFilter()));
    }

    @Override
    public Stream<Object> visit(WhereFilterWithRespectedBarriersImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<Object> visit(DisjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractBarriers::stream);
    }

    @Override
    public Stream<Object> visit(ConjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractBarriers::stream);
    }
}
