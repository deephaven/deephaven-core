//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.*;

import java.util.stream.Stream;

/**
 * Performs a recursive filter extraction against {@code filter}. If {@code filter}, or any sub-filter, is a
 * {@link WhereFilterSerialImpl}, the filter will be included in the returned stream. Otherwise, an empty stream will be
 * returned.
 */
public enum ExtractSerialFilters implements WhereFilter.Visitor<Stream<WhereFilterSerialImpl>> {
    EXTRACT_SERIAL_FILTERS;

    public static Stream<WhereFilterSerialImpl> stream(WhereFilter filter) {
        return filter.walk(EXTRACT_SERIAL_FILTERS);
    }

    public static boolean hasAny(WhereFilter filter) {
        try (final Stream<WhereFilterSerialImpl> stream = stream(filter)) {
            return stream.findAny().isPresent();
        }
    }

    @Override
    public Stream<WhereFilterSerialImpl> visitOther(WhereFilter filter) {
        return Stream.empty();
    }

    @Override
    public Stream<WhereFilterSerialImpl> visit(WhereFilterInvertedImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<WhereFilterSerialImpl> visit(WhereFilterSerialImpl filter) {
        return Stream.of(filter); // return this filter
    }

    @Override
    public Stream<WhereFilterSerialImpl> visit(WhereFilterWithDeclaredBarriersImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<WhereFilterSerialImpl> visit(WhereFilterWithRespectedBarriersImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<WhereFilterSerialImpl> visit(DisjunctiveFilter disjunctiveFilters) {
        return disjunctiveFilters.getFilters().stream().flatMap(ExtractSerialFilters::stream);
    }

    @Override
    public Stream<WhereFilterSerialImpl> visit(ConjunctiveFilter conjunctiveFilters) {
        return conjunctiveFilters.getFilters().stream().flatMap(ExtractSerialFilters::stream);
    }
}
