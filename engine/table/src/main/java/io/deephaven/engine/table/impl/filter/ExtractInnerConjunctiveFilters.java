//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum ExtractInnerConjunctiveFilters implements WhereFilter.Visitor<Stream<WhereFilter>> {
    EXTRACT_INNER_CONJUNCTIVE_FILTERS;

    public static List<WhereFilter> of(final WhereFilter filter) {
        try (final Stream<WhereFilter> stream = stream(filter)) {
            return stream.collect(Collectors.toList());
        }
    }

    public static Stream<WhereFilter> stream(final WhereFilter filter) {
        return filter.walkWhereFilter(EXTRACT_INNER_CONJUNCTIVE_FILTERS);
    }

    @Override
    public Stream<WhereFilter> visitWhereFilterOther(final WhereFilter filter) {
        return Stream.of(filter);
    }

    @Override
    public Stream<WhereFilter> visitWhereFilter(final WhereFilterInvertedImpl filter) {
        return Stream.of(filter);
    }

    @Override
    public Stream<WhereFilter> visitWhereFilter(final WhereFilterSerialImpl filter) {
        return Stream.of(filter);
    }

    @Override
    public Stream<WhereFilter> visitWhereFilter(final WhereFilterWithDeclaredBarriersImpl filter) {
        return Stream.of(filter);
    }

    @Override
    public Stream<WhereFilter> visitWhereFilter(final WhereFilterWithRespectedBarriersImpl filter) {
        return stream(filter.getWrappedFilter())
                .map(wf -> WhereFilterWithRespectedBarriersImpl.of(wf, filter.respectedBarriers()));
    }

    @Override
    public Stream<WhereFilter> visitWhereFilter(final DisjunctiveFilter filter) {
        return Stream.of(filter);
    }

    @Override
    public Stream<WhereFilter> visitWhereFilter(final ConjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractInnerConjunctiveFilters::stream);
    }
}
