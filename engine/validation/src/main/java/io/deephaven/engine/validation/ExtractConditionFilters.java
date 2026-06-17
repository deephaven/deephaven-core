//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.validation;

import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;

import java.util.stream.Stream;

enum ExtractConditionFilters implements WhereFilter.Visitor<Stream<ConditionFilter>> {
    EXTRACT_CONDITION_FILTERS;

    public static Stream<ConditionFilter> of(final WhereFilter filter) {
        return filter.walk(EXTRACT_CONDITION_FILTERS);
    }

    @Override
    public Stream<ConditionFilter> visitOther(WhereFilter filter) {
        return filter instanceof ConditionFilter ? Stream.of((ConditionFilter) filter) : Stream.empty();
    }

    @Override
    public Stream<ConditionFilter> visit(WhereFilterInvertedImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visit(WhereFilterSerialImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visit(WhereFilterWithDeclaredBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visit(WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visit(DisjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractConditionFilters::of);
    }

    @Override
    public Stream<ConditionFilter> visit(ConjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractConditionFilters::of);
    }
}
