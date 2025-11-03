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
        return filter.walkWhereFilter(EXTRACT_CONDITION_FILTERS);
    }

    @Override
    public Stream<ConditionFilter> visitWhereFilterOther(WhereFilter filter) {
        return filter instanceof ConditionFilter ? Stream.of((ConditionFilter) filter) : Stream.empty();
    }

    @Override
    public Stream<ConditionFilter> visitWhereFilter(WhereFilterInvertedImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visitWhereFilter(WhereFilterSerialImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visitWhereFilter(WhereFilterWithDeclaredBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visitWhereFilter(WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter());
    }

    @Override
    public Stream<ConditionFilter> visitWhereFilter(DisjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractConditionFilters::of);
    }

    @Override
    public Stream<ConditionFilter> visitWhereFilter(ConjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractConditionFilters::of);
    }
}
