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

public class ExtractInnerConjunctiveFilters implements WhereFilter.Visitor<List<WhereFilter>> {
    public static final ExtractInnerConjunctiveFilters INSTANCE = new ExtractInnerConjunctiveFilters();

    public static List<WhereFilter> of(final WhereFilter filter) {
        return filter.walkWhereFilter(INSTANCE);
    }

    @Override
    public List<WhereFilter> visitWhereFilter(final WhereFilter filter) {
        final List<WhereFilter> retValue = WhereFilter.Visitor.super.visitWhereFilter(filter);
        if (retValue == null) {
            return List.of(filter);
        }
        return retValue;
    }

    @Override
    public List<WhereFilter> visitWhereFilter(final WhereFilterInvertedImpl filter) {
        return List.of(filter);
    }

    @Override
    public List<WhereFilter> visitWhereFilter(final WhereFilterSerialImpl filter) {
        return List.of(filter);
    }

    @Override
    public List<WhereFilter> visitWhereFilter(final WhereFilterWithDeclaredBarriersImpl filter) {
        return List.of(filter);
    }

    @Override
    public List<WhereFilter> visitWhereFilter(final WhereFilterWithRespectedBarriersImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter()).stream()
                .map(wf -> WhereFilterWithRespectedBarriersImpl.of(wf, filter.respectedBarriers()))
                .collect(Collectors.toList());
    }

    @Override
    public List<WhereFilter> visitWhereFilter(final DisjunctiveFilter filter) {
        return List.of(filter);
    }

    @Override
    public List<WhereFilter> visitWhereFilter(final ConjunctiveFilter filter) {
        return filter.getFilters().stream()
                .flatMap(f -> visitWhereFilter(f).stream())
                .collect(Collectors.toList());
    }
}
