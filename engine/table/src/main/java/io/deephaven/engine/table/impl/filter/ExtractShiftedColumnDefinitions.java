//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.ShiftedColumnDefinition;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterRespectsBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtractShiftedColumnDefinitions
        implements WhereFilter.Visitor<Set<ShiftedColumnDefinition>> {
    public static final ExtractShiftedColumnDefinitions INSTANCE = new ExtractShiftedColumnDefinitions();

    public static Set<ShiftedColumnDefinition> of(final WhereFilter filter) {
        return filter.walkWhereFilter(INSTANCE);
    }

    @Override
    public Set<ShiftedColumnDefinition> visitWhereFilter(final WhereFilter filter) {
        if (filter instanceof AbstractConditionFilter
                && ((AbstractConditionFilter) filter).hasConstantArrayAccess()) {
            return new HashSet<>(((AbstractConditionFilter) filter).getFormulaShiftedColumnDefinitions().getSecond());
        }
        return WhereFilter.Visitor.super.visitWhereFilter(filter);
    }

    @Override
    public Set<ShiftedColumnDefinition> visitWhereFilter(final WhereFilterInvertedImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Set<ShiftedColumnDefinition> visitWhereFilter(final WhereFilterSerialImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Set<ShiftedColumnDefinition> visitWhereFilter(final WhereFilterBarrierImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Set<ShiftedColumnDefinition> visitWhereFilter(final WhereFilterRespectsBarrierImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Set<ShiftedColumnDefinition> visitWhereFilter(final DisjunctiveFilter filter) {
        return filter.getFilters().stream()
                .flatMap(f -> {
                    Set<ShiftedColumnDefinition> res = f.walkWhereFilter(this);
                    return res == null ? Stream.empty() : res.stream();
                })
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ShiftedColumnDefinition> visitWhereFilter(final ConjunctiveFilter filter) {
        return filter.getFilters().stream()
                .flatMap(f -> {
                    Set<ShiftedColumnDefinition> res = f.walkWhereFilter(this);
                    return res == null ? Stream.empty() : res.stream();
                })
                .collect(Collectors.toSet());
    }
}
