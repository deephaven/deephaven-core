//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.ShiftedColumnDefinition;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum ExtractShiftedColumnDefinitions
        implements WhereFilter.Visitor<Stream<ShiftedColumnDefinition>> {
    EXTRACT_SHIFTED_COLUMN_DEFINITIONS;

    public static Set<ShiftedColumnDefinition> of(final WhereFilter filter) {
        try (final Stream<ShiftedColumnDefinition> stream = stream(filter)) {
            return stream.collect(Collectors.toSet());
        }
    }

    public static Stream<ShiftedColumnDefinition> stream(final WhereFilter filter) {
        return filter.walk(EXTRACT_SHIFTED_COLUMN_DEFINITIONS);
    }

    public static boolean hasAny(final WhereFilter filter) {
        try (final Stream<ShiftedColumnDefinition> stream = stream(filter)) {
            return stream.findAny().isPresent();
        }
    }

    @Override
    public Stream<ShiftedColumnDefinition> visitOther(WhereFilter filter) {
        if (filter instanceof AbstractConditionFilter
                && ((AbstractConditionFilter) filter).hasConstantArrayAccess()) {
            return ((AbstractConditionFilter) filter).getFormulaShiftedColumnDefinitions().getSecond().stream();
        }
        return Stream.empty();
    }

    @Override
    public Stream<ShiftedColumnDefinition> visit(final WhereFilterInvertedImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<ShiftedColumnDefinition> visit(final WhereFilterSerialImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<ShiftedColumnDefinition> visit(final WhereFilterWithDeclaredBarriersImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<ShiftedColumnDefinition> visit(final WhereFilterWithRespectedBarriersImpl filter) {
        return stream(filter.getWrappedFilter());
    }

    @Override
    public Stream<ShiftedColumnDefinition> visit(final DisjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractShiftedColumnDefinitions::stream);
    }

    @Override
    public Stream<ShiftedColumnDefinition> visit(final ConjunctiveFilter filter) {
        return filter.getFilters().stream().flatMap(ExtractShiftedColumnDefinitions::stream);
    }
}
