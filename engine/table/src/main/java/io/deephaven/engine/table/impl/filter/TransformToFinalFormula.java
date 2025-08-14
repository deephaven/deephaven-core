//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterRespectsBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

public class TransformToFinalFormula implements WhereFilter.Visitor<WhereFilter> {
    public static final TransformToFinalFormula INSTANCE = new TransformToFinalFormula();

    public static WhereFilter of(final WhereFilter filter) {
        return filter.walkWhereFilter(INSTANCE);
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilter filter) {
        if (filter instanceof AbstractConditionFilter
                && ((AbstractConditionFilter) filter).hasConstantArrayAccess()) {
            return WhereFilterFactory
                    .getExpression(((AbstractConditionFilter) filter).getFormulaShiftedColumnDefinitions().getFirst());
        }
        WhereFilter other = WhereFilter.Visitor.super.visitWhereFilter(filter);
        return other == null ? filter : other;
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterInvertedImpl filter) {
        return WhereFilterInvertedImpl.of(visitWhereFilter(filter.getWrappedFilter()));
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterSerialImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter()).withSerial();
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterBarrierImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter()).withBarriers(filter.barriers());
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterRespectsBarrierImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter()).respectsBarriers(filter.respectedBarriers());
    }

    @Override
    public WhereFilter visitWhereFilter(final DisjunctiveFilter filter) {
        return DisjunctiveFilter.of(
                filter.getFilters().stream()
                        .map(f -> f.walkWhereFilter(this))
                        .toArray(WhereFilter[]::new));
    }

    @Override
    public WhereFilter visitWhereFilter(final ConjunctiveFilter filter) {
        return ConjunctiveFilter.of(
                filter.getFilters().stream()
                        .map(f -> f.walkWhereFilter(this))
                        .toArray(WhereFilter[]::new));
    }
}
