//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterWithDeclaredBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterWithRespectedBarriersImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

public enum TransformToFinalFormula implements WhereFilter.Visitor<WhereFilter> {
    TRANSFORM_TO_FINAL_FORMULA;

    public static WhereFilter of(final WhereFilter filter) {
        return filter.walk(TRANSFORM_TO_FINAL_FORMULA);
    }

    @Override
    public WhereFilter visitOther(WhereFilter filter) {
        if (filter instanceof AbstractConditionFilter
                && ((AbstractConditionFilter) filter).hasConstantArrayAccess()) {
            return WhereFilterFactory
                    .getExpression(((AbstractConditionFilter) filter).getFormulaShiftedColumnDefinitions().getFirst());
        }
        return filter;
    }

    @Override
    public WhereFilter visit(final WhereFilterInvertedImpl filter) {
        return WhereFilterInvertedImpl.of(of(filter.getWrappedFilter()));
    }

    @Override
    public WhereFilter visit(final WhereFilterSerialImpl filter) {
        return of(filter.getWrappedFilter()).withSerial();
    }

    @Override
    public WhereFilter visit(final WhereFilterWithDeclaredBarriersImpl filter) {
        return of(filter.getWrappedFilter()).withDeclaredBarriers(filter.declaredBarriers());
    }

    @Override
    public WhereFilter visit(final WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter()).withRespectedBarriers(filter.respectedBarriers());
    }

    @Override
    public WhereFilter visit(final DisjunctiveFilter filter) {
        return DisjunctiveFilter.of(
                filter.getFilters().stream()
                        .map(TransformToFinalFormula::of)
                        .toArray(WhereFilter[]::new));
    }

    @Override
    public WhereFilter visit(final ConjunctiveFilter filter) {
        return ConjunctiveFilter.of(
                filter.getFilters().stream()
                        .map(TransformToFinalFormula::of)
                        .toArray(WhereFilter[]::new));
    }
}
