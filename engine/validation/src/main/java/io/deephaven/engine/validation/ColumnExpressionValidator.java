//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.validation;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.*;

import java.util.ArrayList;
import java.util.List;

public interface ColumnExpressionValidator {
    WhereFilter[] validateSelectFilters(final String[] conditionalExpressions, final TableDefinition definition);

    void validateColumnExpressions(final SelectColumn[] selectColumns, final String[] originalExpressions,
            final TableDefinition definition);

    void validateConditionFilters(List<ConditionFilter> conditionFilters, TableDefinition definition);

    default void validateWhereFilters(List<WhereFilter> whereFilter, TableDefinition definition) {
        validateConditionFilters(extractConditionFilters(whereFilter), definition);
    }

    private static List<ConditionFilter> extractConditionFilters(List<WhereFilter> whereFilters) {
        final List<ConditionFilter> conditionFilters = new ArrayList<>();

        final WhereFilter.Visitor<Void> visitor = new WhereFilter.Visitor<>() {
            @Override
            public Void visitWhereFilter(WhereFilter filter) {
                if (filter instanceof ConditionFilter) {
                    conditionFilters.add((ConditionFilter) filter);
                    return null;
                }
                return WhereFilter.Visitor.super.visitWhereFilter(filter);
            }

            @Override
            public Void visitWhereFilter(WhereFilterInvertedImpl filter) {
                visitWhereFilter(filter.getWrappedFilter());
                return null;
            }

            @Override
            public Void visitWhereFilter(WhereFilterSerialImpl filter) {
                visitWhereFilter(filter.getWrappedFilter());
                return null;
            }

            @Override
            public Void visitWhereFilter(WhereFilterWithDeclaredBarriersImpl filter) {
                visitWhereFilter(filter.getWrappedFilter());
                return null;
            }

            @Override
            public Void visitWhereFilter(WhereFilterWithRespectedBarriersImpl filter) {
                visitWhereFilter(filter.getWrappedFilter());
                return null;
            }

            @Override
            public Void visitWhereFilter(DisjunctiveFilter filter) {
                filter.getFilters().forEach(this::visitWhereFilter);
                return null;
            }

            @Override
            public Void visitWhereFilter(ConjunctiveFilter filter) {
                filter.getFilters().forEach(this::visitWhereFilter);
                return null;
            }
        };

        whereFilters.forEach(visitor::visitWhereFilter);

        return conditionFilters;
    }
}
