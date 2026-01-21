//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.validation;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
        for (final WhereFilter whereFilter : whereFilters) {
            try (final Stream<ConditionFilter> stream = ExtractConditionFilters.of(whereFilter)) {
                stream.forEachOrdered(conditionFilters::add);
            }
        }
        return conditionFilters;
    }
}
