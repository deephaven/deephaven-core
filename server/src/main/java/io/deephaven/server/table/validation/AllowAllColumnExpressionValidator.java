//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;

import java.util.List;

/**
 * A ColumnExpressionValidator that permits all expressions to be executed.
 *
 * <p>
 * Using this validator without a secure authentication method is unsafe, as arbitrary code can be executed.
 * </p>
 */
public class AllowAllColumnExpressionValidator implements ColumnExpressionValidator {
    @Override
    public WhereFilter[] validateSelectFilters(String[] conditionalExpressions, Table table) {
        return WhereFilterFactory.getExpressions(conditionalExpressions);
    }

    @Override
    public void validateColumnExpressions(SelectColumn[] selectColumns, String[] originalExpressions, Table table) {}

    @Override
    public void validateConditionFilters(List<ConditionFilter> conditionFilters, Table sourceTable) {}
}
