//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;

import java.util.List;

public interface ColumnExpressionValidator {
    WhereFilter[] validateSelectFilters(final String[] conditionalExpressions, final Table table);

    void validateColumnExpressions(final SelectColumn[] selectColumns, final String[] originalExpressions,
            final Table table);

    void validateConditionFilters(List<ConditionFilter> conditionFilters, Table sourceTable);
}
