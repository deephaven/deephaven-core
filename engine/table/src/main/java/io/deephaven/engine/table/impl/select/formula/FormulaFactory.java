//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.TrackingRowSet;

import java.util.Map;

public interface FormulaFactory {
    Formula createFormula(
            String columnName,
            TrackingRowSet rowSet,
            boolean initLazyMap, Map<String, ? extends ColumnSource> columnsToData,
            QueryScopeParam... params);
}
