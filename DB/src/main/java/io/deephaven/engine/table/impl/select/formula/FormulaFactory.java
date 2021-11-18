package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.engine.tables.select.Param;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.TrackingRowSet;

import java.util.Map;

public interface FormulaFactory {
    Formula createFormula(TrackingRowSet rowSet, boolean initLazyMap, Map<String, ? extends ColumnSource> columnsToData,
            Param... params);
}
