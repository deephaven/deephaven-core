package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;

import java.util.List;
import java.util.Map;

public interface FormulaGenerator {
    List<String> initDef(Map<String, ColumnDefinition> columnDefinitionMap,
            Map<String, ? extends ColumnSource> columnsOverride);

    Formula getFormula(RowSet rowSet, Class returnType, boolean initLazyMap,
            Map<String, ? extends ColumnSource> columnsToData,
            Map<String, ? extends ColumnSource> fallThroughColumns, boolean fallThroughContiguous,
            Map<String, ? extends ColumnSource> columnsOverride, RowSet overflowStateRowSet);

    Class getReturnedType();

    List<String> getColumnArrays();

    boolean disallowRefresh();
}
