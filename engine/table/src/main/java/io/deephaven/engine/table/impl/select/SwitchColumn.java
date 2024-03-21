//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.*;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.select.python.FormulaColumnPython;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

public class SwitchColumn implements SelectColumn {

    @NotNull
    private final String expression;
    @NotNull
    private final String columnName;
    private SelectColumn realColumn;
    private final FormulaParserConfiguration parser;


    public SwitchColumn(String columnName, String expression, FormulaParserConfiguration parserConfiguration) {
        this.expression = Require.neqNull(expression, "expression");
        this.columnName = NameValidator.validateColumnName(columnName);
        this.parser = parserConfiguration;
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        if (realColumn == null) {
            if (columnsOfInterest.get(expression) != null) {
                realColumn = new SourceColumn(expression, columnName);
            } else {
                realColumn = FormulaColumn.createFormulaColumn(columnName, expression, parser);
            }
        }
        return realColumn.initInputs(rowSet, columnsOfInterest);
    }

    @Override
    public List<String> initDef(@NotNull Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        return initDef(columnDefinitionMap, QueryCompilerRequestProcessor.immediate());
    }

    @Override
    public List<String> initDef(
            @NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        if (realColumn == null) {
            if (columnDefinitionMap.get(expression) != null) {
                realColumn = new SourceColumn(expression, columnName);
            } else {
                realColumn = FormulaColumn.createFormulaColumn(columnName, expression, parser);
            }
        }
        final List<String> usedColumns = realColumn.initDef(columnDefinitionMap, compilationRequestProcessor);
        if (realColumn instanceof DhFormulaColumn) {
            FormulaColumnPython formulaColumnPython = ((DhFormulaColumn) realColumn).getFormulaColumnPython();
            realColumn = formulaColumnPython != null ? formulaColumnPython : realColumn;
        }
        return usedColumns;
    }

    @Override
    public Class<?> getReturnedType() {
        return getRealColumn().getReturnedType();
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return getRealColumn().getReturnedComponentType();
    }

    @Override
    public List<String> getColumns() {
        return getRealColumn().getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return getRealColumn().getColumnArrays();
    }

    @NotNull
    @Override
    public ColumnSource<?> getDataView() {
        return getRealColumn().getDataView();
    }

    @NotNull
    @Override
    public ColumnSource<?> getLazyView() {
        return getRealColumn().getLazyView();
    }

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public MatchPair getMatchPair() {
        return new MatchPair(columnName, expression);
    }

    @Override
    public WritableColumnSource<?> newDestInstance(long size) {
        return getRealColumn().newDestInstance(size);
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(long size) {
        return getRealColumn().newFlatDestInstance(size);
    }

    @Override
    public boolean isRetain() {
        return false; // We use SourceColumns if there's no "=", so there's no need for something more complicated here.
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        getRealColumn().validateSafeForRefresh(sourceTable);
    }

    @Override
    public String toString() {
        return columnName + "=" + expression;
    }

    public SelectColumn getRealColumn() {
        if (realColumn == null) {
            throw new IllegalStateException(
                    "getRealColumn() is not available until this SwitchColumn is initialized; ensure that initInputs or initDef has been called first");
        }
        return realColumn;
    }

    @Override
    public boolean isStateless() {
        return getRealColumn().isStateless();
    }

    @Override
    public SwitchColumn copy() {
        final SwitchColumn switchColumn = new SwitchColumn(columnName, expression, parser);
        if (realColumn != null) {
            switchColumn.realColumn = realColumn.copy();
        }
        return switchColumn;
    }
}
