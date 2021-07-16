/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.select.python.FormulaColumnPython;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.utils.Index;
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
    public List<String> initInputs(Table table) {
        if (realColumn == null) {
            if (table.getDefinition().getColumn(expression) != null) {
                realColumn = new SourceColumn(expression, columnName);
            } else {
                realColumn = FormulaColumn.createFormulaColumn(columnName, expression, parser);
            }
        }
        return realColumn.initInputs(table);
    }

    @Override
    public List<String> initInputs(Index index, Map<String, ? extends ColumnSource> columnsOfInterest) {
        if (realColumn == null) {
            if (columnsOfInterest.get(expression) != null) {
                realColumn = new SourceColumn(expression, columnName);
            } else {
                realColumn = FormulaColumn.createFormulaColumn(columnName, expression, parser);
            }
        }
        return realColumn.initInputs(index, columnsOfInterest);
    }

    @Override
    public List<String> initDef(Map<String, ColumnDefinition> columnDefinitionMap) {
        if (realColumn == null) {
            if (columnDefinitionMap.get(expression) != null) {
                realColumn = new SourceColumn(expression, columnName);
            } else {
                realColumn = FormulaColumn.createFormulaColumn(columnName, expression, parser);
            }
        }
        List<String> usedColumns = realColumn.initDef(columnDefinitionMap);
        if (realColumn instanceof DhFormulaColumn) {
            FormulaColumnPython formulaColumnPython = ((DhFormulaColumn) realColumn).getFormulaColumnPython();
            realColumn = formulaColumnPython != null ? formulaColumnPython: realColumn;
        }
        return usedColumns;
    }

    @Override
    public Class getReturnedType() {
        return realColumn.getReturnedType();
    }

    @Override
    public List<String> getColumns() {
        return realColumn.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return realColumn.getColumnArrays();
    }

    @NotNull
    @Override
    public ColumnSource getDataView() {
        return realColumn.getDataView();
    }

    @NotNull
    @Override
    public ColumnSource getLazyView() {
        return realColumn.getLazyView();
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
    public WritableSource newDestInstance(long size) {
        return realColumn.newDestInstance(size);
    }

    @Override
    public boolean isRetain() {
        return false; // We use SourceColumns if there's no "=", so there's no need for something more complicated here.
    }

    @Override
    public String toString() {
        return columnName + "=" + expression;
    }

    public SelectColumn getRealColumn() {
        return realColumn;
    }

    @Override
    public boolean disallowRefresh() {
        return Require.neqNull(realColumn, "realColumn").disallowRefresh();
    }

    @Override
    public SwitchColumn copy() {
        return new SwitchColumn(columnName, expression, parser);
    }
}
