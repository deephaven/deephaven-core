package io.deephaven.engine.table.impl.select;

public interface FormulaGeneratorFactory {
    FormulaGenerator forFormula(String formulaString, String columnName);
}
