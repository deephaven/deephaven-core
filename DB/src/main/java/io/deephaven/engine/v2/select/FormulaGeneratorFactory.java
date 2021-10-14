package io.deephaven.engine.v2.select;

public interface FormulaGeneratorFactory {
    FormulaGenerator forFormula(String formulaString, String columnName);
}
