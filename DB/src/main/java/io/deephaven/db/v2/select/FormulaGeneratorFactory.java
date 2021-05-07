package io.deephaven.db.v2.select;

public interface FormulaGeneratorFactory {
    FormulaGenerator forFormula(String formulaString, String columnName);
}
