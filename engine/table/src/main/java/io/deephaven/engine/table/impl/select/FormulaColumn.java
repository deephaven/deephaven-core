/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

public interface FormulaColumn extends SelectColumn {

    static FormulaColumn createFormulaColumn(String columnName, String formulaString,
            FormulaParserConfiguration parser) {
        switch (parser) {
            case Deephaven:
                return new DhFormulaColumn(columnName, formulaString);
            case Numba:
                throw new UnsupportedOperationException("Python formula columns must be created from python");
            default:
                throw new UnsupportedOperationException("Parser support not implemented for " + parser);
        }
    }

    static FormulaColumn createFormulaColumn(String columnName, String formulaString) {
        return createFormulaColumn(columnName, formulaString, FormulaParserConfiguration.parser);
    }

    /**
     * @return true if all rows have a single constant value
     */
    default boolean hasConstantValue() {
        return false;
    }
}
