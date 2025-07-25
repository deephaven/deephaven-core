//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Set;

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

    /**
     * Returns true if the formula expression of the column has Array Access that conforms to "i +/- &lt;constant&gt;"
     * or "ii +/- &lt;constant&gt;".
     *
     * @return true or false
     */
    default boolean hasConstantArrayAccess() {
        return getFormulaShiftedColumnDefinitions() != null && !getFormulaShiftedColumnDefinitions().isEmpty();
    }

    /**
     * Returns a Set of ShiftedColumnDefinitions. If the column formula or expression has Array Access that conforms to
     * "i +/- &lt;constant&gt;" or "ii +/- &lt;constant&gt;". If there is a parsing error for the expression, this
     * method's result is undefined.
     *
     * @return the Set of ShiftedColumnDefinitions or null if there is no constant array access
     */
    default Set<ShiftedColumnDefinition> getFormulaShiftedColumnDefinitions() {
        return null;
    }

    /**
     * Returns a String representation of the formula expression with the shifted column names replaced by the
     * corresponding ShiftedColumnDefinition.getResultColumnName() values.
     *
     * @return the shifted formula string or null if there is no constant array access
     */
    default String getShiftedFormulaString() {
        return null;
    }
}
