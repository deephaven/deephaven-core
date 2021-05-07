package io.deephaven.db.v2.select;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;

public interface FormulaColumn extends SelectColumn {

    static FormulaColumn createFormulaColumn(String columnName, String formulaString, FormulaParserConfiguration parser) {
        switch (parser) {
            case Deephaven:
                return new DhFormulaColumn(columnName, formulaString);
            case Numba:
                return new NumbaFormulaColumn(columnName, formulaString);
            default:
                throw new UnsupportedOperationException("Parser support not implemented for " + parser);
        }
    }

    static FormulaColumn createFormulaColumn(String columnName, String formulaString) {
        return createFormulaColumn(columnName, formulaString, FormulaParserConfiguration.parser);
    }

    ColumnSource updateData(WritableSource result, long destPos, long sourcePos);
}
