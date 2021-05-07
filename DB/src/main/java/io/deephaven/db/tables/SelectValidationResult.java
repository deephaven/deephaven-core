package io.deephaven.db.tables;

import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.analyzers.SelectAndViewAnalyzer;

public class SelectValidationResult {
    private final SelectAndViewAnalyzer analyzer;
    private final SelectColumn[] clonedColumns;

    public SelectValidationResult(SelectAndViewAnalyzer analyzer, SelectColumn[] clonedColumns) {
        this.analyzer = analyzer;
        this.clonedColumns = clonedColumns;
    }

    public SelectAndViewAnalyzer getAnalyzer() {
        return analyzer;
    }

    public SelectColumn[] getClonedColumns() {
        return clonedColumns;
    }
}
