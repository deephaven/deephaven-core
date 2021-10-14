package io.deephaven.engine.tables;

import io.deephaven.engine.v2.select.SelectColumn;
import io.deephaven.engine.v2.select.analyzers.SelectAndViewAnalyzer;

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
