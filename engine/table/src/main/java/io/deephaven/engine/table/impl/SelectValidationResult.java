package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;

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
