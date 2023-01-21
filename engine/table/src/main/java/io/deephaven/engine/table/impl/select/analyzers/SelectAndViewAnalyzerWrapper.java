/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.ShiftedColumnsFactory;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SelectAndViewAnalyzerWrapper {
    public enum UpdateFlavor {
        Update, UpdateView, LazyUpdate, View
    }

    private final SelectAndViewAnalyzer analyzer;
    private final FormulaColumn shiftColumn;
    private final List<SelectColumn> remainingCols;
    private final List<SelectColumn> processedColumns;

    SelectAndViewAnalyzerWrapper(
            SelectAndViewAnalyzer analyzer,
            FormulaColumn shiftColumn,
            List<SelectColumn> remainingCols,
            List<SelectColumn> processedColumns) {
        this.analyzer = analyzer;
        this.shiftColumn = shiftColumn;
        this.remainingCols = remainingCols;
        this.processedColumns = processedColumns;
    }

    public final Map<String, ColumnSource<?>> getPublishedColumnResources() {
        if (shiftColumn == null) {
            return analyzer.getPublishedColumnSources();
        } else {
            return analyzer.getAllColumnSources();
        }
    }

    public final Map<String, String[]> calcEffects() {
        return analyzer.calcEffects(shiftColumn != null);
    }

    public SelectAndViewAnalyzer getAnalyzer() {
        return analyzer;
    }

    public List<SelectColumn> getProcessedColumns() {
        return processedColumns;
    }

    public QueryTable applyShiftsAndRemainingColumns(@NotNull QueryTable queryTable, UpdateFlavor updateFlavor) {
        if (shiftColumn != null) {
            queryTable = (QueryTable) ShiftedColumnsFactory.getShiftedColumnsTable(queryTable, shiftColumn);
        }

        if (remainingCols != null) {
            switch (updateFlavor) {
                case Update: {
                    queryTable = (QueryTable) queryTable.update(remainingCols);
                    break;
                }
                case UpdateView: {
                    queryTable = (QueryTable) queryTable.updateView(remainingCols);
                    break;
                }
                case LazyUpdate: {
                    queryTable = (QueryTable) queryTable.lazyUpdate(remainingCols);
                    break;
                }
                case View: {
                    List<SelectColumn> newResultColumns = new LinkedList<>();
                    for (SelectColumn processed : processedColumns) {
                        newResultColumns.add(new SourceColumn(processed.getName()));
                    }
                    if (shiftColumn != null) {
                        newResultColumns.add(new SourceColumn(shiftColumn.getName()));
                    }
                    newResultColumns.addAll(remainingCols);
                    queryTable = (QueryTable) queryTable.view(newResultColumns);
                    break;
                }
            }
        }

        return queryTable;
    }
}
