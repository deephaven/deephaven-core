//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
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
        Select, View, Update, UpdateView, LazyUpdate
    }

    private final SelectAndViewAnalyzer analyzer;
    private final FormulaColumn shiftColumn;
    private final boolean shiftColumnHasPositiveOffset;
    private final List<SelectColumn> remainingCols;
    private final List<SelectColumn> processedColumns;

    SelectAndViewAnalyzerWrapper(
            SelectAndViewAnalyzer analyzer,
            FormulaColumn shiftColumn,
            boolean shiftColumnHasPositiveOffset,
            List<SelectColumn> remainingCols,
            List<SelectColumn> processedColumns) {
        this.analyzer = analyzer;
        this.shiftColumn = shiftColumn;
        this.shiftColumnHasPositiveOffset = shiftColumnHasPositiveOffset;
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

    public QueryTable applyShiftsAndRemainingColumns(
            @NotNull QueryTable sourceTable, @NotNull QueryTable queryTable, UpdateFlavor updateFlavor) {
        if (shiftColumn != null) {
            queryTable = (QueryTable) ShiftedColumnsFactory.getShiftedColumnsTable(
                    queryTable, shiftColumn, updateFlavor);
        }

        // shift columns may introduce modifies that are not present in the original table; set these before using
        if (sourceTable.isRefreshing()) {
            if (shiftColumn == null && sourceTable.isAddOnly()) {
                queryTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
            }
            if ((shiftColumn == null || !shiftColumnHasPositiveOffset) && sourceTable.isAppendOnly()) {
                // note if the shift offset is non-positive, then this result is still append-only
                queryTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
            }
            if (sourceTable.hasAttribute(Table.TEST_SOURCE_TABLE_ATTRIBUTE)) {
                // be convenient for test authors by propagating the test source table attribute
                queryTable.setAttribute(Table.TEST_SOURCE_TABLE_ATTRIBUTE, true);
            }
            if (sourceTable.isBlink()) {
                // blink tables, although possibly not useful, can have shift columns
                queryTable.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
            }
        }

        boolean isMultiStateSelect = shiftColumn != null || remainingCols != null;
        if (isMultiStateSelect && (updateFlavor == UpdateFlavor.Select || updateFlavor == UpdateFlavor.View)) {
            List<SelectColumn> newResultColumns = new LinkedList<>();
            for (SelectColumn processed : processedColumns) {
                newResultColumns.add(new SourceColumn(processed.getName()));
            }
            if (shiftColumn != null) {
                newResultColumns.add(new SourceColumn(shiftColumn.getName()));
            }
            if (remainingCols != null) {
                newResultColumns.addAll(remainingCols);
            }

            if (updateFlavor == UpdateFlavor.Select) {
                queryTable = (QueryTable) queryTable.select(newResultColumns);
            } else {
                queryTable = (QueryTable) queryTable.view(newResultColumns);
            }
        } else if (remainingCols != null) {
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
                default:
                    throw new IllegalStateException("Unexpected update flavor: " + updateFlavor);
            }
        }

        return queryTable;
    }
}
