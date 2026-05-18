//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.def;

import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.barrage.data.BarrageColumnType;
import org.apache.arrow.flatbuf.Field;

import java.util.Map;

public class ColumnDefinition {
    private final BarrageColumnType columnType;
    private final int columnIndex;

    public ColumnDefinition(int index, Field field) {
        this.columnType = BarrageColumnType.fromArrowField(field);
        columnIndex = index;
    }

    public String getName() {
        return columnType.getColumnName();
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getType() {
        return columnType.deephavenType();
    }

    public boolean isSortable() {
        return "true".equals(columnType.getDeephavenColumnAttr("isSortable"));
    }

    public boolean isStyleColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("isStyle"));
    }

    public boolean isFormatColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("isDateFormat"))
                || "true".equals(columnType.getDeephavenColumnAttr("isNumberFormat"));
    }

    public boolean isPartitionColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("isPartitioning"));
    }

    public boolean isVisible() {
        return !isStyleColumn() && !isFormatColumn() && !isHierarchicalRowDepthColumn()
                && !isHierarchicalRowExpandedColumn();
    }

    public boolean isForRow() {
        return "true".equals(columnType.getDeephavenColumnAttr("isRowStyle"));
    }

    public String getFormatColumnName() {
        String formatColumnName = columnType.getDeephavenColumnAttr("dateFormatColumn");
        if (formatColumnName == null) {
            formatColumnName = columnType.getDeephavenColumnAttr("numberFormatColumn");
        }
        return formatColumnName;
    }

    public String getStyleColumnName() {
        return columnType.getDeephavenColumnAttr("styleColumn");
    }

    public boolean isInputTableKeyColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("inputtable.isKey"));
    }

    public boolean isInputTableValueColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("inputtable.isValue"));
    }

    public String getDescription() {
        return columnType.getDeephavenColumnAttr("description");
    }

    public Column makeJsColumn(int index, Map<Boolean, Map<String, ColumnDefinition>> map) {
        if (isForRow()) {
            return makeColumn(-1, this, null, false, null, null, false, false);
        }
        Map<String, ColumnDefinition> byNameMap = map.get(isRollupConstituentNodeColumn());
        ColumnDefinition format = byNameMap.get(getFormatColumnName());
        ColumnDefinition style = byNameMap.get(getStyleColumnName());

        return makeColumn(index,
                this,
                style == null ? null : style.getColumnIndex(),
                isPartitionColumn(),
                format == null ? null : format.getColumnIndex(),
                getDescription(),
                isInputTableKeyColumn(),
                isInputTableValueColumn());
    }

    private static Column makeColumn(int jsIndex, ColumnDefinition definition,
            Integer styleIndex, boolean isPartitionColumn, Integer formatStringIndex, String description,
            boolean inputTableKeyColumn, boolean inputTableValueColumn) {
        return new Column(jsIndex, definition.getColumnIndex(), styleIndex, definition.getType(),
                definition.getName(), isPartitionColumn, formatStringIndex, description, inputTableKeyColumn,
                inputTableValueColumn,
                definition.isSortable());
    }

    public boolean isHierarchicalExpandByColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("hierarchicalTable.isExpandByColumn"));
    }

    public boolean isHierarchicalRowDepthColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("hierarchicalTable.isRowDepthColumn"));
    }

    public boolean isHierarchicalRowExpandedColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("hierarchicalTable.isRowExpandedColumn"));
    }

    public boolean isRollupAggregatedNodeColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("rollupTable.isAggregatedNodeColumn"));
    }

    public boolean isRollupConstituentNodeColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("rollupTable.isConstituentNodeColumn"));
    }

    public boolean isRollupGroupByColumn() {
        return "true".equals(columnType.getDeephavenColumnAttr("rollupTable.isGroupByColumn"));
    }

    public String getRollupAggregationInputColumn() {
        return columnType.getDeephavenColumnAttr("rollupTable.aggregationInputColumnName");
    }
}
