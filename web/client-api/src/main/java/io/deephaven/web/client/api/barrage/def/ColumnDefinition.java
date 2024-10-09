//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.def;

import io.deephaven.web.client.api.Column;
import org.apache.arrow.flatbuf.Field;

import java.util.Map;

import static io.deephaven.web.client.api.barrage.WebBarrageUtils.keyValuePairs;

public class ColumnDefinition {
    private final Field field;
    private final int columnIndex;
    private final String type;

    private final boolean isSortable;

    private final String styleColumn;
    private final String formatColumn;

    private final boolean isStyleColumn;
    private final boolean isFormatColumn;
    private final boolean isPartitionColumn;
    private final boolean isHierarchicalExpandByColumn;
    private final boolean isHierarchicalRowDepthColumn;
    private final boolean isHierarchicalRowExpandedColumn;
    private final boolean isRollupAggregatedNodeColumn;
    private final boolean isRollupConstituentNodeColumn;
    private final boolean isRollupGroupByColumn;
    private final String rollupAggregationInputColumn;

    // Indicates that this is a style column for the whole row
    private final boolean forRow;
    private final boolean isInputTableKeyColumn;
    private final String description;

    public ColumnDefinition(int index, Field field) {
        Map<String, String> fieldMetadata =
                keyValuePairs("deephaven:", field.customMetadataLength(), field::customMetadata);
        this.field = field;
        columnIndex = index;
        type = fieldMetadata.get("type");
        isSortable = "true".equals(fieldMetadata.get("isSortable"));
        isStyleColumn = "true".equals(fieldMetadata.get("isStyle"));
        isFormatColumn = "true".equals(fieldMetadata.get("isDateFormat"))
                || "true".equals(fieldMetadata.get("isNumberFormat"));
        forRow = "true".equals(fieldMetadata.get("isRowStyle"));

        String formatColumnName = fieldMetadata.get("dateFormatColumn");
        if (formatColumnName == null) {
            formatColumnName = fieldMetadata.get("numberFormatColumn");
        }
        formatColumn = formatColumnName;

        styleColumn = fieldMetadata.get("styleColumn");

        isInputTableKeyColumn = "true".equals(fieldMetadata.get("inputtable.isKey"));

        this.description = fieldMetadata.get("description");

        isPartitionColumn = "true".equals(fieldMetadata.get("isPartitioning"));

        isHierarchicalExpandByColumn = "true".equals(fieldMetadata.get("hierarchicalTable.isExpandByColumn"));
        isHierarchicalRowDepthColumn = "true".equals(fieldMetadata.get("hierarchicalTable.isRowDepthColumn"));
        isHierarchicalRowExpandedColumn = "true".equals(fieldMetadata.get("hierarchicalTable.isRowExpandedColumn"));
        isRollupAggregatedNodeColumn = "true".equals(fieldMetadata.get("rollupTable.isAggregatedNodeColumn"));
        isRollupConstituentNodeColumn = "true".equals(fieldMetadata.get("rollupTable.isConstituentNodeColumn"));
        isRollupGroupByColumn = "true".equals(fieldMetadata.get("rollupTable.isGroupByColumn"));
        rollupAggregationInputColumn = fieldMetadata.get("rollupTable.aggregationInputColumnName");
    }

    public String getName() {
        return field.name();
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getType() {
        return type;
    }

    public boolean isSortable() {
        return isSortable;
    }

    public boolean isStyleColumn() {
        return isStyleColumn;
    }

    public boolean isFormatColumn() {
        return isFormatColumn;
    }

    public boolean isPartitionColumn() {
        return isPartitionColumn;
    }

    public boolean isVisible() {
        return !isStyleColumn() && !isFormatColumn() && !isRollupConstituentNodeColumn()
                && !isHierarchicalRowDepthColumn() && !isHierarchicalRowExpandedColumn();
    }

    public boolean isForRow() {
        return forRow;
    }

    public String getFormatColumnName() {
        return formatColumn;
    }

    public String getStyleColumnName() {
        return styleColumn;
    }

    public boolean isInputTableKeyColumn() {
        return isInputTableKeyColumn;
    }

    public String getDescription() {
        return description;
    }

    public Column makeJsColumn(int index, Map<Boolean, Map<String, ColumnDefinition>> map) {
        if (isForRow()) {
            return makeColumn(-1, this, null, null, false, null, null, false);
        }
        Map<String, ColumnDefinition> byNameMap = map.get(isRollupConstituentNodeColumn());
        ColumnDefinition format = byNameMap.get(getFormatColumnName());
        ColumnDefinition style = byNameMap.get(getStyleColumnName());

        return makeColumn(index,
                this,
                style == null ? null : style.getColumnIndex(),
                style == null ? null : style.getColumnIndex(),
                isPartitionColumn(),
                format == null ? null : format.getColumnIndex(),
                getDescription(),
                isInputTableKeyColumn());
    }

    private static Column makeColumn(int jsIndex, ColumnDefinition definition, Integer numberFormatIndex,
            Integer styleIndex, boolean isPartitionColumn, Integer formatStringIndex, String description,
            boolean inputTableKeyColumn) {
        return new Column(jsIndex, definition.getColumnIndex(), numberFormatIndex, styleIndex, definition.getType(),
                definition.getName(), isPartitionColumn, formatStringIndex, description, inputTableKeyColumn,
                definition.isSortable());
    }

    public boolean isHierarchicalExpandByColumn() {
        return isHierarchicalExpandByColumn;
    }

    public boolean isHierarchicalRowDepthColumn() {
        return isHierarchicalRowDepthColumn;
    }

    public boolean isHierarchicalRowExpandedColumn() {
        return isHierarchicalRowExpandedColumn;
    }

    public boolean isRollupAggregatedNodeColumn() {
        return isRollupAggregatedNodeColumn;
    }

    public boolean isRollupConstituentNodeColumn() {
        return isRollupConstituentNodeColumn;
    }

    public boolean isRollupGroupByColumn() {
        return isRollupGroupByColumn;
    }

    public String getRollupAggregationInputColumn() {
        return rollupAggregationInputColumn;
    }

}
