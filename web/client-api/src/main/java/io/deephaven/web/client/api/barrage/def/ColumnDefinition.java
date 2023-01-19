/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.barrage.def;

import io.deephaven.web.client.api.Column;

import java.util.Map;

public class ColumnDefinition {
    private int columnIndex;
    private String name;
    private String type;

    private String styleColumn;
    private String formatColumn;

    private boolean isStyleColumn;
    private boolean isFormatColumn;
    private boolean isNumberFormatColumn;
    private boolean isPartitionColumn;
    private boolean isHierarchicalExpandByColumn;
    private boolean isHierarchicalRowDepthColumn;
    private boolean isHierarchicalRowExpandedColumn;
    private boolean isRollupAggregatedNodeColumn;
    private boolean isRollupConstituentNodeColumn;
    private boolean isRollupGroupByColumn;
    private String rollupAggregationInputColumn;

    // Indicates that this is a style column for the row
    private boolean forRow;
    private boolean isInputTableKeyColumn;
    private String description;

    public String getName() {
        return name;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isStyleColumn() {
        return isStyleColumn;
    }

    public void setStyleColumn(boolean styleColumn) {
        isStyleColumn = styleColumn;
    }

    public boolean isFormatColumn() {
        return isFormatColumn;
    }

    public void setFormatColumn(boolean formatColumn) {
        isFormatColumn = formatColumn;
    }

    /**
     * @deprecated Use {@link #isFormatColumn()}
     */
    @Deprecated
    public boolean isNumberFormatColumn() {
        return isNumberFormatColumn;
    }

    /**
     * @deprecated Use {@link #setFormatColumn(boolean)}
     */
    @Deprecated
    public void setNumberFormatColumn(boolean numberFormatColumn) {
        isNumberFormatColumn = numberFormatColumn;
    }

    public boolean isPartitionColumn() {
        return isPartitionColumn;
    }

    public void setPartitionColumn(boolean partitionColumn) {
        isPartitionColumn = partitionColumn;
    }

    public boolean isVisible() {
        return !isStyleColumn() && !isFormatColumn() && !isRollupConstituentNodeColumn()
                && !isHierarchicalRowDepthColumn() && !isHierarchicalRowExpandedColumn();
    }

    public boolean isForRow() {
        return forRow;
    }

    public void setForRow(boolean forRow) {
        this.forRow = forRow;
    }

    public String getFormatColumnName() {
        return formatColumn;
    }

    public void setFormatColumnName(String formatColumn) {
        this.formatColumn = formatColumn;
    }

    public String getStyleColumnName() {
        return styleColumn;
    }

    public void setStyleColumnName(String styleColumn) {
        this.styleColumn = styleColumn;
    }

    public void setInputTableKeyColumn(boolean inputTableKeyColumn) {
        this.isInputTableKeyColumn = inputTableKeyColumn;
    }

    public boolean isInputTableKeyColumn() {
        return isInputTableKeyColumn;
    }

    public void setDescription(String description) {
        this.description = description;
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
                format == null || !format.isNumberFormatColumn() ? null : format.getColumnIndex(),
                style == null ? null : style.getColumnIndex(),
                isPartitionColumn(),
                format == null || format.isNumberFormatColumn() ? null : format.getColumnIndex(),
                getDescription(),
                isInputTableKeyColumn());
    }

    private static Column makeColumn(int jsIndex, ColumnDefinition definition, Integer numberFormatIndex,
            Integer styleIndex, boolean isPartitionColumn, Integer formatStringIndex, String description,
            boolean inputTableKeyColumn) {
        return new Column(jsIndex, definition.getColumnIndex(), numberFormatIndex, styleIndex, definition.getType(),
                definition.getName(), isPartitionColumn, formatStringIndex, description, inputTableKeyColumn);
    }

    public boolean isHierarchicalExpandByColumn() {
        return isHierarchicalExpandByColumn;
    }

    public void setHierarchicalExpandByColumn(boolean hierarchicalExpandByColumn) {
        isHierarchicalExpandByColumn = hierarchicalExpandByColumn;
    }

    public boolean isHierarchicalRowDepthColumn() {
        return isHierarchicalRowDepthColumn;
    }

    public void setHierarchicalRowDepthColumn(boolean hierarchicalRowDepthColumn) {
        isHierarchicalRowDepthColumn = hierarchicalRowDepthColumn;
    }

    public boolean isHierarchicalRowExpandedColumn() {
        return isHierarchicalRowExpandedColumn;
    }

    public void setHierarchicalRowExpandedColumn(boolean hierarchicalRowExpandedColumn) {
        isHierarchicalRowExpandedColumn = hierarchicalRowExpandedColumn;
    }

    public boolean isRollupAggregatedNodeColumn() {
        return isRollupAggregatedNodeColumn;
    }

    public void setRollupAggregatedNodeColumn(boolean rollupAggregatedNodeColumn) {
        isRollupAggregatedNodeColumn = rollupAggregatedNodeColumn;
    }

    public boolean isRollupConstituentNodeColumn() {
        return isRollupConstituentNodeColumn;
    }

    public void setRollupConstituentNodeColumn(boolean rollupConstituentNodeColumn) {
        isRollupConstituentNodeColumn = rollupConstituentNodeColumn;
    }

    public boolean isRollupGroupByColumn() {
        return isRollupGroupByColumn;
    }

    public void setRollupGroupByColumn(boolean rollupGroupByColumn) {
        isRollupGroupByColumn = rollupGroupByColumn;
    }

    public String getRollupAggregationInputColumn() {
        return rollupAggregationInputColumn;
    }

    public void setRollupAggregationInputColumn(String rollupAggregationInputColumn) {
        this.rollupAggregationInputColumn = rollupAggregationInputColumn;
    }
}
