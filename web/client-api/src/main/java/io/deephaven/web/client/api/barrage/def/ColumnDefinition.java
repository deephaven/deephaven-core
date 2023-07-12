/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.barrage.def;

import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ColumnDefinition {
    private int columnIndex;
    private String name;
    private String type;

    private String styleColumn;
    private String formatColumn;
    private Map<String, String> databarColumn = new HashMap<>();

    private boolean isStyleColumn;
    private boolean isFormatColumn;
    private boolean isNumberFormatColumn;
    private boolean isDatabarFormatColumn;
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

    public boolean isDatabarFormatColumn() { return isDatabarFormatColumn; }

    public void setDatabarFormatColumn(boolean databarFormatColumn) {
        isDatabarFormatColumn = databarFormatColumn;
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
        return !isStyleColumn() && !isFormatColumn() && !isDatabarFormatColumn() && !isRollupConstituentNodeColumn()
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

    public String getDatabarColumnName(String key) {
        return databarColumn.get(key);
    }

    public Collection<String> getDatabarColumnNames() {
        return databarColumn.values();
    }

//    public void setDatabarColumnName(Map<String, String> databarColumn) {
//    public void setDatabarColumnName(String databarColumn) {
//        this.databarColumn = databarColumn;
//    }

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
            return makeColumn(-1, this, null, null, false, null, null, null, false);
        }
        Map<String, ColumnDefinition> byNameMap = map.get(isRollupConstituentNodeColumn());
        ColumnDefinition format = byNameMap.get(getFormatColumnName());
        ColumnDefinition style = byNameMap.get(getStyleColumnName());
        Integer[] databarFormatIndexRange = new Integer[2];

        // can i optimize by just looking at first and last indexes of getDatabarColumnNames().values()
        for (String colName: getDatabarColumnNames()) {
            if(colName == null) {
                continue;
            }
            if(databarFormatIndexRange[0] == null) {
                databarFormatIndexRange[0] = Integer.MAX_VALUE;
            }
            if(databarFormatIndexRange[1] == null) {
                databarFormatIndexRange[1] = Integer.MIN_VALUE;
            }

            ColumnDefinition column = byNameMap.get(colName);
            databarFormatIndexRange[0] = Math.min(databarFormatIndexRange[0], column.getColumnIndex());
            databarFormatIndexRange[1] = Math.max(databarFormatIndexRange[1], column.getColumnIndex());
        }

        return makeColumn(index,
                this,
                format == null || !format.isNumberFormatColumn() ? null : format.getColumnIndex(),
                style == null ? null : style.getColumnIndex(),
                isPartitionColumn(),
                format == null || format.isNumberFormatColumn() ? null : format.getColumnIndex(),
                databarFormatIndexRange[0] == null || databarFormatIndexRange[1] == null ? null : databarFormatIndexRange,
                getDescription(),
                isInputTableKeyColumn());
    }

    private static Column makeColumn(int jsIndex, ColumnDefinition definition, Integer numberFormatIndex,
            Integer styleIndex, boolean isPartitionColumn, Integer formatStringIndex, Integer[] formatDatabarIndexRange, String description,
            boolean inputTableKeyColumn) {
        return new Column(jsIndex, definition.getColumnIndex(), numberFormatIndex, styleIndex, definition.getType(),
                definition.getName(), isPartitionColumn, formatStringIndex, formatDatabarIndexRange, description, inputTableKeyColumn);
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

    public void setDatabar(String key, String value) {
        this.databarColumn.put(key, value);
    }
}
