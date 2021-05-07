package io.deephaven.web.shared.data;

import io.deephaven.web.shared.data.columns.ColumnData;

import java.io.Serializable;

public class ColumnHolder implements Serializable {
    private String name;
    private String type;
    private ColumnData columnData;
    private boolean grouped;

    public ColumnHolder() {
    }

    public ColumnHolder(String name, String type, ColumnData columnData, boolean grouped) {
        this.name = name;
        this.type = type;
        this.columnData = columnData;
        this.grouped = grouped;
    }

    public String getName() {
        return name;
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

    public ColumnData getColumnData() {
        return columnData;
    }

    public void setColumnData(ColumnData columnData) {
        this.columnData = columnData;
    }

    public boolean isGrouped() {
        return grouped;
    }

    public void setGrouped(boolean grouped) {
        this.grouped = grouped;
    }

    @Override
    public String toString() {
        return "ColumnHolder{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", columnData=" + columnData +
                ", grouped=" + grouped +
                '}';
    }
}
