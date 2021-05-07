package io.deephaven.web.shared.data.plot;

import java.io.Serializable;

public class SourceDescriptor implements Serializable {

    private AxisDescriptor axis;

    private SourceType type;

    private int tableId = -1;
    private int tableMapId = -1;
    private String columnName;

    private String columnType;//only assigned for oneclicks

    private OneClickDescriptor oneClick;

    public AxisDescriptor getAxis() {
        return axis;
    }

    public void setAxis(AxisDescriptor axis) {
        this.axis = axis;
    }

    public SourceType getType() {
        return type;
    }

    public void setType(SourceType type) {
        this.type = type;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public int getTableMapId() {
        return tableMapId;
    }

    public void setTableMapId(final int tableMapId) {
        this.tableMapId = tableMapId;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(final String columnType) {
        this.columnType = columnType;
    }

    public OneClickDescriptor getOneClick() {
        return oneClick;
    }

    public void setOneClick(final OneClickDescriptor oneClick) {
        this.oneClick = oneClick;
    }
}
