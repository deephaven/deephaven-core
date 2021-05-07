package io.deephaven.web.shared.data.plot;

import java.io.Serializable;

public class MultiSeriesSourceDescriptor implements Serializable {
    private AxisDescriptor axis;

    private SourceType type;

    private int tableMapId;
    private String columnName;

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

    public int getTableMapId() {
        return tableMapId;
    }

    public void setTableMapId(int tableMapId) {
        this.tableMapId = tableMapId;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

}
