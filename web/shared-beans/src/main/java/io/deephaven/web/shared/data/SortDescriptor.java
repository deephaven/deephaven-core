package io.deephaven.web.shared.data;

import java.io.Serializable;

public class SortDescriptor implements Serializable {
    private int columnIndex;
    private String dir;
    private String columnName;
    private boolean abs;

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public boolean isAbs() {
        return abs;
    }

    public void setAbs(boolean abs) {
        this.abs = abs;
    }

    @Override
    public String toString() {
        return "SortDescriptor{" +
                "dir='" + dir + '\'' +
                ", columnName='" + columnName + '\'' +
                ", abs=" + abs +
                '}';
    }
}
