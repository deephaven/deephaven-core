package io.deephaven.web.shared.data.treetable;

import io.deephaven.web.shared.data.TableHandle;
import io.deephaven.web.shared.data.columns.ColumnData;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;

public class TreeTableResult implements Serializable {

    private long treeSize;

    private long snapshotStart;
    private long snapshotEnd;

    private ColumnData[] snapshotData;

    private String[] constituentColumnNames;
    private ColumnData[] constituentColumnData;

    private TableDetails[] tableDetails;

    private Key[] keyColumn;
    private Key[] parentKeyColumn;

    private BitSet childPresence;

    private TableHandle sourceTable;

    public long getTreeSize() {
        return treeSize;
    }

    public void setTreeSize(final long treeSize) {
        this.treeSize = treeSize;
    }

    public long getSnapshotStart() {
        return snapshotStart;
    }

    public void setSnapshotStart(final long snapshotStart) {
        this.snapshotStart = snapshotStart;
    }

    public long getSnapshotEnd() {
        return snapshotEnd;
    }

    public void setSnapshotEnd(final long snapshotEnd) {
        this.snapshotEnd = snapshotEnd;
    }

    public ColumnData[] getSnapshotData() {
        return snapshotData;
    }

    public void setSnapshotData(final ColumnData[] snapshotData) {
        this.snapshotData = snapshotData;
    }

    public String[] getConstituentColumnNames() {
        return constituentColumnNames;
    }

    public void setConstituentColumnNames(final String[] constituentColumnNames) {
        this.constituentColumnNames = constituentColumnNames;
    }

    public ColumnData[] getConstituentColumnData() {
        return constituentColumnData;
    }

    public void setConstituentColumnData(final ColumnData[] constituentColumnData) {
        this.constituentColumnData = constituentColumnData;
    }

    public TableDetails[] getTableDetails() {
        return tableDetails;
    }

    public void setTableDetails(final TableDetails[] tableDetails) {
        this.tableDetails = tableDetails;
    }

    public Key[] getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(final Key[] keyColumn) {
        this.keyColumn = keyColumn;
    }

    public Key[] getParentKeyColumn() {
        return parentKeyColumn;
    }

    public void setParentKeyColumn(final Key[] parentKeyColumn) {
        this.parentKeyColumn = parentKeyColumn;
    }

    public BitSet getChildPresence() {
        return childPresence;
    }

    public void setChildPresence(final BitSet childPresence) {
        this.childPresence = childPresence;
    }

    public TableHandle getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(final TableHandle sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public String toString() {
        return "TreeTableResult{" +
                "treeSize=" + treeSize +
                ", snapshotStart=" + snapshotStart +
                ", snapshotEnd=" + snapshotEnd +
                ", snapshotData=" + Arrays.toString(snapshotData) +
                ", constituentColumnNames=" + Arrays.toString(constituentColumnNames) +
                ", constituentColumnData=" + Arrays.toString(constituentColumnData) +
                ", tableDetails=" + Arrays.toString(tableDetails) +
                ", keyColumn=" + Arrays.toString(keyColumn) +
                ", parentKeyColumn=" + Arrays.toString(parentKeyColumn) +
                ", childPresence=" + childPresence +
                ", sourceTable=" + sourceTable +
                '}';
    }
}
