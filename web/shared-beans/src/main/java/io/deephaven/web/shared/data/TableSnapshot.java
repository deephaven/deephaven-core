package io.deephaven.web.shared.data;

import io.deephaven.web.shared.data.columns.ColumnData;

import java.io.Serializable;

public class TableSnapshot implements Serializable {
    public enum SnapshotType {
        INITIAL_SNAPSHOT,
        UPDATE_COLUMN_SNAPSHOT,
        UPDATE_ROW_SNAPSHOT,
        FORCED_SNAPSHOT,
    }

    private SnapshotType snapshotType;

    private long deltaSequence;

    private ColumnData[] dataColumns;
    private RangeSet includedRows;

    private long tableSize;

    public TableSnapshot() {
    }

    public TableSnapshot(RangeSet added, RangeSet includedAdditions, ColumnData[] dataColumns) {
        this.snapshotType = SnapshotType.INITIAL_SNAPSHOT;

        this.dataColumns = dataColumns;
        this.includedRows = includedAdditions;

        this.tableSize = added.size();
    }

    public SnapshotType getSnapshotType() {
        return snapshotType;
    }

    public void setSnapshotType(SnapshotType snapshotType) {
        this.snapshotType = snapshotType;
    }

    public long getDeltaSequence() {
        return deltaSequence;
    }

    public void setDeltaSequence(long deltaSequence) {
        this.deltaSequence = deltaSequence;
    }

    public ColumnData[] getDataColumns() {
        return dataColumns;
    }

    public void setDataColumns(ColumnData[] dataColumns) {
        this.dataColumns = dataColumns;
    }

    public RangeSet getIncludedRows() {
        return includedRows;
    }

    public void setIncludedRows(RangeSet includedRows) {
        this.includedRows = includedRows;
    }

    public void setTableSize(long tableSize) {
        this.tableSize = tableSize;
    }

    public long getTableSize() {
        return tableSize;
    }
}
