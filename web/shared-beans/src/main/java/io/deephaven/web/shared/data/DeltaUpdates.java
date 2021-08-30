package io.deephaven.web.shared.data;

import io.deephaven.web.shared.data.columns.ColumnData;

import java.io.Serializable;

public class DeltaUpdates implements Serializable {
    public static class ColumnAdditions implements Serializable {
        private int columnIndex;
        private ColumnData values;

        public ColumnAdditions() {}

        public ColumnAdditions(int colIndex, ColumnData values) {
            setColumnIndex(colIndex);
            setValues(values);
        }

        public int getColumnIndex() {
            return columnIndex;
        }

        public void setColumnIndex(final int columnIndex) {
            this.columnIndex = columnIndex;
        }

        public ColumnData getValues() {
            return values;
        }

        public void setValues(final ColumnData values) {
            this.values = values;
        }
    }
    public static class ColumnModifications implements Serializable {
        private int columnIndex;
        private RangeSet rowsIncluded;
        public ColumnData values;

        public ColumnModifications() {}

        public ColumnModifications(int columnIndex, RangeSet includedModifications, ColumnData columnData) {
            setColumnIndex(columnIndex);
            setRowsIncluded(includedModifications);
            setValues(columnData);
        }

        public int getColumnIndex() {
            return columnIndex;
        }

        public void setColumnIndex(final int columnIndex) {
            this.columnIndex = columnIndex;
        }

        public RangeSet getRowsIncluded() {
            return rowsIncluded;
        }

        public void setRowsIncluded(final RangeSet rowsIncluded) {
            this.rowsIncluded = rowsIncluded;
        }

        public ColumnData getValues() {
            return values;
        }

        public void setValues(final ColumnData values) {
            this.values = values;
        }
    }

    private long deltaSequence;
    private long firstStep;
    private long lastStep;

    private RangeSet added;
    private RangeSet removed;

    private ShiftedRange[] shiftedRanges;

    private RangeSet includedAdditions;

    private ColumnAdditions[] serializedAdditions;
    private ColumnModifications[] serializedModifications;

    public DeltaUpdates() {}

    public DeltaUpdates(RangeSet added, RangeSet removed, ShiftedRange[] shifted, RangeSet includedAdditions,
            ColumnAdditions[] addedColumnData, ColumnModifications[] modifiedColumnData) {
        setAdded(added);
        setRemoved(removed);
        setShiftedRanges(shifted);
        setIncludedAdditions(includedAdditions);
        setSerializedAdditions(addedColumnData);
        setSerializedModifications(modifiedColumnData);
    }


    public long getDeltaSequence() {
        return deltaSequence;
    }

    public void setDeltaSequence(final long deltaSequence) {
        this.deltaSequence = deltaSequence;
    }

    public long getFirstStep() {
        return firstStep;
    }

    public void setFirstStep(final long firstStep) {
        this.firstStep = firstStep;
    }

    public long getLastStep() {
        return lastStep;
    }

    public void setLastStep(final long lastStep) {
        this.lastStep = lastStep;
    }

    public RangeSet getAdded() {
        return added;
    }

    public void setAdded(final RangeSet added) {
        this.added = added;
    }

    public RangeSet getRemoved() {
        return removed;
    }

    public void setRemoved(final RangeSet removed) {
        this.removed = removed;
    }

    public ShiftedRange[] getShiftedRanges() {
        return shiftedRanges;
    }

    public void setShiftedRanges(final ShiftedRange[] shiftedRanges) {
        this.shiftedRanges = shiftedRanges;
    }

    public RangeSet getIncludedAdditions() {
        return includedAdditions;
    }

    public void setIncludedAdditions(final RangeSet includedAdditions) {
        this.includedAdditions = includedAdditions;
    }

    public ColumnAdditions[] getSerializedAdditions() {
        return serializedAdditions;
    }

    public void setSerializedAdditions(final ColumnAdditions[] serializedAdditions) {
        this.serializedAdditions = serializedAdditions;
    }

    public ColumnModifications[] getSerializedModifications() {
        return serializedModifications;
    }

    public void setSerializedModifications(final ColumnModifications[] serializedModifications) {
        this.serializedModifications = serializedModifications;
    }
}
