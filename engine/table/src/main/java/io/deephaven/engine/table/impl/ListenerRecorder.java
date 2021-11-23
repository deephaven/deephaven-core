package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.table.impl.util.*;

/**
 * A listener recorder stores references to added, removed, modified, and shifted indices; and then notifies a
 * {@link MergedListener} that a change has occurred. The combination of a {@link ListenerRecorder} and
 * {@link MergedListener} should be used when a table has multiple sources, such that each table can process all of it's
 * dependencies at once and fire a single notification to its children.
 */
public class ListenerRecorder extends BaseTable.ListenerImpl {
    protected final String logPrefix;
    protected final boolean isRefreshing;

    private MergedListener mergedListener;

    private long notificationStep = -1;
    private TableUpdate update;

    public ListenerRecorder(String description, Table parent, BaseTable dependent) {
        super(description, parent, dependent);
        this.logPrefix = System.identityHashCode(this) + ": " + description + "ShiftObliviousListener Recorder: ";
        this.isRefreshing = parent.isRefreshing();
    }

    boolean isRefreshing() {
        return isRefreshing;
    }

    public void release() {
        if (update != null) {
            update.release();
            update = null;
        }
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        this.update = upstream.acquire();
        this.notificationStep = LogicalClock.DEFAULT.currentStep();

        // notify the downstream listener merger
        if (mergedListener == null) {
            throw new IllegalStateException("Merged listener not set!");
        }

        mergedListener.notifyChanges();
    }

    public boolean recordedVariablesAreValid() {
        return notificationStep == LogicalClock.DEFAULT.currentStep();
    }

    public void setMergedListener(MergedListener mergedListener) {
        this.mergedListener = mergedListener;
    }

    public long getNotificationStep() {
        return notificationStep;
    }

    public RowSet getAdded() {
        return recordedVariablesAreValid() ? update.added() : RowSetFactory.empty();
    }

    public RowSet getRemoved() {
        return recordedVariablesAreValid() ? update.removed() : RowSetFactory.empty();
    }

    public RowSet getModified() {
        return recordedVariablesAreValid() ? update.modified() : RowSetFactory.empty();
    }

    public RowSet getModifiedPreShift() {
        return recordedVariablesAreValid() ? update.getModifiedPreShift() : RowSetFactory.empty();
    }

    public RowSetShiftData getShifted() {
        return recordedVariablesAreValid() ? update.shifted() : RowSetShiftData.EMPTY;
    }

    public ModifiedColumnSet getModifiedColumnSet() {
        return recordedVariablesAreValid() ? update.modifiedColumnSet() : null;
    }

    public TableUpdate getUpdate() {
        return recordedVariablesAreValid() ? update : null;
    }

    /**
     * The caller is responsible for closing the {@link RowSetShiftDataExpander}.
     * 
     * @return a backwards compatible version of added / removed / modified that account for shifting
     */
    public RowSetShiftDataExpander getExpandedARM() {
        return recordedVariablesAreValid() ? new RowSetShiftDataExpander(update, getParent().getRowSet())
                : RowSetShiftDataExpander.EMPTY;
    }
}
