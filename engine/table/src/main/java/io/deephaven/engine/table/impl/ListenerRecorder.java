//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * A listener recorder stores references to added, removed, modified, and shifted indices; and then notifies a
 * {@link MergedListener} that a change has occurred. The combination of a {@link ListenerRecorder} and
 * {@link MergedListener} should be used when a table has multiple sources, such that each table can process all of its
 * dependencies at once and fire a single notification to its children.
 */
public class ListenerRecorder extends InstrumentedTableUpdateListener {

    protected final Table parent;
    protected final String logPrefix;

    private MergedListener mergedListener;

    private long notificationStep = -1;
    private TableUpdate update;

    public ListenerRecorder(
            @NotNull final String description, @NotNull final Table parent, @Nullable final Object dependent) {
        super(description);
        this.parent = parent;
        logPrefix = System.identityHashCode(this) + ": " + description + " Listener Recorder: ";

        if (parent.isRefreshing()) {
            manage(parent);
            if (dependent instanceof Table) {
                ((Table) dependent).addParentReference(this);
            } else if (dependent instanceof LivenessManager) {
                ((LivenessManager) dependent).manage(this);
            }
        }
    }

    public Table getParent() {
        return parent;
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
        final long currentStep = getUpdateGraph().clock().currentStep();
        Assert.lt(this.notificationStep, "this.notificationStep", currentStep, "currentStep");
        setNotificationStep(currentStep);

        // notify the downstream listener merger
        if (mergedListener == null) {
            throw new IllegalStateException("Merged listener not set");
        }

        mergedListener.notifyChanges();
    }

    @Override
    protected void onFailureInternal(@NotNull final Throwable originalException, @Nullable final Entry sourceEntry) {
        setNotificationStep(getUpdateGraph().clock().currentStep());
        if (mergedListener == null) {
            throw new IllegalStateException("Merged listener not set");
        }
        mergedListener.notifyOnUpstreamError(originalException, sourceEntry);
    }

    protected void setNotificationStep(final long step) {
        this.notificationStep = step;
    }

    @Override
    public boolean canExecute(final long step) {
        return parent.satisfied(step);
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();
        parent.removeUpdateListener(this);
    }

    public boolean recordedVariablesAreValid() {
        return notificationStep == getUpdateGraph().clock().currentStep();
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
}
