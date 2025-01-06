//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Shift-Aware listener for Select or Update. It uses the SelectAndViewAnalyzer to calculate how columns affect other
 * columns, then creates a column set transformer which will be used by onUpdate to transform updates.
 */
class SelectOrUpdateListener extends BaseTable.ListenerImpl {
    private final QueryTable dependent;
    private final TrackingRowSet resultRowSet;
    private final ModifiedColumnSet.Transformer transformer;
    private final SelectAndViewAnalyzer analyzer;

    private volatile boolean updateInProgress = false;
    private final boolean enableParallelUpdate;

    /**
     * @param description Description of this listener
     * @param parent The parent table
     * @param dependent The dependent table
     * @param effects A map from a column name to the column names that it affects
     */
    SelectOrUpdateListener(String description, QueryTable parent, QueryTable dependent, Map<String, String[]> effects,
            SelectAndViewAnalyzer analyzer) {
        super(description, parent, dependent);
        this.dependent = dependent;
        this.resultRowSet = dependent.getRowSet();

        // Now calculate the other dependencies and invert
        final String[] parentNames = new String[effects.size()];
        final ModifiedColumnSet[] mcss = new ModifiedColumnSet[effects.size()];
        int nextIndex = 0;
        for (Map.Entry<String, String[]> entry : effects.entrySet()) {
            parentNames[nextIndex] = entry.getKey();
            mcss[nextIndex] = dependent.newModifiedColumnSet(entry.getValue());
            ++nextIndex;
        }
        transformer = parent.newModifiedColumnSetTransformer(parentNames, mcss);
        this.analyzer = analyzer;
        this.enableParallelUpdate =
                (QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE ||
                        (QueryTable.ENABLE_PARALLEL_SELECT_AND_UPDATE
                                && getUpdateGraph().parallelismFactor() > 1))
                        && analyzer.allowCrossColumnParallelization();
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        if (!tryIncrementReferenceCount()) {
            // If we're no longer live, there's no work to do here.
            return;
        }

        // Attempt to minimize work by sharing computation across all columns:
        // - clear only the keys that no longer exist
        // - create parallel arrays of pre-shift-keys and post-shift-keys so we can move them in chunks

        updateInProgress = true;
        final TableUpdate acquiredUpdate = upstream.acquire();

        final WritableRowSet toClear = resultRowSet.copyPrev();
        final SelectAndViewAnalyzer.UpdateHelper updateHelper =
                new SelectAndViewAnalyzer.UpdateHelper(resultRowSet, acquiredUpdate);
        toClear.remove(resultRowSet);
        JobScheduler jobScheduler;

        if (enableParallelUpdate) {
            jobScheduler = new UpdateGraphJobScheduler(getUpdateGraph());
        } else {
            jobScheduler = new ImmediateJobScheduler();
        }

        // do not allow a double-notify
        final AtomicBoolean hasNotified = new AtomicBoolean();
        analyzer.applyUpdate(acquiredUpdate, toClear, updateHelper, jobScheduler, this,
                () -> {
                    if (!hasNotified.getAndSet(true)) {
                        completionRoutine(acquiredUpdate, jobScheduler, toClear, updateHelper);
                    }
                },
                error -> {
                    if (!hasNotified.getAndSet(true)) {
                        handleException(error);
                    }
                });
    }

    private void handleException(Exception e) {
        try {
            onFailure(e, getEntry());
        } finally {
            updateInProgress = false;
            // Note that this isn't really needed, since onFailure forces reference count to zero, but it seems
            // reasonable to pair with the tryIncrementReferenceCount invocation in onUpdate and match
            // completionRoutine. This also has the effect of "future proofing" this code against changes to onFailure.
            decrementReferenceCount();
        }
    }

    private void completionRoutine(TableUpdate upstream, JobScheduler jobScheduler,
            WritableRowSet toClear, SelectAndViewAnalyzer.UpdateHelper updateHelper) {
        try {
            final TableUpdateImpl downstream = new TableUpdateImpl(upstream.added().copy(), upstream.removed().copy(),
                    upstream.modified().copy(), upstream.shifted(), dependent.getModifiedColumnSetForUpdates());
            transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
            dependent.notifyListeners(downstream);
            upstream.release();
            toClear.close();
            updateHelper.close();
            final BasePerformanceEntry accumulated = jobScheduler.getAccumulatedPerformance();
            // if the entry exists, then we install a terminal notification so that we don't lose the performance from
            // this execution
            if (accumulated != null) {
                getUpdateGraph().addNotification(new TerminalNotification() {
                    @Override
                    public void run() {
                        final PerformanceEntry entry = getEntry();
                        if (entry != null) {
                            entry.accumulate(accumulated);
                        }
                    }
                });
            }
        } finally {
            updateInProgress = false;
            decrementReferenceCount();
        }
    }

    @Override
    public boolean satisfied(long step) {
        return super.satisfied(step) && !updateInProgress;
    }
}
