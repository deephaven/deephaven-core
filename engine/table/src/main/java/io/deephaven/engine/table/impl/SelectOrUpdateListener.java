package io.deephaven.engine.table.impl;

import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;

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
    private final BitSet completedColumns = new BitSet();
    private final BitSet allNewColumns = new BitSet();
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
                QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE || (QueryTable.ENABLE_PARALLEL_SELECT_AND_UPDATE
                        && UpdateGraphProcessor.DEFAULT.getUpdateThreads() > 1)
                        && analyzer.allowCrossColumnParallelization();
        analyzer.setAllNewColumns(allNewColumns);
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        // Attempt to minimize work by sharing computation across all columns:
        // - clear only the keys that no longer exist
        // - create parallel arrays of pre-shift-keys and post-shift-keys so we can move them in chunks

        updateInProgress = true;
        completedColumns.clear();
        final TableUpdate acquiredUpdate = upstream.acquire();

        final WritableRowSet toClear = resultRowSet.copyPrev();
        final SelectAndViewAnalyzer.UpdateHelper updateHelper =
                new SelectAndViewAnalyzer.UpdateHelper(resultRowSet, acquiredUpdate);
        toClear.remove(resultRowSet);
        SelectAndViewAnalyzer.JobScheduler jobScheduler;

        if (enableParallelUpdate) {
            jobScheduler = new SelectAndViewAnalyzer.UpdateGraphProcessorJobScheduler();
        } else {
            jobScheduler = SelectAndViewAnalyzer.ImmediateJobScheduler.INSTANCE;
        }

        analyzer.applyUpdate(acquiredUpdate, toClear, updateHelper, jobScheduler,
                new SelectAndViewAnalyzer.SelectLayerCompletionHandler(allNewColumns, completedColumns) {
                    @Override
                    public void onAllRequiredColumnsCompleted() {
                        completionRoutine(acquiredUpdate, jobScheduler, toClear, updateHelper);
                    }

                    @Override
                    protected void onError(Exception error) {
                        handleException(error);
                    }
                });
    }

    private void handleException(Exception e) {
        dependent.notifyListenersOnError(e, getEntry());
        try {
            if (SystemicObjectTracker.isSystemic(dependent)) {
                AsyncClientErrorNotifier.reportError(e);
            }
        } catch (IOException ioe) {
            throw new UncheckedTableException("Exception in " + getEntry().toString(), e);
        }
        updateInProgress = false;
    }

    private void completionRoutine(TableUpdate upstream, SelectAndViewAnalyzer.JobScheduler jobScheduler,
            WritableRowSet toClear, SelectAndViewAnalyzer.UpdateHelper updateHelper) {
        final TableUpdateImpl downstream = new TableUpdateImpl(upstream.added().copy(), upstream.removed().copy(),
                upstream.modified().copy(), upstream.shifted(), dependent.modifiedColumnSet);
        transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
        dependent.notifyListeners(downstream);
        upstream.release();
        toClear.close();
        updateHelper.close();
        final BasePerformanceEntry accumulated = jobScheduler.getAccumulatedPerformance();
        // if the entry exists, then we install a terminal notification so that we don't lose the performance from this
        // execution
        if (accumulated != null) {
            UpdateGraphProcessor.DEFAULT.addNotification(new TerminalNotification() {
                @Override
                public void run() {
                    synchronized (accumulated) {
                        getEntry().accumulate(accumulated);
                    }
                }
            });
        }
        updateInProgress = false;
    }

    @Override
    public boolean satisfied(long step) {
        return super.satisfied(step) && !updateInProgress;
    }
}
