//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler;
import org.jetbrains.annotations.NotNull;

import java.util.*;
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
    private final SparseArrayColumnSource[] sparseArraysToFree;

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
        final Set<SparseArrayColumnSource<?>> columnsToFree = Collections.newSetFromMap(new IdentityHashMap<>());
        for (final ColumnSource<?> resultSource : dependent.getColumnSources()) {
            if (resultSource instanceof SparseArrayColumnSource && !parent.getColumnSources().contains(resultSource)) {
                columnsToFree.add((SparseArrayColumnSource<?>) resultSource);
            }
        }
        sparseArraysToFree = columnsToFree.toArray(SparseArrayColumnSource[]::new);
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
            clearUnusedCurrentBlocks(toClear);

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

    private void clearUnusedCurrentBlocks(final WritableRowSet toClear) {
        if (sparseArraysToFree.length == 0) {
            return;
        }

        try (final RowSet removeBlocks = getLowestLevelRemovedBlocks(toClear);
                final RowSet removeBlocks2 =
                        removeBlocks.isNonempty() ? getBlock2RemovedBlock(toClear) : RowSetFactory.empty();
                final RowSet removeBlocks1 =
                        removeBlocks2.isNonempty() ? getBlock1RemovedBlock(toClear) : RowSetFactory.empty()) {
            if (removeBlocks.isEmpty()) {
                return;
            }
            for (final SparseArrayColumnSource<?> cs : sparseArraysToFree) {
                cs.clearBlocks(removeBlocks, removeBlocks2, removeBlocks1, resultRowSet.isEmpty());
            }
        }
    }

    @NotNull
    private RowSet getLowestLevelRemovedBlocks(final WritableRowSet toClear) {
        return getRemovedBlocks(toClear, SparseConstants.LOG_BLOCK_SIZE);
    }

    @NotNull
    private RowSet getBlock2RemovedBlock(final WritableRowSet toClear) {
        return getRemovedBlocks(toClear, SparseConstants.LOG_BLOCK_SIZE + SparseConstants.LOG_BLOCK2_SIZE);
    }

    @NotNull
    private RowSet getBlock1RemovedBlock(final WritableRowSet toClear) {
        return getRemovedBlocks(toClear,
                SparseConstants.LOG_BLOCK_SIZE + SparseConstants.LOG_BLOCK2_SIZE + SparseConstants.LOG_BLOCK1_SIZE);
    }

    @NotNull
    private RowSet getRemovedBlocks(final WritableRowSet toClear, final int logBlockSize) {
        final long blockSize = 1L << logBlockSize;
        // if we have anything in our rowset that is clearing out an entire block; it is worth noting those blocks
        final RowSet.SearchIterator remainingInterator = resultRowSet.searchIterator();
        final RowSet.SearchIterator clearIterator = toClear.searchIterator();

        final RowSetBuilderSequential removeBlockBuilder = RowSetFactory.builderSequential();
        long startOfNextBlock = 0;
        while (clearIterator.advance(startOfNextBlock)) {
            final long clearValue = clearIterator.currentValue();
            startOfNextBlock = (clearValue | (blockSize - 1)) + 1;

            // noinspection PointlessBitwiseExpression (Charles understands this version better than -blockSize which
            // IntelliJ suggests)
            final long startOfClearingBlock = clearValue & ~(blockSize - 1);
            if (!remainingInterator.advance(startOfClearingBlock)
                    || remainingInterator.currentValue() >= startOfNextBlock) {
                removeBlockBuilder.appendKey(clearValue >> logBlockSize);
            }
        }
        return removeBlockBuilder.build();
    }

    @Override
    public boolean satisfied(final long step) {
        return super.satisfied(step) && !updateInProgress;
    }
}
