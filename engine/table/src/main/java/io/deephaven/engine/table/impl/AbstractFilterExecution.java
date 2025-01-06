//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * The AbstractFilterExecution incorporates the idea that we have an added and modified RowSet to filter and that there
 * are a resulting pair of added and modified rows representing what was filtered. There is also the possibility that we
 * encounter an exception "exceptionResult" in which case the operation should be considered a failure.
 * <p>
 * The strategy that is used to divide the work is that there is some target split (by default the number of threads in
 * the TableMapTransform or LiveTableMonitor update thread pools) that we will divide our operation into. If there is
 * not enough work (defined by the {@link QueryTable#PARALLEL_WHERE_ROWS_PER_SEGMENT}) for more than one thread, we
 * simply run the operation in the calling thread. After each filter, we "reseed" the operation and recursively divide
 * it. For example, you might imagine we have a sequence of filters like "isBusinessTime" followed by a filter on
 * spread. The isBusinessTime filter would produce unequal results, therefore we do an N-way split on each result set to
 * avoid some threads doing inordinately more work than others.
 * <p>
 * After a unit of work is completed, it percolates the result to its parent. Finally we call a completion routine,
 * which will either notify a downstream table (in the listener case) or set the value of a future (in the
 * initialization case).
 */
abstract class AbstractFilterExecution {
    final BasePerformanceEntry basePerformanceEntry = new BasePerformanceEntry();

    final QueryTable sourceTable;
    final WhereFilter[] filters;

    final boolean runModifiedFilters;
    final ModifiedColumnSet sourceModColumns;

    /**
     * The added RowSet we are filtering.
     */
    final RowSet addedInput;

    /**
     * The modified RowSet we are filtering.
     */
    final RowSet modifiedInput;

    /**
     * For initial filtering we may need to usePrev.
     */
    final boolean usePrev;

    AbstractFilterExecution(
            QueryTable sourceTable,
            WhereFilter[] filters,
            RowSet addedInput,
            RowSet modifiedInput,
            boolean usePrev,
            boolean runModifiedFilters,
            ModifiedColumnSet sourceModColumns) {
        this.sourceTable = sourceTable;
        this.filters = filters;
        this.addedInput = addedInput;
        this.modifiedInput = modifiedInput;
        this.usePrev = usePrev;
        this.runModifiedFilters = runModifiedFilters;
        this.sourceModColumns = sourceModColumns;
    }

    /**
     * Retrieve the {@link JobScheduler} to use for this operation.
     */
    abstract JobScheduler jobScheduler();

    /**
     * This is called when a filter has been completed successfully.
     */
    @FunctionalInterface
    public interface FilterComplete {
        /**
         * Called when a filter has been completed successfully.
         *
         * @param adds the added rows resulting from the filter
         * @param mods the modified rows resulting from the filter
         */
        void accept(@NotNull WritableRowSet adds, @NotNull WritableRowSet mods);
    }

    /**
     * Run the single filter specified by this AbstractFilterExecution and store the results in addedResult and
     * modifyResult. Allows specification of the start and end positions in the added and modified inputs.
     *
     * @param filter the filter to execute
     * @param addsToUse the added input to use for this filter
     * @param addStart the start position in the added input
     * @param addEnd the end position in the added input (exclusive)
     * @param modsToUse the modified input to use for this filter
     * @param modStart the start position in the modified input
     * @param modEnd the end position in the modified input (exclusive)
     * @param onComplete the routine to call after the filter has been successfully executed
     * @param onError the routine to call if a filter raises an exception
     */
    private void doFilter(
            final WhereFilter filter,
            final WritableRowSet addsToUse,
            final long addStart,
            final long addEnd,
            final WritableRowSet modsToUse,
            final long modStart,
            final long modEnd,
            final BiConsumer<WritableRowSet, WritableRowSet> onComplete,
            final Consumer<Exception> onError) {
        if (Thread.interrupted()) {
            throw new CancellationException("interrupted while filtering");
        }
        try {
            final WritableRowSet adds;
            final WritableRowSet mods;
            if (addsToUse != null && addStart < addEnd) {
                try (final RowSet processAdds = addsToUse.subSetByPositionRange(addStart, addEnd)) {
                    adds = filter.filter(processAdds, sourceTable.getRowSet(), sourceTable, usePrev);
                }
            } else {
                adds = null;
            }
            if (modsToUse != null && modStart < modEnd) {
                try (final RowSet processMods = modsToUse.subSetByPositionRange(modStart, modEnd)) {
                    mods = filter.filter(processMods, sourceTable.getRowSet(), sourceTable, usePrev);
                }
            } else {
                mods = null;
            }
            onComplete.accept(adds, mods);
        } catch (Exception e) {
            onError.accept(e);
        }
    }

    /**
     * Run the filter specified by this AbstractFilterExecution in parallel
     *
     * @param filter the filter to execute
     * @param addedInputToUse the added input to use for this filter
     * @param modifiedInputToUse the modified input to use for this filter
     * @param onComplete the routine to call after the filter has been successfully executed
     * @param onError the routine to call if a filter raises an exception
     */
    private void doFilterParallel(
            final WhereFilter filter,
            final WritableRowSet addedInputToUse,
            final WritableRowSet modifiedInputToUse,
            final BiConsumer<WritableRowSet, WritableRowSet> onComplete,
            final Consumer<Exception> onError) {
        if (Thread.interrupted()) {
            throw new CancellationException("interrupted while filtering");
        }

        final long addSize = addedInputToUse == null ? 0 : addedInputToUse.size();
        final long modifySize = modifiedInputToUse == null ? 0 : modifiedInputToUse.size();
        final long updateSize = addSize + modifySize;

        final int targetSegments = (int) Math.min(getTargetSegments(), (updateSize +
                QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT - 1) / QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT);
        final long targetSize = (updateSize + targetSegments - 1) / targetSegments;

        final WritableRowSet addedResult = addSize <= 0 ? null : RowSetFactory.empty();
        final WritableRowSet modifiedResult = modifySize <= 0 ? null : RowSetFactory.empty();

        jobScheduler().iterateParallel(
                ExecutionContext.getContext(),
                this::append,
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, targetSegments,
                (localContext, idx, nec, resume) -> {
                    final long startOffSet = idx * targetSize;
                    final long endOffset = startOffSet + targetSize;

                    final BiConsumer<WritableRowSet, WritableRowSet> onFilterComplete = (adds, mods) -> {
                        // Clean up the row sets created by the filter.
                        try (final RowSet ignored = adds;
                                final RowSet ignored2 = mods) {
                            if (addedResult != null && adds != null) {
                                synchronized (addedResult) {
                                    addedResult.insert(adds);
                                }
                            }
                            if (modifiedResult != null && mods != null) {
                                synchronized (modifiedResult) {
                                    modifiedResult.insert(mods);
                                }
                            }
                        }
                        resume.run();
                    };

                    if (endOffset < addSize) {
                        // Entirely within the added input
                        doFilter(filter,
                                addedInputToUse, startOffSet, endOffset,
                                null, 0, 0,
                                onFilterComplete, nec);
                    } else if (startOffSet < addSize) {
                        // Partially within the added input (might include some modified input)
                        doFilter(filter,
                                addedInputToUse, startOffSet, addSize,
                                modifiedInputToUse, 0, endOffset - addSize,
                                onFilterComplete, nec);
                    } else {
                        // Entirely within the modified input
                        doFilter(filter,
                                null, 0, 0,
                                modifiedInputToUse, startOffSet - addSize, endOffset - addSize,
                                onFilterComplete, nec);
                    }
                }, () -> onComplete.accept(addedResult, modifiedResult), onError);
    }

    public LogOutput append(LogOutput output) {
        return output.append("FilterExecution{")
                .append(System.identityHashCode(this)).append(": ");
    }

    /**
     * Execute all filters; this may execute some filters in parallel when appropriate.
     *
     * @param onComplete the routine to call after the filter has been completely executed.
     * @param onError the routine to call if the filter experiences an exception.
     */
    public void scheduleCompletion(
            @NotNull final AbstractFilterExecution.FilterComplete onComplete,
            @NotNull final Consumer<Exception> onError) {

        // Start with the input row sets and narrow with each filter.
        final MutableObject<WritableRowSet> localAddInput = new MutableObject<>(
                addedInput != null && addedInput.isNonempty()
                        ? addedInput.copy()
                        : null);
        final MutableObject<WritableRowSet> localModInput = new MutableObject<>(
                runModifiedFilters && modifiedInput != null && modifiedInput.isNonempty()
                        ? modifiedInput.copy()
                        : null);
        if (localAddInput.getValue() == null && localModInput.getValue() == null) {
            onComplete.accept(RowSetFactory.empty(), RowSetFactory.empty());
            return;
        }

        // Iterate serially through the filters. Each filter will successively restrict the input to the next filter,
        // until we reach the end of the filter chain.
        jobScheduler().iterateSerial(
                ExecutionContext.getContext(),
                this::append,
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, filters.length,
                (context, idx, nec, resume) -> {
                    // Use the restricted output for the next filter (if this is not the first invocation)
                    final WritableRowSet addsToUse = localAddInput.getValue();
                    final WritableRowSet modsToUse = localModInput.getValue();

                    final long updateSize = (addsToUse != null ? addsToUse.size() : 0)
                            + (modsToUse != null ? modsToUse.size() : 0);

                    final BiConsumer<WritableRowSet, WritableRowSet> onFilterComplete = (adds, mods) -> {
                        // Clean up the row sets created by the filter.
                        try (final RowSet ignored = localAddInput.getValue();
                                final RowSet ignored2 = localModInput.getValue()) {
                            // Store the output as the next filter input.
                            localAddInput.setValue(adds);
                            localModInput.setValue(mods);
                        }
                        resume.run();
                    };

                    // Run serially or parallelized?
                    if (!shouldParallelizeFilter(filters[idx], updateSize)) {
                        doFilter(filters[idx],
                                addsToUse, 0, addsToUse == null ? 0 : addsToUse.size(),
                                modsToUse, 0, modsToUse == null ? 0 : modsToUse.size(),
                                onFilterComplete, nec);
                    } else {
                        doFilterParallel(filters[idx], addsToUse, modsToUse, onFilterComplete, nec);
                    }
                }, () -> {
                    // Return empty RowSets instead of null.
                    final WritableRowSet addedResult = localAddInput.getValue() == null
                            ? RowSetFactory.empty()
                            : localAddInput.getValue();
                    final WritableRowSet modifiedResult = localModInput.getValue() == null
                            ? RowSetFactory.empty()
                            : localModInput.getValue();
                    final BasePerformanceEntry baseEntry = jobScheduler().getAccumulatedPerformance();
                    if (baseEntry != null) {
                        basePerformanceEntry.accumulate(baseEntry);
                    }
                    onComplete.accept(addedResult, modifiedResult);
                }, onError);
    }

    /**
     * @return how many ways should we spit execution
     */
    abstract int getTargetSegments();

    /**
     * Should this operation be allowed to run parallelized?
     */
    abstract boolean permitParallelization();

    /**
     * Should a filter of the given size be parallelized or executed within this thread?
     */
    boolean shouldParallelizeFilter(WhereFilter filter, long numberOfRows) {
        return permitParallelization()
                && numberOfRows != 0
                && (QueryTable.FORCE_PARALLEL_WHERE || numberOfRows / 2 > QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT)
                && filter.permitParallelization();
    }

    /**
     * Should parallelization be allowed for this operation.
     *
     * @param filters the filters that we are operating on
     *
     * @return true if we should permit parallelization (if any filters can be parallelized)
     */
    static boolean permitParallelization(WhereFilter[] filters) {
        final Boolean threadLocal = QueryTable.isParallelWhereDisabledForThread();
        if (threadLocal != null) {
            return !threadLocal;
        }
        if (QueryTable.DISABLE_PARALLEL_WHERE) {
            return false;
        }
        return Arrays.stream(filters).anyMatch(WhereFilter::permitParallelization);
    }
}
