package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
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
    final RowSet modifyInput;

    /**
     * For initial filtering we may need to usePrev.
     */
    final boolean usePrev;

    /**
     * The added and modified with the filter applied. Or an exceptional result.
     */
    WritableRowSet addedResult;
    WritableRowSet modifyResult;

    AbstractFilterExecution(
            QueryTable sourceTable,
            WhereFilter[] filters,
            RowSet addedInput,
            RowSet modifyInput,
            boolean usePrev,
            boolean runModifiedFilters,
            ModifiedColumnSet sourceModColumns) {
        this.sourceTable = sourceTable;
        this.filters = filters;
        this.addedInput = addedInput;
        this.modifyInput = modifyInput;
        this.usePrev = usePrev;
        this.runModifiedFilters = runModifiedFilters;
        this.sourceModColumns = sourceModColumns;
    }

    /**
     * Retrieve the {@link JobScheduler} to use for this operation.
     */
    abstract JobScheduler jobScheduler();

    /**
     * The context for a single filter execution. This stores the results of the filter and the performance entry for
     * the filter execution.
     */
    private static final class FilterExecutionContext implements JobScheduler.JobThreadContext {
        BasePerformanceEntry basePerformanceEntry;
        WritableRowSet addedResult;
        WritableRowSet modifyResult;

        FilterExecutionContext() {}

        @Override
        public void close() {
            SafeCloseable.closeAll(addedResult, modifyResult);
        }

        public void reset() {
            // TODO: having basePerformanceEntry as final and calling BasePerformanceEntry#baseEntryReset() would be
            // better, but it's package-private. Can we expose it?
            basePerformanceEntry = new BasePerformanceEntry();
            try (final SafeCloseable ignored1 = addedResult;
                    final SafeCloseable ignored2 = modifyResult) {
                addedResult = null;
                modifyResult = null;
            }
        }
    }

    /**
     * Run the single filter specified by this AbstractFilterExecution and store the results in addedResult and
     * modifyResult. Allows specification of the start and end positions in the added and modified inputs.
     *
     * @param context the context to use for this filter to accumulate results and performance data
     * @param filter the filter to execute
     * @param addedInputToUse the added input to use for this filter
     * @param addStart the start position in the added input
     * @param addEnd the end position in the added input (exclusive)
     * @param modifiedInputToUse the modified input to use for this filter
     * @param modifiedStart the start position in the modified input
     * @param modifiedEnd the end position in the modified input (exclusive)
     * @param onComplete the routine to call after the filter has been completely executed
     * @param onError the routine to call if a filter raises an exception
     */
    private void doFilter(
            final FilterExecutionContext context,
            final WhereFilter filter,
            final RowSet addedInputToUse,
            final long addStart,
            final long addEnd,
            final RowSet modifiedInputToUse,
            final long modifiedStart,
            final long modifiedEnd,
            final Runnable onComplete,
            final Consumer<Exception> onError) {
        try {
            context.basePerformanceEntry.onBaseEntryStart();
            if (addedInputToUse != null) {
                try (final RowSet processAdds = addedInputToUse.subSetByPositionRange(addStart, addEnd)) {
                    context.addedResult = filter.filter(
                            processAdds, sourceTable.getRowSet(), sourceTable, usePrev);
                }
            }
            if (modifiedInputToUse != null) {
                try (final RowSet processMods = modifiedInputToUse.subSetByPositionRange(modifiedStart, modifiedEnd)) {
                    context.modifyResult = filter.filter(
                            processMods, sourceTable.getRowSet(), sourceTable, usePrev);
                }
            }
            // Explicitly end collection *before* we call onComplete.
            context.basePerformanceEntry.onBaseEntryEnd();
            onComplete.run();
        } catch (Exception e) {
            // Explicitly end collection *before* we call onError.
            context.basePerformanceEntry.onBaseEntryEnd();
            onError.accept(e);
        }
    }

    /**
     * Run the single filter specified by this AbstractFilterExecution and store the results in addedResult and
     * modifyResult. Processes all rows in the added and modified inputs.
     *
     * @param context the context to use for this filter to accumulate results and performance data
     * @param filter the filter to execute
     * @param addedInputToUse the added input to use for this filter
     * @param modifiedInputToUse the modified input to use for this filter
     * @param onComplete the routine to call after the filter has been completely executed
     * @param onError the routine to call if a filter raises an exception
     */
    private void doFilter(
            final FilterExecutionContext context,
            final WhereFilter filter,
            final RowSet addedInputToUse,
            final RowSet modifiedInputToUse,
            final Runnable onComplete,
            final Consumer<Exception> onError) {
        try {
            context.basePerformanceEntry.onBaseEntryStart();
            if (addedInputToUse != null) {
                context.addedResult = filter.filter(
                        addedInputToUse, sourceTable.getRowSet(), sourceTable, usePrev);
            }
            if (modifiedInputToUse != null) {
                context.modifyResult = filter.filter(
                        modifiedInputToUse, sourceTable.getRowSet(), sourceTable, usePrev);
            }
            // Explicitly end collection *before* we call onComplete.
            context.basePerformanceEntry.onBaseEntryEnd();
            onComplete.run();
        } catch (Exception e) {
            // Explicitly end collection *before* we call onError.
            context.basePerformanceEntry.onBaseEntryEnd();
            onError.accept(e);
        }
    }

    /**
     * Run the filter specified by this AbstractFilterExecution in parallel
     *
     * @param filter the filter to execute
     * @param onComplete the routine to call after the filter has been completely executed
     * @param onError the routine to call if a filter raises an exception
     */
    private void doFilterParallel(
            final FilterExecutionContext context,
            final WhereFilter filter,
            final RowSet addedInputToUse,
            final RowSet modifyInputToUse,
            final Runnable onComplete,
            final Consumer<Exception> onError) {
        final long addSize = addedInputToUse == null ? 0 : addedInputToUse.size();
        final long modifySize = modifyInputToUse == null ? 0 : modifyInputToUse.size();
        final long updateSize = addSize + modifySize;

        final int targetSegments = (int) Math.min(getTargetSegments(), (updateSize +
                QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT - 1) / QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT);
        final long targetSize = (updateSize + targetSegments - 1) / targetSegments;

        jobScheduler().iterateParallel(
                ExecutionContext.getContext(),
                this::append,
                FilterExecutionContext::new,
                0, targetSegments,
                (localContext, idx, nec, resume) -> {
                    localContext.reset();

                    final long startOffSet = idx * targetSize;
                    final long endOffset = startOffSet + targetSize;

                    // When this filter is complete, update the overall results. RowSets need to be copied as they
                    // are owned by the context and will be released when the context is closed.
                    final Runnable localResume = () -> {
                        // Accumulate the results into the parent context
                        synchronized (context) {
                            context.basePerformanceEntry.accumulate(localContext.basePerformanceEntry);

                            if (localContext.addedResult != null) {
                                if (context.addedResult == null) {
                                    context.addedResult = localContext.addedResult.copy();
                                } else {
                                    context.addedResult.insert(localContext.addedResult);
                                }
                            }

                            if (localContext.modifyResult != null) {
                                if (context.modifyResult == null) {
                                    context.modifyResult = localContext.modifyResult.copy();
                                } else {
                                    context.modifyResult.insert(localContext.modifyResult);
                                }
                            }
                        }
                        resume.run();
                    };

                    if (endOffset < addSize) {
                        // Entirely within the added input
                        doFilter(localContext, filter,
                                addedInputToUse, startOffSet, endOffset,
                                null, 0, 0,
                                localResume, nec);
                    } else if (startOffSet < addSize) {
                        // Partially within the added input (might include some modified input)
                        doFilter(localContext, filter,
                                addedInputToUse, startOffSet, addSize,
                                modifyInputToUse, 0, endOffset - addSize,
                                localResume, nec);
                    } else {
                        // Entirely within the modified input
                        doFilter(localContext, filter,
                                null, 0, 0,
                                modifyInputToUse, startOffSet - addSize, endOffset - addSize,
                                localResume, nec);
                    }
                }, onComplete, onError);
    }

    RowSet getAddedResult() {
        return addedResult == null ? RowSetFactory.empty() : addedResult;
    }

    RowSet getModifyResult() {
        return modifyResult == null ? RowSetFactory.empty() : modifyResult;
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
            @NotNull final Runnable onComplete,
            @NotNull final Consumer<Exception> onError) {

        // Iterate serially through the filters. Each filter will successively restrict the input to the next filter,
        // until we reach the end of the filter chain.
        jobScheduler().iterateSerial(
                ExecutionContext.getContext(),
                this::append,
                FilterExecutionContext::new,
                0, filters.length,
                (context, idx, nec, resume) -> {
                    context.reset();

                    // Use the restricted output for the next filter (if this is not the first invocation)
                    final RowSet addedInputToUse = addedResult == null ? addedInput : addedResult;
                    final RowSet modifiedInputToUse = modifyResult == null ? modifyInput : modifyResult;

                    final long updateSize = (addedInputToUse == null ? 0 : addedInputToUse.size())
                            + (modifiedInputToUse == null ? 0 : modifiedInputToUse.size());

                    // When this filter is complete, update the overall results. RowSets need to be copied as they
                    // are owned by the context and will be released when the context is closed.
                    final Runnable localResume = () -> {
                        // Because we are running serially, no need to synchronize.
                        basePerformanceEntry.accumulate(context.basePerformanceEntry);
                        addedResult = context.addedResult == null ? null : context.addedResult.copy();
                        modifyResult = context.modifyResult == null ? null : context.modifyResult.copy();
                        resume.run();
                    };

                    // Run serially or parallelized?
                    if (!doParallelization(updateSize)) {
                        doFilter(context, filters[idx], addedInputToUse, modifiedInputToUse, localResume, nec);
                    } else {
                        doFilterParallel(context, filters[idx], addedInputToUse, modifiedInputToUse, localResume, nec);
                    }
                }, onComplete, onError);
    }

    /**
     * @return how many ways should we spit execution
     */
    abstract int getTargetSegments();

    /**
     * Should a filter of the given size be parallelized or executed within this thread?
     */
    abstract boolean doParallelization(long numberOfRows);

    boolean doParallelizationBase(long numberOfRows) {
        return !QueryTable.DISABLE_PARALLEL_WHERE && numberOfRows != 0
                && (QueryTable.FORCE_PARALLEL_WHERE || numberOfRows / 2 > QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT);
    }

    /**
     * Should parallelization be allowed for this operation.
     *
     * @return true if we should permit parallelization
     *
     * @param filters the filters that we are operating on
     */
    static boolean permitParallelization(WhereFilter[] filters) {
        final Boolean threadLocal = QueryTable.isParallelWhereDisabledForThread();
        if (threadLocal != null) {
            return !threadLocal;
        }
        if (QueryTable.DISABLE_PARALLEL_WHERE) {
            return false;
        }
        return Arrays.stream(filters).allMatch(WhereFilter::permitParallelization);
    }
}
