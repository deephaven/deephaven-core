//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.collect.Streams;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.filter.ExtractBarriers;
import io.deephaven.engine.table.impl.filter.ExtractRespectedBarriers;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            final QueryTable sourceTable,
            final WhereFilter[] filters,
            @NotNull final RowSet addedInput,
            @NotNull final RowSet modifiedInput,
            final boolean usePrev,
            final boolean runModifiedFilters,
            final ModifiedColumnSet sourceModColumns) {
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
        void accept(@NotNull RowSet adds, @NotNull RowSet mods);
    }

    private static void cancelIfInterrupted() {
        if (Thread.interrupted()) {
            throw new CancellationException("interrupted while filtering");
        }
    }

    /**
     * Run the filter specified by this AbstractFilterExecution in parallel
     *
     * @param filter the filter to execute
     * @param input the added input to use for this filter
     * @param onComplete the routine to call after the filter has been successfully executed
     * @param onError the routine to call if a filter raises an exception
     */
    private void doFilterParallel(
            final WhereFilter filter,
            @NotNull final RowSet input,
            final Consumer<WritableRowSet> onComplete,
            final Consumer<Exception> onError) {
        final long inputSize = input.size();
        final WritableRowSet inputCopy = input.copy();

        final int targetSegments = (int) Math.min(getTargetSegments(), (inputSize +
                QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT - 1) / QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT);
        final long targetSize = (inputSize + targetSegments - 1) / targetSegments;

        // noinspection resource
        final WritableRowSet filterResult = RowSetFactory.empty();

        jobScheduler().iterateParallel(
                ExecutionContext.getContext(),
                this::appendFilterExecution,
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, targetSegments,
                (localContext, idx, nec, resume) -> {
                    cancelIfInterrupted();
                    final long startOffSet = idx * targetSize;
                    final long endOffset = startOffSet + targetSize;
                    // Filter this segment of the input rows.
                    try (
                            final WritableRowSet subset = inputCopy.subSetByPositionRange(startOffSet, endOffset);
                            final WritableRowSet result = filter(filter, subset)) {
                        synchronized (filterResult) {
                            filterResult.insert(result);
                        }
                    }
                    resume.run();
                },
                () -> onComplete.accept(filterResult),
                inputCopy::close,
                exception -> {
                    try (inputCopy) {
                        onError.accept(exception);
                    }
                });
    }

    public LogOutput appendFilterEstimation(LogOutput output) {
        return output.append("FilterEstimation{")
                .append(System.identityHashCode(this)).append(": ");
    }

    public LogOutput appendFilterExecution(LogOutput output) {
        return output.append("FilterExecution{")
                .append(System.identityHashCode(this)).append(": ");
    }

    /**
     * Simple class to hold a stateless filter and some metadata about it.
     */
    private class StatelessFilter implements Comparable<StatelessFilter>, SafeCloseable {
        /**
         * The index of this filter in the order supplied by the user.
         */
        public final int filterIdx;
        /**
         * The filter to be applied.
         */
        public final WhereFilter filter;
        /**
         * The executor to use for pushdown filtering, or null if pushdown is not supported.
         */
        public final PushdownFilterMatcher pushdownMatcher;
        /**
         * The context to use for pushdown filtering, or null if pushdown is not supported.
         */
        public final PushdownFilterContext context;
        /**
         * The cost of the pushdown filter operation.
         */
        public long pushdownFilterCost = Long.MAX_VALUE;
        /**
         * The result of the pushdown filter operation, or null if pushdown is not supported.
         */
        public PushdownResult pushdownResult;
        /**
         * The barriers declared by this filter.
         */
        public final Collection<Object> declaredBarriers;
        /**
         * The barriers respected by this filter including any implicit recursive dependencies.
         */
        public final Collection<Object> respectedBarriers;

        public StatelessFilter(
                final int filterIdx,
                final WhereFilter filter,
                final PushdownFilterMatcher pushdownMatcher,
                final PushdownFilterContext context,
                final Map<Object, Collection<Object>> barrierDependencies) {
            Require.eqTrue((pushdownMatcher == null) == (context == null),
                    "pushdownExecutor and context must be both null or both non-null");
            this.filterIdx = filterIdx;
            this.filter = filter;
            this.pushdownMatcher = pushdownMatcher;
            this.context = context;
            this.declaredBarriers = ExtractBarriers.of(filter);
            this.respectedBarriers = ExtractRespectedBarriers.of(filter).stream()
                    .flatMap(barrier -> {
                        final Collection<Object> dependencies = barrierDependencies.get(barrier);
                        if (dependencies == null) {
                            return Stream.of(barrier);
                        }
                        return Streams.concat(Stream.of(barrier), dependencies.stream());
                    })
                    .collect(Collectors.toSet());
        }

        /**
         * Schedules pushdown filter cost estimation for {@link #pushdownMatcher}. After {@link #pushdownFilterCost} has
         * been set (or set to {@link Long#MAX_VALUE} if pushdown is not supported), {@code onComplete} will be called.
         */
        public void scheduleUpdatePushdownFilterCost(final RowSet selection, final Runnable onComplete,
                final Consumer<Exception> onError) {
            if (pushdownMatcher == null) {
                pushdownFilterCost = Long.MAX_VALUE;
                onComplete.run();
                return;
            }
            pushdownMatcher.estimatePushdownFilterCost(filter, selection, usePrev, context,
                    jobScheduler(), value -> {
                        pushdownFilterCost = value;
                        onComplete.run();
                    }, onError);
        }

        @Override
        public int compareTo(@NotNull StatelessFilter o) {
            // Does other filter respect a barrier that exists on this filter?
            if (declaredBarriers.stream().anyMatch(o.respectedBarriers::contains)) {
                return -1;
            }
            // Does this filter respect a barrier that exists on the other filter?
            if (o.declaredBarriers.stream().anyMatch(respectedBarriers::contains)) {
                return 1;
            }

            // Primarily sort by push-down cost.
            if (pushdownFilterCost != o.pushdownFilterCost) {
                return Long.compare(pushdownFilterCost, o.pushdownFilterCost);
            }

            // Break ties via original index to preserve the original order of execution for similar cost filters.
            return Integer.compare(filterIdx, o.filterIdx);
        }

        @Override
        public void close() {
            if (context != null) {
                context.close();
            }
            if (pushdownResult != null) {
                pushdownResult.close();
            }
        }
    }

    /**
     * Updates and sorts the filters according to the estimated pushdown cost, starting at the given index.
     * {@code onComplete} is invoked once this is done. If there is an exception during the estimation, sorting, or
     * invocation of {@code onComplete}, {@code onError} will be called with that error.
     */
    private void scheduleAndSortCostEstimates(
            final StatelessFilter[] filters,
            final int startIndex,
            final RowSet selection,
            final Runnable onComplete,
            final Consumer<Exception> onError) {
        jobScheduler().iterateParallel(
                ExecutionContext.getContext(),
                this::appendFilterEstimation,
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                startIndex,
                filters.length - startIndex,
                (taskThreadContext, index, nestedErrorConsumer, resume) -> {
                    filters[index].scheduleUpdatePushdownFilterCost(selection, resume, nestedErrorConsumer);
                },
                () -> {
                    // Sort the filters by non-descending cost, starting at the given index.
                    Arrays.sort(filters, startIndex, filters.length);
                    onComplete.run();
                },
                () -> {
                },
                onError);
    }

    /**
     * Simple extensions to hold either stateless or stateful filter.
     */
    private static class FilterCollection extends ArrayList<WhereFilter> {
    }
    private static class StatelessFilterCollection extends FilterCollection {
    }
    private static class StatefulFilterCollection extends FilterCollection {
    }

    /**
     * Transform an array of {@link WhereFilter filters} into collections of stateful and stateless filters.
     *
     * @param filters the filters to collect
     * @return a list of filter collections
     */
    static List<FilterCollection> collectFilters(WhereFilter[] filters) {
        final List<FilterCollection> filterCollections = new ArrayList<>();
        boolean collectionStateless = true;
        for (final WhereFilter filter : filters) {
            final boolean filterStateless = filter.permitParallelization(); // determines if a filter is stateless
            if (filterCollections.isEmpty() || collectionStateless != filterStateless) {
                filterCollections.add(filterStateless
                        ? new StatelessFilterCollection()
                        : new StatefulFilterCollection());
                collectionStateless = filterStateless;
            }
            filterCollections.get(filterCollections.size() - 1).add(filter);
        }
        return filterCollections;
    }

    /**
     * Execute the final filter (as opposed to the pushdown pre-filters) and pass the result to the consumer.
     */
    private void executeFinalFilter(
            final WhereFilter filter,
            final RowSet input,
            final Consumer<WritableRowSet> resultConsumer,
            final Consumer<Exception> exceptionConsumer) {
        cancelIfInterrupted();
        // Run serially or parallelized?
        final long inputSize = input.size();
        if (!shouldParallelizeFilter(filter, inputSize)) {
            resultConsumer.accept(filter(filter, input));
        } else {
            doFilterParallel(filter, input, resultConsumer, exceptionConsumer);
        }
    }

    /**
     * Execute the stateless filter at the provided index and pass the result to the consumer.
     */
    private void executeStatelessFilter(
            final StatelessFilter[] statelessFilters,
            final int filterIdx,
            final MutableObject<WritableRowSet> localInput,
            final Runnable filterComplete,
            final Consumer<Exception> filterNec) {

        final StatelessFilter sf = Objects.requireNonNull(statelessFilters[filterIdx]);

        // Our ceiling cost is the cost of the next filter in the list, or Long.MAX_VALUE if this is the last filter.
        // This will limit the pushdown filters excecuted during this cycle to this maximum cost.
        final long costCeiling = filterIdx + 1 < statelessFilters.length
                ? statelessFilters[filterIdx + 1].pushdownFilterCost
                : Long.MAX_VALUE;

        // Result consumer for normal filtering.
        final Consumer<WritableRowSet> onFilterComplete = (result) -> {
            // Clean up the row sets created by the filter.
            // Store the output as the next filter input.
            replace(localInput, result);
            // This filter is complete, sort the remaining filters and conclude.
            scheduleAndSortCostEstimates(statelessFilters, filterIdx + 1, localInput.get(), filterComplete,
                    filterNec);
        };

        // Result consumer for push-down filtering.
        final Consumer<PushdownResult> onPushdownComplete = (pushdownResult) -> {
            // Update the context to reflect the filtering already executed..
            sf.context.updateExecutedFilterCost(costCeiling);

            if (pushdownResult.maybeMatch().isEmpty()) {
                localInput.setValue(pushdownResult.match().copy());
                scheduleAndSortCostEstimates(statelessFilters, filterIdx + 1, localInput.get(),
                        filterComplete, filterNec);
                return;
            }

            // We still have some maybe rows, sort the filters again, including the current index.
            scheduleAndSortCostEstimates(statelessFilters, filterIdx, localInput.get(), () -> {
                // If there is a new filter at the current index, need to evaluate it.
                if (!sf.equals(statelessFilters[filterIdx])) {
                    // Use the union of the match and maybe rows as the input for the next filter.
                    localInput.setValue(pushdownResult.match().union(pushdownResult.maybeMatch()));

                    // Store the result for later use by the companion regular filter.
                    sf.pushdownResult = pushdownResult;

                    // Do the next round of filtering with the new filter that bubbled up to the current index.
                    executeStatelessFilter(statelessFilters, filterIdx, localInput, filterComplete, filterNec);
                } else {
                    // Leverage push-down results to reduce the chunk filter input.
                    final Consumer<WritableRowSet> localConsumer = (rows) -> {
                        try (final RowSet ignored = rows; final PushdownResult ignored2 = pushdownResult) {
                            onFilterComplete.accept(rows.union(pushdownResult.match()));
                        }
                    };
                    // Do the final filtering at this position.
                    executeFinalFilter(sf.filter, pushdownResult.maybeMatch(), localConsumer, filterNec);
                }
            }, filterNec);
        };

        final RowSet input = localInput.get();
        if (sf.pushdownMatcher != null && sf.pushdownFilterCost < Long.MAX_VALUE) {
            // Execute the pushdown filter and return.
            sf.pushdownMatcher.pushdownFilter(sf.filter, input, usePrev, sf.context,
                    costCeiling, jobScheduler(), onPushdownComplete, filterNec);
            return;
        }

        if (sf.pushdownResult != null) {
            // Leverage push-down results to reduce the chunk filter input before the final filter.
            final Consumer<WritableRowSet> localConsumer = (rows) -> {
                onFilterComplete.accept(rows.union(sf.pushdownResult.match()));
            };

            sf.pushdownResult.match().retain(input);
            sf.pushdownResult.maybeMatch().retain(input);

            executeFinalFilter(sf.filter, sf.pushdownResult.maybeMatch(), localConsumer, filterNec);
            return;
        }
        executeFinalFilter(sf.filter, input, onFilterComplete, filterNec);
    }

    /**
     * Execute all stateless filters in the collection and return a row set that contains the rows that match every
     * filter.
     *
     * @param filters the filters to execute
     * @param localInput the input to use for this filter, also stores the result after the filters are executed
     * @param collectionResume the routine to call after the filter has been completely executed
     * @param collectionNec the routine to call if the filter experiences an exception
     */
    private void filterStatelessCollection(
            final List<WhereFilter> filters,
            final MutableObject<WritableRowSet> localInput,
            final Runnable collectionResume,
            final Consumer<Exception> collectionNec) {
        // Create stateless filter objects for the filters in this collection.
        final StatelessFilter[] statelessFilters = new StatelessFilter[filters.size()];

        // To properly respect barriers, we need each StatelessFilter to be aware of any respected barrier including
        // transitive implicit dependencies. For example filter A may respect barrier B, defined in filter B, which
        // respects barrier C, defined in filter C. In this case, filter A should also respect barrier C even though
        // it was not explicitly declared in filter A. We build up the set of implicit and explicit inter-barrier
        // dependencies in a dynamic-programming fashion; by adding all dependent respected barriers by the filter that
        // declares the barrier to any filter that respects it.
        final Map<Object, Collection<Object>> barrierDependencies = new HashMap<>();

        try {
            for (int ii = 0; ii < filters.size(); ii++) {
                final WhereFilter filter = filters.get(ii);

                final List<ColumnSource<?>> filterSources = filter.getColumns().stream()
                        .map(sourceTable::getColumnSource).collect(Collectors.toList());
                final PushdownFilterMatcher executor =
                        PushdownFilterMatcher.getPushdownFilterMatcher(filter, filterSources);
                if (executor != null) {
                    final PushdownFilterContext context = executor.makePushdownFilterContext(filter, filter.getColumns()
                            .stream().map(sourceTable::getColumnSource).collect(Collectors.toList()));
                    statelessFilters[ii] = new StatelessFilter(ii, filter, executor, context, barrierDependencies);
                } else {
                    statelessFilters[ii] = new StatelessFilter(ii, filter, null, null, barrierDependencies);
                }
                for (Object barrier : statelessFilters[ii].declaredBarriers) {
                    if (barrierDependencies.containsKey(barrier)) {
                        throw new IllegalArgumentException("Duplicate barrier declared: " + barrier);
                    }
                    barrierDependencies.put(barrier, statelessFilters[ii].respectedBarriers);
                }
            }
        } catch (final Exception ex) {
            informAndCloseAll(collectionNec, ex, statelessFilters);
            return;
        }

        // Sort the filters by cost, with the lowest cost first.
        scheduleAndSortCostEstimates(statelessFilters, 0, localInput.get(),
                // Update and sorting is completed
                () -> {
                    // Iterate serially through the stateless filters in this set. Each filter will successively
                    // restrict the input to the next filter, until we reach the end of the filter chain or no rows
                    // match.
                    jobScheduler().iterateSerial(
                            ExecutionContext.getContext(),
                            this::appendFilterExecution,
                            JobScheduler.DEFAULT_CONTEXT_FACTORY,
                            0, statelessFilters.length,
                            (filterContext, filterIdx, filterNec, filterResume) -> {
                                if (localInput.get().isEmpty()) {
                                    // If there are no rows left to filter, skip this filter.
                                    filterResume.run();
                                    return;
                                }
                                executeStatelessFilter(statelessFilters, filterIdx, localInput, filterResume,
                                        filterNec);
                            },

                            // Jobs have all completed
                            collectionResume,

                            // collectionResume has completed successfully
                            () -> SafeCloseableArray.close(statelessFilters),

                            // Jobs, or collectionResume, has had an error
                            e -> informAndCloseAll(collectionNec, e, statelessFilters));
                },
                // Update and sort, or _orchestration_ of inner scheduling, failed
                e -> informAndCloseAll(collectionNec, e, statelessFilters));
    }

    private static void informAndCloseAll(final Consumer<Exception> collectionNec, final Exception e,
            final StatelessFilter[] statelessFilters) {
        try (final SafeCloseable ignore = () -> SafeCloseableArray.close(statelessFilters)) {
            collectionNec.accept(e);
        }
    }

    /**
     * Execute all stateful filters in the collection and return a row set that contains the rows that match every
     * filter.
     *
     * @param filters the filters to execute
     * @param localInput the input to use for this filter, also stores the result after the filters are executed
     * @param collectionResume the routine to call after the filter has been completely executed
     * @param collectionNec the routine to call if the filter experiences an exception
     */
    private void filterStatefulCollection(
            final List<WhereFilter> filters,
            final MutableObject<WritableRowSet> localInput,
            final Runnable collectionResume,
            final Consumer<Exception> collectionNec) {
        // Iterate serially through the stateful filters in this set. Each filter will successively
        // restrict the input to the next filter, until we reach the end of the filter chain.
        jobScheduler().iterateSerial(
                ExecutionContext.getContext(),
                this::appendFilterExecution,
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, filters.size(),
                (filterContext, filterIdx, filterNec, filterResume) -> {
                    cancelIfInterrupted();
                    final WhereFilter filter = filters.get(filterIdx);
                    // Use the restricted output for the next filter (if this is not the first invocation)
                    final WritableRowSet result = filter(filter, localInput.get());
                    // Clean up the row sets created by the filter.
                    // Store the output as the next filter input.
                    replace(localInput, result);
                    filterResume.run();
                },
                collectionResume,
                () -> {
                },
                collectionNec);
    }

    private WritableRowSet filter(final WhereFilter filter, final RowSet input) {
        // If there are no rows left to filter, skip this filter.
        return input.isEmpty()
                ? RowSetFactory.empty()
                : filter.filter(input, sourceTable.getRowSet(), sourceTable, usePrev);
    }

    private static void replace(final MutableObject<WritableRowSet> obj, final WritableRowSet result) {
        try (final WritableRowSet ignored = obj.get()) {
            obj.setValue(result);
        }
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

        // Union the added and modified inputs (if needed).
        final WritableRowSet input = runModifiedFilters ? addedInput.union(modifiedInput) : addedInput.copy();

        // Short-circuit if there is no input to filter.
        if (input.isEmpty()) {
            onComplete.accept(RowSetFactory.empty(), RowSetFactory.empty());
            return;
        }

        // Start with the input row sets and narrow with each filter.
        final MutableObject<WritableRowSet> localInput = new MutableObject<>(input);

        // Divide the filters into stateful and stateless filter sets.
        final List<FilterCollection> filterCollections = collectFilters(filters);

        // Iterate serially through the filter collections.
        jobScheduler().iterateSerial(
                ExecutionContext.getContext(),
                this::appendFilterExecution,
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, filterCollections.size(),
                (collectionCtx, collectionIdx, collectionNec, collectionResume) -> {
                    final FilterCollection filterCollection = filterCollections.get(collectionIdx);
                    if (filterCollection instanceof StatelessFilterCollection) {
                        filterStatelessCollection(filterCollection, localInput, collectionResume, collectionNec);
                    } else {
                        filterStatefulCollection(filterCollection, localInput, collectionResume, collectionNec);
                    }
                }, () -> {
                    // Return empty RowSets instead of null.
                    final WritableRowSet result = localInput.get();
                    final BasePerformanceEntry baseEntry = jobScheduler().getAccumulatedPerformance();
                    if (baseEntry != null) {
                        basePerformanceEntry.accumulate(baseEntry);
                    }

                    // Separate the added and modified result if necessary.
                    if (runModifiedFilters) {
                        final WritableRowSet addedResult = result.extract(addedInput);
                        onComplete.accept(addedResult, result);
                    } else {
                        onComplete.accept(result, RowSetFactory.empty());
                    }
                },
                () -> {
                },
                onError);
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
