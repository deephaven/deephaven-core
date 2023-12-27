package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.MultiException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * The AbstractFilterExecution incorporates the idea that we have an added and modified RowSet to filter and that there
 * are a resulting pair of added and modified rows representing what was filtered. There is also the possibility that we
 * encounter an exception "exceptionResult" in which case the operation should be considered a failure.
 *
 * The strategy that is used to divide the work is that there is some target split (by default the number of threads in
 * the TableMapTransform or LiveTableMonitor update thread pools) that we will divide our operation into. If there is
 * not enough work (defined by the {@link QueryTable#PARALLEL_WHERE_ROWS_PER_SEGMENT}) for more than one thread, we
 * simply run the operation in the calling thread. After each filter, we "reseed" the operation and recursively divide
 * it. For example, you might imagine we have a sequence of filters like "isBusinessTime" followed by a filter on
 * spread. The isBusinessTime filter would produce unequal results, therefore we do an N-way split on each result set to
 * avoid some threads doing inordinately more work than others.
 *
 * After a unit of work is completed, it percolates the result to its parent. Finally we call a completion routine,
 * which will either notify a downstream table (in the listener case) or set the value of a future (in the
 * initialization case).
 */
abstract class AbstractFilterExecution extends AbstractNotification {
    final BasePerformanceEntry basePerformanceEntry = new BasePerformanceEntry();

    final QueryTable sourceTable;
    final WhereFilter[] filters;

    final boolean runModifiedFilters;
    final ModifiedColumnSet sourceModColumns;

    /**
     * The added RowSet we are filtering, and the positions within that RowSet that we must filter.
     */
    final RowSet addedInput;
    final long addStart;
    final long addEnd;

    /**
     * The modified RowSet we are filtering, and the positions within that RowSet that we must filter.
     */
    final RowSet modifyInput;
    final long modifyStart;
    final long modifyEnd;

    /**
     * For initial filtering we may need to usePrev.
     */
    final boolean usePrev;

    /**
     * The immediate parent of this FilterExecution.
     */
    final AbstractFilterExecution parent;

    /**
     * How many child tasks have been spun off into other threads. We can not perform our combination step until this
     * reaches zero.
     */
    final AtomicInteger remainingChildren = new AtomicInteger();

    /**
     * The added and modified with the filter applied. Or an exceptional result.
     */
    WritableRowSet addedResult;
    WritableRowSet modifyResult;
    Exception exceptionResult;

    /**
     * Which filter are we currently processing, zero-based.
     */
    int filterIndex;

    AbstractFilterExecution(
            QueryTable sourceTable,
            WhereFilter[] filters,
            RowSet addedInput,
            long addStart,
            long addEnd,
            RowSet modifyInput,
            long modifyStart,
            long modifyEnd,
            AbstractFilterExecution parent,
            boolean usePrev,
            boolean runModifiedFilters,
            ModifiedColumnSet sourceModColumns,
            int filterIndex) {
        super(false);
        this.sourceTable = sourceTable;
        this.filters = filters;
        this.addedInput = addedInput;
        this.addStart = addStart;
        this.addEnd = addEnd;
        this.modifyInput = modifyInput;
        this.modifyStart = modifyStart;
        this.modifyEnd = modifyEnd;
        this.parent = parent;
        this.usePrev = usePrev;
        this.runModifiedFilters = runModifiedFilters;
        this.sourceModColumns = sourceModColumns;
        this.filterIndex = filterIndex;
    }

    /**
     * Run as a notification, accumulating performance results.
     */
    @Override
    public void run() {
        try {
            basePerformanceEntry.onBaseEntryStart();
            doFilter(x -> parent.onChildCompleted());
        } finally {
            basePerformanceEntry.onBaseEntryEnd();
        }
    }

    /**
     * Run the filter specified by this AbstractFilterExecution, scheduling the next filter if necessary.
     *
     * @param onCompletion the routine to call after the filter has been completely executed
     */
    public void doFilter(Consumer<AbstractFilterExecution> onCompletion) {
        try {
            if (addedInput != null) {
                if (Thread.interrupted()) {
                    throw new CancellationException("interrupted while filtering");
                }
                try (final RowSet processAdds = addedInput.subSetByPositionRange(addStart, addEnd)) {
                    addedResult = filters[filterIndex].filter(
                            processAdds, sourceTable.getRowSet(), sourceTable, usePrev);
                }
            }
            if (modifyInput != null) {
                if (Thread.interrupted()) {
                    throw new CancellationException("interrupted while filtering");
                }
                try (final RowSet processModifies = modifyInput.subSetByPositionRange(modifyStart, modifyEnd)) {
                    modifyResult = filters[filterIndex].filter(
                            processModifies, sourceTable.getRowSet(), sourceTable, usePrev);
                }
            }
            if (Thread.interrupted()) {
                throw new CancellationException("interrupted while filtering");
            }
            scheduleNextFilter(onCompletion);
        } catch (Exception e) {
            exceptionResult = e;
            onCompletion.accept(this);
        }
    }

    RowSet getAddedResult() {
        return addedResult == null ? RowSetFactory.empty() : addedResult;
    }

    RowSet getModifyResult() {
        return modifyResult == null ? RowSetFactory.empty() : modifyResult;
    }

    /**
     * Collapse other's result into this AbstractFilterExecution's result.
     *
     * @param other the result to combine into this
     */
    void combine(AbstractFilterExecution other) {
        if (this.addedResult == null) {
            this.addedResult = other.addedResult;
        } else if (other.addedResult != null) {
            this.addedResult.insert(other.addedResult);
        }
        if (this.modifyResult == null) {
            this.modifyResult = other.modifyResult;
        } else if (other.modifyResult != null) {
            this.modifyResult.insert(other.modifyResult);
        }
        if (this.exceptionResult == null) {
            this.exceptionResult = other.exceptionResult;
        } else if (other.exceptionResult != null) {
            if (MultiException.class.isAssignableFrom(this.exceptionResult.getClass())) {
                final MultiException exception = (MultiException) this.exceptionResult;
                this.exceptionResult = new MultiException("where()", Stream.concat(
                        Arrays.stream(exception.getCauses()), Stream.of(other.exceptionResult))
                        .toArray(Throwable[]::new));
            } else {
                this.exceptionResult = new MultiException("where()", this.exceptionResult, other.exceptionResult);
            }
        }
    }

    @Override
    public boolean canExecute(long step) {
        // we can execute as soon as we are instantiated
        return true;
    }

    @Override
    public LogOutput append(LogOutput output) {
        return output.append("FilterExecution{")
                .append(System.identityHashCode(this)).append(": ")
                .append(filters[filterIndex].toString())
                .append(", remaining children=").append(remainingChildren.get()).append("}");
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    /**
     * If there is a subsequent filter to execute, schedule it for execution (either in this thread or in another
     * thread), and then execute onCompletion.
     *
     * @param onCompletion
     */
    private void scheduleNextFilter(Consumer<AbstractFilterExecution> onCompletion) {
        if ((filterIndex == filters.length - 1) ||
                ((modifyResult == null || modifyResult.isEmpty()) && (addedResult == null || addedResult.isEmpty()))) {
            onCompletion.accept(this);
            return;
        }
        final AbstractFilterExecution nextFilterExecution = makeChild(
                addedResult, 0, addedResult == null ? 0 : addedResult.size(), modifyResult, 0,
                modifyResult == null ? 0 : modifyResult.size(), filterIndex + 1);
        nextFilterExecution.scheduleCompletion(result -> {
            this.exceptionResult = result.exceptionResult;
            this.addedResult = result.addedResult;
            this.modifyResult = result.modifyResult;
            onCompletion.accept(this);
        });
    }

    /**
     * Cleanup one child reference, and if it is the last reference, invoke onNoChildren.
     */
    protected void onChildCompleted() {
        final int remaining = remainingChildren.decrementAndGet();
        if (remaining < 0) {
            // noinspection ConstantConditions
            throw Assert.statementNeverExecuted();
        }
        if (remaining == 0) {
            onNoChildren();
        }
    }

    /**
     * Execute this filter either completely within this thread; or alternatively split it and assign it to the desired
     * thread pool.
     *
     * @param onCompletion the routine to call after the filter has been completely executed.
     */
    public void scheduleCompletion(Consumer<AbstractFilterExecution> onCompletion) {
        final long updateSize = (addedInput == null ? 0 : addedInput.size())
                + (modifyInput == null ? 0 : modifyInput.size());
        if (!doParallelization(updateSize)) {
            doFilter(onCompletion);
            return;
        }

        final int targetSegments = (int) Math.min(getTargetSegments(), (updateSize +
                QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT - 1) / QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT);
        final long targetSize = (updateSize + targetSegments - 1) / targetSegments;

        // we need to cut this into pieces
        final List<AbstractFilterExecution> subFilters = new ArrayList<>();

        long addedOffset = 0;
        long modifiedOffset = 0;

        while (addedInput != null && addedOffset < addedInput.size()) {
            final long startOffset = addedOffset;
            final long endOffset = addedOffset + targetSize;
            if (!runModifiedFilters || endOffset <= addedInput.size()) {
                subFilters.add(makeChild(addedInput, startOffset, endOffset, null, 0, 0, filterIndex));
            } else {
                subFilters.add(makeChild(addedInput, startOffset, addedInput.size(), modifyInput, 0,
                        modifiedOffset = targetSize - (addedInput.size() - startOffset), filterIndex));
            }
            addedOffset = endOffset;
        }
        while (modifyInput != null && modifiedOffset < modifyInput.size()) {
            subFilters.add(makeChild(
                    null, 0, 0, modifyInput, modifiedOffset, modifiedOffset += targetSize, filterIndex));
        }

        Assert.gtZero(subFilters.size(), "subFilters.size()");

        remainingChildren.set(subFilters.size());

        enqueueSubFilters(subFilters, new CombinationNotification(subFilters, onCompletion));
    }

    /**
     * Enqueue a set of (satisfied) subfilters for execution and the combination notification represented by those
     * subfilters.
     */
    abstract void enqueueSubFilters(
            List<AbstractFilterExecution> subFilters,
            CombinationNotification combinationNotification);

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
     * If a filter throws an uncaught exception, then we invoke this method to percolate the error back to the user.
     */
    abstract void handleUncaughtException(Exception throwable);

    /**
     * When executing on a thread pool, we call accumulatePerformanceEntry with the performance data from that thread
     * pool's execution so that it can be properly attributed to this operation.
     */
    abstract void accumulatePerformanceEntry(BasePerformanceEntry entry);

    /**
     * Called after all child filters have been executed, which can indicate that the combination notification should be
     * run.
     */
    abstract void onNoChildren();

    /**
     * Make a child AbstractFilterExecution of the correct type.
     */
    abstract AbstractFilterExecution makeChild(
            final RowSet addedInput,
            final long addStart,
            final long addEnd,
            final RowSet modifyInput,
            final long modifyStart,
            final long modifyEnd,
            final int filterIndex);

    /**
     * The combination notification executes after all of our child sub filters; combines the result; and calls our
     * completion routine.
     */
    class CombinationNotification extends AbstractNotification {
        private final List<AbstractFilterExecution> subFilters;
        private final Consumer<AbstractFilterExecution> onCompletion;

        public CombinationNotification(List<AbstractFilterExecution> subFilters,
                Consumer<AbstractFilterExecution> onCompletion) {
            super(false);
            this.subFilters = subFilters;
            this.onCompletion = onCompletion;
        }

        @Override
        public boolean canExecute(long step) {
            return remainingChildren.get() == 0;
        }

        @Override
        public void run() {
            BasePerformanceEntry basePerformanceEntry = new BasePerformanceEntry();

            try {
                basePerformanceEntry.onBaseEntryStart();

                final AbstractFilterExecution combined = subFilters.get(0);
                accumulatePerformanceEntry(combined.basePerformanceEntry);
                for (int ii = 1; ii < subFilters.size(); ++ii) {
                    final AbstractFilterExecution executionToCombine = subFilters.get(ii);
                    combined.combine(executionToCombine);
                    accumulatePerformanceEntry(executionToCombine.basePerformanceEntry);
                }
                if (combined.exceptionResult != null) {
                    handleUncaughtException(combined.exceptionResult);
                } else {
                    addedResult = combined.addedResult;
                    modifyResult = combined.modifyResult;
                    onCompletion.accept(combined);
                }
            } catch (Exception e) {
                handleUncaughtException(e);
            } finally {
                basePerformanceEntry.onBaseEntryEnd();
                accumulatePerformanceEntry(basePerformanceEntry);
            }
        }

        @Override
        public LogOutput append(LogOutput output) {
            return output.append("CombinedNotification{")
                    .append(System.identityHashCode(this)).append(": ")
                    .append(filters[filterIndex].toString())
                    .append(", remaining children=").append(remainingChildren.get()).append("}");
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }
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
