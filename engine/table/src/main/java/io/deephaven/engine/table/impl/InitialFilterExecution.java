package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A FilterExecution that is used for initial filters. When we split off sub filters as child jobs, they are enqueued in
 * the {@link io.deephaven.engine.updategraph.OperationInitializer OperationInitializer}.
 */
class InitialFilterExecution extends AbstractFilterExecution {
    private final QueryTable sourceTable;
    private final boolean permitParallelization;
    private final int segmentCount;
    private final WhereFilter[] filters;

    /**
     * The pendingSatisfaction list is global to the root node of this InitialExecutionFilter. The outstanding children
     * allows us to count how many jobs exist. If we have no outstanding jobs, but unsatisfied Notifications then an
     * error has occurred.
     */
    private final IntrusiveDoublyLinkedQueue<NotificationQueue.Notification> pendingSatisfaction;
    private final Map<Thread, Thread> runningChildren;
    private final AtomicBoolean cancelled;

    /**
     * The SubEntry lets us track query performance for the split jobs.
     */
    private BasePerformanceEntry basePerformanceEntry;

    /**
     * The InitialFilterExecution that represents all the work we are doing for this table.
     */
    private final InitialFilterExecution root;

    InitialFilterExecution(
            final QueryTable sourceTable,
            final WhereFilter[] filters,
            final RowSet addedInput,
            final long addStart,
            final long addEnd,
            final InitialFilterExecution parent,
            final int filterIndex,
            final boolean usePrev) {
        super(sourceTable, filters, addedInput, addStart, addEnd, null, 0, 0, parent, usePrev, false,
                ModifiedColumnSet.ALL, filterIndex);
        this.sourceTable = sourceTable;
        permitParallelization = permitParallelization(filters);
        this.filters = filters;
        if (parent == null) {
            pendingSatisfaction = new IntrusiveDoublyLinkedQueue<>(
                    IntrusiveDoublyLinkedNode.Adapter.<NotificationQueue.Notification>getInstance());
            segmentCount = QueryTable.PARALLEL_WHERE_SEGMENTS <= 0
                    ? ExecutionContext.getContext().getOperationInitializer().parallelismFactor()
                    : QueryTable.PARALLEL_WHERE_SEGMENTS;
            runningChildren = Collections.synchronizedMap(new IdentityHashMap<>());
            cancelled = new AtomicBoolean(false);
            this.root = this;
        } else {
            pendingSatisfaction = parent.pendingSatisfaction;
            segmentCount = parent.segmentCount;
            this.root = parent.root;
            runningChildren = null;
            cancelled = null;
        }
    }

    @Override
    void enqueueSubFilters(
            List<AbstractFilterExecution> subFilters,
            AbstractFilterExecution.CombinationNotification combinationNotification) {
        synchronized (pendingSatisfaction) {
            enqueueJobs(subFilters);
            pendingSatisfaction.offer(combinationNotification);
        }
    }

    private void enqueueJobs(Iterable<? extends NotificationQueue.Notification> subFilters) {
        for (NotificationQueue.Notification notification : subFilters) {
            ExecutionContext.getContext().getOperationInitializer().submit(() -> {
                root.runningChildren.put(Thread.currentThread(), Thread.currentThread());
                try {
                    if (!root.cancelled.get()) {
                        notification.run();
                    } else {
                        // we must ensure that we, the parent InitialFilterExecution, are notified of completion
                        onChildCompleted();
                    }
                    if (Thread.interrupted()) {
                        // we would like to throw a query cancellation exception
                        exceptionResult = new CancellationException("thread interrupted");
                    }
                } finally {
                    root.runningChildren.remove(Thread.currentThread());
                }
            });
        }
    }

    @Override
    int getTargetSegments() {
        return segmentCount;
    }

    @Override
    boolean doParallelization(long numberOfRows) {
        return permitParallelization
                && ExecutionContext.getContext().getOperationInitializer().canParallelize()
                && doParallelizationBase(numberOfRows);
    }

    @Override
    void handleUncaughtException(Exception throwable) {
        throw new UnsupportedOperationException(throwable);
    }

    @Override
    void accumulatePerformanceEntry(BasePerformanceEntry entry) {
        synchronized (root) {
            if (root.basePerformanceEntry != null) {
                root.basePerformanceEntry.accumulate(entry);
            } else {
                root.basePerformanceEntry = entry;
            }
        }
    }

    /**
     * Run any satisfied jobs in the pendingSatisfaction list.
     */
    @Override
    void onNoChildren() {
        final IntrusiveDoublyLinkedQueue<NotificationQueue.Notification> satisfied = new IntrusiveDoublyLinkedQueue<>(
                IntrusiveDoublyLinkedNode.Adapter.<NotificationQueue.Notification>getInstance());
        synchronized (pendingSatisfaction) {
            for (final Iterator<NotificationQueue.Notification> it = pendingSatisfaction.iterator(); it.hasNext();) {
                final NotificationQueue.Notification notification = it.next();
                if (notification.canExecute(0)) {
                    satisfied.offer(notification);
                    it.remove();
                }
            }
        }
        if (satisfied.isEmpty()) {
            return;
        }
        satisfied.forEach(NotificationQueue.Notification::run);
    }

    @Override
    InitialFilterExecution makeChild(
            final RowSet addedInput,
            final long addStart,
            final long addEnd,
            final RowSet modifyInput,
            final long modifyStart,
            final long modifyEnd,
            final int filterIndex) {
        Assert.eqNull(modifyInput, "modifyInput");
        return new InitialFilterExecution(sourceTable, filters, addedInput, addStart, addEnd, this, filterIndex,
                usePrev);
    }

    BasePerformanceEntry getBasePerformanceEntry() {
        return basePerformanceEntry;
    }

    void setCancelled() {
        cancelled.set(true);
        runningChildren.forEach((thread, ignored) -> thread.interrupt());
    }
}
