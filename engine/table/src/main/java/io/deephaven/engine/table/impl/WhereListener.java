package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The WhereListener is a MergedListener for computing updated filters
 * ({@link io.deephaven.api.TableOperations#where(io.deephaven.api.filter.Filter)}.
 *
 * <p>
 * We must be a merged listener, because several where operations require more than one input. For example, a
 * {@link DynamicWhereFilter} must wait for both the set table and filtered table to tick.
 *
 * <p>
 * Other filters may be dependent on e.g. a LiveTable to be complete before operating. It is possible we have a static
 * source table, but a refreshing filter in which case our listener recorder is null.
 */
class WhereListener extends MergedListener {

    private final QueryTable sourceTable;
    private final QueryTable.FilteredTable result;
    private final WritableRowSet currentMapping;
    private final WhereFilter[] filters;
    private final ModifiedColumnSet filterColumns;
    private final ListenerRecorder recorder;
    private final long minimumThreadSize;
    private final boolean permitParallelization;
    private final int segmentCount;

    private volatile long initialNotificationStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;
    private volatile long finalNotificationStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

    WhereListener(
            final Logger log,
            final QueryTable sourceTable,
            final ListenerRecorder recorder,
            final QueryTable.FilteredTable result,
            final WhereFilter[] filters) {
        super(recorder == null ? Collections.emptyList() : Collections.singleton(recorder),
                extractDependencies(filters),
                "where(" + Arrays.toString(filters) + ")", result);
        this.sourceTable = sourceTable;
        this.recorder = recorder;
        this.result = result;
        this.currentMapping = result.getRowSet().writableCast();
        this.filters = filters;

        boolean hasColumnArray = false;
        final Set<String> filterColumnNames = new TreeSet<>();
        for (final WhereFilter filter : this.filters) {
            hasColumnArray |= !filter.getColumnArrays().isEmpty();
            filterColumnNames.addAll(filter.getColumns());
            if (filter instanceof LivenessReferent && filter.isRefreshing()) {
                manage((LivenessReferent) filter);
            }
        }
        permitParallelization = AbstractFilterExecution.permitParallelization(filters);
        this.filterColumns = hasColumnArray ? null
                : sourceTable.newModifiedColumnSet(
                        filterColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));

        if (getUpdateGraph().parallelismFactor() > 1) {
            minimumThreadSize = QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT;
        } else {
            minimumThreadSize = Long.MAX_VALUE;
        }
        if (QueryTable.PARALLEL_WHERE_SEGMENTS <= 0) {
            segmentCount = getUpdateGraph().parallelismFactor();
        } else {
            segmentCount = QueryTable.PARALLEL_WHERE_SEGMENTS;
        }
    }

    @NotNull
    private static List<NotificationQueue.Dependency> extractDependencies(@NotNull final WhereFilter[] filters) {
        return Stream.concat(
                Stream.of(filters)
                        .filter(f -> f instanceof NotificationQueue.Dependency)
                        .map(f -> (NotificationQueue.Dependency) f),
                Stream.of(filters)
                        .filter(f -> f instanceof DependencyStreamProvider)
                        .flatMap(f -> ((DependencyStreamProvider) f)
                                .getDependencyStream()))
                .collect(Collectors.toList());
    }

    @Override
    public void process() {
        initialNotificationStep = getUpdateGraph().clock().currentStep();

        if (result.refilterRequested()) {
            final TableUpdate update = recorder != null ? recorder.getUpdate() : null;
            result.doRefilter(this, update);
            return;
        }

        // we should not get here if the recorder is null and we did not request a refilter
        Assert.neqNull(recorder, "recorder");

        final ListenerFilterExecution result = makeFilterExecution();
        final TableUpdate upstream = recorder.getUpdate().acquire();
        result.scheduleCompletion(
                (x) -> completeUpdate(upstream, result.sourceModColumns, result.runModifiedFilters, x));
    }

    private ModifiedColumnSet getSourceModifiedColumnSet() {
        final ModifiedColumnSet sourceModColumns;
        if (recorder.getModifiedColumnSet() != null) {
            sourceModColumns = recorder.getModifiedColumnSet();
        } else {
            sourceModColumns = result.getModifiedColumnSetForUpdates();
            sourceModColumns.clear();
        }
        return sourceModColumns;
    }

    private void completeUpdate(
            final TableUpdate upstream,
            final ModifiedColumnSet sourceModColumns,
            final boolean runFilters,
            final AbstractFilterExecution filterResult) {
        final TableUpdateImpl update = new TableUpdateImpl();

        // intersect removed with pre-shift keyspace
        update.removed = upstream.removed().intersect(currentMapping);
        currentMapping.remove(update.removed);

        // shift keyspace
        upstream.shifted().apply(currentMapping);

        // compute added against filters
        update.added = filterResult.getAddedResult();
        final RowSet matchingModifies = filterResult.getModifyResult();

        // which propagate as mods?
        update.modified = (runFilters ? matchingModifies : upstream.modified()).intersect(currentMapping);

        // remaining matchingModifies are adds
        update.added.writableCast().insert(matchingModifies.minus(update.modified));

        final WritableRowSet modsToRemove;
        if (!runFilters) {
            modsToRemove = RowSetFactory.empty();
        } else {
            modsToRemove = upstream.modified().minus(matchingModifies);
            modsToRemove.writableCast().retain(currentMapping);
        }
        // note modsToRemove is currently in post-shift keyspace
        currentMapping.update(update.added, modsToRemove);

        // move modsToRemove into pre-shift keyspace and add to myRemoved
        upstream.shifted().unapply(modsToRemove);
        update.removed.writableCast().insert(modsToRemove);

        update.modifiedColumnSet = sourceModColumns;
        if (update.modified.isEmpty()) {
            update.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            update.modifiedColumnSet.clear();
        }

        // note shifts are pass-through since filter will never translate keyspace
        update.shifted = upstream.shifted();

        result.notifyListeners(update);

        upstream.release();

        setFinalExecutionStep();
    }

    /**
     * We are only satisfied if our super is satisfied and our initialNotificationStep is equal to our final
     * notification step. The logic here is that the super is satisfied if (1) either we're never going to get executed,
     * or (2) we have already been executed. In case #1, we'll have our initial and final steps from the last execution
     * so no worries. In case #2, we need to wait for the CombinedNotification calling the onCompletion method to have
     * notified the downstream table.
     *
     * @param step the clock step.
     * @return true if dependencies may proceed
     */
    @Override
    public boolean satisfied(long step) {
        if (super.satisfied(step)) {
            return initialNotificationStep == finalNotificationStep;
        }
        return false;
    }

    ListenerFilterExecution makeFilterExecution(RowSet refilter) {
        return new ListenerFilterExecution(refilter, 0, refilter.size(), null, 0, 0, null, false, ModifiedColumnSet.ALL,
                0);
    }

    void setFinalExecutionStep() {
        finalNotificationStep = getUpdateGraph().clock().currentStep();
    }

    ListenerFilterExecution makeFilterExecution() {
        final ModifiedColumnSet sourceModColumns = getSourceModifiedColumnSet();
        final boolean runModifiedFilters = filterColumns == null || sourceModColumns.containsAny(filterColumns);
        return new ListenerFilterExecution(recorder.getAdded(), 0, recorder.getAdded().size(),
                recorder.getModified(), 0, recorder.getModified().size(), null,
                runModifiedFilters, sourceModColumns, 0);
    }

    class ListenerFilterExecution extends AbstractFilterExecution {
        private ListenerFilterExecution(
                final RowSet addedInput,
                final long addStart,
                final long addEnd,
                final RowSet modifyInput,
                final long modifyStart,
                final long modifyEnd,
                final ListenerFilterExecution parent,
                final boolean runModifiedFilters,
                final ModifiedColumnSet sourceModColumns,
                final int filterIndex) {
            super(WhereListener.this.sourceTable, WhereListener.this.filters, addedInput, addStart, addEnd, modifyInput,
                    modifyStart, modifyEnd, parent, false, runModifiedFilters, sourceModColumns, filterIndex);
        }

        @Override
        boolean doParallelization(long numberOfRows) {
            return permitParallelization
                    && (QueryTable.FORCE_PARALLEL_WHERE || getUpdateGraph().parallelismFactor() > 1)
                    && doParallelizationBase(numberOfRows);
        }

        @Override
        void handleUncaughtException(Exception throwable) {
            WhereListener.this.handleUncaughtException(throwable);
        }

        @Override
        void accumulatePerformanceEntry(BasePerformanceEntry entry) {
            WhereListener.this.accumulatePeformanceEntry(entry);
        }

        @Override
        void onNoChildren() {}

        @Override
        ListenerFilterExecution makeChild(
                final RowSet addedInput, final long addStart, final long addEnd, final RowSet modifyInput,
                final long modifyStart, final long modifyEnd, final int filterIndex) {
            return new ListenerFilterExecution(addedInput, addStart, addEnd, modifyInput, modifyStart, modifyEnd, this,
                    runModifiedFilters, sourceModColumns, filterIndex);
        }

        @Override
        void enqueueSubFilters(
                List<AbstractFilterExecution> subFilters,
                CombinationNotification combinationNotification) {
            getUpdateGraph().addNotifications(subFilters);
            getUpdateGraph().addNotification(combinationNotification);
        }

        @Override
        int getTargetSegments() {
            return segmentCount;
        }
    }
}
