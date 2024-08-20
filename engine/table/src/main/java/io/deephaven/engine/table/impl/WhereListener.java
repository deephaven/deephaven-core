//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.DelayedErrorNotifier;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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
    private final boolean permitParallelization;
    private final int segmentCount;

    private volatile long initialNotificationStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;
    private volatile long finalNotificationStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

    private static final AtomicLongFieldUpdater<WhereListener> FINAL_NOTIFICATION_STEP_UPDATER =
            AtomicLongFieldUpdater.newUpdater(WhereListener.class, "finalNotificationStep");

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
        if (QueryTable.PARALLEL_WHERE_SEGMENTS <= 0) {
            segmentCount = getUpdateGraph().parallelismFactor();
        } else {
            segmentCount = QueryTable.PARALLEL_WHERE_SEGMENTS;
        }
        permitParallelization = AbstractFilterExecution.permitParallelization(filters)
                && !QueryTable.DISABLE_PARALLEL_WHERE
                && segmentCount > 1
                && (QueryTable.FORCE_PARALLEL_WHERE || getUpdateGraph().parallelismFactor() > 1);
        this.filterColumns = hasColumnArray ? null
                : sourceTable.newModifiedColumnSet(
                        filterColumnNames.toArray(String[]::new));
    }

    @NotNull
    static List<NotificationQueue.Dependency> extractDependencies(@NotNull final WhereFilter[] filters) {
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

        final ListenerFilterExecution result = makeUpdateFilterExecution();
        final TableUpdate upstream = recorder.getUpdate().acquire();
        result.scheduleCompletion(
                (adds, mods) -> completeUpdate(upstream, result.sourceModColumns, result.runModifiedFilters, adds,
                        mods),
                exception -> errorUpdate(exception, upstream));
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
            final boolean modifiesWereFiltered,
            final WritableRowSet addFilterResult,
            final WritableRowSet modifiedFilterResult) {
        final TableUpdateImpl update = new TableUpdateImpl();
        try (final SafeCloseable ignored = modifiedFilterResult) {
            // Intersect removed with pre-shift keyspace
            update.removed = currentMapping.extract(upstream.removed());

            // Shift keyspace
            upstream.shifted().apply(currentMapping);

            // Compute added against filters
            update.added = addFilterResult;

            if (modifiesWereFiltered) {
                update.modified = modifiedFilterResult.intersect(currentMapping);

                // Matching modifies in the current mapping are adds
                try (final WritableRowSet modsToAdd = modifiedFilterResult.minus(currentMapping)) {
                    update.added.writableCast().insert(modsToAdd);
                }

                // Unmatched upstream mods are removes if they are in our output rowset
                try (final WritableRowSet modsToRemove = upstream.modified().minus(modifiedFilterResult)) {
                    modsToRemove.writableCast().retain(currentMapping);

                    // Note modsToRemove is currently in post-shift keyspace
                    currentMapping.update(update.added, modsToRemove);

                    // Move modsToRemove into pre-shift keyspace and add to myRemoved
                    upstream.shifted().unapply(modsToRemove);
                    update.removed.writableCast().insert(modsToRemove);
                }
            } else {
                update.modified = upstream.modified().intersect(currentMapping);
                currentMapping.insert(update.added);
            }
        }

        update.modifiedColumnSet = sourceModColumns;
        if (update.modified.isEmpty()) {
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
        }

        // note shifts are pass-through since filter will never translate keyspace
        update.shifted = upstream.shifted();

        result.notifyListeners(update);

        // Release the upstream update and set the final notification step.
        finalizeUpdate(upstream);
    }

    private void errorUpdate(final Exception e, final TableUpdate upstream) {
        // Notify listeners that we had an issue refreshing the table.
        if (result.getLastNotificationStep() == result.updateGraph.clock().currentStep()) {
            forceReferenceCountToZero();
            result.delayedErrorReference = new DelayedErrorNotifier(e, entry, result);
        } else {
            result.notifyListenersOnError(e, entry);
            forceReferenceCountToZero();
        }

        // Release the upstream update and set the final notification step.
        finalizeUpdate(upstream);
    }

    void finalizeUpdate(@Nullable final TableUpdate upstream) {
        final long oldStep = FINAL_NOTIFICATION_STEP_UPDATER.get(this);
        final long step = getUpdateGraph().clock().currentStep();
        if (oldStep < step && FINAL_NOTIFICATION_STEP_UPDATER.compareAndSet(this, oldStep, step)) {
            if (upstream != null) {
                upstream.release();
            }
        }
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

    ListenerFilterExecution makeRefilterExecution(final RowSet refilter) {
        return new ListenerFilterExecution(refilter, null, false, ModifiedColumnSet.ALL);
    }

    ListenerFilterExecution makeUpdateFilterExecution() {
        final ModifiedColumnSet sourceModColumns = getSourceModifiedColumnSet();
        final boolean runModifiedFilters = filterColumns == null || sourceModColumns.containsAny(filterColumns);
        return new ListenerFilterExecution(recorder.getAdded(), recorder.getModified(),
                runModifiedFilters, sourceModColumns);
    }

    class ListenerFilterExecution extends AbstractFilterExecution {

        private final JobScheduler jobScheduler;

        private ListenerFilterExecution(
                final RowSet addedInput,
                final RowSet modifyInput,
                final boolean runModifiedFilters,
                final ModifiedColumnSet sourceModColumns) {
            super(WhereListener.this.sourceTable, WhereListener.this.filters, addedInput, modifyInput,
                    false, runModifiedFilters, sourceModColumns);
            // Create the proper JobScheduler for the following parallel tasks
            if (permitParallelization) {
                jobScheduler = new UpdateGraphJobScheduler(getUpdateGraph());
            } else {
                jobScheduler = new ImmediateJobScheduler();
            }
        }

        @Override
        JobScheduler jobScheduler() {
            return jobScheduler;
        }


        @Override
        boolean permitParallelization() {
            return permitParallelization;
        }

        @Override
        int getTargetSegments() {
            return segmentCount;
        }
    }
}
