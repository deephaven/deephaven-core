/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.function.LongPredicate;

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

    private final String[] dependencyColumnNames;
    private final ColumnSource<? extends NotificationQueue.Dependency>[] dependencyColumnSources;
    private final ModifiedColumnSet[] dependencyModifiedColumnSets;

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
                                && UpdateGraphProcessor.DEFAULT.getUpdateThreads() > 1))
                        && analyzer.allowCrossColumnParallelization();
        analyzer.setAllNewColumns(allNewColumns);

        dependencyColumnNames = dependent.getDefinition().getColumnStream()
                .filter(cd -> NotificationQueue.Dependency.class.isAssignableFrom(cd.getDataType()))
                .map(ColumnDefinition::getName)
                .toArray(String[]::new);
        // noinspection unchecked
        dependencyColumnSources = Arrays.stream(dependencyColumnNames)
                .map(cn -> dependent.getColumnSource(cn, NotificationQueue.Dependency.class))
                .toArray(ColumnSource[]::new);
        dependencyModifiedColumnSets = Arrays.stream(dependencyColumnNames)
                .map(dependent::newModifiedColumnSet)
                .toArray(ModifiedColumnSet[]::new);
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

        analyzer.applyUpdate(acquiredUpdate, toClear, updateHelper, jobScheduler, this,
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
        onFailure(e, getEntry());
        updateInProgress = false;
    }

    private void completionRoutine(TableUpdate upstream, SelectAndViewAnalyzer.JobScheduler jobScheduler,
            WritableRowSet toClear, SelectAndViewAnalyzer.UpdateHelper updateHelper) {
        final TableUpdateImpl downstream = new TableUpdateImpl(upstream.added().copy(), upstream.removed().copy(),
                upstream.modified().copy(), upstream.shifted(), dependent.getModifiedColumnSetForUpdates());
        transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
        maybeDeferDownstreamUntilDependencyColumnsSatisfied(downstream);
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

    private void maybeDeferDownstreamUntilDependencyColumnsSatisfied(@NotNull final TableUpdate downstream) {
        final boolean dependencySourcesAffected;
        if (dependencyColumnNames.length == 0) {
            dependencySourcesAffected = false;
        } else if (downstream.added().isNonempty() || downstream.removed().isNonempty()) {
            dependencySourcesAffected = true;
        } else if (downstream.modified().isNonempty()) {
            dependencySourcesAffected = Arrays.stream(dependencyModifiedColumnSets)
                    .anyMatch(downstream.modifiedColumnSet()::containsAny);
        } else {
            // Only shifts
            dependencySourcesAffected = false;
        }
        if (dependencySourcesAffected) {
            UpdateGraphProcessor.DEFAULT.addNotification(new DeferredNotification(dependent, downstream,
                    (final long step) -> dependencyColumnsSatisfied(step, downstream)));
        } else {
            dependent.notifyListeners(downstream);
        }
    }

    private boolean dependencyColumnsSatisfied(final long step, @NotNull final TableUpdate downstream) {
        for (int dci = 0; dci < dependencyColumnSources.length; ++dci) {
            final ColumnSource<? extends NotificationQueue.Dependency> columnSource = dependencyColumnSources[dci];
            final ModifiedColumnSet modifiedColumnSet = dependencyModifiedColumnSets[dci];
            if (downstream.added().isNonempty()) {
                try (final ObjectColumnIterator<? extends NotificationQueue.Dependency> addedDependencies =
                        new ObjectColumnIterator<>(columnSource, downstream.added())) {
                    if (addedDependencies.stream().anyMatch(addedDep -> !addedDep.satisfied(step))) {
                        return false;
                    }
                }
            }
            if (downstream.removed().isNonempty()) {
                try (final ObjectColumnIterator<? extends NotificationQueue.Dependency> removedDependencies =
                        new ObjectColumnIterator<>(columnSource.getPrevSource(), downstream.removed())) {
                    if (removedDependencies.stream().anyMatch(removedDep -> !removedDep.satisfied(step))) {
                        return false;
                    }
                }
            }
            if (downstream.modified().isNonempty() && downstream.modifiedColumnSet().containsAny(modifiedColumnSet)) {
                // @formatter:off
                try (final ObjectColumnIterator<? extends NotificationQueue.Dependency> postModifiedDependencies =
                         new ObjectColumnIterator<>(columnSource, downstream.modified());
                     final ObjectColumnIterator<? extends NotificationQueue.Dependency> preModifiedDependencies =
                         new ObjectColumnIterator<>(columnSource.getPrevSource(), downstream.getModifiedPreShift())) {
                    // @formatter:off
                    while (postModifiedDependencies.hasNext()) {
                        final NotificationQueue.Dependency current = postModifiedDependencies.next();
                        final NotificationQueue.Dependency previous = preModifiedDependencies.next();
                        // This is somewhat conservative. We could instead only check satisfaction when
                        // current != previous, and that would be enough for careful listeners that avoid unnecessary
                        // work, but we don't know that all listeners are careful.
                        if (!current.satisfied(step)) {
                            return false;
                        }
                        if (current != previous && !previous.satisfied(step)) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Deferred notification that cannot execute until a predicate becomes true.
     */
    private static final class DeferredNotification extends AbstractNotification {

        private final BaseTable notifier;
        private final TableUpdate downstream;
        private final LongPredicate trigger;

        private DeferredNotification(
                @NotNull final BaseTable notifier,
                @NotNull final TableUpdate downstream,
                @NotNull final LongPredicate trigger) {
            super(false);
            this.notifier = notifier;
            this.downstream = downstream;
            this.trigger = trigger;
        }

        @Override
        public LogOutput append(@NotNull final LogOutput output) {
            return output.append("Notification{").append(System.identityHashCode(this))
                    .append(" for deferred downstream table update from ").append(notifier).append('}');
        }

        @Override
        public boolean canExecute(final long step) {
            return trigger.test(step);
        }

        @Override
        public ExecutionContext getExecutionContext() {
            return null;
        }

        @Override
        public void run() {
            notifier.notifyListeners(downstream);
        }
    }
}
