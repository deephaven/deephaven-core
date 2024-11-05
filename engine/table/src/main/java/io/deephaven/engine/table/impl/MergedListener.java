//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.table.impl.util.StepUpdater;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A merged listener has a collection of {@link ListenerRecorder}s. Each one must complete before the merged listener
 * fires its sole notification for the cycle.
 * <p>
 * You must use a MergedListener if your result table has multiple sources, otherwise it is possible for a table to
 * produce notifications more than once in a cycle; which is an error.
 */
public abstract class MergedListener extends LivenessArtifact implements NotificationQueue.Dependency {
    private static final Logger log = LoggerFactory.getLogger(MergedListener.class);

    private static final AtomicLongFieldUpdater<MergedListener> LAST_COMPLETED_STEP_UPDATER =
            AtomicLongFieldUpdater.newUpdater(MergedListener.class, "lastCompletedStep");

    private final UpdateGraph updateGraph;

    private final Iterable<? extends ListenerRecorder> recorders;
    private final Iterable<NotificationQueue.Dependency> dependencies;
    private final String listenerDescription;
    protected final QueryTable result;
    @Nullable
    protected final PerformanceEntry entry;
    private final String logPrefix;

    private boolean failed;


    @SuppressWarnings("FieldMayBeFinal")
    private volatile long lastCompletedStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;
    private volatile long lastEnqueuedStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

    private long notificationStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;
    private Throwable upstreamError;
    private TableListener.Entry errorSourceEntry;

    @ReferentialIntegrity
    private Runnable delayedErrorReference;

    protected MergedListener(
            final Iterable<? extends ListenerRecorder> recorders,
            final Iterable<NotificationQueue.Dependency> dependencies,
            final String listenerDescription,
            @Nullable final QueryTable result) {
        this.updateGraph = ExecutionContext.getContext().getUpdateGraph();
        this.recorders = recorders;
        recorders.forEach(this::manage);
        this.dependencies = dependencies;
        this.listenerDescription = listenerDescription;
        this.result = result;
        this.entry = PeriodicUpdateGraph.createUpdatePerformanceEntry(this.updateGraph, listenerDescription);
        this.logPrefix = System.identityHashCode(this) + " " + listenerDescription + " Merged Listener: ";
    }

    private void releaseFromRecorders() {
        recorders.forEach(ListenerRecorder::release);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    protected Iterable<? extends ListenerRecorder> getRecorders() {
        return recorders;
    }

    public boolean isFailed() {
        return failed;
    }

    public final void notifyOnUpstreamError(
            @NotNull final Throwable upstreamError, @Nullable final TableListener.Entry errorSourceEntry) {
        notifyInternal(upstreamError, errorSourceEntry);
    }

    public void notifyChanges() {
        notifyInternal(null, null);
    }

    private void notifyInternal(@Nullable final Throwable upstreamError,
            @Nullable final TableListener.Entry errorSourceEntry) {
        if (failed) {
            return;
        }

        final long currentStep = getUpdateGraph().clock().currentStep();

        synchronized (this) {
            if (notificationStep == currentStep) {
                // noinspection ConstantConditions
                throw Assert.statementNeverExecuted(
                        "MergedListener was fired before all listener recorders were satisfied: listener="
                                + System.identityHashCode(this) + ", currentStep=" + currentStep);
            }

            if (this.upstreamError == null && upstreamError != null) {
                this.upstreamError = upstreamError;
                this.errorSourceEntry = errorSourceEntry;
            }

            // We've already got something in the notification queue that has not yet been executed for the current
            // step.
            if (lastEnqueuedStep == currentStep) {
                return;
            }

            // Otherwise we should have already flushed that notification.
            Assert.assertion(lastEnqueuedStep == notificationStep,
                    "queuedNotificationStep == notificationStep", lastEnqueuedStep, "queuedNotificationStep",
                    notificationStep, "notificationStep", currentStep, "currentStep", this, "MergedListener");

            lastEnqueuedStep = currentStep;
        }

        getUpdateGraph().addNotification(new MergedNotification());
    }

    /**
     * Propagate an error to downstream listeners.
     *
     * @param uncaughtExceptionFromProcess true if the exception was thrown from {@link #process()}, false otherwise
     * @param error the error to propagate
     * @param entry the {@link io.deephaven.engine.table.TableListener.Entry} that threw the error.
     */
    protected void propagateError(
            final boolean uncaughtExceptionFromProcess,
            @NotNull final Throwable error,
            @Nullable final TableListener.Entry entry) {
        failed = true;
        forceReferenceCountToZero();
        propagateErrorDownstream(uncaughtExceptionFromProcess, error, entry);
        try {
            if (systemicResult()) {
                AsyncClientErrorNotifier.reportError(error);
            }
        } catch (IOException ioe) {
            throw new UncheckedTableException("Exception while reporting async error for " + entry, ioe);
        }
    }

    protected boolean systemicResult() {
        return result != null && SystemicObjectTracker.isSystemic(result);
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();
        recorders.forEach(ListenerRecorder::forceReferenceCountToZero);
    }

    protected void propagateErrorDownstream(
            final boolean fromProcess, @NotNull final Throwable error, @Nullable final TableListener.Entry entry) {
        if (result == null) {
            return;
        }
        if (fromProcess && result.satisfied(getUpdateGraph().clock().currentStep())) {
            // If the result is already satisfied (because it managed to send its notification, or was otherwise
            // satisfied) we should not send our error notification on this cycle.
            if (!result.isFailed()) {
                // If the result isn't failed, we need to mark it as such on the next cycle.
                scheduleDelayedErrorNotifier(error, entry, List.of(result));
            }
        } else {
            result.notifyListenersOnError(error, entry);
        }
    }

    protected final void scheduleDelayedErrorNotifier(
            @NotNull final Throwable error,
            @Nullable final TableListener.Entry entry,
            @NotNull final Collection<BaseTable> results) {
        delayedErrorReference = new DelayedErrorNotifier(getUpdateGraph(), error, entry, results);
    }

    private static final class DelayedErrorNotifier implements Runnable {

        private final UpdateGraph updateGraph;
        private final Throwable error;
        private final TableListener.Entry entry;
        private final Collection<WeakReference<BaseTable>> targetReferences;

        private DelayedErrorNotifier(
                @NotNull final UpdateGraph updateGraph,
                @NotNull final Throwable error,
                @Nullable final TableListener.Entry entry,
                @NotNull final Collection<BaseTable> targets) {
            this.error = error;
            this.entry = entry;
            this.targetReferences = targets.stream().map(WeakReference::new).collect(Collectors.toList());
            this.updateGraph = updateGraph;
            updateGraph.addSource(this);
        }

        @Override
        public void run() {
            targetReferences.stream()
                    .map(WeakReference::get)
                    .filter(Objects::nonNull)
                    .forEach(t -> t.notifyListenersOnError(error, entry));
            updateGraph.removeSource(this);
        }
    }

    protected abstract void process();

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("MergedListener(").append(System.identityHashCode(this)).append(")");
    }

    protected boolean canExecute(final long step) {
        return Stream.concat(
                StreamSupport.stream(recorders.spliterator(), false),
                StreamSupport.stream(dependencies.spliterator(), false))
                .allMatch((final NotificationQueue.Dependency dep) -> dep.satisfied(step));
    }

    @Override
    public boolean satisfied(final long step) {
        StepUpdater.checkForOlderStep(step, lastCompletedStep);
        StepUpdater.checkForOlderStep(step, lastEnqueuedStep);

        // Check and see if we've already been completed.
        if (lastCompletedStep == step) {
            getUpdateGraph().logDependencies()
                    .append("Already completed notification for ").append(this).append(", step=").append(step).endl();
            return true;
        }

        // This notification could be enqueued during the course of canExecute, but checking if we're enqueued is a very
        // cheap check that may let us avoid recursively checking all the dependencies.
        if (lastEnqueuedStep == step) {
            getUpdateGraph().logDependencies()
                    .append("Enqueued notification for ").append(this).append(", step=").append(step).endl();
            return false;
        }

        // Recursively check to see if our dependencies have been satisfied.
        if (!canExecute(step)) {
            getUpdateGraph().logDependencies()
                    .append("Dependencies not yet satisfied for ").append(this).append(", step=").append(step).endl();
            return false;
        }

        // Let's check again and see if we got lucky and another thread completed us while we were checking our
        // dependencies.
        if (lastCompletedStep == step) {
            getUpdateGraph().logDependencies()
                    .append("Already completed notification during dependency check for ").append(this)
                    .append(", step=").append(step)
                    .endl();
            return true;
        }

        // We check the queued notification step again after the dependency check. It is possible that something
        // enqueued us while we were evaluating the dependencies, and we must not miss that race.
        if (lastEnqueuedStep == step) {
            getUpdateGraph().logDependencies()
                    .append("Enqueued notification during dependency check for ").append(this)
                    .append(", step=").append(step)
                    .endl();
            return false;
        }

        getUpdateGraph().logDependencies()
                .append("Dependencies satisfied for ").append(this)
                .append(", lastCompleted=").append(lastCompletedStep)
                .append(", lastQueued=").append(lastEnqueuedStep)
                .append(", step=").append(step)
                .endl();

        // Mark this node as completed. All our dependencies have been satisfied, but we are not enqueued, so we can
        // never actually execute.
        StepUpdater.tryUpdateRecordedStep(LAST_COMPLETED_STEP_UPDATER, this, step);
        return true;
    }

    protected void handleUncaughtException(Exception updateException) {
        log.error().append(logPrefix).append("Uncaught exception for entry= ").append(entry)
                .append(": ").append(updateException).endl();
        propagateError(true, updateException, entry);
    }

    protected void accumulatePeformanceEntry(BasePerformanceEntry subEntry) {
        if (entry != null) {
            entry.accumulate(subEntry);
        }
    }

    private class MergedNotification extends AbstractNotification {

        public MergedNotification() {
            super(false);
        }

        @Override
        public void run() {
            final long currentStep = getUpdateGraph().clock().currentStep();
            try {
                if (lastEnqueuedStep != currentStep) {
                    // noinspection ConstantConditions
                    throw Assert.statementNeverExecuted("Notification step mismatch: listener="
                            + System.identityHashCode(MergedListener.this) + ": queuedNotificationStep="
                            + lastEnqueuedStep + ", step=" + currentStep);
                }

                if (upstreamError != null) {
                    propagateError(false, upstreamError, errorSourceEntry);
                    return;
                }

                long added = 0;
                long removed = 0;
                long modified = 0;
                long shifted = 0;

                for (ListenerRecorder recorder : recorders) {
                    if (recorder.getNotificationStep() == currentStep) {
                        added += recorder.getAdded().size();
                        removed += recorder.getRemoved().size();
                        modified += recorder.getModified().size();
                        shifted += recorder.getShifted().getEffectiveSize();
                    }
                }

                if (entry != null) {
                    entry.onUpdateStart(added, removed, modified, shifted);
                }
                try {
                    synchronized (MergedListener.this) {
                        if (notificationStep == lastEnqueuedStep) {
                            // noinspection ConstantConditions
                            throw Assert.statementNeverExecuted("Multiple notifications in the same step: listener="
                                    + System.identityHashCode(MergedListener.this) + ", queuedNotificationStep="
                                    + lastEnqueuedStep);
                        }
                        notificationStep = lastEnqueuedStep;
                    }
                    process();
                    getUpdateGraph().logDependencies()
                            .append("MergedListener has completed execution ")
                            .append(this).endl();
                } finally {
                    if (entry != null) {
                        entry.onUpdateEnd();
                    }
                }
            } catch (Exception updateException) {
                handleUncaughtException(updateException);
            } finally {
                StepUpdater.forceUpdateRecordedStep(LAST_COMPLETED_STEP_UPDATER, MergedListener.this, currentStep);
                releaseFromRecorders();
            }
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("Merged Notification ").append(System.identityHashCode(MergedListener.this))
                    .append(" ").append(listenerDescription);
        }

        @Override
        public boolean canExecute(final long step) {
            return MergedListener.this.canExecute(step);
        }
    }
}
