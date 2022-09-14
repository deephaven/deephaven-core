/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A merged listener has a collection of {@link ListenerRecorder}s. Each one must complete before the merged listener
 * fires its sole notification for the cycle.
 *
 * You must use a MergedListener if your result table has multiple sources, otherwise it is possible for a table to
 * produce notifications more than once in a cycle; which is an error.
 */
public abstract class MergedListener extends LivenessArtifact implements NotificationQueue.Dependency {
    private static final Logger log = LoggerFactory.getLogger(MergedListener.class);

    private final Iterable<? extends ListenerRecorder> recorders;
    private final Iterable<NotificationQueue.Dependency> dependencies;
    private final String listenerDescription;
    protected final QueryTable result;
    private final PerformanceEntry entry;
    private final String logPrefix;

    private long notificationStep = -1;
    private long queuedNotificationStep = -1;
    private long lastCompletedStep;
    private Throwable upstreamError;
    private TableListener.Entry errorSourceEntry;

    private final ExecutionContext executionContext = ExecutionContext.getContextToRecord();

    protected MergedListener(
            Iterable<? extends ListenerRecorder> recorders,
            Iterable<NotificationQueue.Dependency> dependencies,
            String listenerDescription,
            QueryTable result) {
        this.recorders = recorders;
        recorders.forEach(this::manage);
        this.dependencies = dependencies;
        this.listenerDescription = listenerDescription;
        this.result = result;
        this.entry = UpdatePerformanceTracker.getInstance().getEntry(listenerDescription);
        this.logPrefix = System.identityHashCode(this) + " " + listenerDescription + " Merged Listener: ";
    }

    private void releaseFromRecorders() {
        recorders.forEach(ListenerRecorder::release);
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
        final long currentStep = LogicalClock.DEFAULT.currentStep();

        synchronized (this) {
            if (notificationStep == currentStep) {
                // noinspection ConstantConditions
                throw Assert.statementNeverExecuted(
                        "MergedListener was fired before both all listener records completed: listener="
                                + System.identityHashCode(this) + ", currentStep=" + currentStep);
            }

            if (this.upstreamError == null && upstreamError != null) {
                this.upstreamError = upstreamError;
                this.errorSourceEntry = errorSourceEntry;
            }

            // We've already got something in the notification queue that has not yet been executed for the current
            // step.
            if (queuedNotificationStep == currentStep) {
                return;
            }

            // Otherwise we should have already flushed that notification.
            Assert.assertion(queuedNotificationStep == notificationStep,
                    "queuedNotificationStep == notificationStep", queuedNotificationStep, "queuedNotificationStep",
                    notificationStep, "notificationStep", currentStep, "currentStep", this, "MergedListener");

            queuedNotificationStep = currentStep;
        }

        UpdateGraphProcessor.DEFAULT.addNotification(new MergedNotification());
    }

    private void propagateProcessError(Exception updateException) {
        propagateErrorInternal(updateException, entry);
    }

    private void propagateErrorInternal(@NotNull final Throwable error, @Nullable final TableListener.Entry entry) {
        forceReferenceCountToZero();
        recorders.forEach(ListenerRecorder::forceReferenceCountToZero);
        propagateErrorDownstream(error, entry);
        try {
            if (systemicResult()) {
                AsyncClientErrorNotifier.reportError(error);
            }
        } catch (IOException ioe) {
            throw new UncheckedTableException("Exception while reporting async error for " + entry, ioe);
        }
    }

    protected boolean systemicResult() {
        return SystemicObjectTracker.isSystemic(MergedListener.this.result);
    }

    protected void propagateErrorDownstream(@NotNull final Throwable error, @Nullable final TableListener.Entry entry) {
        result.notifyListenersOnError(error, entry);
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
        // Check and see if we've already been completed.
        if (lastCompletedStep == step) {
            UpdateGraphProcessor.DEFAULT.logDependencies()
                    .append("Already completed notification for ").append(this).append(", step=").append(step).endl();
            return true;
        }

        // This notification could be enqueued during the course of canExecute, but checking if we're enqueued is a very
        // cheap check that may let us avoid recursively checking all the dependencies.
        if (queuedNotificationStep == step) {
            UpdateGraphProcessor.DEFAULT.logDependencies()
                    .append("Enqueued notification for ").append(this).append(", step=").append(step).endl();
            return false;
        }

        // Recursively check to see if our dependencies have been satisfied.
        if (!canExecute(step)) {
            UpdateGraphProcessor.DEFAULT.logDependencies()
                    .append("Dependencies not yet satisfied for ").append(this).append(", step=").append(step).endl();
            return false;
        }

        // Let's check again and see if we got lucky and another thread completed us while we were checking our
        // dependencies.
        if (lastCompletedStep == step) {
            UpdateGraphProcessor.DEFAULT.logDependencies()
                    .append("Already completed notification during dependency check for ").append(this)
                    .append(", step=").append(step)
                    .endl();
            return true;
        }

        // We check the queued notification step again after the dependency check. It is possible that something
        // enqueued us while we were evaluating the dependencies, and we must not miss that race.
        if (queuedNotificationStep == step) {
            UpdateGraphProcessor.DEFAULT.logDependencies()
                    .append("Enqueued notification during dependency check for ").append(this)
                    .append(", step=").append(step)
                    .endl();
            return false;
        }

        UpdateGraphProcessor.DEFAULT.logDependencies()
                .append("Dependencies satisfied for ").append(this)
                .append(", lastCompleted=").append(lastCompletedStep)
                .append(", lastQueued=").append(queuedNotificationStep)
                .append(", step=").append(step)
                .endl();

        // Mark this node as completed. All our dependencies have been satisfied, but we are not enqueued, so we can
        // never actually execute.
        lastCompletedStep = step;
        return true;
    }

    private class MergedNotification extends AbstractNotification {

        public MergedNotification() {
            super(false);
        }

        @Override
        public void run() {
            final long currentStep = LogicalClock.DEFAULT.currentStep();
            try {
                if (queuedNotificationStep != currentStep) {
                    // noinspection ConstantConditions
                    throw Assert.statementNeverExecuted("Notification step mismatch: listener="
                            + System.identityHashCode(MergedListener.this) + ": queuedNotificationStep="
                            + queuedNotificationStep + ", step=" + currentStep);
                }

                if (upstreamError != null) {
                    propagateErrorInternal(upstreamError, errorSourceEntry);
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

                entry.onUpdateStart(added, removed, modified, shifted);
                try {
                    synchronized (MergedListener.this) {
                        if (notificationStep == queuedNotificationStep) {
                            // noinspection ConstantConditions
                            throw Assert.statementNeverExecuted("Multiple notifications in the same step: listener="
                                    + System.identityHashCode(MergedListener.this) + ", queuedNotificationStep="
                                    + queuedNotificationStep);
                        }
                        notificationStep = queuedNotificationStep;
                    }
                    process();
                    UpdateGraphProcessor.DEFAULT.logDependencies().append("MergedListener has completed execution ")
                            .append(this).endl();
                } finally {
                    entry.onUpdateEnd();
                }
            } catch (Exception updateException) {
                log.error().append(logPrefix).append("Uncaught exception for entry= ").append(entry)
                        .append(": ").append(updateException).endl();
                propagateProcessError(updateException);
            } finally {
                lastCompletedStep = currentStep;
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

        @Override
        public ExecutionContext getExecutionContext() {
            return executionContext;
        }
    }
}
