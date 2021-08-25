/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.tables.utils.QueryPerformanceNugget;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.utils.AbstractNotification;
import io.deephaven.db.v2.utils.AsyncClientErrorNotifier;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * A merged listener has a collection of {@link ListenerRecorder}s. Each one must complete before
 * the merged listener fires its sole notification for the cycle.
 *
 * You must use a MergedListener if your result table has multiple sources, otherwise it is possible
 * for a table to produce notifications more than once in a cycle; which is an error.
 */
public abstract class MergedListener extends LivenessArtifact
    implements NotificationQueue.Dependency {
    private static final Logger log = LoggerFactory.getLogger(MergedListener.class);

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String logPrefix;

    private final Collection<? extends ListenerRecorder> recorders;
    private final Collection<NotificationQueue.Dependency> dependencies;

    protected QueryTable result;
    private long notificationClock = -1;
    private long queuedNotificationClock = -1;
    private final String listenerDescription;

    private long lastCompletedStep;

    private final UpdatePerformanceTracker.Entry entry;

    protected MergedListener(Collection<? extends ListenerRecorder> recorders,
        Collection<NotificationQueue.Dependency> dependencies, String listenerDescription,
        QueryTable result) {
        this.recorders = recorders;
        recorders.forEach(this::manage);
        this.dependencies = dependencies;
        this.result = result;
        this.listenerDescription = listenerDescription;
        this.entry = UpdatePerformanceTracker.getInstance().getEntry(listenerDescription);
        this.logPrefix =
            System.identityHashCode(this) + " " + listenerDescription + " Merged Listener: ";
    }

    private void releaseFromRecorders() {
        recorders.forEach(ListenerRecorder::release);
    }

    public void notifyChanges() {
        final long currentStep = LogicalClock.DEFAULT.currentStep();

        synchronized (this) {
            if (notificationClock == currentStep) {
                throw Assert.statementNeverExecuted(
                    "MergedListener was fired before both all listener records completed: listener="
                        + System.identityHashCode(this) + ", currentStep=" + currentStep);
            }

            // we've already got something in the notification queue that has not yet been executed
            // for the current step.
            if (queuedNotificationClock == currentStep) {
                return;
            }

            // Otherwise we should have already flushed that notification.
            Assert.assertion(queuedNotificationClock == notificationClock,
                "queuedNotificationClock == notificationClock", queuedNotificationClock,
                "queuedNotificationClock", notificationClock, "notificationClock", currentStep,
                "currentStep", this, "MergedListener");

            queuedNotificationClock = currentStep;
        }

        LiveTableMonitor.DEFAULT.addNotification(new AbstractNotification(false) {
            @Override
            public void run() {
                try {
                    if (queuedNotificationClock != LogicalClock.DEFAULT.currentStep()) {
                        throw Assert.statementNeverExecuted("Notification step mismatch: listener="
                            + System.identityHashCode(MergedListener.this)
                            + ": queuedNotificationClock=" + queuedNotificationClock + ", step="
                            + LogicalClock.DEFAULT.currentStep());
                    }

                    long added = 0;
                    long removed = 0;
                    long modified = 0;
                    long shifted = 0;

                    for (ListenerRecorder recorder : recorders) {
                        if (recorder.getNotificationStep() == LogicalClock.DEFAULT.currentStep()) {
                            added += recorder.getAdded().size();
                            removed += recorder.getRemoved().size();
                            modified += recorder.getModified().size();
                            shifted += recorder.getShifted().getEffectiveSize();
                        }
                    }

                    entry.onUpdateStart(added, removed, modified, shifted);
                    try {
                        synchronized (MergedListener.this) {
                            if (notificationClock == queuedNotificationClock) {
                                throw Assert.statementNeverExecuted(
                                    "Multiple notifications in the same step: listener="
                                        + System.identityHashCode(MergedListener.this)
                                        + ", queuedNotificationClock=" + queuedNotificationClock);
                            }
                            notificationClock = queuedNotificationClock;
                        }
                        process();
                        LiveTableMonitor.DEFAULT.logDependencies()
                            .append("MergedListener has completed execution ").append(this).endl();
                    } finally {
                        entry.onUpdateEnd();
                        lastCompletedStep = LogicalClock.DEFAULT.currentStep();
                    }
                } catch (Exception updateException) {
                    log.error().append(logPrefix).append("Uncaught exception for entry= ")
                        .append(entry)
                        .append(": ").append(updateException).endl();
                    notifyOnError(updateException);
                    try {
                        if (systemicResult()) {
                            AsyncClientErrorNotifier.reportError(updateException);
                        }
                    } catch (IOException ioe) {
                        throw new RuntimeException("Exception in " + entry.toString(), ioe);
                    }
                } finally {
                    releaseFromRecorders();
                }
            }

            @Override
            public LogOutput append(LogOutput logOutput) {
                return logOutput.append("Merged Notification ")
                    .append(System.identityHashCode(MergedListener.this)).append(" ")
                    .append(listenerDescription);
            }

            @Override
            public boolean canExecute(final long step) {
                return MergedListener.this.canExecute(step);
            }
        });
    }

    protected void notifyOnError(Exception updateException) {
        notifyOnError(updateException, MergedListener.this.result);
    }

    protected boolean systemicResult() {
        return SystemicObjectTracker.isSystemic(MergedListener.this.result);
    }

    protected void notifyOnError(Exception updateException, QueryTable downstream) {
        Assert.neqNull(downstream, "downstream");
        downstream.notifyListenersOnError(updateException, entry);
    }

    protected abstract void process();

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("MergedListener(").append(System.identityHashCode(this))
            .append(")");
    }

    protected boolean canExecute(final long step) {
        return Stream.concat(recorders.stream(), dependencies.stream())
            .allMatch((final NotificationQueue.Dependency dep) -> dep.satisfied(step));
    }

    @Override
    public boolean satisfied(final long step) {
        if (lastCompletedStep == step) {
            LiveTableMonitor.DEFAULT.logDependencies()
                .append("MergedListener has previously been completed ").append(this).endl();
            return true;
        }
        if (queuedNotificationClock == step) {
            LiveTableMonitor.DEFAULT.logDependencies()
                .append("MergedListener has queued notification ").append(this).endl();
            return false;
        }
        if (canExecute(step)) {
            LiveTableMonitor.DEFAULT.logDependencies()
                .append("MergedListener has dependencies satisfied ").append(this).endl();
            // mark this node as completed, because both our parents have been satisfied; but we are
            // not enqueued; so we can never actually execute
            lastCompletedStep = step;
            return true;
        }
        return false;
    }
}
