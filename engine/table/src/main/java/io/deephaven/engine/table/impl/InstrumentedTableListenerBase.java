/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.table.impl.util.AsyncErrorLogger;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.util.Utils;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class InstrumentedTableListenerBase extends LivenessArtifact
        implements TableListener, NotificationQueue.Dependency {

    private static final AtomicLongFieldUpdater<InstrumentedTableListenerBase> LAST_COMPLETED_STEP_UPDATER =
            AtomicLongFieldUpdater.newUpdater(InstrumentedTableListenerBase.class, "lastCompletedStep");
    private static final AtomicLongFieldUpdater<InstrumentedTableListenerBase> LAST_ENQUEUED_STEP_UPDATER =
            AtomicLongFieldUpdater.newUpdater(InstrumentedTableListenerBase.class, "lastEnqueuedStep");

    private static final Logger log = LoggerFactory.getLogger(ShiftObliviousInstrumentedListener.class);

    private final PerformanceEntry entry;
    private final boolean terminalListener;

    private boolean failed = false;
    private static volatile boolean verboseLogging = Configuration
            .getInstance()
            .getBooleanWithDefault("ShiftObliviousInstrumentedListener.verboseLogging", false);

    private volatile long lastCompletedStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;
    private volatile long lastEnqueuedStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

    InstrumentedTableListenerBase(@Nullable String description, boolean terminalListener) {
        this.entry = UpdatePerformanceTracker.getInstance().getEntry(description);
        this.terminalListener = terminalListener;
    }

    @Override
    public String toString() {
        return Utils.getSimpleNameFor(this) + '-' + entry.getDescription();
    }

    public static boolean setVerboseLogging(boolean enableVerboseLogging) {
        boolean original = InstrumentedTableListenerBase.verboseLogging;
        InstrumentedTableListenerBase.verboseLogging = enableVerboseLogging;
        return original;
    }

    public PerformanceEntry getEntry() {
        return entry;
    }

    @Override
    public NotificationQueue.ErrorNotification getErrorNotification(Throwable originalException, Entry sourceEntry) {
        return new ErrorNotification(originalException, sourceEntry == null ? entry : sourceEntry);
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("ShiftObliviousInstrumentedListener:(identity=").append(System.identityHashCode(this))
                .append(", ")
                .append(entry).append(")");
    }

    public boolean canExecute(final long step) {
        return UpdateGraphProcessor.DEFAULT.satisfied(step);
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
        if (lastEnqueuedStep == step) {
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
        if (lastEnqueuedStep == step) {
            UpdateGraphProcessor.DEFAULT.logDependencies()
                    .append("Enqueued notification during dependency check for ").append(this)
                    .append(", step=").append(step)
                    .endl();
            return false;
        }

        UpdateGraphProcessor.DEFAULT.logDependencies()
                .append("Dependencies satisfied for ").append(this)
                .append(", lastCompleted=").append(lastCompletedStep)
                .append(", lastQueued=").append(lastEnqueuedStep)
                .append(", step=").append(step)
                .endl();

        // Mark this node as completed. All our dependencies have been satisfied, but we are not enqueued, so we can
        // never actually execute.
        final long oldLastCompletedStep = LAST_COMPLETED_STEP_UPDATER.getAndSet(this, step);
        Assert.lt(oldLastCompletedStep, "oldLastCompletedStep", step, "step");
        return true;
    }

    @Override
    public void onFailure(Throwable originalException, Entry sourceEntry) {
        onFailureInternal(originalException, sourceEntry == null ? entry : sourceEntry);
    }

    protected abstract void onFailureInternal(Throwable originalException, Entry sourceEntry);

    protected final void onFailureInternalWithDependent(final BaseTable dependent, final Throwable originalException,
            final Entry sourceEntry) {
        dependent.notifyListenersOnError(originalException, sourceEntry);

        // Secondary notification to client error monitoring
        try {
            if (SystemicObjectTracker.isSystemic(dependent)) {
                AsyncClientErrorNotifier.reportError(originalException);
            }
        } catch (IOException e) {
            throw new UncheckedTableException("Exception in " + sourceEntry.toString(), originalException);
        }
    }

    public class ErrorNotification extends AbstractNotification implements NotificationQueue.ErrorNotification {

        private final Throwable originalException;
        private final Entry sourceEntry;

        ErrorNotification(Throwable originalException, Entry sourceEntry) {
            super(terminalListener);
            this.originalException = originalException;
            this.sourceEntry = sourceEntry;
        }

        @Override
        public void run() {
            if (failed) {
                return;
            }
            failed = true;
            try {
                AsyncErrorLogger.log(DateTimeUtils.currentTime(), entry, sourceEntry, originalException);
            } catch (IOException e) {
                log.error().append("Error logging failure from ").append(entry).append(": ").append(e).endl();
            }
            try {
                onFailureInternal(originalException, sourceEntry);
            } catch (Exception e) {
                log.error().append("Error propagating failure from ").append(sourceEntry).append(": ").append(e).endl();
            }
        }

        @Override
        public boolean canExecute(final long step) {
            return InstrumentedTableListenerBase.this.canExecute(step);
        }

        @Override
        public LogOutput append(LogOutput output) {
            return output.append("ErrorNotification{").append("originalException=")
                    .append(originalException.getMessage()).append(", sourceEntry=").append(sourceEntry).append("}");
        }
    }

    protected abstract class NotificationBase extends AbstractNotification implements LogOutputAppendable {

        final TableUpdate update;

        NotificationBase(final TableUpdate update) {
            super(terminalListener);
            this.update = update.acquire();

            final long currentStep = LogicalClock.DEFAULT.currentStep();
            if (lastCompletedStep == currentStep) {
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted("Enqueued after lastCompletedStep already set to current step: " + this
                        + ", step=" + currentStep + ", lastCompletedStep=" + lastCompletedStep);
            }

            final long oldLastEnqueuedStep =
                    LAST_ENQUEUED_STEP_UPDATER.getAndSet(InstrumentedTableListenerBase.this, currentStep);
            Assert.lt(oldLastEnqueuedStep, "oldLastEnqueuedStep", currentStep, "currentStep");
        }

        @Override
        public abstract void run();

        @Override
        public final String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public final LogOutput append(LogOutput logOutput) {
            return logOutput.append("Notification:(step=")
                    .append(LogicalClock.DEFAULT.currentStep())
                    .append(", listener=")
                    .append(System.identityHashCode(InstrumentedTableListenerBase.this))
                    .append(")")
                    .append(entry);
        }

        @Override
        public final boolean canExecute(final long step) {
            return InstrumentedTableListenerBase.this.canExecute(step);
        }

        void doRun(final Runnable invokeOnUpdate) {
            try {
                doRunInternal(invokeOnUpdate);
            } finally {
                update.release();
            }
        }

        private void doRunInternal(final Runnable invokeOnUpdate) {
            if (failed) {
                return;
            }

            entry.onUpdateStart(update.added(), update.removed(), update.modified(), update.shifted());

            final long currentStep = LogicalClock.DEFAULT.currentStep();
            try {
                Assert.eq(lastEnqueuedStep, "lastEnqueuedStep", currentStep, "currentStep");
                if (lastCompletedStep >= currentStep) {
                    throw new IllegalStateException(
                            "Execution began after lastCompletedStep already set to current step: " + this
                                    + ", step=" + currentStep + ", lastCompletedStep=" + lastCompletedStep);
                }

                invokeOnUpdate.run();
            } catch (Exception e) {
                final LogEntry en = log.error().append("Uncaught exception for entry= ");

                final boolean useVerboseLogging = verboseLogging;
                if (useVerboseLogging) {
                    en.append(entry);
                } else {
                    en.append(entry.getDescription());
                }

                en.append(", added.size()=").append(update.added().size())
                        .append(", modified.size()=").append(update.modified().size())
                        .append(", removed.size()=").append(update.removed().size())
                        .append(", shifted.size()=").append(update.shifted().size())
                        .append(", modifiedColumnSet=").append(update.modifiedColumnSet().toString())
                        .append(":\n").append(e).endl();

                if (useVerboseLogging) {
                    // This is a failure and shouldn't happen, so it is OK to be verbose here. Particularly as it is not
                    // clear what is actually going on in some cases of assertion failure related to the indices.
                    log.error().append("ShiftObliviousListener is: ").append(this.toString()).endl();
                    log.error().append("Added: ").append(update.added().toString()).endl();
                    log.error().append("Modified: ").append(update.modified().toString()).endl();
                    log.error().append("Removed: ").append(update.removed().toString()).endl();
                    log.error().append("Shifted: ").append(update.shifted().toString()).endl();
                }

                // If the table has an error, we should cease processing further updates.
                failed = true;
                onFailureInternal(e, entry);
            } finally {
                entry.onUpdateEnd();
                final long oldLastCompletedStep =
                        LAST_COMPLETED_STEP_UPDATER.getAndSet(InstrumentedTableListenerBase.this, currentStep);
                Assert.lt(oldLastCompletedStep, "oldLastCompletedStep", currentStep, "currentStep");
            }
        }
    }
}
