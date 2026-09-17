//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.updategraph.NotificationQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link MergedListener} that retains its result while it processes an update or propagates an upstream error, and
 * does nothing if the result is already gone.
 * <p>
 * A result built by a lock-free snapshot attempt is released when that attempt is rejected, which can happen after
 * something has already queued a notification for it. Running that notification would refilter a table nobody holds and
 * then notify it, or fail that table and report the failure against it.
 */
abstract class GuardedMergedListener extends MergedListener {

    GuardedMergedListener(
            @NotNull final Iterable<? extends ListenerRecorder> recorders,
            @NotNull final Iterable<NotificationQueue.Dependency> dependencies,
            @NotNull final String listenerDescription,
            @NotNull final QueryTable result) {
        super(recorders, dependencies, listenerDescription, result);
    }

    @Override
    protected final void process() {
        if (!result.tryRetainReference()) {
            return;
        }
        try {
            processRetained();
        } finally {
            result.dropReference();
        }
    }

    @Override
    protected final void propagateError(
            final boolean uncaughtExceptionFromProcess,
            @NotNull final Throwable error,
            @Nullable final TableListener.Entry entry) {
        if (!result.tryRetainReference()) {
            return;
        }
        try {
            super.propagateError(uncaughtExceptionFromProcess, error, entry);
        } finally {
            result.dropReference();
        }
    }

    /**
     * Process this step's work, with the result retained for the duration.
     */
    protected abstract void processRetained();
}
