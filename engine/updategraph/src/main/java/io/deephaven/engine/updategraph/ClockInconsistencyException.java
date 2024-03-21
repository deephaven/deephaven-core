//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * Runtime exception thrown by update processing code that observes evidence of clock inconsistencies. For example,
 * {@link NotificationQueue.Dependency#satisfied(long) dependency satisfaction} may throw an instance of this class if
 * the requested clock step is lower than the last satisfied clock step. In practice, this may identify bugs or improper
 * update graph mixing in update processing, or allow concurrent snapshots to fail fast if they've already missed a
 * clock change.
 */
public class ClockInconsistencyException extends UncheckedDeephavenException {

    public ClockInconsistencyException(@NotNull final String message) {
        super(message);
    }

    public ClockInconsistencyException(@NotNull final String message, @NotNull final Throwable cause) {
        super(message, cause);
    }
}
