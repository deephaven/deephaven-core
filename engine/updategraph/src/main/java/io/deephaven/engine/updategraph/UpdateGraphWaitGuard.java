//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.util.SafeCloseable;

/**
 * Forbids the current thread from waiting for any {@link UpdateGraph}, either to acquire one of its locks or for its
 * update cycle to satisfy a dependency.
 * <p>
 * A thread that holds a monitor other threads may wait for, while those threads hold an update graph lock or process
 * its updates, must not itself wait for the update graph. Such a thread may forbid waits, attempt its work, and if the
 * work would wait, abandon it with an {@link UpdateGraphWaitForbiddenException}, release the monitor and retry the work
 * while holding the update graph lock. A lock that is available without waiting may still be acquired, and a timed
 * attempt to acquire one does not wait for its timeout.
 */
public final class UpdateGraphWaitGuard {

    private static final ThreadLocal<Boolean> waitsForbidden = ThreadLocal.withInitial(() -> false);

    private UpdateGraphWaitGuard() {}

    /**
     * Forbid the current thread from waiting for any update graph until the result is closed.
     *
     * @return A {@link SafeCloseable} that restores whether the current thread was forbidden to wait
     */
    public static SafeCloseable forbidWaits() {
        final boolean previouslyForbidden = waitsForbidden.get();
        waitsForbidden.set(true);
        return () -> waitsForbidden.set(previouslyForbidden);
    }

    /**
     * @return Whether the current thread is forbidden from waiting for any update graph
     */
    public static boolean waitsForbidden() {
        return waitsForbidden.get();
    }
}
