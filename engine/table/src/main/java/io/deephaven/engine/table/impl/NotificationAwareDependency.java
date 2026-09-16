//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.NotificationQueue;

/**
 * A {@link NotificationQueue.Dependency} that changes its state in place, keeping no previous version, so a snapshot
 * using previous values cannot read it consistently across such a change.
 * <p>
 * This deliberately does not extend {@link NotificationStepSource}, whose step means "finished dispatching" and lets
 * {@link OperationSnapshotControlEx} treat a dependency as satisfied without asking. This interface reports the
 * opposite, that a change has begun, so conflating the two would let a snapshot read state that is still being
 * rewritten. Completion is reported through {@link #satisfied(long)} as usual.
 */
public interface NotificationAwareDependency extends NotificationQueue.Dependency {

    /**
     * Did this dependency begin changing the state it guards on {@code step}?
     * <p>
     * Record the step from a {@code volatile} field <em>before</em> changing that state, never after: a reader that
     * observed the change must see this return {@code true} when it checks, or it will accept a torn read as
     * consistent. Do not report a change for a notification that leaves the state unchanged.
     *
     * @param step The step to test
     * @return Whether the guarded state began changing on {@code step}
     */
    boolean stateChangedOnStep(long step);
}
