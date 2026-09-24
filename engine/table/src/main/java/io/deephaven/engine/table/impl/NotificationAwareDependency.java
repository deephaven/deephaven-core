//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.updategraph.NotificationQueue;

/**
 * A {@link NotificationQueue.Dependency} that changes its state in place, keeping no previous version, so a snapshot
 * using previous values cannot read it consistently across such a change.
 * <p>
 * An operation reads that state inside a snapshot attempt that may fail, so it {@link #subscribe(long) subscribes} only
 * when that attempt commits, and only if the state has not changed since the attempt began. That is the atomic
 * check-and-subscribe a source table offers through {@link BaseTable#addUpdateListener(TableUpdateListener, long)
 * addUpdateListener}, so an unsubscribed dependency never has a result to notify.
 * <p>
 * This deliberately does not extend {@link NotificationStepSource}, whose step means "finished dispatching" and lets
 * {@link OperationSnapshotControlEx} treat a dependency as satisfied without asking. This interface reports the
 * opposite, that a change has begun, so conflating the two would let a snapshot read state that is still being
 * rewritten. Completion is reported through {@link #satisfied(long)} as usual.
 */
public interface NotificationAwareDependency extends NotificationQueue.Dependency {

    /**
     * The step on which this dependency last began changing the state it guards, or
     * {@link NotificationStepReceiver#NULL_NOTIFICATION_STEP} if it never has.
     * <p>
     * Record the step in a {@code volatile} field <em>before</em> changing that state, never after: a reader that
     * observed the change must see the new step when it checks, or it will accept a torn read as consistent. Do not
     * record a step for a notification that leaves the guarded state unchanged.
     *
     * @return The step on which the guarded state last began changing
     */
    long lastStateChangeStep();

    /**
     * Start delivering state changes to the operation whose snapshot attempt is committing. The check and the
     * subscription are one atomic step, taken against the same guard the state changes take, so a change either reaches
     * that operation or is the reason it is refused.
     *
     * @param requiredLastStateChangeStep The {@link #lastStateChangeStep()} read when the attempt began
     * @return Whether the operation was subscribed
     * @throws RuntimeException If this dependency has failed, so no attempt over it can ever commit
     */
    boolean subscribe(long requiredLastStateChangeStep);

    /**
     * Stop delivering state changes, for an attempt rejected after this dependency accepted it.
     */
    void unsubscribe();
}
