//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.stream.Stream;

/**
 * Simple {@link LivenessManager} implementation.
 */
public class StandaloneLivenessManager implements LivenessManager, LogOutputAppendable {

    final boolean enforceStrongReachability;

    transient RetainedReferenceTracker<StandaloneLivenessManager> tracker;

    /**
     * @param enforceStrongReachability Whether this {@link LivenessManager} should maintain strong references to its
     *        referents
     */
    @SuppressWarnings("WeakerAccess") // Needed in order to deserialize Serializable subclass instances
    public StandaloneLivenessManager(final boolean enforceStrongReachability) {
        this.enforceStrongReachability = enforceStrongReachability;
        initializeTransientFieldsForLiveness();
    }

    /**
     * Package-private for {@link java.io.Serializable} sub-classes to use in <code>readObject</code> <em>only</em>.
     * Public to allow unit tests in another package to work around mock issues where the constructor is never invoked.
     */
    @VisibleForTesting
    public final void initializeTransientFieldsForLiveness() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        tracker = new RetainedReferenceTracker<>(this, enforceStrongReachability);
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: Created tracker ").append(Utils.REFERENT_FORMATTER, tracker)
                    .append(" for ").append(Utils.REFERENT_FORMATTER, this).endl();
        }
    }

    @Override
    public final boolean tryManage(@NotNull final LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: ").append(this).append(" managing ")
                    .append(referent.getReferentDescription()).endl();
        }
        if (!referent.tryRetainReference()) {
            return false;
        }
        tracker.addReference(referent);
        return true;
    }

    @Override
    public final boolean tryUnmanage(@NotNull final LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        tracker.dropReference(referent);
        return true;
    }

    @Override
    public final boolean tryUnmanage(@NotNull final Stream<? extends LivenessReferent> referents) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        tracker.dropReferences(referents);
        return true;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput
                .append("StandaloneLivenessManager{enforceStrongReachability=")
                .append(enforceStrongReachability)
                .append("}");
    }
}
