//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.stream.Stream;

/**
 * {@link LivenessNode} implementation that relies on reference counting to determine its liveness.
 */
public abstract class ReferenceCountedLivenessNode extends ReferenceCountedLivenessReferent implements LivenessNode {

    final boolean enforceStrongReachability;

    transient RetainedReferenceTracker<ReferenceCountedLivenessNode> tracker;

    /**
     * @param enforceStrongReachability Whether this {@link LivenessManager} should maintain strong references to its
     *        referents
     */
    @SuppressWarnings("WeakerAccess") // Needed in order to deserialize Serializable subclass instances
    protected ReferenceCountedLivenessNode(final boolean enforceStrongReachability) {
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
    public WeakReference<? extends LivenessReferent> getWeakReference() {
        return tracker;
    }

    @Override
    public final boolean tryManage(@NotNull final LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: ").append(getReferentDescription()).append(" managing ")
                    .append(referent.getReferentDescription()).endl();
        }
        if (!tryRetainReference()) {
            return false;
        }
        try {
            if (!referent.tryRetainReference()) {
                return false;
            }
            tracker.addReference(referent);
        } finally {
            dropReference();
        }
        return true;
    }

    @Override
    public final boolean tryUnmanage(@NotNull final LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        if (!tryRetainReference()) {
            return false;
        }
        try {
            tracker.dropReference(referent);
        } finally {
            dropReference();
        }
        return true;
    }

    @Override
    public final boolean tryUnmanage(@NotNull final Stream<? extends LivenessReferent> referents) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        if (!tryRetainReference()) {
            return false;
        }
        try {
            tracker.dropReferences(referents);
        } finally {
            dropReference();
        }
        return true;
    }

    @Override
    public final void onReferenceCountAtZero() {
        try (final SafeCloseable ignored = tracker.enqueueReferencesForDrop()) {
            super.onReferenceCountAtZero();
        }
    }
}
