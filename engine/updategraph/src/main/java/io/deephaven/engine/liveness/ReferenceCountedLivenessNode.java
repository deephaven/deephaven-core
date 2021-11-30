package io.deephaven.engine.liveness;


import io.deephaven.util.Utils;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.referencecounting.ReferenceCounted;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;

/**
 * {@link LivenessNode} implementation that relies on reference counting to determine its liveness.
 */
public abstract class ReferenceCountedLivenessNode extends ReferenceCounted implements LivenessNode {

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
    public final boolean tryRetainReference() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        return tryIncrementReferenceCount();
    }

    @Override
    public final void dropReference() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: Releasing ").append(Utils.REFERENT_FORMATTER, this).endl();
        }
        if (!tryDecrementReferenceCount()) {
            throw new LivenessStateException(
                    getReferentDescription() + " could not be released as it was no longer live");
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
        if (!referent.tryRetainReference()) {
            return false;
        }
        tracker.addReference(referent);
        return true;
    }

    /**
     * <p>
     * Attempt to release (destructively when necessary) resources held by this object. This may render the object
     * unusable for subsequent operations. Implementations should be sure to call super.destroy().
     * <p>
     * This is intended to only ever be used as a side effect of decreasing the reference count to 0.
     */
    protected void destroy() {}

    @Override
    protected final void onReferenceCountAtZero() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            throw new IllegalStateException(
                    "Reference count on " + this + " reached zero while liveness reference tracking is disabled");
        }
        try {
            destroy();
        } catch (Exception e) {
            Liveness.log.warn().append("Exception while destroying ").append(Utils.REFERENT_FORMATTER, this)
                    .append(" after reference count reached zero: ").append(e).endl();
        }
        tracker.ensureReferencesDropped();
    }
}
