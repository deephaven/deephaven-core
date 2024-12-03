//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.util.Utils;
import io.deephaven.util.referencecounting.ReferenceCounted;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.lang.ref.WeakReference;

/**
 * {@link LivenessReferent} implementation that relies on reference counting to determine its liveness.
 */
public class ReferenceCountedLivenessReferent extends ReferenceCounted implements LivenessReferent {

    public final boolean tryRetainReference() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        return tryIncrementReferenceCount();
    }

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
        return new WeakReference<>(this);
    }

    /**
     * Attempt to release (destructively when necessary) resources held by this object. This may render the object
     * unusable for subsequent operations. Implementations should be sure to call super.destroy().
     * <p>
     * This is intended to only ever be used as a side effect of decreasing the reference count to 0.
     */
    @OverridingMethodsMustInvokeSuper
    protected void destroy() {}

    @Override
    protected void onReferenceCountAtZero() {
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
    }
}
