/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.liveness;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * {@link ReleasableLivenessManager} to manage exactly one object, passed at construction time or managed later.
 */
public class SingletonLivenessManager implements ReleasableLivenessManager {

    private static final AtomicReferenceFieldUpdater<SingletonLivenessManager, WeakReference> RETAINED_REFERENCE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SingletonLivenessManager.class, WeakReference.class,
                    "retainedReference");

    private volatile WeakReference<? extends LivenessReferent> retainedReference;

    @SuppressWarnings("WeakerAccess")
    public SingletonLivenessManager() {}

    public SingletonLivenessManager(@NotNull final LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        referent.retainReference();
        initializeRetainedReference(referent.getWeakReference());
    }

    private void initializeRetainedReference(
            @NotNull final WeakReference<? extends LivenessReferent> retainedReference) {
        this.retainedReference = retainedReference;
    }

    private boolean setRetainedReference(@NotNull final WeakReference<? extends LivenessReferent> retainedReference) {
        return RETAINED_REFERENCE_UPDATER.compareAndSet(this, null, retainedReference);
    }

    private boolean clearRetainedReference(@NotNull final WeakReference<? extends LivenessReferent> retainedReference) {
        return RETAINED_REFERENCE_UPDATER.compareAndSet(this, retainedReference, null);
    }

    private WeakReference<? extends LivenessReferent> getRetainedReference() {
        // noinspection unchecked
        return (WeakReference<? extends LivenessReferent>) RETAINED_REFERENCE_UPDATER.getAndSet(this, null);
    }

    @Override
    public final boolean tryManage(@NotNull LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return true;
        }
        if (!referent.tryRetainReference()) {
            return false;
        }
        if (!setRetainedReference(referent.getWeakReference())) {
            referent.dropReference();
            throw new UnsupportedOperationException("SingletonLivenessManager can only manage one referent");
        }
        return true;
    }

    @Override
    public final void release() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        final WeakReference<? extends LivenessReferent> localRetainedReference = getRetainedReference();
        final LivenessReferent retained;
        if (localRetainedReference != null
                && clearRetainedReference(localRetainedReference)
                && (retained = localRetainedReference.get()) != null) {
            retained.dropReference();
        }
    }
}
