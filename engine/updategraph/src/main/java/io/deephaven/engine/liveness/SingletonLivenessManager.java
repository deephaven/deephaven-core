//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.base.reference.CleanupReference;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

/**
 * {@link ReleasableLivenessManager} to manage exactly one object, passed at construction time or managed later.
 */
public class SingletonLivenessManager implements ReleasableLivenessManager {

    @SuppressWarnings("rawtypes")
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

    private WeakReference<? extends LivenessReferent> getAndClearRetainedReference() {
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

    /**
     * @inheritDoc
     * @implNote This is equivalent to {@link #release()} if {@code referent} is the one managed by this manager.
     */
    @Override
    public boolean tryUnmanage(@NotNull LivenessReferent referent) {
        final WeakReference<? extends LivenessReferent> localRetainedReference;
        if (!Liveness.REFERENCE_TRACKING_DISABLED
                && (localRetainedReference = retainedReference) != null
                && localRetainedReference.get() == referent) {
            release();
        }
        return true;
    }

    /**
     * @inheritDoc
     * @implNote This is equivalent to {@link #release()} if any element of {@code referents} is the one managed by this
     *           manager.
     */
    @Override
    public boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents) {
        referents.forEach(this::tryUnmanage);
        return true;
    }

    @Override
    public final void release() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        final WeakReference<? extends LivenessReferent> localRetainedReference = getAndClearRetainedReference();
        if (localRetainedReference == null) {
            return;
        }
        final LivenessReferent retained;
        if ((retained = localRetainedReference.get()) != null) {
            retained.dropReference();
        } else if (localRetainedReference instanceof CleanupReference) {
            ((CleanupReference<?>) localRetainedReference).cleanup();
        }
    }
}
