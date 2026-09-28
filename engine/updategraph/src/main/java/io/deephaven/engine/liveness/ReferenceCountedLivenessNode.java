//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
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

    /**
     * Ensure that this node's {@link RetainedReferenceTracker} is kept alive and will fire cleanup even if this node is
     * garbage-collected without an explicit call to {@link #onReferenceCountAtZero()}.
     * <p>
     * Registers a phantom reference to this node via {@link CleanupReferenceProcessorInstance#LIVENESS} whose action
     * captures the tracker. This keeps the tracker strongly reachable (via the registration set) until after this node
     * is collected, guaranteeing that the tracker's {@link RetainedReferenceTracker#cleanup()} will be invoked.
     * <p>
     * Note, each call registers a new cleanup action. If you call this method multiple times, you may have multiple
     * cleanup actions registered, and thus multiple calls to {@link RetainedReferenceTracker#cleanup()} when this node
     * is collected.
     */
    public final void ensureCleanupOnGC() {
        CleanupReferenceProcessorInstance.LIVENESS.registerPhantom(this, tracker::cleanup);
    }

    @Override
    public final boolean tryManage(@NotNull final LivenessReferent referent) {
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

    /**
     * Find a managed reference that matches the given predicate.
     * 
     * @param referentPredicate a predicate to test against our managed items
     * @return an Optional of a LivenessReferent that matches the given predicate; or empty if no such reference exists
     */
    protected Optional<? extends LivenessReferent> findAnyManagedReferent(
            Predicate<LivenessReferent> referentPredicate) {
        return tracker.findAny(referentPredicate);
    }

    /**
     * Apply consumer to each managed reference.
     */
    protected void forEachManagedReference(final Consumer<LivenessReferent> consumer) {
        tracker.forEach(consumer);
    }
}
