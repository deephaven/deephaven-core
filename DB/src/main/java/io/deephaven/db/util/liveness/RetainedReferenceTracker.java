package io.deephaven.db.util.liveness;

import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.base.reference.WeakCleanupReference;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.util.Utils;
import io.deephaven.util.datastructures.hash.IdentityKeyedObjectKey;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * <p>{@link WeakCleanupReference} to a {@link LivenessManager} that tracks the manager's retained
 * {@link LivenessReferent}s, in order to guarantee that they will each have their references dropped
 * exactly once via an idempotent cleanup process.
 * <p>This cleanup process is initiated one of two ways:
 * <ol>
 * <li>The manager invokes it directly via {@link #ensureReferencesDropped()} because it is releasing all of its
 * retained references.</li>
 * <li>A {@link io.deephaven.util.reference.CleanupReferenceProcessor} or similar code invokes {@link #cleanup()} after
 * the manager is garbage-collected.</li>
 * </ol>
 *
 * @IncludeAll
 */
final class RetainedReferenceTracker<TYPE extends LivenessManager> extends WeakCleanupReference<TYPE> {

    private static final AtomicIntegerFieldUpdater<RetainedReferenceTracker> OUTSTANDING_STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(RetainedReferenceTracker.class, "outstandingState");
    private static final int NOT_OUTSTANDING = 0;
    private static final int OUTSTANDING = 1;

    private static final AtomicInteger outstandingCount = new AtomicInteger(0);

    private static final ThreadLocal<Queue<WeakReference<? extends LivenessReferent>>> tlPendingDropReferences = new ThreadLocal<>();
    private static final ThreadLocal<SoftReference<Queue<WeakReference<? extends LivenessReferent>>>> tlSavedQueueReference = new ThreadLocal<>();

    private final List<WeakReference<? extends LivenessReferent>> retainedReferences = new ArrayList<>();

    @SuppressWarnings("FieldMayBeFinal") // We are using an AtomicIntegerFieldUpdater (via reflection) to change this
    private volatile int outstandingState = OUTSTANDING;

    RetainedReferenceTracker(@NotNull final TYPE manager) {
        super(manager, CleanupReferenceProcessorInstance.LIVENESS.getReferenceQueue());
        outstandingCount.getAndIncrement();
        if (Liveness.DEBUG_MODE_ENABLED) {
            ProcessEnvironment.getDefaultLog().info().append("Creating ").append(Utils.REFERENT_FORMATTER, this).append(" at ").append(new LivenessDebugException()).endl();
        }
    }

    @Override
    public final String toString() {
        return Utils.makeReferentDescription(this);
    }

    /**
     * Add a {@link LivenessReferent} to drop a reference to on {@link #cleanup()} or {@link #ensureReferencesDropped()}.
     * This is not permitted if {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been invoked.
     *
     * @param referent The referent to drop on cleanup
     * @throws LivenessStateException If {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been
     * invoked
     */
    synchronized final void addReference(@NotNull final LivenessReferent referent) throws LivenessStateException {
        checkOutstanding();
        retainedReferences.add(referent.getWeakReference());
    }

    /**
     * <p>Remove at most one existing reference to referent from this tracker, so that it will no longer be dropped on
     * {@link #cleanup()} or {@link #ensureReferencesDropped()}, and drop it immediately.
     * <p>This is not permitted if {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been invoked.
     *
     * @param referent The referent to remove
     * @throws LivenessStateException If {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been
     * invoked
     */
    synchronized final void dropReference(@NotNull final LivenessReferent referent) throws LivenessStateException {
        checkOutstanding();
        for (int rrLast = retainedReferences.size() - 1, rri = 0; rri <= rrLast; ++rri) {
            final WeakReference<? extends LivenessReferent> retainedReference = retainedReferences.get(rri);
            final boolean found = retainedReference.get() == referent;
            final boolean cleared = !found && retainedReference.get() == null;
            if (!found && !cleared) {
                continue;
            }
            if (rri != rrLast) {
                retainedReferences.set(rri, retainedReferences.get(rrLast));
            }
            retainedReferences.remove(rrLast--);
            if (found) {
                referent.dropReference();
                return;
            }
        }
    }

    /**
     * <p>Remove at most one existing reference to each input referent from this tracker, so that it will no longer be
     * dropped on {@link #cleanup()} or {@link #ensureReferencesDropped()}, and drop it immediately.
     * <p>This is not permitted if {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been invoked.
     *
     * @param referents The referents to remove
     * @throws LivenessStateException If {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been
     * invoked
     */
    final void dropReferences(@NotNull final Collection<? extends LivenessReferent> referents) throws LivenessStateException {
        final Set<LivenessReferent> referentsToRemove = new KeyedObjectHashSet<>(IdentityKeyedObjectKey.getInstance());
        referentsToRemove.addAll(referents);
        synchronized (this) {
            checkOutstanding();
            for (int rrLast = retainedReferences.size() - 1, rri = 0; rri <= rrLast; ++rri) {
                final WeakReference<? extends LivenessReferent> retainedReference = retainedReferences.get(rri);
                final boolean found = referentsToRemove.remove(retainedReference.get());
                final boolean cleared = !found && retainedReference.get() == null;
                if (!found && !cleared) {
                    continue;
                }
                if (rri != rrLast) {
                    retainedReferences.set(rri, retainedReferences.get(rrLast));
                }
                retainedReferences.remove(rrLast--);
                if (found) {
                    final LivenessReferent referent = retainedReference.get();
                    if (referent != null) { // Probably unnecessary, unless the referents collection is engaged in some reference trickery internally, but better safe than sorry.
                        referent.dropReference();
                    }
                }
            }
        }
    }

    /**
     * <p>Move all {@link LivenessReferent}s previously added to this tracker to other, which becomes responsible for
     * dropping them.
     * <p>This is not permitted if {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been invoked.
     *
     * @param other The other tracker
     * @throws LivenessStateException If {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been
     * invoked
     */
    synchronized final void transferReferencesTo(@NotNull final RetainedReferenceTracker other) {
        checkOutstanding();
        for (@NotNull final WeakReference<? extends LivenessReferent> retainedReference : retainedReferences) {
            final LivenessReferent retained = retainedReference.get();
            if (retained != null) {
                other.addReference(retained);
            }
        }
        retainedReferences.clear();
    }

    /**
     * <p>Remove all {@link LivenessReferent}s previously added to this tracker, unless they have been transferred,
     * without dropping them. Uses to make references "permanent".
     * <p>This is not permitted if {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been invoked.
     *
     * @throws LivenessStateException If {@link #cleanup()} or {@link #ensureReferencesDropped()} has already been
     * invoked
     */
    synchronized void makeReferencesPermanent() {
        checkOutstanding();
        retainedReferences.clear();
    }

    private void checkOutstanding() {
        if (outstandingState == NOT_OUTSTANDING) {
            throw new LivenessStateException("RetainedReferenceTracker " + this + " has already performed cleanup for manager " + get());
        }
    }

    @Override
    public final void cleanup() {
        ensureReferencesDroppedInternal(true);
    }

    /**
     * <p>Initiate the idempotent cleanup process. This will drop all retained references if their referents still
     * exist. No new references may be added to or dropped from this tracker.
     */
    final void ensureReferencesDropped() {
        ensureReferencesDroppedInternal(false);
    }

    private void ensureReferencesDroppedInternal(final boolean onCleanup) {
        if (!OUTSTANDING_STATE_UPDATER.compareAndSet(this, OUTSTANDING, NOT_OUTSTANDING)) {
            return;
        }
        if (Liveness.DEBUG_MODE_ENABLED || (onCleanup && Liveness.CLEANUP_LOG_ENABLED)) {
            Liveness.log.info().append("LivenessDebug: Ensuring references dropped ").append(onCleanup ? "(on cleanup) " : "").append("for ").append(Utils.REFERENT_FORMATTER, this).endl();
        }
        outstandingCount.decrementAndGet();

        Queue<WeakReference<? extends LivenessReferent>> pendingDropReferences = tlPendingDropReferences.get();
        final boolean processDrops = pendingDropReferences == null;
        if (processDrops) {
            final SoftReference<Queue<WeakReference<? extends LivenessReferent>>> savedQueueReference = tlSavedQueueReference.get();
            if (savedQueueReference == null || (pendingDropReferences = savedQueueReference.get()) == null) {
                tlSavedQueueReference.set(new SoftReference<>(pendingDropReferences = new ArrayDeque<>()));
            }
            tlPendingDropReferences.set(pendingDropReferences);
        }

        synchronized (this) {
            for (@NotNull final WeakReference<? extends LivenessReferent> retainedReference : retainedReferences) {
                final LivenessReferent retained = retainedReference.get();
                if (retained != null) {
                    pendingDropReferences.add(retainedReference);
                }
            }
            retainedReferences.clear();
        }

        if (processDrops) {
            try {
                WeakReference<? extends LivenessReferent> pendingDropReference;
                while ((pendingDropReference = pendingDropReferences.poll()) != null) {
                    final LivenessReferent pendingDrop = pendingDropReference.get();
                    if (pendingDrop != null) {
                        pendingDrop.dropReference();
                    }
                }
            } finally {
                tlPendingDropReferences.set(null);
            }
        }
    }

    /**
     * <p>Get the number of outstanding trackers (instances of RetainedReferenceTracker that have not had their
     * {@link #cleanup()} or {@link #ensureReferencesDropped()} method called).
     * <p>Note that this number represents the liveness system's current knowledge of the number of live references in
     * the system.
     *
     * @return The number of outstanding trackers
     */
    static int getOutstandingCount() {
        return outstandingCount.get();
    }
}
