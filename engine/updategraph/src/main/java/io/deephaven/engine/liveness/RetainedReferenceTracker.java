//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.base.cache.RetentionCache;
import io.deephaven.base.reference.WeakCleanupReference;
import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.Utils;
import io.deephaven.util.datastructures.hash.KeyIdentityKeyedObjectKey;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Stream;

/**
 * <p>
 * {@link WeakCleanupReference} to a {@link LivenessManager} that tracks the manager's retained
 * {@link LivenessReferent}s, in order to guarantee that they will each have their references dropped exactly once via
 * an idempotent cleanup process.
 * <p>
 * This cleanup process is initiated one of two ways:
 * <ol>
 * <li>The manager invokes it directly via {@link #enqueueReferencesForDrop()} because it is releasing all of its
 * retained references.</li>
 * <li>A {@link io.deephaven.util.reference.CleanupReferenceProcessor} or similar code invokes {@link #cleanup()} after
 * the manager is garbage-collected.</li>
 * </ol>
 */
final class RetainedReferenceTracker<TYPE extends LivenessManager> extends WeakCleanupReference<TYPE> {

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<RetainedReferenceTracker> OUTSTANDING_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RetainedReferenceTracker.class, "outstandingState");
    private static final int NOT_OUTSTANDING = 0;
    private static final int OUTSTANDING = 1;

    private static final AtomicInteger outstandingCount = new AtomicInteger(0);

    private static final ThreadLocal<PendingDropsTracker> tlPendingDropsTracker = new ThreadLocal<>();
    private static final ThreadLocal<SoftReference<PendingDropsTracker>> tlSavedTrackerReference = new ThreadLocal<>();

    private static final Logger log = LoggerFactory.getLogger(RetainedReferenceTracker.class);

    private final Impl impl;

    @SuppressWarnings("FieldMayBeFinal") // We are using an AtomicIntegerFieldUpdater (via reflection) to change this
    private volatile int outstandingState = OUTSTANDING;

    /**
     * Construct a RetainedReferenceTracker.
     *
     * @param manager The {@link LivenessManager} that's using this to track its referents
     * @param enforceStrongReachability Whether this tracker should maintain strong references to the added referents
     */
    RetainedReferenceTracker(@NotNull final TYPE manager, final boolean enforceStrongReachability) {
        super(manager, CleanupReferenceProcessorInstance.LIVENESS.getReferenceQueue());
        impl = enforceStrongReachability ? new StrongImpl() : new WeakImpl();
        outstandingCount.getAndIncrement();
        if (Liveness.DEBUG_MODE_ENABLED) {
            log.info()
                    .append("Creating ").append(Utils.REFERENT_FORMATTER, this)
                    .append(" at ").append(new LivenessDebugException())
                    .endl();
        }
    }

    @Override
    public String toString() {
        return Utils.makeReferentDescription(this);
    }

    /**
     * Add a {@link LivenessReferent} to drop a reference to on {@link #cleanup()} or
     * {@link #enqueueReferencesForDrop()}. This is not permitted if {@link #cleanup()} or
     * {@link #enqueueReferencesForDrop()} has already been invoked.
     *
     * @param referent The referent to drop on cleanup
     * @throws LivenessStateException If {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been
     *         invoked
     */
    synchronized void addReference(@NotNull final LivenessReferent referent) throws LivenessStateException {
        checkOutstanding();
        impl.add(referent);
    }

    /**
     * <p>
     * Remove at most one existing reference to referent from this tracker, so that it will no longer be dropped on
     * {@link #cleanup()} or {@link #enqueueReferencesForDrop()}, and drop it immediately.
     * <p>
     * This is not permitted if {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been invoked.
     *
     * @param referent The referent to remove
     * @throws LivenessStateException If {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been
     *         invoked
     */
    synchronized void dropReference(@NotNull final LivenessReferent referent) throws LivenessStateException {
        checkOutstanding();
        impl.drop(referent);
    }

    /**
     * <p>
     * Remove at most one existing reference to each input referent from this tracker, so that it will no longer be
     * dropped on {@link #cleanup()} or {@link #enqueueReferencesForDrop()}, and drop it immediately.
     * <p>
     * This is not permitted if {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been invoked.
     *
     * @param referents The referents to remove
     * @throws LivenessStateException If {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been
     *         invoked
     */
    synchronized void dropReferences(@NotNull final Stream<? extends LivenessReferent> referents)
            throws LivenessStateException {
        checkOutstanding();
        impl.drop(referents);
    }

    /**
     * <p>
     * Move all {@link LivenessReferent}s previously added to this tracker to other, which becomes responsible for
     * dropping them.
     * <p>
     * This is not permitted if {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been invoked.
     *
     * @param other The other tracker
     * @throws LivenessStateException If {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been
     *         invoked
     */
    synchronized void transferReferencesTo(@NotNull final RetainedReferenceTracker<?> other) {
        checkOutstanding();
        impl.transferReferencesTo(other);
    }

    /**
     * <p>
     * Remove all {@link LivenessReferent}s previously added to this tracker, unless they have been transferred, without
     * dropping them. Uses to make references "permanent".
     * <p>
     * This is not permitted if {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been invoked.
     *
     * @throws LivenessStateException If {@link #cleanup()} or {@link #enqueueReferencesForDrop()} has already been
     *         invoked
     */
    synchronized void makeReferencesPermanent() {
        checkOutstanding();
        impl.makePermanent();
    }

    private void checkOutstanding() {
        if (outstandingState == NOT_OUTSTANDING) {
            throw new LivenessStateException(
                    "RetainedReferenceTracker " + this + " has already performed cleanup for manager " + get());
        }
    }

    /**
     * Ensure that references are dropped even if this RetainedReferenceTracker's manager is garbage-collected. As a
     * last resort, this will be invoked by the {@link CleanupReferenceProcessorInstance#LIVENESS liveness cleanup
     * reference processor}, but it may also be invoked by any other RetainedReferenceTracker that observes that this
     * RetainedReferenceTracker no longer refers to its manager. This method is idempotent.
     */
    @Override
    public void cleanup() {
        // noinspection EmptyTryBlock
        try (final SafeCloseable ignored = ensureReferencesDroppedInternal(true)) {
        }
    }

    /**
     * <p>
     * Initiate the idempotent cleanup process. This will enqueue all retained references to be dropped if their
     * referents still exist. No new references may be added to or dropped from this tracker.
     * 
     * @apiNote This should be invoked in proactive cleanup scenarios before any destructive cleanup operations are
     *          undertaken, in order to allow the system to record the state of the retention graph and propagate
     *          proactive cleanup as far as possible.
     *
     * @return A {@link SafeCloseable} that will process the queued drops if necessary. Must be called exactly once,
     *         after any other desired cleanup has been performed.
     */
    SafeCloseable enqueueReferencesForDrop() {
        return ensureReferencesDroppedInternal(false);
    }

    private SafeCloseable ensureReferencesDroppedInternal(final boolean onCleanup) {
        if (!OUTSTANDING_STATE_UPDATER.compareAndSet(this, OUTSTANDING, NOT_OUTSTANDING)) {
            return () -> {
            };
        }
        if (Liveness.DEBUG_MODE_ENABLED || (onCleanup && Liveness.CLEANUP_LOG_ENABLED)) {
            Liveness.log.info().append("LivenessDebug: Ensuring references dropped ")
                    .append(onCleanup ? "(on cleanup) " : "").append("for ").append(Utils.REFERENT_FORMATTER, this)
                    .endl();
        }
        outstandingCount.decrementAndGet();

        PendingDropsTracker pendingDropsTracker = tlPendingDropsTracker.get();
        final boolean processDrops = pendingDropsTracker == null;
        if (processDrops) {
            final SoftReference<PendingDropsTracker> savedTrackerReference = tlSavedTrackerReference.get();
            if (savedTrackerReference == null || (pendingDropsTracker = savedTrackerReference.get()) == null) {
                tlSavedTrackerReference.set(new SoftReference<>(pendingDropsTracker = new PendingDropsTracker()));
            }
            tlPendingDropsTracker.set(pendingDropsTracker);
        }

        synchronized (this) {
            impl.enqueueReferencesForDrop(pendingDropsTracker, onCleanup);
        }

        if (processDrops) {
            final PendingDropsTracker finalPendingDropsTracker = pendingDropsTracker;
            return () -> {
                try {
                    finalPendingDropsTracker.dropAll();
                } finally {
                    tlPendingDropsTracker.remove();
                }
            };
        }
        return () -> {
        };
    }

    /**
     * <p>
     * Get the number of outstanding trackers (instances of RetainedReferenceTracker that have not had their
     * {@link #cleanup()} or {@link #enqueueReferencesForDrop()} method called).
     * <p>
     * Note that this number represents the liveness system's current knowledge of the number of live references in the
     * system.
     *
     * @return The number of outstanding trackers
     */
    static int getOutstandingCount() {
        return outstandingCount.get();
    }

    private interface Impl {

        void add(@NotNull final LivenessReferent referent);

        void drop(@NotNull final LivenessReferent referent);

        void drop(@NotNull final Stream<? extends LivenessReferent> referents);

        void enqueueReferencesForDrop(@NotNull PendingDropsTracker tracker, boolean onCleanup);

        void transferReferencesTo(@NotNull RetainedReferenceTracker<?> other);

        void makePermanent();
    }

    private static final class WeakImpl implements Impl {

        private final List<WeakReference<? extends LivenessReferent>> retainedReferences = new ArrayList<>();

        @Override
        public void add(@NotNull final LivenessReferent referent) {
            retainedReferences.add(referent.getWeakReference());
        }

        @Override
        public void drop(@NotNull final LivenessReferent referent) {
            for (int rrLast = retainedReferences.size() - 1, rri = 0; rri <= rrLast;) {
                final WeakReference<? extends LivenessReferent> retainedReference = retainedReferences.get(rri);
                final boolean cleared;
                final boolean found;
                {
                    final LivenessReferent retained = retainedReference.get();
                    cleared = retained == null;
                    found = !cleared && retained == referent;
                }
                if (!cleared && !found) {
                    ++rri;
                    continue;
                }
                if (rri != rrLast) {
                    retainedReferences.set(rri, retainedReferences.get(rrLast));
                }
                retainedReferences.remove(rrLast--);
                if (cleared && retainedReference instanceof RetainedReferenceTracker) {
                    ((RetainedReferenceTracker<?>) retainedReference).cleanup();
                }
                if (found) {
                    referent.dropReference();
                    return;
                }
            }
        }

        @Override
        public void drop(@NotNull final Stream<? extends LivenessReferent> referents) {
            final KeyedObjectHashMap<LivenessReferent, DropRequestState> referentsToRemove =
                    new KeyedObjectHashMap<>(DropRequestState.KEYED_OBJECT_KEY);
            referents.forEach(
                    referent -> referentsToRemove.putIfAbsent(referent, DropRequestState::new).incrementDrops());
            if (referentsToRemove.isEmpty()) {
                return;
            }
            for (int rrLast = retainedReferences.size() - 1, rri = 0; rri <= rrLast;) {
                final WeakReference<? extends LivenessReferent> retainedReference = retainedReferences.get(rri);
                final boolean cleared;
                final DropRequestState foundState;
                {
                    final LivenessReferent retained = retainedReference.get();
                    cleared = retained == null;
                    foundState = cleared ? null : referentsToRemove.get(retained);
                }
                if (!cleared && foundState == null) {
                    ++rri;
                    continue;
                }
                if (rri != rrLast) {
                    retainedReferences.set(rri, retainedReferences.get(rrLast));
                }
                retainedReferences.remove(rrLast--);
                if (cleared && retainedReference instanceof RetainedReferenceTracker) {
                    ((RetainedReferenceTracker<?>) retainedReference).cleanup();
                }
                if (foundState != null && foundState.doDrop()) {
                    referentsToRemove.remove(foundState.referent);
                    if (referentsToRemove.isEmpty()) {
                        return;
                    }
                }
            }
        }

        @Override
        public void enqueueReferencesForDrop(@NotNull final PendingDropsTracker tracker, final boolean onCleanup) {
            if (onCleanup) {
                retainedReferences.forEach(tracker::addOnCleanup);
            } else {
                retainedReferences.forEach(tracker::addOnEnsureDropped);
            }
            retainedReferences.clear();
        }

        @Override
        public void transferReferencesTo(@NotNull final RetainedReferenceTracker<?> other) {
            retainedReferences.forEach((final WeakReference<? extends LivenessReferent> retainedReference) -> {
                final LivenessReferent retained = retainedReference.get();
                if (retained != null) {
                    other.addReference(retained);
                } else if (retainedReference instanceof RetainedReferenceTracker) {
                    ((RetainedReferenceTracker<?>) retainedReference).cleanup();
                }
            });
            retainedReferences.clear();
        }

        @Override
        public void makePermanent() {
            retainedReferences.clear();
        }
    }

    private static final class StrongImpl implements Impl {

        private static final RetentionCache<LivenessReferent> permanentReferences = new RetentionCache<>();

        private final List<LivenessReferent> retained = new ArrayList<>();

        @Override
        public void add(@NotNull final LivenessReferent referent) {
            retained.add(referent);
        }

        @Override
        public void drop(@NotNull final LivenessReferent referent) {
            final int rLast = retained.size() - 1;
            for (int ri = 0; ri <= rLast; ++ri) {
                final LivenessReferent current = retained.get(ri);
                if (current == referent) {
                    if (ri != rLast) {
                        retained.set(ri, retained.get(rLast));
                    }
                    retained.remove(rLast);
                    current.dropReference();
                    return;
                }
            }
        }

        @Override
        public void drop(@NotNull final Stream<? extends LivenessReferent> referents) {
            final KeyedObjectHashMap<LivenessReferent, DropRequestState> referentsToRemove =
                    new KeyedObjectHashMap<>(DropRequestState.KEYED_OBJECT_KEY);
            referents.forEach(
                    referent -> referentsToRemove.putIfAbsent(referent, DropRequestState::new).incrementDrops());
            if (referentsToRemove.isEmpty()) {
                return;
            }
            for (int rLast = retained.size() - 1, ri = 0; ri <= rLast;) {
                final LivenessReferent current = retained.get(ri);
                final DropRequestState foundState = referentsToRemove.get(current);
                if (foundState != null) {
                    if (ri != rLast) {
                        retained.set(ri, retained.get(rLast));
                    }
                    retained.remove(rLast--);
                    if (foundState.doDrop()) {
                        referentsToRemove.remove(current);
                        if (referentsToRemove.isEmpty()) {
                            return;
                        }
                    }
                } else {
                    ++ri;
                }
            }
        }

        @Override
        public void enqueueReferencesForDrop(@NotNull final PendingDropsTracker tracker, final boolean onCleanup) {
            if (onCleanup) {
                retained.forEach(tracker::addOnCleanup);
            } else {
                retained.forEach(tracker::addOnEnsureDropped);
            }
            retained.clear();
        }

        @Override
        public void transferReferencesTo(@NotNull final RetainedReferenceTracker<?> other) {
            retained.forEach(other::addReference);
            retained.clear();
        }

        @Override
        public void makePermanent() {
            // See LivenessScope.transferTo: This is currently unreachable code, but implemented for completeness
            retained.forEach(permanentReferences::retain);
            retained.clear();
        }
    }

    /**
     * A state that tracks the number of times a referent should be dropped.
     */
    private static final class DropRequestState {

        private static final KeyedObjectKey<LivenessReferent, DropRequestState> KEYED_OBJECT_KEY =
                new KeyIdentityKeyedObjectKey<>() {
                    @Override
                    public LivenessReferent getKey(@NotNull final DropRequestState dropRequestState) {
                        return dropRequestState.referent;
                    }
                };

        private final LivenessReferent referent;

        private int timesToDrop;

        private DropRequestState(@NotNull final LivenessReferent referent) {
            this.referent = referent;
        }

        void incrementDrops() {
            ++timesToDrop;
        }

        boolean doDrop() {
            referent.dropReference();
            return --timesToDrop == 0;
        }
    }

    /**
     * A tracker for drops that are pending on the current thread, used to avoid deep recursion when ensuring that
     * references are dropped.
     */
    private static final class PendingDropsTracker {

        private final Queue<Object> pendingDrops = new ArrayDeque<>();

        void addOnCleanup(@NotNull final WeakReference<? extends LivenessReferent> reference) {
            /*
             * Enqueue the WeakReference, taking no position w.r.t. reachability of the referent.
             */
            pendingDrops.add(reference);
        }

        void addOnEnsureDropped(@NotNull final WeakReference<? extends LivenessReferent> reference) {
            /*
             * Preserve reachability from the time of invocation. This is intended to make sure that proactive cleanup
             * respects the state of the world at the time it was initiated.
             */
            final LivenessReferent referent = reference.get();
            pendingDrops.add(referent == null ? reference : referent);
        }

        void addOnCleanup(@NotNull final LivenessReferent referent) {
            /*
             * We enqueue the WeakReference, rather than the LivenessReferent itself. Since our manager has been
             * collected, it's inappropriate to enforce strong reachability to its references.
             */
            pendingDrops.add(referent.getWeakReference());
        }

        void addOnEnsureDropped(@NotNull final LivenessReferent referent) {
            /*
             * We enqueue the LivenessReferent itself, rather than a WeakReference. Since the manager is explicitly
             * ensuring that we drop its references, we should preserve reachability from the time of invocation.
             */
            pendingDrops.add(referent);
        }

        void dropAll() {
            Object next;
            while ((next = pendingDrops.poll()) != null) {
                // noinspection unchecked
                final WeakReference<? extends LivenessReferent> pendingDropReference = next instanceof WeakReference
                        ? (WeakReference<? extends LivenessReferent>) next
                        : null;
                final LivenessReferent pendingDrop = pendingDropReference == null
                        ? (LivenessReferent) next
                        : pendingDropReference.get();
                if (pendingDrop != null) {
                    pendingDrop.dropReference();
                } else if (pendingDropReference instanceof RetainedReferenceTracker) {
                    ((RetainedReferenceTracker<?>) pendingDropReference).cleanup();
                }
            }
        }
    }
}
