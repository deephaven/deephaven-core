//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.reference;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * {@link SimpleReference} implementation with built-in reference-counting and pooling support.
 */
public abstract class PooledObjectReference<REFERENT_TYPE> implements SimpleReference<REFERENT_TYPE> {

    /**
     * Field updater for {@code state}.
     */
    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<PooledObjectReference> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PooledObjectReference.class, "state");

    /**
     * An available reference with zero outstanding permits will have {@code state == 1}.
     */
    private static final int AVAILABLE_ZERO_PERMITS = 1;

    /**
     * A cleared reference with zero outstanding permits will have {@code state == 0}.
     */
    private static final int CLEARED_ZERO_PERMITS = 0;

    /**
     * The bit we use to denote availability.
     */
    private static final int STATE_AVAILABLE_BIT = 1;

    /**
     * The quantity to add to state when incrementing the number of outstanding permits.
     */
    private static final int STATE_PERMIT_ACQUIRE_QUANTITY = 2;

    /**
     * The quantity to add to state when decrementing the number of outstanding permits.
     */
    private static final int STATE_PERMIT_RELEASE_QUANTITY = -STATE_PERMIT_ACQUIRE_QUANTITY;

    private static boolean stateAllowsAcquire(final int currentState) {
        return currentState > CLEARED_ZERO_PERMITS;
    }

    private static boolean stateIsAvailable(final int currentState) {
        return (currentState & STATE_AVAILABLE_BIT) != 0;
    }

    private static boolean stateIsCleared(final int currentState) {
        return (currentState & STATE_AVAILABLE_BIT) == 0;
    }

    private static int calculateNewStateForClear(final int currentState) {
        return currentState ^ STATE_AVAILABLE_BIT;
    }

    private static int calculateNewStateForAcquire(final int currentState) {
        return currentState + STATE_PERMIT_ACQUIRE_QUANTITY;
    }

    /**
     * Try to atomically update {@code state}.
     *
     * @param currentState The expected value
     * @param newState The desired result value
     * @return Whether {@code state} was successfully updated
     */
    private boolean tryUpdateState(final int currentState, final int newState) {
        return STATE_UPDATER.compareAndSet(this, currentState, newState);
    }

    /**
     * Atomically decrement the number of outstanding permits and get the new value of {@code state}.
     *
     * @return The new value of {@code state}
     */
    private int decrementOutstandingPermits() {
        return STATE_UPDATER.addAndGet(this, STATE_PERMIT_RELEASE_QUANTITY);
    }

    /**
     * The actual referent. Set to null after {@code state == CLEARED_ZERO_PERMITS} by the responsible thread, which
     * returns it to the pool.
     */
    private volatile REFERENT_TYPE referent;

    /**
     * The state of this reference. The lowest bit is used to denote whether this reference is available (1) or cleared
     * (0). The higher bits represent an integer count of the number of outstanding permits.
     */
    private volatile int state = AVAILABLE_ZERO_PERMITS;

    /**
     * Construct a new PooledObjectReference to the supplied referent.
     *
     * @param referent The referent of this reference
     */
    protected PooledObjectReference(@NotNull final REFERENT_TYPE referent) {
        this.referent = Objects.requireNonNull(referent, "referent");
    }

    /**
     * Get the referent. It is an error to call this method if the caller does not have any outstanding permits. Callers
     * are encouraged to use this in the try block of a try-finally pattern:
     *
     * <pre>
     * if (!ref.acquire()) {
     *     return;
     * }
     * try {
     *     doSomethingWith(ref.get());
     * } finally {
     *     ref.release();
     * }
     * </pre>
     *
     * @return The referent if this reference has not been cleared, null otherwise (which implies an error by the
     *         caller)
     */
    @Override
    public final REFERENT_TYPE get() {
        return referent;
    }

    /**
     * Acquire an active use permit. Callers should pair this with a corresponding {@link #release()}, ideally with a
     * try-finally pattern:
     *
     * <pre>
     * if (!ref.acquire()) {
     *     return;
     * }
     * try {
     *     doSomethingWith(ref.get());
     * } finally {
     *     ref.release();
     * }
     * </pre>
     *
     * @return Whether a permit was acquired
     */
    public final boolean acquire() {
        int currentState;
        while (stateAllowsAcquire(currentState = state)) {
            final int newState = calculateNewStateForAcquire(currentState);
            if (tryUpdateState(currentState, newState)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Acquire an active use permit if this has not been {@link #clear() cleared}. This is useful in situations where
     * callers want to fail-fast and don't need to guarantee reentrancy. Callers should pair this with a corresponding
     * {@link #release()}, ideally with a try-finally pattern:
     *
     * <pre>
     * if (!ref.acquireIfAvailable()) {
     *     return;
     * }
     * try {
     *     doSomethingWith(ref.get());
     * } finally {
     *     ref.release();
     * }
     * </pre>
     *
     * @return Whether a permit was acquired
     */
    public final boolean acquireIfAvailable() {
        int currentState;
        while (stateAllowsAcquire(currentState = state) && stateIsAvailable(currentState)) {
            final int newState = calculateNewStateForAcquire(currentState);
            if (tryUpdateState(currentState, newState)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Acquire an active use permit and return the referent, if possible. Callers should pair this with a corresponding
     * {@link #release()}, ideally with a try-finally pattern:
     *
     * <pre>
     * final Object obj;
     * if ((obj = ref.acquireAndGet()) == null) {
     *     return;
     * }
     * try {
     *     doSomethingWith(obj);
     * } finally {
     *     ref.release();
     * }
     * </pre>
     *
     * @return The referent, or null if no permit could be acquired
     */
    @Nullable
    public final REFERENT_TYPE acquireAndGet() {
        return acquire() ? referent : null;
    }

    /**
     * Release a single active use permit. It is a serious error to release more permits than acquired. Callers are
     * encouraged to use this in the finally block of a try-finally pattern:
     *
     * <pre>
     * if (!ref.acquire()) {
     *     return;
     * }
     * try {
     *     doSomethingWith(ref.get());
     * } finally {
     *     ref.release();
     * }
     * </pre>
     */
    public final void release() {
        final int newState = decrementOutstandingPermits();
        if (newState < 0) {
            throw new IllegalStateException(this + " released more than acquired");
        }
        maybeReturnReferentToPool(newState);
    }

    /**
     * Clear this reference (and return its referent to the pool) when it no longer has any outstanding permits, which
     * may mean immediately if the number of outstanding permits is already zero. All invocations after the first will
     * have no effect.
     */
    @Override
    public final void clear() {
        int currentState;
        while (stateIsAvailable(currentState = state)) {
            final int newState = calculateNewStateForClear(currentState);
            if (tryUpdateState(currentState, newState)) {
                maybeReturnReferentToPool(newState);
                return;
            }
        }
    }

    /**
     * Return the referent to the pool.
     *
     * @param referent The referent to return
     */
    protected abstract void returnReferentToPool(@NotNull REFERENT_TYPE referent);

    private void maybeReturnReferentToPool(int newState) {
        if (stateAllowsAcquire(newState)) {
            return;
        }
        final REFERENT_TYPE localReferent = referent;
        referent = null;
        returnReferentToPool(localReferent);
    }
}
