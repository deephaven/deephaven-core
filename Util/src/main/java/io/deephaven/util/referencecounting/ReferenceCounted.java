/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.referencecounting;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Implements a recurring reference counting pattern - a concurrent reference count that should refuse to go below zero,
 * and invokes {@link #onReferenceCountAtZero()} exactly once when the count returns to zero.
 */
public abstract class ReferenceCounted implements LogOutputAppendable, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Field updater for referenceCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger}
     * for each instance.
     */
    private static final AtomicIntegerFieldUpdater<ReferenceCounted> REFERENCE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ReferenceCounted.class, "referenceCount");

    /**
     * This constant represents a "zero" reference count value that doesn't prevent increasing the reference count.
     */
    private static final int INITIAL_ZERO_VALUE = -1;

    /**
     * Since we've reserved -1 as our initial reference count value, our maximum is really one less.
     */
    private static final int MAXIMUM_VALUE = -3;

    /**
     * This is our "one" reference count value.
     */
    private static final int ONE_VALUE = 1;

    /**
     * This is our normal "zero" reference count value (terminal state).
     */
    private static final int NORMAL_TERMINAL_ZERO_VALUE = 0;

    /**
     * This is a marker "zero" reference count value (terminal state), signifying that a reference count was set to zero
     * under exceptional circumstances, and additional attempts to drop the reference count should be treated as
     * successful so as not to violate constraints.
     */
    private static final int FORCED_TERMINAL_ZERO_VALUE = -2;

    /**
     * The actual value of our reference count.
     */
    private transient volatile int referenceCount;

    protected ReferenceCounted() {
        this(0);
    }

    /**
     * @param initialValue The initial value for the reference count, taken as an unsigned integer. Must not be one of
     *        the reserved values {@value #INITIAL_ZERO_VALUE} or {@value #FORCED_TERMINAL_ZERO_VALUE}.
     */
    @SuppressWarnings("WeakerAccess")
    protected ReferenceCounted(final int initialValue) {
        initializeReferenceCount(initialValue);
    }

    private void readObject(@NotNull final ObjectInputStream in) {
        initializeReferenceCount(0);
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append(Utils.REFERENT_FORMATTER, this).append('[').append(getCurrentReferenceCount())
                .append(']');
    }

    private void initializeReferenceCount(final int initialValue) {
        if (initialValue == INITIAL_ZERO_VALUE || initialValue == FORCED_TERMINAL_ZERO_VALUE) {
            throw new IllegalArgumentException("Invalid initial reference count " + initialValue);
        }
        referenceCount = initialValue == 0 ? INITIAL_ZERO_VALUE : initialValue;
    }

    private int getCurrentReferenceCount() {
        return referenceCount;
    }

    private boolean tryUpdateReferenceCount(final int expected, final int update) {
        return REFERENCE_COUNT_UPDATER.compareAndSet(this, expected, update);
    }

    /**
     * Reset this reference count to its initial state for reuse.
     */
    public final void resetReferenceCount() {
        if (!tryUpdateReferenceCount(NORMAL_TERMINAL_ZERO_VALUE, INITIAL_ZERO_VALUE)
                && !tryUpdateReferenceCount(FORCED_TERMINAL_ZERO_VALUE, INITIAL_ZERO_VALUE)) {
            throw new IllegalStateException(
                    Utils.makeReferentDescription(this) + "'s reference count is non-zero and cannot be reset");
        }
    }

    private static boolean isInitialZero(final int countValue) {
        return countValue == INITIAL_ZERO_VALUE;
    }

    private static boolean isTerminalZero(final int countValue) {
        return countValue == NORMAL_TERMINAL_ZERO_VALUE || countValue == FORCED_TERMINAL_ZERO_VALUE;
    }

    private static boolean isZero(final int countValue) {
        return isInitialZero(countValue) || isTerminalZero(countValue);
    }

    /**
     * Increment the reference count by 1, if it has not already been decreased to 0.
     *
     * @return Whether the reference count was successfully incremented
     * @throws IllegalStateException If the reference count is already at its maximum referenceCount
     */
    public final boolean tryIncrementReferenceCount() {
        int currentReferenceCount;
        while (!isTerminalZero(currentReferenceCount = getCurrentReferenceCount())) {
            if (currentReferenceCount == MAXIMUM_VALUE) {
                throw new IllegalStateException(
                        Utils.makeReferentDescription(this) + "'s reference count cannot exceed maximum value");
            }
            if (tryUpdateReferenceCount(currentReferenceCount,
                    isInitialZero(currentReferenceCount) ? ONE_VALUE : currentReferenceCount + 1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Increment the reference count by one, if it has not already been decreased to zero.
     *
     * @throws IllegalStateException If the reference count was not successfully incremented
     */
    public final void incrementReferenceCount() {
        if (!tryIncrementReferenceCount()) {
            throw new IllegalStateException(
                    Utils.makeReferentDescription(this) + "'s reference count has already reached zero");
        }
    }

    /**
     * Decrement the reference count by one, if it has ever been increased and has not already been decreased to zero.
     * Invokes the implementation's {@link #onReferenceCountAtZero()} method if decrementing to zero.
     *
     * @return Whether the reference count was successfully decremented
     */
    @SuppressWarnings({"WeakerAccess", "BooleanMethodIsAlwaysInverted"})
    public final boolean tryDecrementReferenceCount() {
        int currentReferenceCount;
        while (!isZero(currentReferenceCount = getCurrentReferenceCount())) {
            if (tryUpdateReferenceCount(currentReferenceCount, currentReferenceCount - 1)) {
                if (currentReferenceCount == ONE_VALUE) { // Did we just CAS from 1 to 0?
                    onReferenceCountAtZero();
                }
                return true;
            }
        }
        return currentReferenceCount == FORCED_TERMINAL_ZERO_VALUE;
    }

    /**
     * Force the reference count to zero. If it was non-zero, this will have the same side effects as returning to zero
     * normally, but subsequent invocations of {@link #decrementReferenceCount()} and
     * {@link #tryDecrementReferenceCount()} will act as if the reference count was successfully decremented until
     * {@link #resetReferenceCount()} is invoked.
     */
    public final void forceReferenceCountToZero() {
        int currentReferenceCount;
        while (!isZero(currentReferenceCount = getCurrentReferenceCount())) {
            if (tryUpdateReferenceCount(currentReferenceCount, FORCED_TERMINAL_ZERO_VALUE)) {
                onReferenceCountAtZero();
            }
        }
    }

    /**
     * Decrement the reference count by one, if it has ever been increased and has not already been decreased to zero.
     * Invokes the implementation's {@link #onReferenceCountAtZero()} method if decrementing to zero.
     *
     * @throws IllegalStateException If the reference count was not successfully decremented
     */
    public final void decrementReferenceCount() {
        if (!tryDecrementReferenceCount()) {
            throw new IllegalStateException(
                    Utils.makeReferentDescription(this) + "'s reference count has been decreased more than increased");
        }
    }

    /**
     * Callback method that will be invoked when the reference count returns to zero.
     */
    protected abstract void onReferenceCountAtZero();
}
