//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.jetbrains.annotations.NotNull;

import java.util.function.LongSupplier;

/**
 * Sizes a bounded pool of reusable items from the traffic it sees. The pool reports each borrow and return, and reads
 * the {@link #capacity() capacity} it may hold; when the capacity falls below what the pool holds, the pool discards
 * the difference. The pool holds whatever lock it needs: this class is not thread-safe.
 *
 * <p>
 * The capacity adapts between the capacity the adaptor was created with and a maximum.
 *
 * <p>
 * It grows by one at each miss that a larger pool would have served. The adaptor keeps a counter of turned-away items:
 * each item the pool turns away for being full adds one, and each miss charged against it, below, subtracts one. The
 * pool turns items away only when it is full, so a pool larger by the counter would hold exactly that many more items
 * now, whatever order the borrows and returns came in. A miss that occurs while the counter is positive is therefore an
 * item the pool could have reused and did not: a regretted turn-away. It grows the capacity by one and decrements the
 * counter. On the other hand, a miss while the counter is zero, as when borrowers keep what they borrow, changes
 * neither the counter nor the capacity. The counter never exceeds the room left to grow, since turned-away items beyond
 * that could never be kept. Items are commonly borrowed in bursts and returned together, as when an update cycle
 * borrows storage for previous values and returns it all when the cycle completes. One cycle that the pool is too small
 * for turns the shortfall away; the next grows the pool by it, during its burst, and from then on reuses everything. No
 * clock is needed, and the capacity never grows past what some burst actually used, since each unit of growth was a
 * miss.
 *
 * <p>
 * It shrinks on a window of time. When a window sees the pool never empty, the fewest items it held in that window were
 * never needed, and the capacity discards the shrink fraction of them, rounded up, but never goes below the capacity it
 * was created with. The items the window did need are always kept: with a capacity of 100 and a window that never drew
 * the pool below 40 items, a fraction of 0.9 discards 36 of those 40, leaving a capacity of 64, while a fraction of 0
 * keeps the capacity once grown. The window must be at least as long as the slowest cycle that borrows from the pool,
 * since a window that ends partway through a burst sees items that burst was still about to borrow as unneeded.
 */
public final class CapacityAdaptor {
    private final int minimumCapacity;
    private final int maximumCapacity;
    private final LongSupplier nanoClock;
    private final int clockReadMask;
    private final long windowNanos;
    private final double shrinkFraction;

    /** The most items the pool may hold now. */
    private int capacity;

    /**
     * The items the pool turned away, less the misses already charged against them, and never more than the room to
     * grow: how many more items a larger pool would hold right now.
     */
    private int turnedAway;

    private long windowStart;
    private int operations;
    /** The fewest items the pool held in this window. */
    private int windowMinSize;

    /**
     * @param capacity The capacity to start with, and the least to shrink to
     * @param maximumCapacity The most the capacity may grow to, at least {@code capacity}
     * @param nanoClock The clock the windows are measured on, in nanoseconds
     * @param operationsPerClockRead The borrows and returns between reads of the clock, a power of two
     * @param windowNanos The nanoseconds of traffic each shrink of the capacity is judged on
     * @param shrinkFraction The fraction, from 0 to 1, of the items a window never needed that the capacity discards
     */
    public CapacityAdaptor(final int capacity, final int maximumCapacity, @NotNull final LongSupplier nanoClock,
            final int operationsPerClockRead, final long windowNanos, final double shrinkFraction) {
        if (capacity < 0 || maximumCapacity < capacity) {
            throw new IllegalArgumentException(
                    "capacity " + capacity + " must be non-negative and at most maximumCapacity " + maximumCapacity);
        }
        if (Integer.bitCount(operationsPerClockRead) != 1) {
            throw new IllegalArgumentException(
                    "operationsPerClockRead " + operationsPerClockRead + " must be a power of two");
        }
        if (windowNanos <= 0) {
            throw new IllegalArgumentException("windowNanos " + windowNanos + " must be positive");
        }
        if (!(shrinkFraction >= 0 && shrinkFraction <= 1)) {
            throw new IllegalArgumentException("shrinkFraction " + shrinkFraction + " must be from 0 to 1");
        }
        this.minimumCapacity = capacity;
        this.maximumCapacity = maximumCapacity;
        this.capacity = capacity;
        this.nanoClock = nanoClock;
        this.clockReadMask = operationsPerClockRead - 1;
        this.windowNanos = windowNanos;
        this.shrinkFraction = shrinkFraction;
        this.windowStart = nanoClock.getAsLong();
        this.windowMinSize = 0;
    }

    /**
     * @return the most items the pool may hold now; the pool discards any it holds beyond this
     */
    public int capacity() {
        return capacity;
    }

    /**
     * @return the most items the capacity may grow to
     */
    public int maximumCapacity() {
        return maximumCapacity;
    }

    /**
     * The pool is about to borrow an item.
     *
     * @param held The items the pool holds
     */
    public void borrowing(final int held) {
        maybeShrink(held);
    }

    /**
     * The pool lent out an item it held.
     *
     * @param held The items the pool holds now
     */
    public void borrowed(final int held) {
        windowMinSize = Math.min(windowMinSize, held);
    }

    /**
     * The pool was empty, so the borrower constructed an item.
     */
    public void missed() {
        windowMinSize = 0;
        if (turnedAway > 0) {
            // A regretted turn-away: a pool this much larger would have held an item for this borrow, so grow to hold
            // it next time.
            ++capacity;
            --turnedAway;
        }
    }

    /**
     * The pool is about to take back an item.
     *
     * @param held The items the pool holds
     */
    public void returning(final int held) {
        maybeShrink(held);
    }

    /**
     * The pool was full, so it turned the returned item away.
     */
    public void dropped() {
        if (turnedAway < maximumCapacity - capacity) {
            ++turnedAway;
        }
    }

    /**
     * At the end of a window, discard the shrink fraction of the items the pool never needed.
     *
     * @param held The items the pool holds, before it discards any beyond the capacity this leaves
     */
    private void maybeShrink(final int held) {
        if ((++operations & clockReadMask) != 0) {
            return;
        }
        final long now = nanoClock.getAsLong();
        if (now - windowStart < windowNanos) {
            return;
        }
        if (windowMinSize > 0) {
            // rounded up, so that any fraction above 0 reaches the minimum
            final int shrink = (int) Math.ceil(windowMinSize * shrinkFraction);
            capacity = Math.max(minimumCapacity, capacity - shrink);
        }
        windowStart = now;
        // what the pool holds once it discards the items beyond the capacity
        windowMinSize = Math.min(held, capacity);
    }
}
