//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

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
 * It shrinks at the end of each window, which the pool's owner signals through {@link #endWindow(int)}. When a window
 * sees the pool never empty, the fewest items it held in that window were never needed, and the capacity discards the
 * shrink fraction of them, rounded up, but never goes below the capacity it was created with. The items the window did
 * need are always kept: with a capacity of 100 and a window that never drew the pool below 40 items, a fraction of 0.9
 * discards 36 of those 40, leaving a capacity of 64, while a fraction of 0 keeps the capacity once grown. A window
 * should span at least one whole cycle of each client that borrows from the pool, since a window that ends partway
 * through a burst sees items that burst was still about to borrow as unneeded. The adaptor reads no clock: the borrows
 * and returns it counts only update counters.
 */
public final class CapacityAdaptor {
    private final int minimumCapacity;
    private final int maximumCapacity;
    private final double shrinkFraction;

    /** The most items the pool may hold now. */
    private int capacity;

    /**
     * The items the pool turned away, less the misses already charged against them, and never more than the room to
     * grow: how many more items a larger pool would hold right now.
     */
    private int turnedAway;

    /** The fewest items the pool held in this window. */
    private int windowMinSize;

    /**
     * @param capacity The capacity to start with, and the least to shrink to
     * @param maximumCapacity The most the capacity may grow to, at least {@code capacity}
     * @param shrinkFraction The fraction, from 0 to 1, of the items a window never needed that the capacity discards
     */
    public CapacityAdaptor(final int capacity, final int maximumCapacity, final double shrinkFraction) {
        if (capacity < 0 || maximumCapacity < capacity) {
            throw new IllegalArgumentException(
                    "capacity " + capacity + " must be non-negative and at most maximumCapacity " + maximumCapacity);
        }
        if (!(shrinkFraction >= 0 && shrinkFraction <= 1)) {
            throw new IllegalArgumentException("shrinkFraction " + shrinkFraction + " must be from 0 to 1");
        }
        this.minimumCapacity = capacity;
        this.maximumCapacity = maximumCapacity;
        this.capacity = capacity;
        this.shrinkFraction = shrinkFraction;
        this.windowMinSize = 0;
    }

    /**
     * @return the most items the pool may hold now; the pool discards any it holds beyond this
     */
    public int capacity() {
        return capacity;
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
     * The pool was full, so it turned the returned item away.
     */
    public void dropped() {
        if (turnedAway < maximumCapacity - capacity) {
            ++turnedAway;
        }
    }

    /**
     * End the window: discard the shrink fraction of the items the pool never needed in it, and start the next.
     *
     * @param held The items the pool holds, before it discards any beyond the capacity this leaves
     */
    public void endWindow(final int held) {
        if (windowMinSize > 0) {
            // rounded up, so that any fraction above 0 reaches the minimum
            final int shrink = (int) Math.ceil(windowMinSize * shrinkFraction);
            capacity = Math.max(minimumCapacity, capacity - shrink);
        }
        // what the pool holds once it discards the items beyond the capacity
        windowMinSize = Math.min(held, capacity);
    }
}
