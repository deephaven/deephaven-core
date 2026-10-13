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
 * The capacity adapts between the capacity the adaptor was created with and a maximum. Items are commonly borrowed in
 * bursts and returned together, as when an update cycle borrows storage for previous values and returns it all when the
 * cycle completes. When a window of time and the one before it see the pool turn returned items away because it is
 * full, and also construct new items because it is empty, the capacity grows to hold the largest single burst, the most
 * items borrowed between two returns, but by no more than the most items turned away between two borrows, since items a
 * burst keeps need no room. When a window sees the pool never empty, the fewest items it held in that window were never
 * needed, and the capacity discards the shrink fraction of them, rounded up, but never goes below the capacity it was
 * created with. The items the window did need are always kept: with a capacity of 100 and a window that never drew the
 * pool below 40 items, a fraction of 0.9 discards 36 of those 40, leaving a capacity of 64, while a fraction of 0 keeps
 * the capacity once grown.
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

    private long windowStart;
    private int operations;
    /** The items constructed because the pool was empty, in this window. */
    private int windowMisses;
    /** The items turned away because the pool was full, in this window. */
    private int windowDrops;
    /** The items borrowed since the last return: the size of the current burst. */
    private int burstBorrows;
    /** The largest {@link #burstBorrows} of this window. */
    private int windowMaxBurst;
    /** The items turned away since the last borrow. */
    private int dropRun;
    /** The largest {@link #dropRun} of this window. */
    private int windowMaxDropRun;
    /** The fewest items the pool held in this window. */
    private int windowMinSize;
    /**
     * The previous window's {@link #windowMisses}, {@link #windowDrops}, {@link #windowMaxBurst}, and
     * {@link #windowMaxDropRun}, judged with this window's so that a burst split by the end of a window still counts.
     */
    private int previousMisses;
    private int previousDrops;
    private int previousMaxBurst;
    private int previousMaxDropRun;

    /**
     * @param capacity The capacity to start with, and the least to shrink to
     * @param maximumCapacity The most the capacity may grow to, at least {@code capacity}
     * @param nanoClock The clock the windows are measured on, in nanoseconds
     * @param operationsPerClockRead The borrows and returns between reads of the clock, a power of two
     * @param windowNanos The nanoseconds of traffic each adjustment of the capacity is judged on
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
        adapt(held);
        windowMaxBurst = Math.max(windowMaxBurst, ++burstBorrows);
        dropRun = 0;
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
        ++windowMisses;
    }

    /**
     * The pool is about to take back an item.
     *
     * @param held The items the pool holds
     */
    public void returning(final int held) {
        burstBorrows = 0;
        adapt(held);
    }

    /**
     * The pool was full, so it turned the returned item away.
     */
    public void dropped() {
        ++windowDrops;
        windowMaxDropRun = Math.max(windowMaxDropRun, ++dropRun);
    }

    /**
     * At the end of a window, grow to the largest burst if the pool both turned items away and constructed new ones, or
     * else discard the shrink fraction of the items it never needed.
     *
     * @param held The items the pool holds, before it discards any beyond the capacity this leaves
     */
    private void adapt(final int held) {
        if ((++operations & clockReadMask) != 0) {
            return;
        }
        final long now = nanoClock.getAsLong();
        if (now - windowStart < windowNanos) {
            return;
        }
        // A burst's misses and its drops may fall on either side of the end of a window, as when an update cycle
        // borrows before the end of a window and returns after it, so this window is judged with the one before.
        final int misses = windowMisses + previousMisses;
        final int drops = windowDrops + previousDrops;
        final int maxBurst = Math.max(windowMaxBurst, previousMaxBurst);
        final int maxDropRun = Math.max(windowMaxDropRun, previousMaxDropRun);
        if (drops > 0 && misses > 0) {
            // Items were thrown away and then constructed again, so a larger pool would have kept them. A pool as large
            // as the largest burst keeps every item a burst returns, however full it was when the burst began; the
            // items turned away at once bound the room that was missing.
            final long needed = Math.min(maxBurst, (long) capacity + maxDropRun);
            if (needed > capacity) {
                capacity = (int) Math.min(maximumCapacity, needed);
            }
            // the growth used this window's traffic, which the next window must not count again
            windowMisses = windowDrops = windowMaxBurst = windowMaxDropRun = 0;
        } else if (windowMinSize > 0) {
            // rounded up, so that any fraction above 0 reaches the minimum
            final int shrink = (int) Math.ceil(windowMinSize * shrinkFraction);
            capacity = Math.max(minimumCapacity, capacity - shrink);
        }
        windowStart = now;
        previousMisses = windowMisses;
        previousDrops = windowDrops;
        previousMaxBurst = windowMaxBurst;
        previousMaxDropRun = windowMaxDropRun;
        windowMisses = 0;
        windowDrops = 0;
        windowMaxBurst = 0;
        windowMaxDropRun = 0;
        // what the pool holds once it discards the items beyond the capacity
        windowMinSize = Math.min(held, capacity);
    }
}
