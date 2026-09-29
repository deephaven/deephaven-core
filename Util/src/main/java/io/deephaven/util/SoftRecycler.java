//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import io.deephaven.configuration.Configuration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * This class makes a little "recycle bin" for your objects of type T. When you want an object, call borrowItem(). When
 * you do so, either a fresh T will be constructed for you, or a reused T will be pulled from the recycle bin. When you
 * are done with the object and want to recycle it, call returnItem(). Additionally, the items are held by
 * SoftReferences, so the garbage collector may reclaim them if it feels like it. The items are borrowed in LIFO order,
 * which hopefully is somewhat cache-friendly.
 *
 * <p>
 * The recycle bin holds at most its current capacity, which adapts to its traffic between the capacity it was created
 * with and a maximum. Items are commonly borrowed in bursts and returned together, as when an update cycle borrows
 * storage for previous values and returns it all when the cycle completes. When a window of time and the one before it
 * see the bin turn returned items away because it is full, and also construct new items because it is empty, the
 * capacity grows to hold the largest single burst, the most items borrowed between two returns, but by no more than the
 * most items turned away between two borrows, since items a burst keeps need no room. When a window sees the bin never
 * empty, the fewest items it held in that window were never needed, and the capacity discards the shrink fraction of
 * them, rounded up, but never goes below the capacity it was created with. The items the window did need are always
 * kept: with a capacity of 100 and a window that never drew the bin below 40 items, a fraction of 0.9 discards 36 of
 * those 40, leaving a capacity of 64, while a fraction of 0 keeps the capacity once grown. The maximum is
 * {@value #MAXIMUM_CAPACITY_PROPERTY} unless a constructor gives one; it defaults to no limit, since the garbage
 * collector reclaims the items under memory pressure. The window is {@value #WINDOW_MILLIS_PROPERTY} milliseconds, 1000
 * by default, and the fraction is {@value #SHRINK_FRACTION_PROPERTY}, 0.5 by default.
 *
 * <p>
 * Note that the caller has no special obligation to return a borrowed item nor to return borrowed items in any
 * particular order. If your code has a need to keep a borrowed item forever, there is no problem with that. But if you
 * want your objects to be reused, you have to return them.
 */
public class SoftRecycler<T> {
    /** The property that bounds how far every recycler's capacity may grow. */
    public static final String MAXIMUM_CAPACITY_PROPERTY = "SoftRecycler.maximumCapacity";

    /** The property for the milliseconds of traffic each adjustment of the capacity is judged on. */
    public static final String WINDOW_MILLIS_PROPERTY = "SoftRecycler.windowMillis";
    /**
     * The property for the fraction, from 0 to 1, of the items a window never needed that the capacity discards; the
     * rest of them, and every item the window did need, are kept. 0 never shrinks, 0.9 discards 90% of the unneeded
     * items and keeps 10%, and 1 shrinks to what the window needed.
     */
    public static final String SHRINK_FRACTION_PROPERTY = "SoftRecycler.shrinkFraction";

    private static final int DEFAULT_MAXIMUM_CAPACITY =
            Configuration.getInstance().getIntegerWithDefault(MAXIMUM_CAPACITY_PROPERTY, Integer.MAX_VALUE);
    private static final long DEFAULT_WINDOW_NANOS =
            1_000_000L * Configuration.getInstance().getLongWithDefault(WINDOW_MILLIS_PROPERTY, 1000);
    private static final double DEFAULT_SHRINK_FRACTION =
            Configuration.getInstance().getDoubleWithDefault(SHRINK_FRACTION_PROPERTY, 0.5);
    /** The clock is read once per this many borrows and returns, a power of two. */
    private static final int OPERATIONS_PER_CLOCK_READ = 4096;

    private final int minimumCapacity;
    private final int maximumCapacity;
    private final Supplier<T> constructItem;
    private final Consumer<T> sanitizeItem;
    private final List<SoftReferenceWithIndex<T>> recycleBin;
    private final ReferenceQueue<T> retirementQueue;
    private final LongSupplier nanoClock;
    private final int clockReadMask;
    private final long windowNanos;
    private final double shrinkFraction;

    /** The most items the recycle bin may hold now. */
    private int capacity;

    private long windowStart;
    private int operations;
    /** The items constructed because the recycle bin was empty, in this window. */
    private int windowMisses;
    /** The items turned away because the recycle bin was full, in this window. */
    private int windowDrops;
    /** The items borrowed since the last return: the size of the current burst. */
    private int burstBorrows;
    /** The largest {@link #burstBorrows} of this window. */
    private int windowMaxBurst;
    /** The items turned away since the last borrow. */
    private int dropRun;
    /** The largest {@link #dropRun} of this window. */
    private int windowMaxDropRun;
    /** The fewest items the recycle bin held in this window. */
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
     * @param capacity The capacity the recycler starts with, and the least it shrinks to; it grows with its traffic up
     *        to {@value #MAXIMUM_CAPACITY_PROPERTY}
     * @param constructItem A callback that creates a new item
     * @param sanitizeItem Optional. A callback that sanitizes the item before reuse. Pass null if no sanitization is
     *        needed.
     */
    public SoftRecycler(int capacity, Supplier<T> constructItem, Consumer<T> sanitizeItem) {
        this(capacity, Math.max(capacity, DEFAULT_MAXIMUM_CAPACITY), constructItem, sanitizeItem);
    }

    /**
     * @param capacity The capacity the recycler starts with, and the least it shrinks to; it grows with its traffic up
     *        to {@code maximumCapacity}
     * @param maximumCapacity The most items the recycler may grow to hold, at least {@code capacity}
     * @param constructItem A callback that creates a new item
     * @param sanitizeItem Optional. A callback that sanitizes the item before reuse. Pass null if no sanitization is
     *        needed.
     */
    public SoftRecycler(int capacity, int maximumCapacity, Supplier<T> constructItem, Consumer<T> sanitizeItem) {
        this(capacity, maximumCapacity, constructItem, sanitizeItem, System::nanoTime, OPERATIONS_PER_CLOCK_READ,
                DEFAULT_WINDOW_NANOS, DEFAULT_SHRINK_FRACTION);
    }

    @VisibleForTesting
    SoftRecycler(final int capacity, final int maximumCapacity, @NotNull final Supplier<T> constructItem,
            final Consumer<T> sanitizeItem, @NotNull final LongSupplier nanoClock, final int operationsPerClockRead,
            final long windowNanos, final double shrinkFraction) {
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
        this.constructItem = constructItem;
        this.sanitizeItem = sanitizeItem;
        this.recycleBin = new ArrayList<>();
        this.retirementQueue = new ReferenceQueue<>();
        this.nanoClock = nanoClock;
        this.clockReadMask = operationsPerClockRead - 1;
        this.windowNanos = windowNanos;
        this.shrinkFraction = shrinkFraction;
        this.windowStart = nanoClock.getAsLong();
        this.windowMinSize = 0;
    }

    // I'm a little sad that these are synchronized
    public T borrowItem() {
        synchronized (this) {
            adapt();
            windowMaxBurst = Math.max(windowMaxBurst, ++burstBorrows);
            dropRun = 0;
            // Working backwards, try to find an item that is still live
            while (!recycleBin.isEmpty()) {
                // Peel off the last SoftReference. If it still has a value, return that value to the caller. Otherwise,
                // toss it and move on to the next.
                T item = recycleBin.remove(recycleBin.size() - 1).get();
                if (item != null) {
                    windowMinSize = Math.min(windowMinSize, recycleBin.size());
                    return item;
                }
            }
            windowMinSize = 0;
            ++windowMisses;
        }

        // Recycle bin empty, so make a new item.
        return constructItem.get();
    }

    public void returnItem(T item) {
        // Make sure the item is squeaky-clean for the next user.
        if (sanitizeItem != null) {
            sanitizeItem.accept(item);
        }
        synchronized (this) {
            burstBorrows = 0;
            adapt();
            // Get the expired SoftReferences out of the queue so we have an accurate count.
            cleanup();
            final int size = recycleBin.size();
            if (size >= capacity) {
                // Sorry, recycle bin full.
                ++windowDrops;
                windowMaxDropRun = Math.max(windowMaxDropRun, ++dropRun);
                return;
            }
            recycleBin.add(new SoftReferenceWithIndex<>(item, retirementQueue, size));
        }
    }

    /**
     * @return the most items the recycle bin may hold now
     */
    @VisibleForTesting
    synchronized int getCapacity() {
        return capacity;
    }

    /**
     * @return the most items the recycle bin may grow to hold
     */
    @VisibleForTesting
    int getMaximumCapacity() {
        return maximumCapacity;
    }

    /**
     * At the end of a window, grow to the largest burst if the recycle bin both turned items away and constructed new
     * ones, or else discard the shrink fraction of the items it never needed. Called with the lock held.
     */
    private void adapt() {
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
            // Items were thrown away and then constructed again, so a larger bin would have kept them. A bin as large
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
            while (recycleBin.size() > capacity) {
                recycleBin.remove(recycleBin.size() - 1);
            }
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
        windowMinSize = recycleBin.size();
    }

    private void cleanup() {
        // Process all the SoftReferences that have lost their referents, and remove them from the recycle bin.
        while (true) {
            SoftReferenceWithIndex<T> sri = (SoftReferenceWithIndex<T>) retirementQueue.poll();
            if (sri == null) {
                break;
            }
            // If this SoftReference is still in the recycle bin (it may or may not be), we remove it from the recycle
            // bin. In order to do this remove efficiently, rather than moving all the items down to fill the empty
            // slot, we just replace the item at the current slot with the item at the end.
            // The SoftReferenceWithIndex objects always know what position they are at in the recycleBin.
            final int destIndex = sri.index;
            if (destIndex < recycleBin.size() && sri == recycleBin.get(destIndex)) {
                final int lastIndex = recycleBin.size() - 1;
                SoftReferenceWithIndex<T> lastSri = recycleBin.remove(lastIndex);
                if (destIndex != lastIndex) {
                    // Move the item that was formerly in the last position to the recently-evicted position. Also
                    // update the object's position in the array.
                    lastSri.index = destIndex;
                    recycleBin.set(destIndex, lastSri);
                }
            }
        }
    }

    private static class SoftReferenceWithIndex<T> extends SoftReference<T> {
        private int index;

        SoftReferenceWithIndex(T referent, ReferenceQueue<? super T> q, int index) {
            super(referent, q);
            this.index = index;
        }
    }
}
