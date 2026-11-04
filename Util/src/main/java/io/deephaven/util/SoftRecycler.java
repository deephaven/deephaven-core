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
import java.util.function.Supplier;

/**
 * This class makes a little "recycle bin" for your objects of type T. When you want an object, call
 * {@link #borrowItem()}. When you do so, either a fresh T will be constructed for you, or a reused T will be pulled
 * from the recycle bin. When you are done with the object and want to recycle it, call {@link #returnItem(Object)}. The
 * items you return are held by SoftReferences, so the garbage collector may reclaim them if it feels like it.
 * Otherwise, previously-returned items are re-borrowed in LIFO order, which hopefully is somewhat cache-friendly.
 * <p>
 * The caller has no special obligation to return a borrowed item nor to return borrowed items in any particular order.
 * If your code has a need to keep a borrowed item forever, there is no problem with that. But if you want your objects
 * to be reused, you have to return them. This is true even for registered recyclers which adapt their capacity to their
 * traffic; an item kept forever simply never counts toward it.
 * <p>
 * The recycle bin holds at most its current capacity, which a {@link CapacityAdaptor} adapts to its traffic between the
 * capacity the recycler was created with and its maximum. The capacity grows on its own, but shrinks only when the
 * recycler's window ends, and windows end only for recyclers registered through
 * {@link SoftRecyclerWindow#register(SoftRecycler)}, which ends them from the update graphs' cycles. An unregistered
 * recycler grows to its maximum and stays there. The shrink fraction is the {@value #SHRINK_FRACTION_PROPERTY}
 * property, 0.1 by default.
 */
public class SoftRecycler<T> {
    /** The property for the fewest milliseconds of traffic each shrink of the capacity is judged on. */
    public static final String WINDOW_MILLIS_PROPERTY = "SoftRecycler.windowMillis";
    /**
     * The property for the fraction, from 0 to 1, of the items a window never needed that the capacity discards; the
     * rest of them, and every item the window did need, are kept. 0 never shrinks, 0.9 discards 90% of the unneeded
     * items and keeps 10%, and 1 shrinks to what the window needed.
     */
    public static final String SHRINK_FRACTION_PROPERTY = "SoftRecycler.shrinkFraction";

    private static final double DEFAULT_SHRINK_FRACTION =
            Configuration.getInstance().getDoubleWithDefault(SHRINK_FRACTION_PROPERTY, 0.1);

    private final Supplier<T> constructItem;
    private final Consumer<T> sanitizeItem;
    private final List<SoftReferenceWithIndex<T>> recycleBin;
    private final ReferenceQueue<T> retirementQueue;
    private final CapacityAdaptor adaptor;

    /**
     * @param capacity The capacity the recycler starts with, and the least it shrinks to; it grows with its traffic up
     *        to {@code maximumCapacity}
     * @param maximumCapacity The most items the recycler may grow to hold, at least {@code capacity}. Use
     *        {@link Integer#MAX_VALUE} for no maximum: the items are soft references, which the garbage collector
     *        reclaims under memory pressure, so an unlimited recycler is not a leak.
     * @param constructItem A callback that creates a new item
     * @param sanitizeItem Optional. A callback that sanitizes the item before reuse. Pass null if no sanitization is
     *        needed.
     */
    public SoftRecycler(int capacity, int maximumCapacity, Supplier<T> constructItem, Consumer<T> sanitizeItem) {
        this(capacity, maximumCapacity, constructItem, sanitizeItem, DEFAULT_SHRINK_FRACTION);
    }

    @VisibleForTesting
    SoftRecycler(final int capacity, final int maximumCapacity, @NotNull final Supplier<T> constructItem,
            final Consumer<T> sanitizeItem, final double shrinkFraction) {
        this.constructItem = constructItem;
        this.sanitizeItem = sanitizeItem;
        this.recycleBin = new ArrayList<>();
        this.retirementQueue = new ReferenceQueue<>();
        this.adaptor = new CapacityAdaptor(capacity, maximumCapacity, shrinkFraction);
    }

    /**
     * End this recycler's window, shrinking it by the items the window never needed.
     */
    @VisibleForTesting
    synchronized void endWindow() {
        // get the expired SoftReferences out of the queue so we have an accurate count
        cleanup();
        adaptor.endWindow(recycleBin.size());
        trimToCapacity();
    }

    // I'm a little sad that these are synchronized
    public T borrowItem() {
        synchronized (this) {
            // Working backwards, try to find an item that is still live
            while (!recycleBin.isEmpty()) {
                // Peel off the last SoftReference. If it still has a value, return that value to the caller. Otherwise,
                // toss it and move on to the next.
                T item = recycleBin.remove(recycleBin.size() - 1).get();
                if (item != null) {
                    adaptor.borrowed(recycleBin.size());
                    return item;
                }
            }
            adaptor.missed();
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
            // Get the expired SoftReferences out of the queue so we have an accurate count.
            cleanup();
            final int size = recycleBin.size();
            if (size >= adaptor.capacity()) {
                // Sorry, recycle bin full.
                adaptor.dropped();
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
        return adaptor.capacity();
    }

    /**
     * Discard the items beyond the capacity, from the back, which needs no other entry re-indexed. Called with the lock
     * held.
     */
    private void trimToCapacity() {
        final int capacity = adaptor.capacity();
        while (recycleBin.size() > capacity) {
            recycleBin.remove(recycleBin.size() - 1);
        }
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
