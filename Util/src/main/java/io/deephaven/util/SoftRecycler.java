/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * This class makes a little "recycle bin" for your objects of type T. When you want an object, call
 * borrowItem(). When you do so, either a fresh T will be constructed for you, or a reused T will be
 * pulled from the recycle bin. When you are done with the object and want to recycle it, call
 * returnItem(). This class will keep a maximum of 'capacity' items in its recycle bin.
 * Additionally, the items are held by SoftReferences, so the garbage collector may reclaim them if
 * it feels like it. The items are borrowed in LIFO order, which hopefully is somewhat
 * cache-friendly.
 *
 * Note that the caller has no special obligation to return a borrowed item nor to return borrowed
 * items in any particular order. If your code has a need to keep a borrowed item forever, there is
 * no problem with that. But if you want your objects to be reused, you have to return them.
 */
public class SoftRecycler<T> {
    private final int capacity;
    private final Supplier<T> constructItem;
    private final Consumer<T> sanitizeItem;
    private final List<SoftReferenceWithIndex<T>> recycleBin;
    private final ReferenceQueue<T> retirementQueue;

    /**
     * @param capacity Capacity of the recycler
     * @param constructItem A callback that creates a new item
     * @param sanitizeItem Optional. A callback that sanitizes the item before reuse. Pass null if
     *        no sanitization is needed.
     */
    public SoftRecycler(int capacity, Supplier<T> constructItem, Consumer<T> sanitizeItem) {
        this.capacity = capacity;
        this.constructItem = constructItem;
        this.sanitizeItem = sanitizeItem;
        this.recycleBin = new ArrayList<>();
        this.retirementQueue = new ReferenceQueue<>();
    }

    // I'm a little sad that these are synchronized
    public T borrowItem() {
        synchronized (this) {
            // Working backwards, try to find an item that is still live
            while (!recycleBin.isEmpty()) {
                // Peel off the last SoftReference. If it still has a value, return that value to
                // the caller. Otherwise,
                // toss it and move on to the next.
                T item = recycleBin.remove(recycleBin.size() - 1).get();
                if (item != null) {
                    return item;
                }
            }
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
            if (size >= capacity) {
                // Sorry, recycle bin full.
                return;
            }
            recycleBin.add(new SoftReferenceWithIndex<>(item, retirementQueue, size));
        }
    }

    private void cleanup() {
        // Process all the SoftReferences that have lost their referents, and remove them from the
        // recycle bin.
        while (true) {
            SoftReferenceWithIndex<T> sri = (SoftReferenceWithIndex<T>) retirementQueue.poll();
            if (sri == null) {
                break;
            }
            // If this SoftReference is still in the recycle bin (it may or may not be), we remove
            // it from the recycle
            // bin. In order to do this remove efficiently, rather than moving all the items down to
            // fill the empty
            // slot, we just replace the item at the current slot with the item at the end.
            // The SoftReferenceWithIndex objects always know what position they are at in the
            // recycleBin.
            final int destIndex = sri.index;
            if (destIndex < recycleBin.size() && sri == recycleBin.get(destIndex)) {
                final int lastIndex = recycleBin.size() - 1;
                SoftReferenceWithIndex<T> lastSri = recycleBin.remove(lastIndex);
                if (destIndex != lastIndex) {
                    // Move the item that was formerly in the last position to the recently-evicted
                    // position. Also
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
