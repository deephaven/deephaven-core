package io.deephaven.engine.table.impl.updateby.hashing;

import io.deephaven.configuration.Configuration;

/**
 * A Min-Heap priority queue used to order {@link UpdateBySlotTracker.UpdateTracker} instances for classifying row keys
 * to buckets.
 */
public class TrackerPriorityQueue {
    private static final int doublingAllocThreshold = Configuration.getInstance()
            .getIntegerWithDefault("TrackerPriorityQueue.doublingAllocThreshold", 4 * 1024 * 1024);

    // Things are nicer (integer division will be bit shift) if this is a power of 2, but it is not mandatory.
    private static final int linearAllocStep =
            Configuration.getInstance().getIntegerWithDefault("TrackerPriorityQueue.linearAllocStep", 1024 * 1024);

    /** The trackers, slot 0 is unused. */
    private UpdateBySlotTracker.UpdateTracker[] trackers;

    /**
     * The size of the queue (invariant: size < start.length - 1). Note since we don't use element 0 in start and end
     * arrays, this size does not match the normal invariant in array access where the last element used is an array a[]
     * is a[size - 1]; in our case the last element used is a[size].
     */
    private int size = 0;

    /**
     * Create a TrackerPriorityQueue with the given initial capacity.
     *
     * @param initialCapacity how many ranges should we allocate room for
     */
    public TrackerPriorityQueue(final int initialCapacity) {
        trackers = new UpdateBySlotTracker.UpdateTracker[initialCapacity + 1];
    }

    /**
     * Adds an element to the queue.
     */
    public void add(final UpdateBySlotTracker.UpdateTracker tracker) {
        final int newSize = size + 1;
        ensureCapacityFor(newSize);

        trackers[newSize] = tracker;
        size = newSize;
        fixUp(size);
    }

    /**
     * Pop the top element from the queue.
     *
     * @return The item popped
     */
    public UpdateBySlotTracker.UpdateTracker pop() {
        if (size == 0) {
            return null;
        }

        final UpdateBySlotTracker.UpdateTracker atTop = trackers[1];
        if (--size > 0) {
            trackers[1] = trackers[size + 1];
            fixDown(1);
        }

        return atTop;
    }

    private void ensureCapacityFor(final int lastIndex) {
        final int minCapacity = lastIndex + 1;
        if (minCapacity < trackers.length) {
            return;
        }

        int newCapacity = trackers.length;
        while (newCapacity < minCapacity && newCapacity < doublingAllocThreshold) {
            newCapacity = 2 * newCapacity;
        }

        if (newCapacity < minCapacity) {
            final int delta = minCapacity - doublingAllocThreshold;
            final int steps = (delta + linearAllocStep - 1) / linearAllocStep;
            newCapacity = doublingAllocThreshold + steps * linearAllocStep;
        }

        final UpdateBySlotTracker.UpdateTracker[] newTrackers = new UpdateBySlotTracker.UpdateTracker[newCapacity];
        System.arraycopy(trackers, 1, newTrackers, 1, size);
        trackers = newTrackers;
    }

    /**
     * move queue[itemIndex] up the heap until its start is >= that of its parent.
     */
    private void fixUp(int itemIndex) {
        if (itemIndex <= 1) {
            return;
        }

        final UpdateBySlotTracker.UpdateTracker item = trackers[itemIndex];
        int parentIndex = itemIndex >> 1;
        UpdateBySlotTracker.UpdateTracker parent;
        while (itemIndex > 1 && valueOf(item) < valueOf(parent = trackers[parentIndex])) {
            trackers[itemIndex] = parent;
            itemIndex = parentIndex;
            parentIndex = itemIndex >> 1;
        }
        trackers[itemIndex] = item;
    }

    /**
     * move queue[itemIndex] down the heap until its start is <= those of its children.
     */
    private void fixDown(@SuppressWarnings("SameParameterValue") int itemIndex) {
        // Start the smallest child at the left and then adjust
        int smallestChildIdx = itemIndex << 1;
        if (smallestChildIdx > size) {
            return;
        }

        final UpdateBySlotTracker.UpdateTracker item = trackers[itemIndex];
        UpdateBySlotTracker.UpdateTracker smallestChild = trackers[smallestChildIdx];
        UpdateBySlotTracker.UpdateTracker nextChild;
        // Just pick the smallest of the two values.
        if (smallestChildIdx < size && valueOf(nextChild = trackers[smallestChildIdx + 1]) < valueOf(smallestChild)) {
            smallestChild = nextChild;
            smallestChildIdx++;
        }

        if (valueOf(smallestChild) < valueOf(item)) {
            trackers[itemIndex] = smallestChild;
            itemIndex = smallestChildIdx;
            smallestChildIdx = itemIndex << 1;
            while (smallestChildIdx <= size) {
                smallestChild = trackers[smallestChildIdx];
                if (smallestChildIdx < size
                        && valueOf(nextChild = trackers[smallestChildIdx + 1]) < valueOf(smallestChild)) {
                    smallestChild = nextChild;
                    smallestChildIdx++;
                }

                if (valueOf(smallestChild) >= valueOf(item)) {
                    break;
                }
                trackers[itemIndex] = smallestChild;

                itemIndex = smallestChildIdx;
                smallestChildIdx = itemIndex << 1;
            }

            trackers[itemIndex] = item;
        }
    }

    private long valueOf(final UpdateBySlotTracker.UpdateTracker tracker) {
        return tracker.getIterator().currentValue();
    }
}
