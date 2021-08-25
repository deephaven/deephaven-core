/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.configuration.Configuration;

/**
 * A RandomBuilder type that uses a priority queue of ranges.
 *
 * Each range entered into the Index is stored in a priority queue, backed by two long arrays. One array contains the
 * start elements, the second array contains the end elements. The priority function is the start element.
 *
 * We may have many overlapping ranges in the priority queue; as an optimization, if two adjacent ranges are entered
 * into the queue consecutively, the range is not stored in the queue more than once.
 */
public class RangePriorityQueueBuilder {
    private static final int doublingAllocThreshold = Configuration.getInstance().getIntegerForClassWithDefault(
            MixedBuilder.class, "doublingAllocThreshold", 128 * 1024);
    // Things are nicer (integer division will be bit shift) if this is a power of 2, but it is not mandatory.
    private static final int linearAllocStep = Configuration.getInstance().getIntegerForClassWithDefault(
            MixedBuilder.class, "linearAllocStep", 128 * 1024);

    /** The range start keys, slot 0 is unused. */
    private long[] start;
    /** The range end keys, slot 0 is unused; (invariant: end.length == start.length). */
    private long[] end;

    /** The index of the last entered value in start/end. */
    private int lastEntered = -1;

    /**
     * The size of the queue (invariant: size < start.length - 1). Note since we don't use element 0 in start and end
     * arrays, this size does not match the normal invariant in array access where the last element used is an array a[]
     * is a[size - 1]; in our case the last element used is a[size].
     */
    private int size = 0;

    /**
     * Create a RangePriorityQueueBuilder with the given initial capacity.
     *
     * @param initialCapacity how many ranges should we allocate room for
     */
    RangePriorityQueueBuilder(final int initialCapacity) {
        start = new long[initialCapacity + 1];
        end = new long[initialCapacity + 1];
    }

    void reset() {
        lastEntered = -1;
        size = 0;
    }

    /**
     * Returns true if the priority queue contains no elements.
     */
    private boolean isEmpty() {
        return size == 0;
    }

    /**
     *
     * Returns our internal queue size. This is not necessarily the size of the resulting index.
     */
    public int size() {
        return size;
    }

    private void ensureCapacityFor(final int lastIndex) {
        final int minCapacity = lastIndex + 1;
        if (minCapacity < start.length) {
            return;
        }
        int newCapacity = start.length;
        while (newCapacity < minCapacity && newCapacity < doublingAllocThreshold) {
            newCapacity = 2 * newCapacity;
        }
        if (newCapacity < minCapacity) {
            final int delta = minCapacity - doublingAllocThreshold;
            final int steps = (delta + linearAllocStep - 1) / linearAllocStep;
            newCapacity = doublingAllocThreshold + steps * linearAllocStep;
        }
        final long[] newStart = new long[newCapacity];
        System.arraycopy(start, 1, newStart, 1, size);
        start = newStart;
        final long[] newEnd = new long[newCapacity];
        System.arraycopy(end, 1, newEnd, 1, size);
        end = newEnd;
    }

    /**
     * Adds an element to the range queues.
     */
    private void enter(final long startKey, final long endKey) {
        if (lastEntered >= 1 &&
                endKey >= start[lastEntered] - 1 &&
                startKey <= end[lastEntered] + 1) {
            // the endPosition is after the start position, and the start position is before the end position,
            // so we overlap this range
            if (endKey > end[lastEntered]) {
                end[lastEntered] = endKey;
            }
            if (startKey < start[lastEntered]) {
                start[lastEntered] = startKey;
                fixUp(lastEntered);
            }
            return;
        }
        final int newSize = size + 1;
        ensureCapacityFor(newSize);
        start[newSize] = startKey;
        end[newSize] = endKey;
        size = newSize;
        lastEntered = size;
        fixUp(size);
        // assert testInvariant("after fixUp in enter-add");
    }

    /**
     * Return the next range start.
     */
    private long topStart() {
        return start[1];
    }

    /**
     * Return the next range end.
     */
    private long topEnd() {
        return end[1];
    }

    /**
     * Number of ranges, used only for unit tests to confirm the adjacency merging.
     * 
     * @return the number of ranges enqueued
     */
    int rangeCount() {
        return size;
    }

    /**
     * Remove the top element from the queue.
     *
     * @return true if there was an element to remove; false otherwise.
     */
    @SuppressWarnings("UnusedReturnValue")
    private boolean removeTop() {
        if (size == 0) {
            return false;
        }

        if (--size > 0) {
            start[1] = start[size + 1];
            end[1] = end[size + 1];

            // start[size+1] = 0;
            // end[size+1] = 0;

            fixDown(1);
        }

        return true;
    }

    /** move queue[itemIndex] up the heap until its start is >= that of its parent. */
    private void fixUp(int itemIndex) {
        if (itemIndex > 1) {
            final long itemStartKey = start[itemIndex];
            final long itemEndKey = end[itemIndex];
            int parentIndex = itemIndex >> 1;
            long parent = start[parentIndex];
            if (itemStartKey < parent) {
                start[itemIndex] = parent;
                end[itemIndex] = end[parentIndex];
                itemIndex = parentIndex;
                parentIndex = itemIndex >> 1;
                while (itemIndex > 1 && itemStartKey < (parent = start[parentIndex])) {
                    start[itemIndex] = parent;
                    end[itemIndex] = end[parentIndex];
                    itemIndex = parentIndex;
                    parentIndex = itemIndex >> 1;
                }
                start[itemIndex] = itemStartKey;
                end[itemIndex] = itemEndKey;
                lastEntered = itemIndex;
            }
        }
    }

    /** move queue[itemIndex] down the heap until its start is <= those of its children. */
    private void fixDown(@SuppressWarnings("SameParameterValue") int itemIndex) {
        int childIndex = itemIndex << 1;
        if (childIndex <= size) {
            final long itemStartKey = start[itemIndex];
            final long itmEndKey = end[itemIndex];
            long child = start[childIndex];
            long child2;
            if (childIndex < size && (child2 = start[childIndex + 1]) < child) {
                child = child2;
                childIndex++;
            }
            if (child < itemStartKey) {
                start[itemIndex] = child;
                end[itemIndex] = end[childIndex];
                itemIndex = childIndex;
                childIndex = itemIndex << 1;
                while (childIndex <= size) {
                    child = start[childIndex];
                    if (childIndex < size && (child2 = start[childIndex + 1]) < child) {
                        child = child2;
                        childIndex++;
                    }
                    if (child >= itemStartKey) {
                        break;
                    }
                    start[itemIndex] = child;
                    end[itemIndex] = end[childIndex];
                    itemIndex = childIndex;
                    childIndex = itemIndex << 1;
                }
                start[itemIndex] = itemStartKey;
                end[itemIndex] = itmEndKey;
            }
        }
    }

    private void populateSequentialBuilder(final TreeIndexImpl.SequentialBuilder sequentialBuilder) {
        long lastEnd = -1;
        while (!isEmpty()) {
            long firstKey = topStart();
            final long lastKey = topEnd();
            removeTop();

            if (lastKey <= lastEnd) {
                continue;
            }
            if (firstKey <= lastEnd) {
                firstKey = lastEnd + 1;
            }
            sequentialBuilder.appendRange(firstKey, lastKey);
            lastEnd = lastKey;
        }

        reset();
    }

    private TreeIndexImpl getTreeIndexImplInternal() {
        final TreeIndexImpl.SequentialBuilder sequentialBuilder = new TreeIndexImplSequentialBuilder();
        populateSequentialBuilder(sequentialBuilder);
        return sequentialBuilder.getTreeIndexImpl();
    }

    public TreeIndexImpl getTreeIndexImpl() {
        final TreeIndexImpl ix = getTreeIndexImplInternal();
        start = end = null;
        return ix;
    }

    public TreeIndexImpl getTreeIndexImplAndReset() {
        final TreeIndexImpl ix = getTreeIndexImplInternal();
        reset();
        return ix;
    }

    public void addKey(long key) {
        enter(key, key);
    }

    public void addRange(long firstKey, long lastKey) {
        // offensively, we do this in query table
        if (firstKey > lastKey) {
            return;
        }
        enter(firstKey, lastKey);
    }
}
