/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PairwiseFloatRingBuffer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import gnu.trove.list.array.TIntArrayList;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.SafeCloseable;

import java.util.NoSuchElementException;

/***
 * Store this data in the form of a binary tree where the latter half of the chunk is treated as a ring buffer and
 * pairwise results of the `DoubleFunction` are stored in the parent nodes.  We do lazy evaluation by maintaining a
 * 'dirty' index list and computing the ultimate pairwise result only when requested by `evaluate()'
 *
 * To keep the parent-node finding math easy and consistent between the ring buffer and the computation tree, the binary
 * tree is shifted by one index so the root (and final result of computation) ends up in index 1 (instead of 0 which is
 * un-used)
 */

public class PairwiseDoubleRingBuffer implements SafeCloseable {
    // use a sized double chunk for underlying storage
    private WritableDoubleChunk<Values> storageChunk;
    private final TIntArrayList dirtyIndices;
    private boolean allDirty;

    private final DoubleFunction pairwiseFunction;
    private final double emptyVal;

    // this measures internal storage capacity (chunk is twice this size)
    private int capacity;
    private int chunkSize;

    private int head;
    private int tail;

    @FunctionalInterface
    public interface DoubleFunction {
        /**
         * Applies this function to the given arguments.
         *
         * @param a the first function argument
         * @param b the second function argument
         * @return the function result
         */
        double apply(double a, double b);
    }

    /**
     * Create a ring buffer for double values that will perform pairwise evaluation of the internal data values using
     * an efficient binary-tree implementation to compute only changed values.  The buffer will grow exponentially as
     * items are pushed into it but will not shrink as values are removed
     *
     * @param initialSize the minimum size for the structure to hold
     * @param emptyVal an innocuous value that will not affect the user-provided function results. for example, 0.0f
     *                 for performing addition/subtraction, 1.0f for performing multiplication/division
     * @param pairwiseFunction the user provided function for evaluation, takes two double parameters and returns a
     *                         double. This function will be applied repeatedly to pairs of data values until the final
     *                         result is available
     */
    public PairwiseDoubleRingBuffer(int initialSize, double emptyVal, DoubleFunction pairwiseFunction) {
        // increase to next power of two
        this.capacity = Integer.highestOneBit(initialSize) * 2;
        this.chunkSize = capacity * 2;
        this.storageChunk = WritableDoubleChunk.makeWritableChunk(chunkSize);
        this.dirtyIndices = new TIntArrayList(chunkSize);
        this.pairwiseFunction = pairwiseFunction;
        this.emptyVal = emptyVal;

        this.storageChunk.fillWithValue(0, chunkSize, emptyVal);
        this.head = this.tail = this.capacity;
        this.allDirty = false;
    }

    private void evaluateRangeFast(int start, int end) {
        // everything in this range needs to be reevaluated
        for (int left = start & 0xFFFFFFFE; left < end; left += 2) {
            final int right = left + 1;
            final int parent = left / 2;

            // load the data values
            final double leftVal = storageChunk.get(left);
            final double rightVal = storageChunk.get(right);

            // compute & store
            final double computeVal = pairwiseFunction.apply(leftVal, rightVal);
            storageChunk.set(parent, computeVal);

            // mark the parent dirty
            dirtyIndices.add(parent);
        }
    }

    public double evaluate() {
        // if all dirty, recompute all values
        if (allDirty) {
            if (head < tail) {
                evaluateRangeFast(head, tail);
            } else {
                evaluateRangeFast(head, chunkSize);
                evaluateRangeFast(capacity, tail);
            }
        }

        // work through all the dirty bits from high to low until none remain.
        int dirtyIndex = 0;
        while (dirtyIndex < dirtyIndices.size()) {
            final int left = dirtyIndices.get(dirtyIndex) & 0xFFFFFFFE; // clear the final bit to force evenness
            final int right = left + 1;

            // this isn't the typical parent = (n-1)/2 because the tree is right-shifted by one
            final int parent = left / 2;

            // load the data values
            final double leftVal = storageChunk.get(left);
            final double rightVal = storageChunk.get(right);
            final double parentVal = storageChunk.get(parent);

            final double computeVal = pairwiseFunction.apply(leftVal, rightVal);
            if (parentVal != computeVal) {
                storageChunk.set(parent, computeVal);
                // mark the parent dirty (if not the last)
                if (parent > 1) {
                    dirtyIndices.add(parent);
                }
            }
            // how far should we advance
            final int nextIndex = dirtyIndex + 1;
            if (nextIndex < dirtyIndices.size() && dirtyIndices.get(nextIndex) == right) {
                dirtyIndex += 2;
            } else {
                dirtyIndex++;
            }
        }
        allDirty = false;
        dirtyIndices.clear();

        // final value is in index 1
        return storageChunk.get(1);
    }

    private void grow(int increase) {
        int oldCapacity = capacity;
        int oldChunkSize = chunkSize;

        int size = size();

        final int minLength = size + increase;

        // double the current capacity until there is sufficient space for the increase
        while (capacity <= minLength) {
            capacity *= 2;
            chunkSize = capacity * 2;
        }

        // transfer to the new chunk
        WritableDoubleChunk<Values> oldChunk = storageChunk;
        storageChunk = WritableDoubleChunk.makeWritableChunk(chunkSize);

        // fill the pairwise tree (0 to capacity) with empty value
        storageChunk.fillWithValue(0, capacity, emptyVal);

        // move the data to the new chunk, note that we store the ring data in the second half of the array

        if (tail >= head) {
            storageChunk.copyFromTypedChunk(oldChunk, head, capacity, size);
        } else {
            final int firstCopyLen = oldChunkSize - head;
            storageChunk.copyFromTypedChunk(oldChunk, head, capacity, firstCopyLen);
            storageChunk.copyFromTypedChunk(oldChunk, oldCapacity, capacity + firstCopyLen , size - firstCopyLen);
        }
        tail = capacity + size;

        // fill the unused storage with the empty value
        storageChunk.fillWithValue(tail, chunkSize - tail, emptyVal);

        // free the old data chunk
        oldChunk.close();
        head = capacity;

        // TODO: investigate moving precomputed results also.  Since we are re-ordering the data values, would be
        // tricky to maintain order but a recursive function could probably do it efficiently.  For now, make life easy
        // by setting all input dirty so the tree is recomputed on next `evaluate()`
        this.dirtyIndices.clear();
        allDirty = true;
    }

    private void grow() {
        grow(1);
    }

    public void push(double val) {
        if (isFull()) {
            grow();
        }
        // add the new data
        storageChunk.set(tail, val);
        if (!allDirty) {
            dirtyIndices.add(tail);
        }

        // move the tail
        tail = ((tail + 1) % capacity) + capacity;
    }

    public void pushUnsafe(double val) {
        // add the new data
        storageChunk.set(tail, val);
        if (!allDirty) {
            dirtyIndices.add(tail);
        }

        // move the tail
        tail = ((tail + 1) % capacity) + capacity;
    }

    /**
     * Ensure that there is sufficient empty space to store {@code count} items in the buffer. If the buffer is
     * {@code growable}, this may result in an internal growth operation. This call should be used in conjunction with
     * {@link #pushUnsafe(double)}.
     *
     * @param count the amount of empty entries in the buffer after this call
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     */
    public void ensureRemaining(int count) {
        if (remaining() < count) {
            grow(count);
        }
    }

    public void pushEmptyValue() {
        push(emptyVal);
    }

    public double pop() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        double val = storageChunk.get(head);
        storageChunk.set(head, emptyVal);
        if (!allDirty) {
            dirtyIndices.add(head);
        }

        // move the head
        head = ((head + 1) % capacity) + capacity;
        return val;
    }

    public double popUnsafe() {
        double val = storageChunk.get(head);
        storageChunk.set(head, emptyVal);
        if (!allDirty) {
            dirtyIndices.add(head);
        }

        // move the head
        head = ((head + 1) % capacity) + capacity;
        return val;
    }

    public double[] pop(int count) {
        if (size() < count) {
            throw new NoSuchElementException();
        }
        final double[] result = new double[count];
        final int firstCopyLen = chunkSize - head;

        if (tail > head || firstCopyLen >= count) {
            storageChunk.copyToArray(head, result, 0, count);
            storageChunk.fillWithValue(head, count, emptyVal);
            if (!allDirty) {
                for (int ii = 0; ii < count; ii++) {
                    dirtyIndices.add(head + ii);
                }
            }
        } else {
            storageChunk.copyToArray(head, result, 0, firstCopyLen);
            storageChunk.fillWithValue(head, firstCopyLen, emptyVal);
            storageChunk.copyToArray(capacity, result, firstCopyLen, count - firstCopyLen);
            storageChunk.fillWithValue(capacity, count - firstCopyLen, emptyVal);
            if (!allDirty) {
                for (int ii = 0; ii < firstCopyLen; ii++) {
                    dirtyIndices.add(head + ii);
                }
                for (int ii = 0; ii < count - firstCopyLen; ii++) {
                    dirtyIndices.add(capacity + ii);
                }
            }
        }

        // move the head
        head = ((head + count) % capacity) + capacity;
        return result;
    }

    public boolean isFull() {
        return ((tail + 1) % capacity) + capacity == head;
    }

    public int size() {
        return tail >= head
                ? (tail - head) :
                (tail + (capacity - head));
    }

    public boolean isEmpty() {
        return tail == head;
    }

    public double peek(double onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storageChunk.get(head);
    }

    public double poll(double onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        double e = storageChunk.get(head);
        head = (head + 1) % capacity + capacity;
        return e;
    }

    public double front() {
        return front(0);
    }

    public double front(int offset) {
        if (offset < 0 || offset >= size()) {
            throw new NoSuchElementException();
        }
        return storageChunk.get((head + offset) % capacity + capacity);
    }

    public double back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return tail == capacity ? storageChunk.get(chunkSize - 1) : storageChunk.get(tail - 1);
    }

    public double peekBack(double onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return tail == capacity ? storageChunk.get(chunkSize - 1) : storageChunk.get(tail - 1);
    }

    public double element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storageChunk.get(head);
    }

    public int capacity() {
        return capacity - 1;
    }

    public int remaining() {
        return capacity() - size();
    }

    public void clear() {
        head = tail = capacity;
        dirtyIndices.clear();
        allDirty = false;
    }

    @Override
    public void close() {
        try (final WritableDoubleChunk<Values> ignoredChunk = storageChunk) {
            // close the closable items
        }
    }
}
