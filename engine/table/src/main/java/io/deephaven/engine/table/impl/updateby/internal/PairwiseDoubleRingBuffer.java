/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PairwiseFloatRingBuffer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.NoSuchElementException;

import static io.deephaven.util.QueryConstants.NULL_INT;

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
    /** We are limited to a buffer size exactly a power of two.  Since we store the values and the pairwise tree in the
     * same buffer, we are limited to a max capacity of 2^29 (500M) entries
     */
    private static final int PAIRWISE_MAX_CAPACITY = 1 << 29;
    // use a sized double chunk for underlying storage
    private WritableDoubleChunk<Values> storageChunk;

    private final DoubleFunction pairwiseFunction;
    private final double emptyVal;

    // this measures internal storage capacity (chunk is twice this size)
    private int capacity;
    private int chunkSize;

    private int head;
    private int tail;
    private int size;

    private int dirtyPushHead;
    private int dirtyPushTail;
    private boolean allDirty;
    private int dirtyPopHead;
    private int dirtyPopTail;

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
        Assert.eqTrue(initialSize <= PAIRWISE_MAX_CAPACITY, "PairwiseDoubleRingBuffer initialSize <= PAIRWISE_MAX_CAPACITY");

        // use next larger power of 2
        this.capacity = Integer.highestOneBit(initialSize - 1) << 1;
        this.chunkSize = capacity * 2;
        this.storageChunk = WritableDoubleChunk.makeWritableChunk(chunkSize);
        this.pairwiseFunction = pairwiseFunction;
        this.emptyVal = emptyVal;

        clear();
    }

    @VisibleForTesting
    public static boolean rangesCollapse(final int x1, final int y1, final int x2, final int y2) {
        // Ranges overlap when the start of each range is leq the end of the other.  In this case, we want  to extend
        // detection to when the ranges are consecutive as well.  Do this by adding one to the range ends then compare.
        return x1 <= (y2 + 1) && x2 <= (y1 + 1);
    }

    private void evaluateRangeFast(int start, int end) {
        // everything in this range needs to be reevaluated
        for (int left = start & 0xFFFFFFFE; left <= end; left += 2) {
            final int right = left + 1;
            final int parent = left / 2;

            // load the data values
            final double leftVal = storageChunk.get(left);
            final double rightVal = storageChunk.get(right);

            // compute & store
            final double computeVal = pairwiseFunction.apply(leftVal, rightVal);
            storageChunk.set(parent, computeVal);
        }
    }

    @VisibleForTesting
    public double evaluateTree(int startA, int endA) {
        while (endA > 1) {
            // compute this level
            evaluateRangeFast(startA, endA);

            // compute the new parents
            startA /= 2;
            endA /= 2;
        }
        return storageChunk.get(endA);
    }

    @VisibleForTesting
    public double evaluateTree(int startA, int endA, int startB, int endB) {
        while (endB > 1) {
            if (rangesCollapse(startA, endA, startB, endB)) {
                // all collapse together into a single range
                return evaluateTree(Math.min(startA, startB), Math.max(endA, endB));
            } else {
                // compute this level
                evaluateRangeFast(startA, endA);
                evaluateRangeFast(startB, endB);

                // compute the new parents
                startA /= 2;
                endA /= 2;
                startB /= 2;
                endB /= 2;
            }
        }
        throw Assert.statementNeverExecuted();
    }

    @VisibleForTesting
    public double evaluateTree(int startA, int endA, int startB, int endB, int startC, int endC) {
        while (endC > 1) {
            if (rangesCollapse(startA, endA, startB, endB)) {
                // A and B overlap
                return evaluateTree(Math.min(startA, startB), Math.max(endA, endB), startC, endC);
            } else if (rangesCollapse(startB, endB, startC, endC)) {
                // B and C overlap
                return evaluateTree(startA, endA, Math.min(startB, startC), Math.max(endB, endC));
            } else {
                // no collapse
                evaluateRangeFast(startA, endA);
                evaluateRangeFast(startB, endB);
                evaluateRangeFast(startC, endC);

                // compute the new parents
                startA /= 2;
                endA /= 2;
                startB /= 2;
                endB /= 2;
                startC /= 2;
                endC /= 2;
            }
        }
        throw Assert.statementNeverExecuted();
    }

    public double evaluate() {
        final boolean pushDirty = dirtyPushHead != NULL_INT;
        final boolean popDirty = dirtyPopHead != NULL_INT;

        // This is a nested complex set of `if` statements that are used to set the correct and minimal initial
        // conditions for the evaluation.  The calls to evaluateTree recurse no more than twice (as the ranges
        // overlap and the calculation simplifies).

        final double value;

        if (allDirty) {
            value = evaluateTree(capacity, chunkSize - 1);
        } else if (pushDirty && popDirty) {
            if (dirtyPushHead > dirtyPushTail && dirtyPopHead > dirtyPopTail) {
                // both are wrapped
                value = evaluateTree(capacity, Math.max(dirtyPushTail, dirtyPopTail), Math.min(dirtyPushHead, dirtyPopHead), chunkSize - 1);
            } else if (dirtyPushHead > dirtyPushTail) {
                // push wrapped, pop is not
                value = evaluateTree(capacity, dirtyPushTail, dirtyPopHead, dirtyPopTail, dirtyPushHead, chunkSize - 1);
            } else if (dirtyPopHead > dirtyPopTail) {
                // pop wrapped, push is not
                value = evaluateTree(capacity, dirtyPopTail, dirtyPushHead, dirtyPushTail, dirtyPopHead, chunkSize - 1);
            } else {
                // neither are wrapped, can evaluate directly
                value = evaluateTree(dirtyPushHead, dirtyPushTail, dirtyPopHead, dirtyPopTail);
            }
        } else if (pushDirty) {
            if (dirtyPushHead > dirtyPushTail) {
                // wrapped
                value = evaluateTree(capacity, dirtyPushTail, dirtyPushHead, chunkSize - 1);
            } else {
                value = evaluateTree(dirtyPushHead, dirtyPushTail);
            }
        } else if (popDirty) {
            if (dirtyPopHead > dirtyPopTail) {
                // wrapped
                value = evaluateTree(capacity, dirtyPopTail, dirtyPopHead, chunkSize - 1);
            } else {
                value = evaluateTree(dirtyPopHead, dirtyPopTail);
            }
        } else {
            value = storageChunk.get(1);
        }

        clearDirty();

        return value;
    }

    private void grow(int increase) {
        int oldCapacity = capacity;
        int oldChunkSize = chunkSize;

        // assert that we are not asking for the impossible
        Assert.eqTrue(PAIRWISE_MAX_CAPACITY - increase >= size, "PairwiseDoubleRingBuffer size <= PAIRWISE_MAX_CAPACITY");

        WritableDoubleChunk<Values> oldChunk = storageChunk;

        capacity = Integer.highestOneBit(size + increase - 1) << 1;
        chunkSize = capacity * 2;
        storageChunk = WritableDoubleChunk.makeWritableChunk(chunkSize);

        // fill the pairwise tree (0 to capacity) with empty value
        storageChunk.fillWithValue(0, capacity, emptyVal);

        // move the data to the new chunk, note that we store the ring data in the second half of the array
        final int firstCopyLen = Math.min(oldChunkSize - head, size);

        // do the copying
        storageChunk.copyFromTypedChunk(oldChunk, head, capacity, firstCopyLen);
        storageChunk.copyFromTypedChunk(oldChunk, oldCapacity, capacity + firstCopyLen , size - firstCopyLen);
        tail = capacity + size;

        // fill the unused storage with the empty value
        storageChunk.fillWithValue(tail, chunkSize - tail, emptyVal);

        // free the old data chunk
        oldChunk.close();
        head = capacity;

        // TODO: investigate moving precomputed results also.  Since we are re-ordering the data values, would be
        // tricky to maintain order but a recursive function could probably do it efficiently.  For now, make life easy
        // by setting all input dirty so the tree is recomputed on next `evaluate()`

        // treat all these like pushed data
        dirtyPushHead = head;
        dirtyPushTail = size;

        dirtyPopHead = NULL_INT;
        dirtyPopTail = NULL_INT;
    }

    private void grow() {
        grow(1);
    }

    public void push(double val) {
        if (isFull()) {
            grow();
        }
        pushUnsafe(val);
    }

    public void pushUnsafe(double val) {
        storageChunk.set(tail, val);
        if (dirtyPushHead == NULL_INT) {
            dirtyPushHead = tail;
        } else if (dirtyPushHead == tail) {
            // wrapped around, everything will be dirty until evaluated
            allDirty = true;
        }
        dirtyPushTail = tail;

        // move the tail
        tail = ((tail + 1) % capacity) + capacity;
        size++;
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
        return popUnsafe();
    }

    public double popUnsafe() {
        double val = storageChunk.get(head);
        storageChunk.set(head, emptyVal);

        if (dirtyPopHead == NULL_INT) {
            dirtyPopHead = head;
        } else if (dirtyPopHead == head) {
            // wrapped around, everything will be dirty until evaluated
            allDirty = true;
        }
        dirtyPopTail = head;


        // move the head
        head = ((head + 1) % capacity) + capacity;
        size--;
        return val;
    }

    public double[] pop(int count) {
        if (size() < count) {
            throw new NoSuchElementException();
        }
        final double[] result = new double[count];

        final int firstCopyLen = Math.min(chunkSize - head, count);
        storageChunk.copyToArray(head, result, 0, firstCopyLen);
        storageChunk.fillWithValue(head, firstCopyLen, emptyVal);
        storageChunk.copyToArray(capacity, result, firstCopyLen, count - firstCopyLen);
        storageChunk.fillWithValue(capacity, count - firstCopyLen, emptyVal);

        if (dirtyPopHead == NULL_INT) {
            dirtyPopHead = head;
        }
        dirtyPopTail = ((head + count - 1) % capacity) + capacity;

        // move the head
        head = ((head + count) % capacity) + capacity;
        size -= count;
        return result;
    }

    public boolean isFull() {
        return size == capacity;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
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
        return popUnsafe();
    }

    public double front() {
        return front(0);
    }

    public double front(int offset) {
        if (offset < 0 || offset >= size) {
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
        return capacity;
    }

    public int remaining() {
        return capacity - size;
    }

    private void clearDirty() {
        dirtyPushHead = dirtyPopHead = NULL_INT;
        dirtyPushTail = dirtyPopTail = NULL_INT;

        allDirty = false;
    }

    public void clear() {
        // fill with the empty value
        storageChunk.fillWithValue(0, chunkSize, emptyVal);

        head = tail = capacity;
        size = 0;

        clearDirty();
    }

    @Override
    public void close() {
        try (final WritableDoubleChunk<Values> ignoredChunk = storageChunk) {
            // close the closable items and assign null
            storageChunk = null;
        }
    }
}
