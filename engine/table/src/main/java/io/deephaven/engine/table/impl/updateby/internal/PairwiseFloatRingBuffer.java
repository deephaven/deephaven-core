/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.NoSuchElementException;

import static io.deephaven.util.QueryConstants.NULL_INT;

/***
 * Store this data in the form of a binary tree where the latter half of the chunk is treated as a ring buffer and
 * pairwise results of the `FloatFunction` are stored in the parent nodes.  We do lazy evaluation by maintaining a
 * 'dirty' index list and computing the ultimate pairwise result only when requested by `evaluate()'
 *
 * To keep the parent-node finding math easy and consistent between the ring buffer and the computation tree, the binary
 * tree is shifted by one index so the root (and final result of computation) ends up in index 1 (instead of 0 which is
 * un-used)
 */

public class PairwiseFloatRingBuffer implements SafeCloseable {
    /** We are limited to a buffer size exactly a power of two.  Since we store the values and the pairwise tree in the
     * same buffer, we are limited to a max capacity of 2^29 (500M) entries
     */
    private static final int PAIRWISE_MAX_CAPACITY = 1 << 29;
    // use a sized float chunk for underlying storage
    private WritableFloatChunk<Values> storageChunk;

    private final FloatFunction pairwiseFunction;
    private final float emptyVal;

    // this measures internal storage capacity (chunk is twice this size)
    private int capacity;
    private int chunkSize;

    private int head;
    private int tail;
    private int size;

    private int dirtyPushHead;
    private int dirtyPushTail;
    private int dirtyPopHead;
    private int dirtyPopTail;

    @FunctionalInterface
    public interface FloatFunction {
        /**
         * Applies this function to the given arguments.
         *
         * @param a the first function argument
         * @param b the second function argument
         * @return the function result
         */
        float apply(float a, float b);
    }

    /**
     * Create a ring buffer for float values that will perform pairwise evaluation of the internal data values using
     * an efficient binary-tree implementation to compute only changed values.  The buffer will grow exponentially as
     * items are pushed into it but will not shrink as values are removed
     *
     * @param initialSize the minimum size for the structure to hold
     * @param emptyVal an innocuous value that will not affect the user-provided function results. for example, 0.0f
     *                 for performing addition/subtraction, 1.0f for performing multiplication/division
     * @param pairwiseFunction the user provided function for evaluation, takes two float parameters and returns a
     *                         float. This function will be applied repeatedly to pairs of data values until the final
     *                         result is available
     */
    public PairwiseFloatRingBuffer(int initialSize, float emptyVal, FloatFunction pairwiseFunction) {
        Assert.eqTrue(initialSize <= PAIRWISE_MAX_CAPACITY, "PairwiseFloatRingBuffer initialSize <= PAIRWISE_MAX_CAPACITY");

        // use next larger power of 2
        this.capacity = Integer.highestOneBit(initialSize - 1) << 1;
        this.chunkSize = capacity * 2;
        this.storageChunk = WritableFloatChunk.makeWritableChunk(chunkSize);
        this.pairwiseFunction = pairwiseFunction;
        this.emptyVal = emptyVal;

        clear();
    }

    private void evaluateRangeFast(int start, int end) {
        // everything in this range needs to be reevaluated
        for (int left = start & 0xFFFFFFFE; left <= end; left += 2) {
            final int right = left + 1;
            final int parent = left / 2;

            // load the data values
            final float leftVal = storageChunk.get(left);
            final float rightVal = storageChunk.get(right);

            // compute & store
            final float computeVal = pairwiseFunction.apply(leftVal, rightVal);
            storageChunk.set(parent, computeVal);
        }
    }

    @VisibleForTesting
    public float evaluateTree(int startA, int endA) {
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
    public float evaluateTree(int startA, int endA, int startB, int endB) {
        while (endB > 1) {
            if (endA >= startB - 1) {
                // all collapse together into a single range
                return evaluateTree(startA, endB);
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
    public float evaluateTree(int startA, int endA, int startB, int endB, int startC, int endC) {
        while (endC > 1) {
            if (endA >= startC - 1 || (endA >= startB - 1 && endB >= startC - 1)) {
                // all collapse together into a single range
                return evaluateTree(startA, endC);
            } else if (endA >= startB - 1) {
                // A and B collapse
                return evaluateTree(startA, endB, startC, endC);
            } else if (endB >= startC - 1) {
                // B and C collapse
                return evaluateTree(startA, endA, startB, endC);
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

    public float evaluate() {
        final boolean pushDirty = dirtyPushHead != NULL_INT;
        final boolean popDirty = dirtyPopHead != NULL_INT;

        // This is a nested complex set of `if` statements that are used to set the correct and minimal initial
        // conditions for the evaluation.  The calls to evaluateTree recurse no more than twice (as the ranges
        // overlap and the calculation simplifies).

        final float value;

        if (pushDirty && popDirty) {
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
                // neither wrapped
                if (dirtyPushHead > dirtyPopHead) {
                    value = evaluateTree(dirtyPopHead, dirtyPopTail, dirtyPushHead, dirtyPushTail);
                } else {
                    value = evaluateTree(dirtyPushHead, dirtyPushTail, dirtyPopHead, dirtyPopTail);
                }
            }
        } else if (pushDirty) {
            if (dirtyPushHead > dirtyPushTail) {
                value = evaluateTree(capacity, dirtyPushTail, dirtyPushHead, chunkSize - 1);
            } else {
                value = evaluateTree(dirtyPushHead, dirtyPushTail);
            }
        } else if (popDirty) {
            if (dirtyPopHead > dirtyPopTail) {
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
        Assert.eqTrue(PAIRWISE_MAX_CAPACITY - increase >= size, "PairwiseFloatRingBuffer size <= PAIRWISE_MAX_CAPACITY");

        WritableFloatChunk<Values> oldChunk = storageChunk;

        capacity = Integer.highestOneBit(size + increase - 1) << 1;
        chunkSize = capacity * 2;
        storageChunk = WritableFloatChunk.makeWritableChunk(chunkSize);

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

    public void push(float val) {
        if (isFull()) {
            grow();
        }
        pushUnsafe(val);
    }

    public void pushUnsafe(float val) {
        storageChunk.set(tail, val);
        if (dirtyPushHead == NULL_INT) {
            dirtyPushHead = tail;
        }
        dirtyPushTail = tail;

        // move the tail
        tail = ((tail + 1) % capacity) + capacity;
        size++;
    }

    /**
     * Ensure that there is sufficient empty space to store {@code count} items in the buffer. If the buffer is
     * {@code growable}, this may result in an internal growth operation. This call should be used in conjunction with
     * {@link #pushUnsafe(float)}.
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

    public float pop() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return popUnsafe();
    }

    public float popUnsafe() {
        float val = storageChunk.get(head);
        storageChunk.set(head, emptyVal);

        if (dirtyPopHead == NULL_INT) {
            dirtyPopHead = head;
        }
        dirtyPopTail = head;

        // move the head
        head = ((head + 1) % capacity) + capacity;
        size--;
        return val;
    }

    public float[] pop(int count) {
        if (size() < count) {
            throw new NoSuchElementException();
        }
        final float[] result = new float[count];

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

    public float peek(float onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storageChunk.get(head);
    }

    public float poll(float onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return popUnsafe();
    }

    public float front() {
        return front(0);
    }

    public float front(int offset) {
        if (offset < 0 || offset >= size) {
            throw new NoSuchElementException();
        }
        return storageChunk.get((head + offset) % capacity + capacity);
    }

    public float back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return tail == capacity ? storageChunk.get(chunkSize - 1) : storageChunk.get(tail - 1);
    }

    public float peekBack(float onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return tail == capacity ? storageChunk.get(chunkSize - 1) : storageChunk.get(tail - 1);
    }

    public float element() {
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
        try (final WritableFloatChunk<Values> ignoredChunk = storageChunk) {
            // close the closable items and assign null
            storageChunk = null;
        }
    }
}
