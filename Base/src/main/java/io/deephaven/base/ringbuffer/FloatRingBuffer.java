/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBuffer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.base.ringbuffer;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Assert;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * A trivial circular buffer for primitive values, like java.util.concurrent.ArrayBlockingQueue but without all the
 * synchronization and collection cruft. Storage is between head (incl.) and tail (excl.) wrapping around the end of the
 * array. If the buffer is *not* growable, it will make room for a new element by dropping off the oldest element in the
 * buffer instead.
 */
public class FloatRingBuffer implements Serializable {
    protected final boolean growable;
    protected float[] storage;
    protected int head, tail, size;

    private void grow(int increase) {
        if (growable) {
            // assert that we are not asking for the impossible
            Assert.eqTrue(ArrayUtil.MAX_ARRAY_SIZE - increase >= size, "FloatRingBuffer size <= MAX_ARRAY_SIZE");

            // make sure we cap out at ArrayUtil.MAX_ARRAY_SIZE
            final int newLength =
                    Math.toIntExact(Math.min(ArrayUtil.MAX_ARRAY_SIZE, Long.highestOneBit(size + increase - 1) << 1));
            float[] newStorage = new float[newLength];

            // three scenarios: size is zero so nothing to copy, head is before tail so only one copy needed, head
            // after tail so two copies needed. Make two calls for simplicity and branch-prediction friendliness.

            // compute the size of the first copy
            final int firstCopyLen = Math.min(storage.length - head, size);

            // do the copying
            System.arraycopy(storage, head, newStorage, 0, firstCopyLen);
            System.arraycopy(storage, 0, newStorage, firstCopyLen, size - firstCopyLen);

            // reset the pointers
            tail = size;
            head = 0;
            storage = newStorage;
        }
    }

    private void grow() {
        grow(1);
    }

    public boolean isFull() {
        return size == storage.length;
    }

    /**
     * Create an unbounded-growth ring buffer of float primitives
     *
     * @param capacity minimum capacity of ring buffer
     */
    public FloatRingBuffer(int capacity) {
        this(capacity, true);
    }

    /**
     * Create a ring buffer of float primitives
     *
     * @param capacity minimum capacity of ring buffer
     * @param growable whether to allow growth when the buffer is full.
     */
    public FloatRingBuffer(int capacity, boolean growable) {
        Assert.eqTrue(capacity <= ArrayUtil.MAX_ARRAY_SIZE, "FloatRingBuffer size <= MAX_ARRAY_SIZE");

        this.growable = growable;
        if (growable) {
            // use next larger power of 2
            storage = new float[Integer.highestOneBit(capacity - 1) << 1];
        } else {
            // might as well use exact size and not over-allocate
            storage = new float[capacity];
        }

        tail = head = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return storage.length;
    }

    public int remaining() {
        return storage.length - size;
    }

    public void clear() {
        size = tail = head = 0;
    }

    /**
     * Adds an entry to the ring buffer, will throw an exception if buffer is full. For a graceful failure, use
     * {@link #offer(float)}
     *
     * @param e the float to be added to the buffer
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     * @return {@code true} if the float was added successfully
     */
    public boolean add(float e) {
        if (isFull()) {
            if (!growable) {
                throw new UnsupportedOperationException("Ring buffer is full and growth is disabled");
            } else {
                grow();
            }
        }
        addUnsafe(e);
        return true;
    }

    /**
     * Ensure that there is sufficient empty space to store {@code count} items in the buffer. If the buffer is
     * {@code growable}, this may result in an internal growth operation. This call should be used in conjunction with
     * {@link #addUnsafe(float)}.
     *
     * @param count the minimum number of empty entries in the buffer after this call
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     */
    public void ensureRemaining(int count) {
        if (remaining() < count) {
            if (!growable) {
                throw new UnsupportedOperationException("Ring buffer is full and growth is disabled");
            } else {
                grow(count);
            }
        }
    }

    /**
     * Add values unsafely (will silently overwrite values if the buffer is full). This call should be used in
     * conjunction with {@link #ensureRemaining(int)}.
     *
     * @param e the value to add to the buffer
     */
    public void addUnsafe(float e) {
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        size++;
    }

    /**
     * Add an entry to the ring buffer. If the buffer is full, will overwrite the oldest entry with the new one.
     *
     * @param e the float to be added to the buffer
     * @param notFullResult value to return is the buffer is not full
     * @return the overwritten entry if the buffer is full, the provided value otherwise
     */
    public float addOverwrite(float e, float notFullResult) {
        float result = notFullResult;
        if (isFull()) {
            result = remove();
        }
        addUnsafe(e);
        return result;
    }

    /**
     * Attempt to add an entry to the ring buffer. If the buffer is full, the write will fail and the buffer will not
     * grow even if allowed.
     *
     * @param e the float to be added to the buffer
     * @return true if the value was added successfully, false otherwise
     */
    public boolean offer(float e) {
        if (isFull()) {
            return false;
        }
        addUnsafe(e);
        return true;
    }

    public float[] remove(int count) {
        if (size < count) {
            throw new NoSuchElementException();
        }
        final float[] result = new float[count];
        final int firstCopyLen = storage.length - head;

        if (tail >= head || firstCopyLen >= count) {
            System.arraycopy(storage, head, result, 0, count);
        } else {
            System.arraycopy(storage, head, result, 0, firstCopyLen);
            System.arraycopy(storage, 0, result, firstCopyLen, count - firstCopyLen);
        }
        head = (head + count) % storage.length;
        size -= count;
        return result;
    }

    public float remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return removeUnsafe();
    }

    public float removeUnsafe() {
        float e = storage[head];
        head = (head + 1) % storage.length;
        size--;
        return e;
    }

    public float poll(float onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return removeUnsafe();
    }

    public float element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[head];
    }

    public float peek(float onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[head];
    }

    public float front() {
        return front(0);
    }

    public float front(int offset) {
        if (offset >= size) {
            throw new NoSuchElementException();
        }
        return storage[(head + offset) % storage.length];
    }

    public float back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return tail == 0 ? storage[storage.length - 1] : storage[tail - 1];
    }

    public float peekBack(float onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return tail == 0 ? storage[storage.length - 1] : storage[tail - 1];
    }

    public Iterator iterator() {
        return new Iterator();
    }

    public class Iterator {
        int count = -1;

        public boolean hasNext() {
            return count + 1 < size;
        }

        public float next() {
            count++;
            return storage[(head + count) % storage.length];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public float[] getAll() {
        float[] result = new float[size];
        if (result.length > 0) {
            if (tail > head) {
                System.arraycopy(storage, head, result, 0, tail - head);
            } else {
                System.arraycopy(storage, head, result, 0, storage.length - head);
                System.arraycopy(storage, 0, result, storage.length - head, tail);
            }
        }
        return result;
    }
}
