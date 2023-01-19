/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBuffer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.base.ringbuffer;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * A trivial circular buffer for primitive values, like java.util.concurrent.ArrayBlockingQueue but without all the
 * synchronization and collection cruft. Storage is between head (incl.) and tail (excl.) wrapping around the end of the
 * array. If the buffer is *not* growable, it will make room for a new element by dropping off the oldest element in the
 * buffer instead.
 */
public class DoubleRingBuffer implements Serializable {
    protected final boolean growable;
    protected double[] storage;
    protected int head, tail;

    private void grow(int increase) {
        if (growable) {
            final int size = size();

            final int minLength = size + increase + 1;
            int newLength = storage.length * 2;
            while (newLength < minLength) {
                newLength = newLength * 2;
            }
            double[] newStorage = new double[newLength];
            if (tail >= head) {
                System.arraycopy(storage, head, newStorage, 0, size);
            } else {
                final int firstCopyLen = storage.length - head;
                System.arraycopy(storage, head, newStorage, 0, firstCopyLen);
                System.arraycopy(storage, 0, newStorage, firstCopyLen, size - firstCopyLen);
            }
            tail = size;
            head = 0;
            storage = newStorage;
        }
    }

    private void grow() {
        grow(1);
    }

    public boolean isFull() {
        return (tail + 1) % storage.length == head;
    }

    /**
     * Create an unbounded-growth ring buffer of double primitives
     *
     * @param capacity minimum capacity of ring buffer
     */
    public DoubleRingBuffer(int capacity) {
        this(capacity, true);
    }

    /**
     * Create a ring buffer of double primitives
     *
     * @param capacity minimum capacity of ring buffer
     * @param growable whether to allow growth when the buffer is full.
     */
    public DoubleRingBuffer(int capacity, boolean growable) {
        this.growable = growable;
        this.storage = new double[capacity + 1];
        this.tail = this.head = 0;
    }

    public boolean isEmpty() {
        return tail == head;
    }

    public int size() {
        return tail >= head ? (tail - head) : (tail + (storage.length - head));
    }

    public int capacity() {
        return storage.length - 1;
    }

    public int remaining() {
        return capacity() - size();
    }

    public void clear() {
        tail = head = 0;
    }

    /**
     * Adds an entry to the ring buffer, will throw an exception if buffer is full. For a graceful failure, use
     * {@link #offer(double)}
     *
     * @param e the double to be added to the buffer
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     * @return {@code true} if the double was added successfully
     */
    public boolean add(double e) {
        if (isFull()) {
            if (!growable) {
                throw new UnsupportedOperationException("Ring buffer is full and growth is disabled");
            } else {
                grow();
            }
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    /**
     * Ensure that there is sufficient empty space to store {@code count} items in the buffer. If the buffer is
     * {@code growable}, this may result in an internal growth operation. This call should be used in conjunction with
     * {@link #addUnsafe(double)}.
     *
     * @param count the amount of empty entries in the buffer after this call
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
     * conjunction with * {@link #ensureRemaining(int)}.
     *
     * @param e the value to add to the buffer
     */
    public void addUnsafe(double e) {
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
    }

    /**
     * Add an entry to the ring buffer. If the buffer is full, will overwrite the oldest entry with the new one.
     *
     * @param e the double to be added to the buffer
     * @param notFullResult value to return is the buffer is not full
     * @return the overwritten entry if the buffer is full, the provided value otherwise
     */
    public double addOverwrite(double e, double notFullResult) {
        double result = notFullResult;
        if (isFull()) {
            result = remove();
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return result;
    }

    /**
     * Attempt to add an entry to the ring buffer. If the buffer is full, the write will fail and the buffer will not
     * grow even if allowed.
     *
     * @param e the double to be added to the buffer
     * @return true if the value was added successfully, false otherwise
     */
    public boolean offer(double e) {
        if (isFull()) {
            return false;
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public double[] remove(int count) {
        if (size() < count) {
            throw new NoSuchElementException();
        }
        final double[] result = new double[count];
        final int firstCopyLen = storage.length - head;

        if (tail >= head || firstCopyLen >= count) {
            System.arraycopy(storage, head, result, 0, count);
        } else {
            System.arraycopy(storage, head, result, 0, firstCopyLen);
            System.arraycopy(storage, 0, result, firstCopyLen, count - firstCopyLen);
        }
        head = (head + count) % storage.length;
        return result;
    }

    public double remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        double e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public double removeUnsafe() {
        double e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public double poll(double onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        double e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public double element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[head];
    }

    public double peek(double onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[head];
    }

    public double front() {
        return front(0);
    }

    public double front(int offset) {
        if (offset >= size()) {
            throw new NoSuchElementException();
        }
        return storage[(head + offset) % storage.length];
    }

    public double back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return tail == 0 ? storage[storage.length - 1] : storage[tail - 1];
    }

    public double peekBack(double onEmpty) {
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
            return count + 1 < size();
        }

        public double next() {
            count++;
            return storage[(head + count) % storage.length];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public double[] getAll() {
        double[] result = new double[size()];
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
