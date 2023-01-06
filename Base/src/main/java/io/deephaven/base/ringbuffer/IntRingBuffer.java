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
public class IntRingBuffer implements Serializable {
    protected final boolean growable;
    protected int[] storage;
    protected int head, tail;

    private void grow() {
        if (growable) {
            int[] newStorage = new int[storage.length * 2];
            if (tail > head) {
                System.arraycopy(storage, head, newStorage, 0, tail - head);
                tail = tail - head;
            } else {
                System.arraycopy(storage, head, newStorage, 0, storage.length - head);
                System.arraycopy(storage, 0, newStorage, storage.length - head, tail);
                tail += storage.length - head;
            }
            head = 0;
            storage = newStorage;
        }
    }

    public boolean isFull() {
        return (tail + 1) % storage.length == head;
    }

    /**
     * Create an unbounded-growth ring buffer of int primitives
     *
     * @param capacity minimum capacity of ring buffer
     */
    public IntRingBuffer(int capacity) {
        this(capacity, true);
    }

    /**
     * Create a ring buffer of int primitives
     *
     * @param capacity minimum capacity of ring buffer
     * @param growable whether to allow growth when the buffer is full.
     */
    public IntRingBuffer(int capacity, boolean growable) {
        this.growable = growable;
        this.storage = new int[capacity + 1];
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
     * {@link #offer(int)}
     *
     * @param e the int to be added to the buffer
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     * @return {@code true} if the int was added successfully
     */
    public boolean add(int e) {
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
     * Add an entry to the ring buffer. If the buffer is full, will overwrite the oldest entry with the new one.
     *
     * @param e the int to be added to the buffer
     * @param notFullResult value to return is the buffer is not full
     * @return the overwritten entry if the buffer is full, the provided value otherwise
     */
    public int addOverwrite(int e, int notFullResult) {
        int result = notFullResult;
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
     * @param e the int to be added to the buffer
     * @return true if the value was added successfully, false otherwise
     */
    public boolean offer(int e) {
        if (isFull()) {
            return false;
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public int remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        int e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public int poll(int onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        int e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public int element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[head];
    }

    public int peek(int onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[head];
    }

    public int front() {
        return front(0);
    }

    public int front(int offset) {
        if (offset >= size()) {
            throw new NoSuchElementException();
        }
        return storage[(head + offset) % storage.length];
    }

    public int back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return tail == 0 ? storage[storage.length - 1] : storage[tail - 1];
    }

    public int peekBack(int onEmpty) {
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

        public int next() {
            count++;
            return storage[(head + count) % storage.length];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public int[] getAll() {
        int[] result = new int[size()];
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
