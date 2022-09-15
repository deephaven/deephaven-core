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
public class FloatRingBuffer implements Serializable {
    protected final boolean growable;
    protected float[] storage;
    protected int head, tail;

    private void grow() {
        if (growable) {
            float[] newStorage = new float[storage.length * 2];
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
        } else {
            head = (head + 1) % storage.length;
        }
    }

    public boolean isFull() {
        return (tail + 1) % storage.length == head;
    }

    public FloatRingBuffer(int capacity) {
        this(capacity, true);
    }

    public FloatRingBuffer(int capacity, boolean growable) {
        this.growable = growable;
        this.storage = new float[capacity + 1];
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

    public boolean add(float e) {
        if (isFull()) {
            grow();
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public float addOverwrite(float e, float notFullResult) {
        float result = notFullResult;
        if (isFull()) {
            result = remove();
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return result;
    }

    public boolean offer(float e) {
        if (isFull()) {
            return false;
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public float remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        float e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public float poll(float onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        float e = storage[head];
        head = (head + 1) % storage.length;
        return e;
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
        if (offset >= size()) {
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
            return count + 1 < size();
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
        int n = size(), h = head;
        float[] result = new float[n];
        for (int i = 0; i < n; ++i) {
            result[i] = storage[h];
            h = (h + 1) % storage.length;
        }
        return result;
    }
}
