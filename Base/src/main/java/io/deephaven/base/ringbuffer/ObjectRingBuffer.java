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
public class ObjectRingBuffer implements Serializable {
    protected final boolean growable;
    protected Object[] storage;
    protected int head, tail;

    private void grow() {
        if (growable) {
            Object[] newStorage = new Object[storage.length * 2];
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

    public ObjectRingBuffer(int capacity) {
        this(capacity, true);
    }

    public ObjectRingBuffer(int capacity, boolean growable) {
        this.growable = growable;
        this.storage = new Object[capacity + 1];
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

    public boolean add(Object e) {
        if (isFull()) {
            grow();
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public Object addOverwrite(Object e, Object notFullResult) {
        Object result = notFullResult;
        if (isFull()) {
            result = remove();
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return result;
    }

    public boolean offer(Object e) {
        if (isFull()) {
            return false;
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public Object remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        Object e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public Object poll(Object onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        Object e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public Object element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[head];
    }

    public Object peek(Object onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[head];
    }

    public Object front() {
        return front(0);
    }

    public Object front(int offset) {
        if (offset >= size()) {
            throw new NoSuchElementException();
        }
        return storage[(head + offset) % storage.length];
    }

    public Object back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return tail == 0 ? storage[storage.length - 1] : storage[tail - 1];
    }

    public Object peekBack(Object onEmpty) {
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

        public Object next() {
            count++;
            return storage[(head + count) % storage.length];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public Object[] getAll() {
        int n = size(), h = head;
        Object[] result = new Object[n];
        for (int i = 0; i < n; ++i) {
            result[i] = storage[h];
            h = (h + 1) % storage.length;
        }
        return result;
    }
}
