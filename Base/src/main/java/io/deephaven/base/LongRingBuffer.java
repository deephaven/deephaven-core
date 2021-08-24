/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * A trivial circular buffer for primitive longs, like java.util.concurrent.ArrayBlockingQueue but
 * without all the synchronization and collection cruft. Storage is between head (incl.) and tail
 * (excl.) wrapping around the end of the array. If the buffer is *not* growable, it will make room
 * for a new element by dropping off the oldest element in the buffer instead.
 */
public class LongRingBuffer implements Serializable {

    public static final boolean GROWABLE_NO = false;
    public static final boolean GROWABLE_YES = true;

    protected final boolean growable;
    protected long[] storage;
    protected int head, tail;

    private void grow() {
        if (growable) {
            long[] newStorage = new long[storage.length * 2];
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

    public LongRingBuffer(int capacity) {
        this(capacity, true);
    }

    public LongRingBuffer(int capacity, boolean growable) {
        this.growable = growable;
        this.storage = new long[capacity + 1];
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

    public boolean add(long e) {
        if (isFull()) {
            grow();
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public long addOverwrite(long e, long notFullResult) {
        long result = notFullResult;
        if (isFull()) {
            result = remove();
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return result;
    }

    public boolean offer(long e) {
        if (isFull()) {
            return false;
        }
        storage[tail] = e;
        tail = (tail + 1) % storage.length;
        return true;
    }

    public long remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        long e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public long poll(long onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        long e = storage[head];
        head = (head + 1) % storage.length;
        return e;
    }

    public long element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[head];
    }

    public long peek(long onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[head];
    }

    public long front() {
        return front(0);
    }

    public long front(int offset) {
        if (offset >= size()) {
            throw new NoSuchElementException();
        }
        return storage[(head + offset) % storage.length];
    }

    public long back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return tail == 0 ? storage[storage.length - 1] : storage[tail - 1];
    }

    public long peekBack(long onEmpty) {
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

        public long next() {
            count++;
            return storage[(head + count) % storage.length];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public long[] getAll() {
        int n = size(), h = head;
        long[] result = new long[n];
        for (int i = 0; i < n; ++i) {
            result[i] = storage[h];
            h = (h + 1) % storage.length;
        }
        return result;
    }
}
