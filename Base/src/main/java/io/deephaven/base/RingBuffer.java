/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A trivial circular buffer, like java.util.concurrent.ArrayBlockingQueue but without all the synchronization and
 * collection cruft.
 */
public class RingBuffer<E> {
    private Object[] storage;
    private int indexMask;
    private int head, tail;

    private void grow() {
        Object[] newStorage = new Object[storage.length << 1];
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
        indexMask = storage.length - 1;
    }

    public boolean isFull() {
        return ((tail + 1) & indexMask) == head;
    }

    public RingBuffer(int capacity) {
        int log2cap = MathUtil.ceilLog2(capacity + 1);
        storage = new Object[1 << log2cap];
        indexMask = storage.length - 1;
        tail = head = 0;
    }

    public boolean isEmpty() {
        return tail == head;
    }

    public int size() {
        return tail >= head ? (tail - head) : (tail + (storage.length - head));
    }

    public void clear() {
        tail = head = 0;
        Arrays.fill(storage, null);
    }

    public int capacity() {
        return storage.length - 1;
    }

    public boolean add(E e) {
        if (isFull()) {
            grow();
        }
        storage[tail] = e;
        tail = (tail + 1) & indexMask;
        return true;
    }

    public boolean addFirst(E e) {
        if (isFull()) {
            grow();
        }
        head = (head - 1) & indexMask;
        storage[head] = e;
        return true;
    }

    public E addOverwrite(E e) {
        E result = null;
        if (isFull()) {
            result = remove();
        }
        storage[tail] = e;
        tail = (tail + 1) & indexMask;
        return result;
    }

    public boolean offer(E e) {
        if (isFull()) {
            return false;
        }
        storage[tail] = e;
        tail = (tail + 1) & indexMask;
        return true;
    }

    public boolean offerFirst(E e) {
        if (isFull()) {
            return false;
        }
        head = (head - 1) & indexMask;
        storage[head] = e;
        return true;
    }

    public E remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        E e = (E) storage[head];
        storage[head] = null;
        head = (head + 1) & indexMask;
        return e;
    }

    public E poll() {
        if (isEmpty()) {
            return null;
        }
        E e = (E) storage[head];
        storage[head] = null;
        head = (head + 1) & indexMask;
        return e;
    }

    public E element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return (E) storage[head];
    }

    public E peek() {
        if (isEmpty()) {
            return null;
        }
        return (E) storage[head];
    }

    public E peek(int offset) {
        if (offset >= size()) {
            return null;
        }
        return (E) storage[(head + offset) & indexMask];
    }


    public E front() {
        return front(0);
    }

    public E front(int offset) {
        if (offset >= size()) {
            throw new NoSuchElementException();
        }
        return (E) storage[(head + offset) & indexMask];
    }

    public E removeAtSwapLast(int offset) {
        if (offset >= size()) {
            throw new NoSuchElementException();
        }
        final int index = (head + offset) & indexMask;
        final E removed = (E) storage[index];
        tail = (tail - 1) & indexMask;
        if (index != tail) {
            storage[index] = storage[tail];
        }
        return removed;
    }

    public E back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return (E) (tail == 0 ? storage[storage.length - 1] : storage[tail - 1]);
    }

    public E peekLast() {
        if (isEmpty()) {
            return null;
        }
        return (E) (tail == 0 ? storage[storage.length - 1] : storage[tail - 1]);
    }

    public E peekLast(int offset) {
        if (offset >= size()) {
            return null;
        }
        return (E) storage[(tail - 1 - offset) & indexMask];
    }

    public Iterator iterator() {
        return new Iterator();
    }

    public class Iterator {
        int count = -1;

        public boolean hasNext() {
            return count + 1 < size();
        }

        public E next() {
            count++;
            return (E) storage[(head + count) & indexMask];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
