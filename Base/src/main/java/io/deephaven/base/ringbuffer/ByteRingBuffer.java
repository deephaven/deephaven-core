/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBuffer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.base.ringbuffer;

import io.deephaven.base.verify.Assert;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * A simple circular buffer for primitive values, like java.util.concurrent.ArrayBlockingQueue but without the
 * synchronization and collection overhead. Storage is between head (inclusive) and tail (exclusive) using incrementing
 * {@code long} values. Head and tail will not wrap around; instead we use storage arrays sized to 2^N to allow fast
 * determination of storage indices through a mask operation.
 */
public class ByteRingBuffer implements Serializable {
    /** Maximum capacity is the highest power of two that can be allocated (i.e. <= than ArrayUtil.MAX_ARRAY_SIZE). */
    private final int RING_BUFFER_MAX_CAPACITY = 1 << 30; // ~1B entries
    private final boolean growable;
    private byte[] storage;
    private int mask;
    private long head;
    private long tail;

    /**
     * Create an unbounded-growth ring buffer of byte primitives.
     *
     * @param capacity minimum capacity of the ring buffer
     */
    public ByteRingBuffer(int capacity) {
        this(capacity, true);
    }

    /**
     * Create a ring buffer of byte primitives.
     *
     * @param capacity minimum capacity of ring buffer
     * @param growable whether to allow growth when the buffer is full.
     */
    public ByteRingBuffer(int capacity, boolean growable) {
        Assert.leq(capacity, "ByteRingBuffer capacity", RING_BUFFER_MAX_CAPACITY);

        this.growable = growable;

        // use next larger power of 2 for our storage
        final int newCapacity;
        if (capacity < 2) {
            // sensibly handle the size=0 and size=1 cases
            newCapacity = 1;
        } else {
            newCapacity = Integer.highestOneBit(capacity - 1) << 1;
        }

        // reset the data structure members
        storage = new byte[newCapacity];
        mask = storage.length - 1;
        tail = head = 0;
    }

    private void grow(int increase) {
        final int size = size();
        final long newSize = size + increase;
        // assert that we are not asking for the impossible
        Assert.leq(newSize, "ByteRingBuffer capacity", RING_BUFFER_MAX_CAPACITY);

        final byte[] newStorage = new byte[Integer.highestOneBit((int) newSize - 1) << 1];

        // move the current data to the new buffer
        copyRingBufferToArray(newStorage);

        // reset the data structure members
        storage = newStorage;
        mask = storage.length - 1;
        tail = size;
        head = 0;
    }

    private void copyRingBufferToArray(byte[] dest) {
        final int size = size();
        final int storageHead = (int) (head & mask);

        // firstCopyLen is either the size of the ring buffer, the distance from head to the end, or the size
        // of the destination buffer, whichever is smallest.
        final int firstCopyLen = Math.min(Math.min(storage.length - storageHead, size), dest.length);

        // secondCopyLen is either the number of uncopied elements remaining from the first copy,
        // or the amount of space remaining in the dest array, whichever is smaller.
        final int secondCopyLen = Math.min(size - firstCopyLen, dest.length - firstCopyLen);

        System.arraycopy(storage, storageHead, dest, 0, firstCopyLen);
        System.arraycopy(storage, 0, dest, firstCopyLen, secondCopyLen);
    }

    public boolean isFull() {
        return size() == storage.length;
    }

    public boolean isEmpty() {
        return tail == head;
    }

    public int size() {
        return (int) (tail - head);
    }

    public int capacity() {
        return storage.length;
    }

    public int remaining() {
        return storage.length - size();
    }

    public void clear() {
        tail = head = 0;
    }

    /**
     * Adds an entry to the ring buffer, will throw an exception if buffer is full. For a graceful failure, use
     * {@link #offer(byte)}
     *
     * @param e the byte to be added to the buffer
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     * @return {@code true} if the byte was added successfully
     */
    public boolean add(byte e) {
        if (isFull()) {
            if (!growable) {
                throw new UnsupportedOperationException("Ring buffer is full and growth is disabled");
            } else {
                grow(1);
            }
        }
        addUnsafe(e);
        return true;
    }

    /**
     * Ensure that there is sufficient empty space to store {@code count} items in the buffer. If the buffer is
     * {@code growable}, this may result in an internal growth operation. This call should be used in conjunction with
     * {@link #addUnsafe(byte)}.
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
     * Add values without overflow detection. Making this call when the buffer is full will result in this data
     * structure becoming corrupted and unusable. This call must be used in conjunction with
     * {@link #ensureRemaining(int)} or {@link #remaining()} to verify remaining capacity if sufficient.
     *
     * @param e the value to add to the buffer
     */
    public void addUnsafe(byte e) {
        storage[(int) (tail++ & mask)] = e;
    }

    /**
     * Add an entry to the ring buffer. If the buffer is full, will overwrite the oldest entry with the new one.
     *
     * @param e the byte to be added to the buffer
     * @param notFullResult value to return is the buffer is not full
     * @return the overwritten entry if the buffer is full, the provided value otherwise
     */
    public byte addOverwrite(byte e, byte notFullResult) {
        byte result = notFullResult;
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
     * @param e the byte to be added to the buffer
     * @return true if the value was added successfully, false otherwise
     */
    public boolean offer(byte e) {
        if (isFull()) {
            return false;
        }
        addUnsafe(e);
        return true;
    }

    public byte[] remove(int count) {
        if (size() < count) {
            throw new NoSuchElementException();
        }
        final byte[] result = new byte[count];
        copyRingBufferToArray(result);
        head += count;
        return result;
    }

    public byte remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return removeUnsafe();
    }

    /**
     * Remove values without empty-buffer detection. Making this call when the buffer is empty will result in this data
     * structure becoming corrupted and unusable. This call must be used in conjunction with {@link #size()} to verify
     * the buffer contains the data items to retrieve.
     *
     * @return the value removed from the buffer
     */
    public byte removeUnsafe() {
        return storage[(int) (head++ & mask)];
    }

    public byte poll(byte onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return removeUnsafe();
    }

    public byte element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[(int) (head & mask)];
    }

    public byte peek(byte onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[(int) (head & mask)];
    }

    public byte front() {
        return front(0);
    }

    public byte front(int offset) {
        if (offset >= size()) {
            throw new NoSuchElementException();
        }
        return storage[(int) ((head + offset) & mask)];
    }

    public byte back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[(int) ((tail - 1) & mask)];
    }

    public byte peekBack(byte onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[(int) ((tail - 1) & mask)];
    }

    public byte[] getAll() {
        byte[] result = new byte[size()];
        copyRingBufferToArray(result);
        return result;
    }

    public Iterator iterator() {
        return new Iterator();
    }

    public class Iterator {
        int count = -1;

        public boolean hasNext() {
            return count + 1 < size();
        }

        public byte next() {
            count++;
            return storage[(int) ((head + count) & mask)];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
