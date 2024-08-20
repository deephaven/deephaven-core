//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBuffer and run "./gradlew replicateRingBuffers" to regenerate
//
// @formatter:off
package io.deephaven.base.ringbuffer;

import io.deephaven.base.MathUtil;
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
    static final long FIXUP_THRESHOLD = 1L << 62;
    final boolean growable;
    byte[] storage;
    int mask;
    long head;
    long tail;

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
        Assert.leq(capacity, "ByteRingBuffer capacity", MathUtil.MAX_POWER_OF_2);

        this.growable = growable;

        // use next larger power of 2 for our storage
        // reset the data structure members
        storage = new byte[MathUtil.roundUpPowerOf2(capacity)];
        mask = storage.length - 1;
        tail = head = 0;
    }

    /**
     * Increase the capacity of the ring buffer.
     * 
     * @param increase Increase amount. The ring buffer's capacity will be increased by at least this amount.
     */
    protected void grow(int increase) {
        final int size = size();
        final long newCapacity = (long) storage.length + increase;
        // assert that we are not asking for the impossible
        Assert.leq(newCapacity, "ByteRingBuffer capacity", MathUtil.MAX_POWER_OF_2);

        final byte[] newStorage = new byte[MathUtil.roundUpPowerOf2((int) newCapacity)];

        // move the current data to the new buffer
        copyRingBufferToArray(newStorage);

        // reset the data structure members
        storage = newStorage;
        mask = storage.length - 1;
        tail = size;
        head = 0;
    }

    /**
     * Copy the contents of the buffer to a destination buffer. If the destination buffer capacity is smaller than
     * {@code size()}, the copy will not fail but will terminate after the buffer is full.
     * 
     * @param dest The destination buffer.
     */
    protected void copyRingBufferToArray(byte[] dest) {
        final int size = size();
        final int storageHead = (int) (head & mask);

        // firstCopyLen is either the size of the ring buffer, the distance from head to the end of the storage array,
        // or the size of the destination buffer, whichever is smallest.
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
        return Math.toIntExact(tail - head);
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
     * Add values without overflow detection. The caller *must* ensure that there is at least one element of free space
     * in the ring buffer before calling this method. The caller may use {@link #ensureRemaining(int)} or
     * {@link #remaining()} for this purpose.
     *
     * @param e the value to add to the buffer
     */
    public void addUnsafe(byte e) {
        // This is an extremely paranoid wrap check that in all likelihood will never run. With FIXUP_THRESHOLD at
        // 1 << 62, and the user pushing 2^32 values per second(!), it will take 68 years to wrap this counter .
        if (tail >= FIXUP_THRESHOLD) {
            // Reset [head, tail]
            final long thisLength = tail - head;
            head = head & mask;
            tail = head + thisLength;
        }

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
        byte val = notFullResult;
        if (isFull()) {
            val = remove();
        }
        addUnsafe(e);
        return val;
    }

    /**
     * Attempt to add an entry to the ring buffer. If the buffer is full, the add will fail and the buffer will not grow
     * even if growable.
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

    /**
     * Remove multiple elements from the front of the ring buffer
     *
     * @param count The number of elements to remove.
     * @throws NoSuchElementException if the buffer is empty
     */
    public byte[] remove(int count) {
        final int size = size();
        if (size < count) {
            throw new NoSuchElementException();
        }
        final byte[] result = new byte[count];
        // region object-bulk-remove
        copyRingBufferToArray(result);
        // endregion object-bulk-remove
        head += count;
        return result;
    }

    /**
     * Remove one element from the front of the ring buffer.
     *
     * @throws NoSuchElementException if the buffer is empty
     */
    public byte remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return removeUnsafe();
    }


    /**
     * Remove an element without empty buffer detection. The caller *must* ensure that there is at least one element in
     * the ring buffer. The {@link #size()} method may be used for this purpose.
     *
     * @return the value removed from the buffer
     */
    public byte removeUnsafe() {
        final int idx = (int) (head++ & mask);
        byte val = storage[idx];
        // region object-remove
        // endregion object-remove
        return val;
    }

    /**
     * If the ring buffer is non-empty, removes the element at the head of the ring buffer. Otherwise does nothing.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The removed element if the ring buffer was non-empty, otherwise the value of 'onEmpty'
     */
    public byte poll(byte onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return removeUnsafe();
    }

    /**
     * If the ring buffer is non-empty, returns the element at the head of the ring buffer.
     *
     * @throws NoSuchElementException if the buffer is empty
     * @return The head element if the ring buffer is non-empty, otherwise the value of 'onEmpty'
     */
    public byte element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[(int) (head & mask)];
    }

    /**
     * If the ring buffer is non-empty, returns the element at the head of the ring buffer. Otherwise returns the
     * specified element.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The head element if the ring buffer is non-empty, otherwise the value of 'onEmpty'
     */
    public byte peek(byte onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[(int) (head & mask)];
    }

    /**
     * Returns the element at the head of the ring buffer
     *
     * @return The element at the head of the ring buffer
     */
    public byte front() {
        return front(0);
    }

    /**
     * Returns the element at the specified offset in the ring buffer.
     *
     * @param offset The specified offset.
     * @throws NoSuchElementException if the buffer is empty
     * @return The element at the specified offset
     */
    public byte front(int offset) {
        if (offset < 0 || offset >= size()) {
            throw new NoSuchElementException();
        }
        return storage[(int) ((head + offset) & mask)];
    }

    /**
     * Returns the element at the tail of the ring buffer
     * 
     * @throws NoSuchElementException if the buffer is empty
     * @return The element at the tail of the ring buffer
     */
    public byte back() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return storage[(int) ((tail - 1) & mask)];
    }

    /**
     * If the ring buffer is non-empty, returns the element at the tail of the ring buffer. Otherwise returns the
     * specified element.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The tail element if the ring buffer is non-empty, otherwise the value of 'onEmpty'
     */
    public byte peekBack(byte onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return storage[(int) ((tail - 1) & mask)];
    }

    /**
     * Make a copy of the elements in the ring buffer.
     * 
     * @return An array containing a copy of the elements in the ring buffer.
     */
    public byte[] getAll() {
        byte[] result = new byte[size()];
        copyRingBufferToArray(result);
        return result;
    }

    /**
     * Create an iterator for the ring buffer
     */
    public Iterator iterator() {
        return new Iterator();
    }

    public class Iterator {
        int cursor = -1;

        public boolean hasNext() {
            return cursor + 1 < size();
        }

        public byte next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            cursor++;
            return storage[(int) ((head + cursor) & mask)];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
