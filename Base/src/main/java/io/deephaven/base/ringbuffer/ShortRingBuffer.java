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
import org.jetbrains.annotations.TestOnly;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * A simple circular buffer for primitive values, like java.util.concurrent.ArrayBlockingQueue but without the
 * synchronization and collection overhead. Storage is between head (inclusive) and tail (exclusive) using incrementing
 * {@code long} values. Head and tail will not wrap around; instead we use storage arrays sized to 2^N to allow fast
 * determination of storage indices through a mask operation.
 */
public class ShortRingBuffer implements RingBuffer, Serializable {
    static final long FIXUP_THRESHOLD = 1L << 62;
    final boolean growable;
    short[] storage;
    int mask;
    long head;
    long tail;

    /**
     * Create an unbounded-growth ring buffer of short primitives.
     *
     * @param capacity minimum capacity of the ring buffer
     */
    public ShortRingBuffer(final int capacity) {
        this(capacity, true);
    }

    /**
     * Create a ring buffer of short primitives.
     *
     * @param capacity minimum capacity of ring buffer
     * @param growable whether to allow growth when the buffer is full.
     */
    public ShortRingBuffer(final int capacity, final boolean growable) {
        Assert.leq(capacity, "ShortRingBuffer capacity", MathUtil.MAX_POWER_OF_2);

        this.growable = growable;

        // use next larger power of 2 for our storage
        // reset the data structure members
        storage = new short[MathUtil.roundUpPowerOf2(capacity)];
        mask = storage.length - 1;
        tail = head = 0;
    }

    /**
     * Increase the capacity of the ring buffer.
     * 
     * @param increase Increase amount. The ring buffer's capacity will be increased by at least this amount.
     */
    protected void grow(final int increase) {
        final int size = size();
        final long newCapacity = (long) storage.length + increase;
        // assert that we are not asking for the impossible
        Assert.leq(newCapacity, "ShortRingBuffer capacity", MathUtil.MAX_POWER_OF_2);

        final short[] newStorage = new short[MathUtil.roundUpPowerOf2((int) newCapacity)];

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
    protected void copyRingBufferToArray(final short[] dest) {
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

    @Override
    public boolean isFull() {
        return size() == storage.length;
    }

    @Override
    public boolean isEmpty() {
        return tail == head;
    }

    @Override
    public int size() {
        return Math.toIntExact(tail - head);
    }

    @Override
    public int capacity() {
        return storage.length;
    }

    @Override
    public int remaining() {
        return storage.length - size();
    }

    @Override
    public void clear() {
        // region object-bulk-clear
        // endregion object-bulk-clear
        tail = head = 0;
    }

    /**
     * Adds an entry to the ring buffer, will throw an exception if buffer is full. For a graceful failure, use
     * {@link #offer(short)}
     *
     * @param e the short to be added to the buffer
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     * @return {@code true} if the short was added successfully
     */
    public boolean add(final short e) {
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
     * {@link #addUnsafe(short)}.
     *
     * @param count the minimum number of empty entries in the buffer after this call
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     */
    @Override
    public void ensureRemaining(final int count) {
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
    public void addUnsafe(final short e) {
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
     * @param e the short to be added to the buffer
     * @param notFullResult value to return is the buffer is not full
     * @return the overwritten entry if the buffer is full, the provided value otherwise
     */
    public short addOverwrite(final short e, final short notFullResult) {
        short val = notFullResult;
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
     * @param e the short to be added to the buffer
     * @return true if the value was added successfully, false otherwise
     */
    public boolean offer(final short e) {
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
    public short[] remove(final int count) {
        final int size = size();
        if (size < count) {
            throw new NoSuchElementException();
        }
        final short[] result = new short[count];
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
    public short remove() {
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
    public short removeUnsafe() {
        final int idx = (int) (head++ & mask);
        short val = storage[idx];
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
    public short poll(final short onEmpty) {
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
    public short element() {
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
    public short peek(final short onEmpty) {
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
    public short front() {
        return front(0);
    }

    /**
     * Returns the element at the specified offset in the ring buffer.
     *
     * @param offset The specified offset.
     * @throws NoSuchElementException if the buffer is empty
     * @return The element at the specified offset
     */
    public short front(final int offset) {
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
    public short back() {
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
    public short peekBack(final short onEmpty) {
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
    public short[] getAll() {
        short[] result = new short[size()];
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

        public short next() {
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

    /**
     * Get the storage array for this ring buffer. This is intended for testing and debugging purposes only.
     *
     * @return The storage array for this ring buffer.
     */
    @TestOnly
    public short[] getStorage() {
        return storage;
    }
}
