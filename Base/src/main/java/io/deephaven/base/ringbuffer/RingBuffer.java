//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.ringbuffer;

import org.jetbrains.annotations.NotNull;

/**
 * Generic interface for a ring buffer and factory methods for buffer creation.
 */
public interface RingBuffer {
    @NotNull
    static <T> RingBuffer makeRingBuffer(
            @NotNull final Class<?> dataType,
            final int capacity,
            final boolean growable) {
        final RingBuffer result;
        if (dataType == char.class || dataType == Character.class) {
            result = new CharRingBuffer(capacity, growable);
        } else if (dataType == byte.class || dataType == Byte.class) {
            result = new ByteRingBuffer(capacity, growable);
        } else if (dataType == double.class || dataType == Double.class) {
            result = new DoubleRingBuffer(capacity, growable);
        } else if (dataType == float.class || dataType == Float.class) {
            result = new FloatRingBuffer(capacity, growable);
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new IntRingBuffer(capacity, growable);
        } else if (dataType == long.class || dataType == Long.class) {
            result = new LongRingBuffer(capacity, growable);
        } else if (dataType == short.class || dataType == Short.class) {
            result = new ShortRingBuffer(capacity, growable);
        } else {
            result = new ObjectRingBuffer<T>(capacity, growable);
        }
        return result;
    }

    /**
     * Whether the buffer is completely full.
     */
    boolean isFull();

    /**
     * Whether the buffer is entirely empty.
     */
    boolean isEmpty();

    /**
     * Return how many items are currently in the buffer.
     */
    int size();

    /**
     * Return how many items can fit in the buffer at its current capacity. If the buffer can grow, this number can
     * change.
     */
    int capacity();

    /**
     * Return how many free slots exist in this buffer at its current capacity.
     */
    int remaining();

    /**
     * Clear the buffer of all values. If this is an object ring buffer, this will additionally set all values to
     * {@code null}.
     */
    void clear();

    /**
     * Ensure that at least {@code count} free slots are available in the buffer. If the buffer is growable, the
     * capacity may be increased to accommodate the new slots. If the buffer is not growable and there are insufficient
     * free slots, an {@link UnsupportedOperationException} will be thrown.
     *
     * @param count the number of free slots to ensure are available
     * @throws UnsupportedOperationException if the buffer is not growable and there are insufficient free slots
     */
    void ensureRemaining(int count);
}
