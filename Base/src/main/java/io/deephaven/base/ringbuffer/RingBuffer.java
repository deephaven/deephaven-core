//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.ringbuffer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Generic interface for a ring buffer and factory methods for buffer creation.
 */
public interface RingBuffer {
    @NotNull
    static <T> RingBuffer makeRingBuffer(
            @NotNull final Class<?> dataType,
            @Nullable final Class<T> componentType,
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
        // noinspection unchecked
        return result;
    }

    boolean isFull();

    boolean isEmpty();

    int size();

    int capacity();

    int remaining();

    void clear();

    void ensureRemaining(int count);
}
