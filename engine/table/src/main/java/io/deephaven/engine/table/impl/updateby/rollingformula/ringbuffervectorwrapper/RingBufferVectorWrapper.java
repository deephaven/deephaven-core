//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.*;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This helper class will expose a {@link Vector} interface for a {@link RingBuffer}.
 */
public interface RingBufferVectorWrapper<T extends Vector<T>> extends Vector<T> {
    /**
     * Create a {@link RingBufferVectorWrapper} for the given {@link RingBuffer}. Optionally, a component type can be
     * supplied (although it will be ignored for all wrappers but {@link ObjectRingBufferVectorWrapper}).
     */
    static <T extends Vector<T>> RingBufferVectorWrapper<T> makeRingBufferVectorWrapper(
            @NotNull final RingBuffer buffer,
            @Nullable final Class<?> componentType) {
        final RingBufferVectorWrapper<?> result;
        final Class<?> bufferClass = buffer.getClass();
        if (bufferClass == CharRingBuffer.class) {
            result = new CharRingBufferVectorWrapper((CharRingBuffer) buffer);
        } else if (bufferClass == ByteRingBuffer.class) {
            result = new ByteRingBufferVectorWrapper((ByteRingBuffer) buffer);
        } else if (bufferClass == DoubleRingBuffer.class) {
            result = new DoubleRingBufferVectorWrapper((DoubleRingBuffer) buffer);
        } else if (bufferClass == FloatRingBuffer.class) {
            result = new FloatRingBufferVectorWrapper((FloatRingBuffer) buffer);
        } else if (bufferClass == IntRingBuffer.class) {
            result = new IntRingBufferVectorWrapper((IntRingBuffer) buffer);
        } else if (bufferClass == LongRingBuffer.class) {
            result = new LongRingBufferVectorWrapper((LongRingBuffer) buffer);
        } else if (bufferClass == ShortRingBuffer.class) {
            result = new ShortRingBufferVectorWrapper((ShortRingBuffer) buffer);
        } else {
            // noinspection unchecked
            result = new ObjectRingBufferVectorWrapper<>((ObjectRingBuffer<T>) buffer, (Class<T>) componentType);
        }
        // noinspection unchecked
        return (RingBufferVectorWrapper<T>) result;
    }
}
