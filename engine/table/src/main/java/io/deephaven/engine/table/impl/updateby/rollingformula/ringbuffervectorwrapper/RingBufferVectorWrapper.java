//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.*;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RingBufferVectorWrapper<T extends Vector<T>> extends Vector<T> {
    static <T extends Vector<T>> RingBufferVectorWrapper<T> makeRingBufferVectorWrapper(
            @NotNull final Class<?> dataType,
            @Nullable final Class<?> componentType,
            @NotNull final RingBuffer buffer) {
        final RingBufferVectorWrapper<?> result;
        if (buffer instanceof CharRingBuffer) {
            result = new CharRingBufferVectorWrapper((CharRingBuffer) buffer);
        } else if (buffer instanceof ByteRingBuffer) {
            result = new ByteRingBufferVectorWrapper((ByteRingBuffer) buffer);
        } else if (buffer instanceof DoubleRingBuffer) {
            result = new DoubleRingBufferVectorWrapper((DoubleRingBuffer) buffer);
        } else if (buffer instanceof FloatRingBuffer) {
            result = new FloatRingBufferVectorWrapper((FloatRingBuffer) buffer);
        } else if (buffer instanceof IntRingBuffer) {
            result = new IntRingBufferVectorWrapper((IntRingBuffer) buffer);
        } else if (buffer instanceof LongRingBuffer) {
            result = new LongRingBufferVectorWrapper((LongRingBuffer) buffer);
        } else if (buffer instanceof ShortRingBuffer) {
            result = new ShortRingBufferVectorWrapper((ShortRingBuffer) buffer);
        } else {
            // noinspection unchecked
            result = new ObjectRingBufferVectorWrapper<T>((ObjectRingBuffer<T>) buffer, (Class<T>) componentType);
        }
        // noinspection unchecked
        return (RingBufferVectorWrapper<T>) result;
    }
}
