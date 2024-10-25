//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.*;
import io.deephaven.chunk.Chunk;

/**
 * This helper provides an abstract interface for consuming UpdateBy window input data values and storing in a
 * {@link RingBuffer}. The UpdateBy code produces a chunk of input data and lists of push/pop instructions for managing
 * the current window values. This class abstracts the underlying buffer type and provides a common interface for the
 * UpdateBy code to interact with the buffer.
 */
public abstract class RingBufferWindowConsumer {
    /**
     * Create a new {@link RingBufferWindowConsumer} for the given buffer.
     *
     * @param buffer the buffer to manage
     * @return a new RingBufferWindowConsumer
     */
    public static RingBufferWindowConsumer create(final RingBuffer buffer) {
        final Class<?> bufferClass = buffer.getClass();

        if (bufferClass == CharRingBuffer.class) {
            return new CharRingBufferWindowConsumer((CharRingBuffer) buffer);
        } else if (bufferClass == ByteRingBuffer.class) {
            return new ByteRingBufferWindowConsumer((ByteRingBuffer) buffer);
        } else if (bufferClass == DoubleRingBuffer.class) {
            return new DoubleRingBufferWindowConsumer((DoubleRingBuffer) buffer);
        } else if (bufferClass == FloatRingBuffer.class) {
            return new FloatRingBufferWindowConsumer((FloatRingBuffer) buffer);
        } else if (bufferClass == IntRingBuffer.class) {
            return new IntRingBufferWindowConsumer((IntRingBuffer) buffer);
        } else if (bufferClass == LongRingBuffer.class) {
            return new LongRingBufferWindowConsumer((LongRingBuffer) buffer);
        } else if (bufferClass == ShortRingBuffer.class) {
            return new ShortRingBufferWindowConsumer((ShortRingBuffer) buffer);
        }
        return new ObjectRingBufferWindowConsumer<>((ObjectRingBuffer<?>) buffer);
    }

    /**
     * Set the input chunk for this consumer. This will be stored and the push instructions will index into this buffer
     * for newly provided values.
     *
     * @param inputChunk the input chunk
     */
    public abstract void setInputChunk(Chunk<?> inputChunk);

    /**
     * Push {@code count} values from the input chunk into the ring buffer, beginning at {@code index}.
     *
     * @param index the index of the first value to push
     * @param count the count of values to push
     */
    public abstract void push(int index, int count);

    /**
     * Pop {@code count} values from the ring buffer.
     *
     * @param count the count of values to pop
     */
    public abstract void pop(int count);

    /**
     * Reset the ring buffer to its initial state. If this is an object ring buffer, this will set all values to
     * {@code null}.
     */
    public abstract void reset();
}
