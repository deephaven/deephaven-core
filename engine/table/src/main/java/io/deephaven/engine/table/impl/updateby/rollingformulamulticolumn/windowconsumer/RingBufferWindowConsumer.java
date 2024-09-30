//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.*;
import io.deephaven.chunk.Chunk;

public abstract class RingBufferWindowConsumer {
    public static RingBufferWindowConsumer create(final RingBuffer buffer) {
        if (buffer instanceof CharRingBuffer) {
            return new CharRingBufferWindowConsumer((CharRingBuffer) buffer);
        } else if (buffer instanceof ByteRingBuffer) {
            return new ByteRingBufferWindowConsumer((ByteRingBuffer) buffer);
        } else if (buffer instanceof DoubleRingBuffer) {
            return new DoubleRingBufferWindowConsumer((DoubleRingBuffer) buffer);
        } else if (buffer instanceof FloatRingBuffer) {
            return new FloatRingBufferWindowConsumer((FloatRingBuffer) buffer);
        } else if (buffer instanceof IntRingBuffer) {
            return new IntRingBufferWindowConsumer((IntRingBuffer) buffer);
        } else if (buffer instanceof LongRingBuffer) {
            return new LongRingBufferWindowConsumer((LongRingBuffer) buffer);
        } else if (buffer instanceof ShortRingBuffer) {
            return new ShortRingBufferWindowConsumer((ShortRingBuffer) buffer);
        }
        return new ObjectRingBufferWindowConsumer<>((ObjectRingBuffer<?>) buffer);
    }

    public abstract void setInfluencerValuesChunk(Chunk<?> influencerValuesChunk);

    public abstract void push(int index, int length);

    public abstract void pop(int length);

    public abstract void reset();
}
