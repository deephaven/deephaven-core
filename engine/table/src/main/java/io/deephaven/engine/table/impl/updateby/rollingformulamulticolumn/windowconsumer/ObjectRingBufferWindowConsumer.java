//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.ObjectRingBuffer;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.Chunk;

public class ObjectRingBufferWindowConsumer<T> extends RingBufferWindowConsumer {
    private final ObjectRingBuffer<T> objectRingBuffer;

    private ObjectChunk<T, ?> influencerValuesChunk;

    public ObjectRingBufferWindowConsumer(ObjectRingBuffer<T> objectRingBuffer) {
        this.objectRingBuffer = objectRingBuffer;
    }

    @Override
    public void setInfluencerValuesChunk(final Chunk<?> influencerValuesChunk) {
        this.influencerValuesChunk = influencerValuesChunk.asObjectChunk();
    }

    @Override
    public void push(int index, int length) {
        objectRingBuffer.ensureRemaining(length);
        for (int i = 0; i < length; i++) {
            objectRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int length) {
        for (int i = 0; i < length; i++) {
            objectRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        objectRingBuffer.clear();
    }
}
