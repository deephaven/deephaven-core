//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.ObjectRingBuffer;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.Chunk;

class ObjectRingBufferWindowConsumer<T> extends RingBufferWindowConsumer {
    private final ObjectRingBuffer<T> objectRingBuffer;

    private ObjectChunk<T, ?> influencerValuesChunk;

    ObjectRingBufferWindowConsumer(ObjectRingBuffer<T> objectRingBuffer) {
        this.objectRingBuffer = objectRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asObjectChunk();
    }

    @Override
    public void push(int index, int count) {
        objectRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            objectRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            objectRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        objectRingBuffer.clear();
    }
}
