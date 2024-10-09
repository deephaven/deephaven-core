//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.CharRingBuffer;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;

class CharRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final CharRingBuffer charRingBuffer;

    private CharChunk<?> influencerValuesChunk;

    CharRingBufferWindowConsumer(CharRingBuffer charRingBuffer) {
        this.charRingBuffer = charRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asCharChunk();
    }

    @Override
    public void push(int index, int count) {
        charRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            charRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            charRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        charRingBuffer.clear();
    }
}
