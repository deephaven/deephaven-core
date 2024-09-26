//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.CharRingBuffer;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;

public class CharRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final CharRingBuffer charRingBuffer;

    private CharChunk<?> influencerValuesChunk;

    public CharRingBufferWindowConsumer(CharRingBuffer charRingBuffer) {
        this.charRingBuffer = charRingBuffer;
    }

    @Override
    public void setInfluencerValuesChunk(final Chunk<?> influencerValuesChunk) {
        this.influencerValuesChunk = influencerValuesChunk.asCharChunk();
    }

    @Override
    public void push(int index, int length) {
        charRingBuffer.ensureRemaining(length);
        for (int i = 0; i < length; i++) {
            charRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int length) {
        for (int i = 0; i < length; i++) {
            charRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        charRingBuffer.clear();
    }
}
