//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferWindowConsumer and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;

class IntRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final IntRingBuffer intRingBuffer;

    private IntChunk<?> influencerValuesChunk;

    IntRingBufferWindowConsumer(IntRingBuffer intRingBuffer) {
        this.intRingBuffer = intRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asIntChunk();
    }

    @Override
    public void push(int index, int count) {
        intRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            intRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            intRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        intRingBuffer.clear();
    }
}
