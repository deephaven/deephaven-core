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

public class IntRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final IntRingBuffer intRingBuffer;

    private IntChunk<?> influencerValuesChunk;

    public IntRingBufferWindowConsumer(IntRingBuffer intRingBuffer) {
        this.intRingBuffer = intRingBuffer;
    }

    @Override
    public void setInfluencerValuesChunk(final Chunk<?> influencerValuesChunk) {
        this.influencerValuesChunk = influencerValuesChunk.asIntChunk();
    }

    @Override
    public void push(int index, int length) {
        intRingBuffer.ensureRemaining(length);
        for (int i = 0; i < length; i++) {
            intRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int length) {
        for (int i = 0; i < length; i++) {
            intRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        intRingBuffer.clear();
    }
}
