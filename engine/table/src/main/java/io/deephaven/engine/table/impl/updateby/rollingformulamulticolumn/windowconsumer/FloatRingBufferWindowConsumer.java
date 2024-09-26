//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferWindowConsumer and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.FloatRingBuffer;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;

public class FloatRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final FloatRingBuffer floatRingBuffer;

    private FloatChunk<?> influencerValuesChunk;

    public FloatRingBufferWindowConsumer(FloatRingBuffer floatRingBuffer) {
        this.floatRingBuffer = floatRingBuffer;
    }

    @Override
    public void setInfluencerValuesChunk(final Chunk<?> influencerValuesChunk) {
        this.influencerValuesChunk = influencerValuesChunk.asFloatChunk();
    }

    @Override
    public void push(int index, int length) {
        floatRingBuffer.ensureRemaining(length);
        for (int i = 0; i < length; i++) {
            floatRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int length) {
        for (int i = 0; i < length; i++) {
            floatRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        floatRingBuffer.clear();
    }
}
