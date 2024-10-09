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

class FloatRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final FloatRingBuffer floatRingBuffer;

    private FloatChunk<?> influencerValuesChunk;

    FloatRingBufferWindowConsumer(FloatRingBuffer floatRingBuffer) {
        this.floatRingBuffer = floatRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asFloatChunk();
    }

    @Override
    public void push(int index, int count) {
        floatRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            floatRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            floatRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        floatRingBuffer.clear();
    }
}
