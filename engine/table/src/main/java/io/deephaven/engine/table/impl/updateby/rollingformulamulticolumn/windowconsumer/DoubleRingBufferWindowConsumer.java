//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferWindowConsumer and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.DoubleRingBuffer;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;

class DoubleRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final DoubleRingBuffer doubleRingBuffer;

    private DoubleChunk<?> influencerValuesChunk;

    DoubleRingBufferWindowConsumer(DoubleRingBuffer doubleRingBuffer) {
        this.doubleRingBuffer = doubleRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asDoubleChunk();
    }

    @Override
    public void push(int index, int count) {
        doubleRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            doubleRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            doubleRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        doubleRingBuffer.clear();
    }
}
