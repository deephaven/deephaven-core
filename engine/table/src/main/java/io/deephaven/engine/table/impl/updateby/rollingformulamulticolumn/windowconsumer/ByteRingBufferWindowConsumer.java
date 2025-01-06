//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferWindowConsumer and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;

class ByteRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final ByteRingBuffer byteRingBuffer;

    private ByteChunk<?> influencerValuesChunk;

    ByteRingBufferWindowConsumer(ByteRingBuffer byteRingBuffer) {
        this.byteRingBuffer = byteRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asByteChunk();
    }

    @Override
    public void push(int index, int count) {
        byteRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            byteRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            byteRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        byteRingBuffer.clear();
    }
}
