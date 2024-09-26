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

public class ByteRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final ByteRingBuffer byteRingBuffer;

    private ByteChunk<?> influencerValuesChunk;

    public ByteRingBufferWindowConsumer(ByteRingBuffer byteRingBuffer) {
        this.byteRingBuffer = byteRingBuffer;
    }

    @Override
    public void setInfluencerValuesChunk(final Chunk<?> influencerValuesChunk) {
        this.influencerValuesChunk = influencerValuesChunk.asByteChunk();
    }

    @Override
    public void push(int index, int length) {
        byteRingBuffer.ensureRemaining(length);
        for (int i = 0; i < length; i++) {
            byteRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int length) {
        for (int i = 0; i < length; i++) {
            byteRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        byteRingBuffer.clear();
    }
}
