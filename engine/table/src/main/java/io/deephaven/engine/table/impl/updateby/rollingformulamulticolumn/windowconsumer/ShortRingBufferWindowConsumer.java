//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferWindowConsumer and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.ShortRingBuffer;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;

public class ShortRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final ShortRingBuffer shortRingBuffer;

    private ShortChunk<?> influencerValuesChunk;

    public ShortRingBufferWindowConsumer(ShortRingBuffer shortRingBuffer) {
        this.shortRingBuffer = shortRingBuffer;
    }

    @Override
    public void setInfluencerValuesChunk(final Chunk<?> influencerValuesChunk) {
        this.influencerValuesChunk = influencerValuesChunk.asShortChunk();
    }

    @Override
    public void push(int index, int length) {
        shortRingBuffer.ensureRemaining(length);
        for (int i = 0; i < length; i++) {
            shortRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int length) {
        for (int i = 0; i < length; i++) {
            shortRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        shortRingBuffer.clear();
    }
}
