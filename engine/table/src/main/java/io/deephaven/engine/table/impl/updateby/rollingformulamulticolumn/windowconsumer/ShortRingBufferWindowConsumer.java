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

class ShortRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final ShortRingBuffer shortRingBuffer;

    private ShortChunk<?> influencerValuesChunk;

    ShortRingBufferWindowConsumer(ShortRingBuffer shortRingBuffer) {
        this.shortRingBuffer = shortRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asShortChunk();
    }

    @Override
    public void push(int index, int count) {
        shortRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            shortRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            shortRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        shortRingBuffer.clear();
    }
}
