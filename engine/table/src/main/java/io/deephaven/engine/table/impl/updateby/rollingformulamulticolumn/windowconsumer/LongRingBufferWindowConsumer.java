//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferWindowConsumer and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;

class LongRingBufferWindowConsumer extends RingBufferWindowConsumer {
    private final LongRingBuffer longRingBuffer;

    private LongChunk<?> influencerValuesChunk;

    LongRingBufferWindowConsumer(LongRingBuffer longRingBuffer) {
        this.longRingBuffer = longRingBuffer;
    }

    @Override
    public void setInputChunk(final Chunk<?> inputChunk) {
        this.influencerValuesChunk = inputChunk.asLongChunk();
    }

    @Override
    public void push(int index, int count) {
        longRingBuffer.ensureRemaining(count);
        for (int i = 0; i < count; i++) {
            longRingBuffer.add(influencerValuesChunk.get(index + i));
        }
    }

    @Override
    public void pop(int count) {
        for (int i = 0; i < count; i++) {
            longRingBuffer.remove();
        }
    }

    @Override
    public void reset() {
        longRingBuffer.clear();
    }
}
