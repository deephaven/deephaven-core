//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferVectorWrapper and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.vector.LongSubVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import io.deephaven.vector.LongVectorSlice;

public class LongRingBufferVectorWrapper implements LongVector, RingBufferVectorWrapper<LongVector> {
    private final LongRingBuffer ringBuffer;

    public LongRingBufferVectorWrapper(final LongRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public long get(long index) {
        return ringBuffer.front((int) index);
    }

    @Override
    public LongVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new LongVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public LongVector subVectorByPositions(final long[] positions) {
        return new LongSubVector(this, positions);
    }

    @Override
    public long[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public long[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public LongVector getDirect() {
        return new LongVectorDirect(ringBuffer.getAll());
    }
}
