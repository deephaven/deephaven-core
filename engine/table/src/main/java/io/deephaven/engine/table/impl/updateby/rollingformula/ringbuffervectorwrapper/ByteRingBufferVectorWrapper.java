//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferVectorWrapper and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.vector.ByteSubVector;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import io.deephaven.vector.ByteVectorSlice;

public class ByteRingBufferVectorWrapper implements ByteVector, RingBufferVectorWrapper<ByteVector> {
    private final ByteRingBuffer ringBuffer;

    public ByteRingBufferVectorWrapper(final ByteRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public byte get(long index) {
        return ringBuffer.front((int) index);
    }

    @Override
    public ByteVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ByteVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public ByteVector subVectorByPositions(final long[] positions) {
        return new ByteSubVector(this, positions);
    }

    @Override
    public byte[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public byte[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public ByteVector getDirect() {
        return new ByteVectorDirect(ringBuffer.getAll());
    }
}
