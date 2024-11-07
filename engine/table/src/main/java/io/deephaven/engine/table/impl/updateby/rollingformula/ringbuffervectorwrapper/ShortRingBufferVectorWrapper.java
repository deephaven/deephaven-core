//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferVectorWrapper and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.ShortRingBuffer;
import io.deephaven.vector.ShortSubVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;
import io.deephaven.vector.ShortVectorSlice;

public class ShortRingBufferVectorWrapper implements ShortVector, RingBufferVectorWrapper<ShortVector> {
    private final ShortRingBuffer ringBuffer;

    public ShortRingBufferVectorWrapper(final ShortRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public short get(long index) {
        return ringBuffer.front((int) index);
    }

    @Override
    public ShortVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ShortVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public ShortVector subVectorByPositions(final long[] positions) {
        return new ShortSubVector(this, positions);
    }

    @Override
    public short[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public short[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public ShortVector getDirect() {
        return new ShortVectorDirect(ringBuffer.getAll());
    }
}
