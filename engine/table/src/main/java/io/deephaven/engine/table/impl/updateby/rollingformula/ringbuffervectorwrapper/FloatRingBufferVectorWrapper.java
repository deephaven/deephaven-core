//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferVectorWrapper and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.FloatRingBuffer;
import io.deephaven.vector.FloatSubVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.vector.FloatVectorSlice;

public class FloatRingBufferVectorWrapper implements FloatVector, RingBufferVectorWrapper<FloatVector> {
    private final FloatRingBuffer ringBuffer;

    public FloatRingBufferVectorWrapper(final FloatRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public float get(long index) {
        return ringBuffer.front((int) index);
    }

    @Override
    public FloatVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new FloatVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public FloatVector subVectorByPositions(final long[] positions) {
        return new FloatSubVector(this, positions);
    }

    @Override
    public float[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public float[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public FloatVector getDirect() {
        return new FloatVectorDirect(ringBuffer.getAll());
    }
}
