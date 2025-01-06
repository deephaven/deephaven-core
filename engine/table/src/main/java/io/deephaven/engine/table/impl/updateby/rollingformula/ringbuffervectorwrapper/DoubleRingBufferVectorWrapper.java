//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferVectorWrapper and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.DoubleRingBuffer;
import io.deephaven.vector.DoubleSubVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.DoubleVectorSlice;

public class DoubleRingBufferVectorWrapper implements DoubleVector, RingBufferVectorWrapper<DoubleVector> {
    private final DoubleRingBuffer ringBuffer;

    public DoubleRingBufferVectorWrapper(final DoubleRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public double get(long index) {
        return ringBuffer.front((int) index);
    }

    @Override
    public DoubleVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new DoubleVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public DoubleVector subVectorByPositions(final long[] positions) {
        return new DoubleSubVector(this, positions);
    }

    @Override
    public double[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public double[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public DoubleVector getDirect() {
        return new DoubleVectorDirect(ringBuffer.getAll());
    }
}
