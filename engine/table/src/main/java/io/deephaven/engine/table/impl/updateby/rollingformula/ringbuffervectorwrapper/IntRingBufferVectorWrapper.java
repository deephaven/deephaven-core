//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRingBufferVectorWrapper and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.vector.IntSubVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.vector.IntVectorSlice;

public class IntRingBufferVectorWrapper implements IntVector, RingBufferVectorWrapper<IntVector> {
    private final IntRingBuffer ringBuffer;

    public IntRingBufferVectorWrapper(final IntRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public int get(long index) {
        return ringBuffer.front((int) index);
    }

    @Override
    public IntVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new IntVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public IntVector subVectorByPositions(final long[] positions) {
        return new IntSubVector(this, positions);
    }

    @Override
    public int[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public int[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public IntVector getDirect() {
        return new IntVectorDirect(ringBuffer.getAll());
    }
}
