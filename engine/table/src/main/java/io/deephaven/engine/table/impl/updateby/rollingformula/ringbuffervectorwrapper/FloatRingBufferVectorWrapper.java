/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBufferVectorWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.FloatRingBuffer;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;

public class FloatRingBufferVectorWrapper implements FloatVector, RingBufferVectorWrapper {
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
        return ringBuffer.front((int)index);
    }

    @Override
    public FloatVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on FloatRingBufferVectorWrapper");
    }

    @Override
    public FloatVector subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on FloatRingBufferVectorWrapper");
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
