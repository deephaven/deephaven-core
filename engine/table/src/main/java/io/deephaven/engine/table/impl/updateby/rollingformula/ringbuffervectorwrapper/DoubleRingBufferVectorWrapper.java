/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBufferVectorWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.DoubleRingBuffer;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;

public class DoubleRingBufferVectorWrapper implements DoubleVector {
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
        return ringBuffer.front((int)index);
    }

    @Override
    public DoubleVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on DoubleRingBufferVectorWrapper");
    }

    @Override
    public DoubleVector subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on DoubleRingBufferVectorWrapper");
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
