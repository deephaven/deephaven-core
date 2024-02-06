/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBufferVectorWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;

public class IntRingBufferVectorWrapper implements IntVector, RingBufferVectorWrapper {
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
        return ringBuffer.front((int)index);
    }

    @Override
    public IntVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on IntRingBufferVectorWrapper");
    }

    @Override
    public IntVector subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on IntRingBufferVectorWrapper");
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
