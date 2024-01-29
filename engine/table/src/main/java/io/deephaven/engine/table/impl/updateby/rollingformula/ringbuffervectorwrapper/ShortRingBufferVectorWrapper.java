/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBufferVectorWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.ShortRingBuffer;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;

public class ShortRingBufferVectorWrapper implements ShortVector {
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
        return ringBuffer.front((int)index);
    }

    @Override
    public ShortVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on ShortRingBufferVectorWrapper");
    }

    @Override
    public ShortVector subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on ShortRingBufferVectorWrapper");
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
