/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBufferVectorWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;

public class LongRingBufferVectorWrapper implements LongVector, RingBufferVectorWrapper {
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
        return ringBuffer.front((int)index);
    }

    @Override
    public LongVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on LongRingBufferVectorWrapper");
    }

    @Override
    public LongVector subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on LongRingBufferVectorWrapper");
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
