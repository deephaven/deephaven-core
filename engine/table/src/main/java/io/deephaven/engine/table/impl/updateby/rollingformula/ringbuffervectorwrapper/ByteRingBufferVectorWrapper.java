/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBufferVectorWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;

public class ByteRingBufferVectorWrapper implements ByteVector, RingBufferVectorWrapper {
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
        return ringBuffer.front((int)index);
    }

    @Override
    public ByteVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on ByteRingBufferVectorWrapper");
    }

    @Override
    public ByteVector subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on ByteRingBufferVectorWrapper");
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
