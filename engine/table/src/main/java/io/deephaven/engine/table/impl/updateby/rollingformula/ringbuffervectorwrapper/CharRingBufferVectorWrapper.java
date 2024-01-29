package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.CharRingBuffer;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;

public class CharRingBufferVectorWrapper implements CharVector {
    private final CharRingBuffer ringBuffer;

    public CharRingBufferVectorWrapper(final CharRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public char get(long index) {
        return ringBuffer.front((int)index);
    }

    @Override
    public CharVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on CharRingBufferVectorWrapper");
    }

    @Override
    public CharVector subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on CharRingBufferVectorWrapper");
    }

    @Override
    public char[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public char[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public CharVector getDirect() {
        return new CharVectorDirect(ringBuffer.getAll());
    }
}
