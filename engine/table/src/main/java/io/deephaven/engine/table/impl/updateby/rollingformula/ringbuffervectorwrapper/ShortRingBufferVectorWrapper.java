/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRingBufferVectorWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.ShortRingBuffer;
import io.deephaven.vector.ShortSubVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;
import io.deephaven.vector.ShortVectorSlice;

public class ShortRingBufferVectorWrapper implements ShortVector, RingBufferVectorWrapper {
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
    public ShortVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ShortVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public ShortVector subVectorByPositions(final long[] positions) {
        return new ShortSubVector(this, positions);
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
