//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.CharRingBuffer;
import io.deephaven.vector.CharSubVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.vector.CharVectorSlice;

public class CharRingBufferVectorWrapper implements CharVector, RingBufferVectorWrapper<CharVector> {
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
        return ringBuffer.front((int) index);
    }

    @Override
    public CharVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new CharVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public CharVector subVectorByPositions(final long[] positions) {
        return new CharSubVector(this, positions);
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
