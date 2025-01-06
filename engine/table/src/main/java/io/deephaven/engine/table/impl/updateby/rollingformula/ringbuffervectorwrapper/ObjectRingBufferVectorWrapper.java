//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.ObjectRingBuffer;
import io.deephaven.vector.ObjectSubVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.vector.ObjectVectorSlice;

public class ObjectRingBufferVectorWrapper<T> implements ObjectVector<T>, RingBufferVectorWrapper<ObjectVector<T>> {
    private final ObjectRingBuffer<T> ringBuffer;
    private final Class<T> componentType;

    public ObjectRingBufferVectorWrapper(final ObjectRingBuffer<T> ringBuffer, final Class<T> componentType) {
        this.ringBuffer = ringBuffer;
        this.componentType = componentType;
    }

    @Override
    public long size() {
        return ringBuffer.size();
    }

    @Override
    public T get(long index) {
        return ringBuffer.front((int) index);
    }

    @Override
    public ObjectVector<T> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ObjectVectorSlice<>(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    @Override
    public ObjectVector<T> subVectorByPositions(final long[] positions) {
        return new ObjectSubVector<>(this, positions);
    }

    @Override
    public T[] toArray() {
        return ringBuffer.getAll();
    }

    @Override
    public T[] copyToArray() {
        return ringBuffer.getAll();
    }

    @Override
    public ObjectVector<T> getDirect() {
        return new ObjectVectorDirect<>(ringBuffer.getAll());
    }

    @Override
    public Class<T> getComponentType() {
        return componentType;
    }
}
