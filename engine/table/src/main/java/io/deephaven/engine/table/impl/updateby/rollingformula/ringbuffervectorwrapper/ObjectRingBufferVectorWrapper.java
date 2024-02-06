package io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper;

import io.deephaven.base.ringbuffer.ObjectRingBuffer;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;

public class ObjectRingBufferVectorWrapper<T> implements ObjectVector<T>, RingBufferVectorWrapper {
    private final ObjectRingBuffer ringBuffer;
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
        // noinspection unchecked
        return (T)ringBuffer.front((int)index);
    }

    @Override
    public ObjectVector<T> subVector(long fromIndexInclusive, long toIndexExclusive) {
        throw new UnsupportedOperationException("subVector not supported on CharRingBufferVectorWrapper");
    }

    @Override
    public ObjectVector<T> subVectorByPositions(long[] positions) {
        throw new UnsupportedOperationException("subVectorByPositions not supported on CharRingBufferVectorWrapper");
    }

    @Override
    public T[] toArray() {
        // noinspection unchecked
        return (T[])ringBuffer.getAll();
    }

    @Override
    public T[] copyToArray() {
        // noinspection unchecked
        return (T[])ringBuffer.getAll();
    }

    @Override
    public ObjectVector<T> getDirect() {
        // noinspection unchecked
        return new ObjectVectorDirect<T>((T[])ringBuffer.getAll());
    }

    @Override
    public Class<T> getComponentType() {
        return componentType;
    }
}
