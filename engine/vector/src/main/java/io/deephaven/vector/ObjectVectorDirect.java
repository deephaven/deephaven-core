/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import java.util.Arrays;

public class ObjectVectorDirect<T> implements ObjectVector<T> {

    private static final long serialVersionUID = 9111886364211462917L;

    private final T[] data;
    private final Class<T> componentType;

    public ObjectVectorDirect(T... data) {
        this.data = data;
        componentType = (Class<T>) (data == null ? Object.class : data.getClass().getComponentType());
    }

    @Override
    public T get(long i) {
        if (i < 0 || i > data.length - 1) {
            return null;
        }
        return data[(int) i];
    }

    @Override
    public ObjectVector<T> subVector(long fromIndexInclusive, long toIndexExclusive) {
        return new ObjectVectorSlice<>(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    @Override
    public ObjectVector<T> subVectorByPositions(long[] positions) {
        return new ObjectSubVector<>(this, positions);
    }

    @Override
    public T[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public Class<T> getComponentType() {
        return componentType;
    }

    @Override
    public ObjectVectorDirect<T> getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return ObjectVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof ObjectVectorDirect) {
            return Arrays.equals(data, ((ObjectVectorDirect) obj).data);
        }
        return ObjectVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return ObjectVector.hashCode(this);
    }
}
