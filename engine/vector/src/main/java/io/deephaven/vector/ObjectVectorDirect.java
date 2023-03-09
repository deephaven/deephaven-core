/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.util.Arrays;

/**
 * An {@link ObjectVector} backed by an array.
 */
public class ObjectVectorDirect<COMPONENT_TYPE> implements ObjectVector<COMPONENT_TYPE> {

    public static final ObjectVector<?> ZERO_LENGTH_VECTOR = new ObjectVectorDirect<>();

    private final COMPONENT_TYPE[] data;
    private final Class<COMPONENT_TYPE> componentType;

    @SuppressWarnings("unchecked")
    public ObjectVectorDirect(final COMPONENT_TYPE... data) {
        this.data = data;
        componentType = (Class<COMPONENT_TYPE>) (data == null ? Object.class : data.getClass().getComponentType());
    }

    @Override
    public COMPONENT_TYPE get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return null;
        }
        return data[LongSizedDataStructure.intSize("ObjectVectorDirect.get", index)];
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ObjectVectorSlice<>(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVectorByPositions(final long[] positions) {
        return new ObjectSubVector<>(this, positions);
    }

    @Override
    public COMPONENT_TYPE[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public Class<COMPONENT_TYPE> getComponentType() {
        return componentType;
    }

    @Override
    public ObjectVectorDirect<COMPONENT_TYPE> getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return ObjectVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof ObjectVectorDirect) {
            return Arrays.equals(data, ((ObjectVectorDirect<?>) obj).data);
        }
        return ObjectVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return ObjectVector.hashCode(this);
    }
}
