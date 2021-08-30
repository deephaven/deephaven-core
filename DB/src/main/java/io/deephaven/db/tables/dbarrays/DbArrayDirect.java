/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import java.util.Arrays;

public class DbArrayDirect<T> implements DbArray<T> {

    private static final long serialVersionUID = 9111886364211462917L;

    private final T[] data;
    private final Class<T> componentType;

    public DbArrayDirect(T... data) {
        this.data = data;
        componentType =
            (Class<T>) (data == null ? Object.class : data.getClass().getComponentType());
    }

    @Override
    public T get(long i) {
        if (i < 0 || i > data.length - 1) {
            return null;
        }
        return data[(int) i];
    }

    @Override
    public DbArray<T> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArraySlice<>(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    @Override
    public DbArray<T> subArrayByPositions(long[] positions) {
        return new DbSubArray<>(this, positions);
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
    public DbArrayDirect<T> getDirect() {
        return this;
    }

    @Override
    public T getPrev(long offset) {
        return get(offset);
    }

    @Override
    public final String toString() {
        return DbArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbArrayDirect) {
            return Arrays.equals(data, ((DbArrayDirect) obj).data);
        }
        return DbArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbArray.hashCode(this);
    }
}
