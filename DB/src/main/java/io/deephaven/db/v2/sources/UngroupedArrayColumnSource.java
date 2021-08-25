/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

public class UngroupedArrayColumnSource<T> extends UngroupedColumnSource<T>
        implements MutableColumnSourceGetDefaults.ForObject<T> {
    private ColumnSource innerSource;

    @Override
    public Class<?> getComponentType() {
        return innerSource.getComponentType().getComponentType();
    }

    public UngroupedArrayColumnSource(ColumnSource innerSource) {
        // noinspection unchecked
        super(innerSource.getComponentType());
        this.innerSource = innerSource;
    }

    @Override
    public T get(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index >> base;
        long offset = index & ((1 << base) - 1);
        // noinspection unchecked
        T[] array = (T[]) innerSource.get(segment);
        if (offset >= array.length) {
            return null;
        }
        return array[(int) offset];
    }


    @Override
    public T getPrev(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index >> getPrevBase();
        long offset = index & ((1 << getPrevBase()) - 1);
        // noinspection unchecked
        T[] array = (T[]) innerSource.getPrev(segment);
        if (offset >= array.length) {
            return null;
        }
        return array[(int) offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
