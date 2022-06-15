/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

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
    public T get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >> base;
        long offset = rowKey & ((1 << base) - 1);
        // noinspection unchecked
        T[] array = (T[]) innerSource.get(segment);
        if (offset >= array.length) {
            return null;
        }
        return array[(int) offset];
    }


    @Override
    public T getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >> getPrevBase();
        long offset = rowKey & ((1 << getPrevBase()) - 1);
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
