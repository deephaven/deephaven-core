/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.vector.ObjectVector;

public class UngroupedObjectVectorColumnSource<T> extends UngroupedColumnSource<T>
        implements MutableColumnSourceGetDefaults.ForObject<T> {
    private final ColumnSource<ObjectVector<T>> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return innerSource.getComponentType().getComponentType();
    }

    public UngroupedObjectVectorColumnSource(ColumnSource<ObjectVector<T>> innerSource) {
        // noinspection unchecked
        super((Class<T>) innerSource.getComponentType());
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource
                && ((UngroupableColumnSource) innerSource).isUngroupable();
    }

    @Override
    public T get(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index >> base;
        long offset = index & ((1 << base) - 1);
        if (isUngroupable) {
            // noinspection unchecked
            return (T) ((UngroupableColumnSource) innerSource).getUngrouped(segment, (int) offset);
        } else {
            return (innerSource.get(segment)).get((int) offset);
        }
    }

    @Override
    public T getPrev(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index >> getPrevBase();
        long offset = index & ((1 << getPrevBase()) - 1);

        if (isUngroupable) {
            // noinspection unchecked
            return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(segment, (int) offset);
        } else {
            Assert.neqNull(innerSource, "innerSource");
            ObjectVector<T> prevArray = innerSource.getPrev(segment);
            Assert.neqNull(prevArray, "prevArray");
            return prevArray.get((int) offset);
        }
    }

    @Override
    public Boolean getBoolean(long index) {
        return (Boolean) get(index);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        return (Boolean) getPrev(index);
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
