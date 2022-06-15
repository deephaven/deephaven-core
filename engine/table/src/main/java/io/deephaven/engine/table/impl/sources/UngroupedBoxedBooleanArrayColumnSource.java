/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

public class UngroupedBoxedBooleanArrayColumnSource extends UngroupedColumnSource<Boolean> implements MutableColumnSourceGetDefaults.ForBoolean {
    private ColumnSource<Boolean[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedBooleanArrayColumnSource(ColumnSource<Boolean[]> innerSource) {
        super(Boolean.class);
        this.innerSource = innerSource;
    }

    @Override
    public Boolean get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        Boolean[] array = innerSource.get(segment);
        if(offset >= array.length) {
            return null;
        }
        return array[offset];
    }

    @Override
    public Boolean getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        Boolean[] array = innerSource.getPrev(segment);
        if(offset >= array.length) {
            return null;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
