//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

public class UngroupedBooleanArrayColumnSource extends UngroupedColumnSource<Boolean>
        implements MutableColumnSourceGetDefaults.ForBoolean {
    private ColumnSource<boolean[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBooleanArrayColumnSource(ColumnSource<boolean[]> innerSource) {
        super(Boolean.class);
        this.innerSource = innerSource;
    }

    @Override
    public Boolean get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >> base;
        int offset = (int) (rowKey & ((1 << base) - 1));
        boolean[] array = innerSource.get(segment);
        if (offset >= array.length) {
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
        int offset = (int) (rowKey & ((1 << getPrevBase()) - 1));
        boolean[] array = innerSource.getPrev(segment);
        if (offset >= array.length) {
            return null;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}

