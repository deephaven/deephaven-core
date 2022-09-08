/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedCharArrayColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UngroupedLongArrayColumnSource extends UngroupedColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong {
    private ColumnSource<long[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedLongArrayColumnSource(ColumnSource<long[]> innerSource) {
        super(Long.class);
        this.innerSource = innerSource;
    }

    @Override
    public long getLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        long[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_LONG;
        }
        return array[offset];
    }

    @Override
    public long getPrevLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        long[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_LONG;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
