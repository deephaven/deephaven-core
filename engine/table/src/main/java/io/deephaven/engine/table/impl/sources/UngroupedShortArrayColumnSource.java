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

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class UngroupedShortArrayColumnSource extends UngroupedColumnSource<Short> implements MutableColumnSourceGetDefaults.ForShort {
    private ColumnSource<short[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedShortArrayColumnSource(ColumnSource<short[]> innerSource) {
        super(Short.class);
        this.innerSource = innerSource;
    }

    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        short[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_SHORT;
        }
        return array[offset];
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        short[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_SHORT;
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
