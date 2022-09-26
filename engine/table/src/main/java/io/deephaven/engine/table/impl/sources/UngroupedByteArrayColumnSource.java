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

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class UngroupedByteArrayColumnSource extends UngroupedColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte {
    private ColumnSource<byte[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedByteArrayColumnSource(ColumnSource<byte[]> innerSource) {
        super(Byte.class);
        this.innerSource = innerSource;
    }

    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        byte[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_BYTE;
        }
        return array[offset];
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        byte[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_BYTE;
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
