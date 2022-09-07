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

import static io.deephaven.util.QueryConstants.NULL_INT;

public class UngroupedIntArrayColumnSource extends UngroupedColumnSource<Integer> implements MutableColumnSourceGetDefaults.ForInt {
    private ColumnSource<int[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedIntArrayColumnSource(ColumnSource<int[]> innerSource) {
        super(Integer.class);
        this.innerSource = innerSource;
    }

    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        int[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_INT;
        }
        return array[offset];
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        int[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_INT;
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
