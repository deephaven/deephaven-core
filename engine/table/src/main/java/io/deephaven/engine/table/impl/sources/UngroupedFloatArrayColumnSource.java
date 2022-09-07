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

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class UngroupedFloatArrayColumnSource extends UngroupedColumnSource<Float> implements MutableColumnSourceGetDefaults.ForFloat {
    private ColumnSource<float[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedFloatArrayColumnSource(ColumnSource<float[]> innerSource) {
        super(Float.class);
        this.innerSource = innerSource;
    }

    @Override
    public float getFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        float[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_FLOAT;
        }
        return array[offset];
    }

    @Override
    public float getPrevFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        float[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_FLOAT;
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
