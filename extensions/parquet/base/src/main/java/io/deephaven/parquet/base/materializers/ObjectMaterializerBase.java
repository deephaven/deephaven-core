//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

import java.util.Arrays;

abstract class ObjectMaterializerBase<TYPE> implements PageMaterializer {

    private final TYPE nullValue;
    final TYPE[] data;

    ObjectMaterializerBase(TYPE nullValue, TYPE[] data) {
        this.nullValue = nullValue;
        this.data = data;
    }

    @Override
    public final void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public final Object fillAll() {
        fillValues(0, data.length);
        return data;
    }

    @Override
    public final Object data() {
        return data;
    }
}
