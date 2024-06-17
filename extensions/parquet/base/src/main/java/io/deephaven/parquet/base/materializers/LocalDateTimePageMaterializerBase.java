//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

import java.time.LocalDateTime;
import java.util.Arrays;

abstract class LocalDateTimePageMaterializerBase implements PageMaterializer {

    final LocalDateTime nullValue;
    final LocalDateTime[] data;

    LocalDateTimePageMaterializerBase(LocalDateTime nullValue, int numValues) {
        this.nullValue = nullValue;
        this.data = new LocalDateTime[numValues];
    }

    @Override
    public void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public Object fillAll() {
        fillValues(0, data.length);
        return data;
    }

    @Override
    public Object data() {
        return data;
    }
}
