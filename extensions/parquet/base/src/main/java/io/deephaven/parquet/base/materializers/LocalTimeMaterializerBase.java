//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

import java.time.LocalTime;
import java.util.Arrays;

abstract class LocalTimeMaterializerBase implements PageMaterializer {

    final LocalTime nullValue;
    final LocalTime[] data;

    LocalTimeMaterializerBase(LocalTime nullValue, int numValues) {
        this.nullValue = nullValue;
        this.data = new LocalTime[numValues];
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
