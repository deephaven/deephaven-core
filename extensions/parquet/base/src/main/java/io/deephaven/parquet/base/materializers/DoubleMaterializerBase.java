//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LongMaterializerBase and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

import java.util.Arrays;

abstract class DoubleMaterializerBase implements PageMaterializer {

    private final double nullValue;
    final double[] data;

    DoubleMaterializerBase(double nullValue, int numValues) {
        this.nullValue = nullValue;
        this.data = new double[numValues];
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
