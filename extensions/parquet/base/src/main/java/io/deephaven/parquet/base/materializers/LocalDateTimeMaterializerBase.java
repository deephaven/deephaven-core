//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalTimeMaterializerBase and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

import java.time.LocalDateTime;
import java.util.Arrays;

abstract class LocalDateTimeMaterializerBase implements PageMaterializer {

    final LocalDateTime nullValue;
    final LocalDateTime[] data;

    LocalDateTimeMaterializerBase(LocalDateTime nullValue, int numValues) {
        this.nullValue = nullValue;
        this.data = new LocalDateTime[numValues];
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
