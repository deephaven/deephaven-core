//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

import java.util.Arrays;

// TODO Is it okay if I use this class as a base class for the other object type materializers, like LocalDate,
// LocalDateTime, String, BigDecimal, BigInteger?
abstract class ObjectMaterializerBase<TYPE> implements PageMaterializer {

    final TYPE nullValue;
    final TYPE[] data;

    ObjectMaterializerBase(TYPE nullValue, int numValues) {
        this.nullValue = nullValue;
        // noinspection unchecked
        this.data = (TYPE[]) new Object[numValues];
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
