//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;

import java.util.Arrays;

abstract class LongPageMaterializerBase implements PageMaterializer {

    final long nullValue;
    final long[] data;

    /**
     * @return a long value read from the dataReader.
     */
    abstract long readLong();

    LongPageMaterializerBase(long nullValue, int numValues) {
        this.nullValue = nullValue;
        this.data = new long[numValues];
    }

    @Override
    public void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = readLong();
        }
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
