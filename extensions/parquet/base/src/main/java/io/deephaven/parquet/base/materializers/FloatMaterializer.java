//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

import java.util.Arrays;

public class FloatMaterializer implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new FloatMaterializer(dataReader, (float) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new FloatMaterializer(dataReader, numValues);
        }
    };

    private final PageValueReader dataReader;

    private final float nullValue;
    private final float[] data;

    private FloatMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private FloatMaterializer(PageValueReader dataReader, float nullValue, int numValues) {
        this.dataReader = dataReader;
        this.nullValue = nullValue;
        this.data = new float[numValues];
    }

    @Override
    public void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = dataReader.readFloat();
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
