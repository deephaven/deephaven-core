//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.util.Arrays;

public class FloatMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new FloatPageMaterializer(dataReader, (float) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new FloatPageMaterializer(dataReader, numValues);
        }
    };

    private static final class FloatPageMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final float nullValue;
        final float[] data;

        private FloatPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        private FloatPageMaterializer(ValuesReader dataReader, float nullValue, int numValues) {
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
}
